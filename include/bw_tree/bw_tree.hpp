/*
 * Copyright 2021 Database Group, Nagoya University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <functional>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>

#include "component/delta_record.hpp"
#include "component/mapping_table.hpp"
#include "component/node.hpp"
#include "memory/epoch_based_gc.hpp"

namespace dbgroup::index::bw_tree
{
/**
 * @brief A class to represent Bw-tree.
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Comp a comparetor class for keys.
 */
template <class Key, class Payload, class Comp = ::std::less<Key>>
class BwTree
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using DeltaType = component::DeltaType;
  using Metadata = component::Metadata;
  using NodeRC = component::NodeRC;
  using DeltaRC = component::DeltaRC;
  using NodeType = component::NodeType;
  using Node_t = component::Node<Key, Comp>;
  using DeltaRecord_t = component::DeltaRecord<Key, Comp>;
  using Mapping_t = std::atomic<Node_t *>;
  using MappingTable_t = component::MappingTable<Key, Comp>;
  using NodeGC_t = ::dbgroup::memory::EpochBasedGC<Node_t>;
  using NodeStack = std::vector<std::atomic_uintptr_t *>;
  using Binary_t = std::remove_pointer_t<Payload>;
  using Binary_p = std::unique_ptr<Binary_t, component::PayloadDeleter<Binary_t>>;

 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  BwTree(const BwTree &) = delete;
  BwTree &operator=(const BwTree &) = delete;
  BwTree(BwTree &&) = delete;
  BwTree &operator=(BwTree &&) = delete;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  /**
   * @brief Destroy the BwTree object.
   *
   */
  ~BwTree() = default;

  /*####################################################################################
   * Public read APIs
   *##################################################################################*/

  /**
   * @brief Read a payload of a specified key if it exists.
   *
   * This function returns two return codes: kSuccess and kKeyNotExist. If a return code
   * is kSuccess, a returned pair contains a target payload. If a return code is
   * kKeyNotExist, the value of a returned payload is undefined.
   *
   * @param key a target key.
   * @return a read payload or std::nullopt.
   */
  auto
  Read(const Key &key)  //
      -> std::optional<Payload>
  {
    const auto &guard = gc_.CreateEpochGuard();

    // traverse to a target leaf node
    consol_page_ = nullptr;
    auto &&stack = SearchLeafNode(key, kClosed);

    // check whether the leaf node has a target key
    auto [ptr, rc] = CheckExistence(key, stack);
    if (consol_page_ != nullptr) {
      // if a long delta-chain is found, consolidate it
      TryConsolidation(consol_page_, key, kClosed, stack);
    }

    if (rc == NodeRC::kKeyNotExist) return std::nullopt;

    Payload payload{};
    if (rc == NodeRC::kKeyInDelta) {
      auto *delta = reinterpret_cast<DeltaRecord_t *>(ptr);
      delta->CopyPayload(payload);
    } else {
      auto *node = reinterpret_cast<Node_t *>(ptr);
      node->CopyPayload(node->GetMetadata(rc), payload);
    }

    return std::make_optional(std::move(payload));
  }

  /*####################################################################################
   * Public write APIs
   *##################################################################################*/

  /**
   * @brief Delete a target kay from the index.
   *
   * This function performs a uniqueness check in its processing. If a specified key
   * exist, this function deletes it. If a specified key does not exist in the index,
   * this function does nothing and returns kKeyNotExist as a return code.
   *
   * Note that if a target key is binary data, it is required to specify its length in
   * bytes.
   *
   * @param key a target key to be written.
   * @param key_length the length of a target key.
   * @retval kSuccess if deleted.
   * @retval kKeyNotExist if a specified key does not exist.
   */
  auto
  Delete(  //
      [[maybe_unused]] const Key &key,
      [[maybe_unused]] const size_t key_length = sizeof(Key))  //
      -> ReturnCode
  {
    // not implemented yet

    return ReturnCode::kSuccess;
  }

  /*####################################################################################
   * Not refactored yet......
   *##################################################################################*/

 private:
  NodeStack
  SearchLeftEdgeLeaf()
  {
    NodeStack stack;
    stack.reserve(kExpectedTreeHeight);

    // get a logical page of a root node
    Mapping_t *page_id = root_.load(mo_relax);
    stack.emplace_back(page_id);

    // traverse a Bw-tree
    Node_t *cur_node = page_id->load(mo_relax);
    while (!cur_node->IsLeaf()) {
      if (cur_node->GetDeltaNodeType() == DeltaType::kNotDelta) {
        // reach a base page and get left edge node
        Mapping_t *child_page =
            cur_node->template GetPayload<Mapping_t *>(cur_node->GetMetadata(0));
        stack.emplace_back(child_page);
        cur_node = child_page->load(mo_relax);
      } else {
        // go to the next delta record or base node
        cur_node = cur_node->GetNextNode();
      }
    }
    return stack;
  }

 public:
  /*####################################################################################
   * Public classes
   *##################################################################################*/

  /**
   * @brief A class to represent a iterator for scan results.
   *
   * @tparam Key a target key class
   * @tparam Payload a target payload class
   * @tparam Compare a key-comparator class
   */
  class RecordIterator
  {
    using BwTree_t = BwTree<Key, Payload, Comp>;

   private:
    /*##################################################################################
     * Internal member variables
     *################################################################################*/

    /// a pointer to BwTree to perform continuous scan
    BwTree_t *bw_tree_;

    /// node
    Node_t *node_;

    /// the number of records in this node.
    size_t record_count_;

    /// an index of a current record
    size_t current_idx_;

   public:
    /*##################################################################################
     * Public constructors/destructors
     *################################################################################*/
    constexpr RecordIterator() {}
    constexpr RecordIterator(BwTree_t *bw_tree, Node_t *node, size_t current_idx)
        : bw_tree_{bw_tree},
          node_{node},
          record_count_{node->GetRecordCount()},
          current_idx_{current_idx}
    {
    }

    ~RecordIterator() = default;

    RecordIterator(const RecordIterator &) = delete;
    RecordIterator &operator=(const RecordIterator &) = delete;
    constexpr RecordIterator(RecordIterator &&) = default;
    constexpr RecordIterator &operator=(RecordIterator &&) = default;

    /*##################################################################################
     * Public operators for iterators
     *################################################################################*/

    /**
     * @return std::pair<Key, Payload>: a current key and payload pair
     */
    constexpr std::pair<Key, Payload>
    operator*() const
    {
      return {GetKey(), GetPayload()};
    }

    /**
     * @brief Forward an iterator.
     *
     */
    void
    operator++()
    {
      current_idx_++;
    }

    /*##################################################################################
     * Public getters/setters
     *################################################################################*/

    /**
     * @brief Check if there are any records left.
     *
     * function may call a scan function internally to get a next leaf node.
     *
     * @retval true if there are any records or next node left.
     * @retval false if there are no records and node left.
     */
    bool
    HasNext()
    {
      if (current_idx_ < record_count_) return true;
      if (node_->GetSiblingNode() == nullptr) return false;

      auto *next_node = node_->GetSiblingNode()->load(mo_relax);
      delete (node_);
      node_ = bw_tree_->LeafScan(next_node);
      record_count_ = node_->GetRecordCount();
      current_idx_ = 0;
      return HasNext();
    }

    /**
     * @return Key: a key of a current record
     */
    constexpr Key
    GetKey() const
    {
      if constexpr (IsVariableLengthData<Key>()) {
        return reinterpret_cast<const Key>(node_->GetKeyAddr(node_->GetMetadata(current_idx_)));
      } else {
        return *reinterpret_cast<const Key *>(node_->GetKeyAddr(node_->GetMetadata(current_idx_)));
      }
    }

    /**
     * @return Payload: a payload of a current record
     */
    constexpr Payload
    GetPayload() const
    {
      Payload payload{};
      node_->CopyPayload(node_->GetMetadata(current_idx_), payload);
      return payload;
    }
  };

  /*####################################################################################
   * Public constructor/destructor
   *##################################################################################*/

  /**
   * @brief Construct a new BwTree object.
   *
   * @param gc_interval_microsec GC internal [us]
   */
  explicit BwTree(const size_t gc_interval_microsec = 100000)
      : root_{nullptr}, mapping_table_{}, gc_{gc_interval_microsec}
  {
    // create an empty leaf node
    Mapping_t *child_page_id = mapping_table_.GetNewLogicalID();
    Node_t *empty_leaf = Node_t::CreateNode(kHeaderLength, NodeType::kLeaf, 0UL, nullptr);
    empty_leaf->SetLowMeta(Metadata{0, 0, 0});
    empty_leaf->SetHighMeta(Metadata{0, 0, 0});
    child_page_id->store(empty_leaf, mo_relax);

    // create an empty Bw-tree
    Mapping_t *root_page_id = mapping_table_.GetNewLogicalID();
    auto offset = kHeaderLength + sizeof(Metadata) + sizeof(Mapping_t *);
    Node_t *initial_root = Node_t::CreateNode(offset, NodeType::kInternal, 1UL, nullptr);
    initial_root->SetLowMeta(Metadata{0, 0, 0});
    initial_root->SetHighMeta(Metadata{0, 0, 0});
    initial_root->template SetPayload<Mapping_t *>(offset, child_page_id, sizeof(Mapping_t *));
    initial_root->SetMetadata(0, Metadata{offset, 0, sizeof(Mapping_t *)});
    root_page_id->store(initial_root, mo_relax);
    root_.store(root_page_id, mo_relax);

    // start garbage collector for removed nodes
    gc_.StartGC();
  }

  /**
   * @brief Perform a range scan with specified keys.
   *
   * @param begin_key the pointer of a begin key of a range scan.
   * @param begin_closed a flag to indicate whether the begin side of a range is closed.
   * @return RecordIterator: an iterator to access target records.
   */
  RecordIterator
  Scan(  //
      const Key &begin_key,
      bool begin_closed = false)
  {
    const auto guard = gc_.CreateEpochGuard();
    Mapping_t *consol_node = nullptr;
    const auto *key_addr = component::GetAddr(begin_key);
    const auto node_stack = SearchLeafNode(key_addr, begin_closed, consol_node);
    Mapping_t *page_id = node_stack.back();
    Node_t *page = LeafScan(page_id->load(mo_relax));
    return RecordIterator{this, page, page->SearchRecord(key_addr, begin_closed).second};
  }

  /**
   * @brief Perform a range scan from left edge.
   *
   * @return RecordIterator: an iterator to access target records.
   */
  RecordIterator
  Begin()
  {
    const auto guard = gc_.CreateEpochGuard();
    const auto node_stack = SearchLeftEdgeLeaf();
    Mapping_t *page_id = node_stack.back();
    Node_t *page = LeafScan(page_id->load(mo_relax));
    return RecordIterator{this, page, 0};
  }

  /**
   * @brief Consolidate leaf node for range scan
   *
   * @param node the target node.
   * @retval the pointer to consolidated leaf node
   */

  Node_t *
  LeafScan(  //
      Node_t *target_node)
  {
    // collect and sort delta records
    std::vector<Record> records;
    records.reserve(kMaxDeltaNodeNum * 4);
    const auto [base_node, end_node] = SortDeltaRecords(target_node, records);

    // reserve a page for a consolidated node
    auto [offset, base_rec_num] = CalculatePageSize(base_node, end_node, records);

    const auto sib_page = GetSiblingPage(end_node);
    Node_t *page = Node_t::CreateNode(offset, component::NodeType::kLeaf, 0, sib_page);
    // copy active records
    CopyLeafRecords(page, offset, base_node, base_rec_num, records);
    return page;
  }

  /*####################################################################################
   * Public write APIs
   *##################################################################################*/

  /**
   * @brief Write (i.e., upsert) a specified kay/payload pair.
   *
   * If a specified key does not exist in the index, this function performs an insert
   * operation. If a specified key has been already inserted, this function perfroms an
   * update operation. Thus, this function always returns kSuccess as a return code.
   *
   * Note that if a target key/payload is binary data, it is required to specify its
   * length in bytes.
   *
   * @param key a target key to be written.
   * @param payload a target payload to be written.
   * @param key_length the length of a target key.
   * @param payload_length the length of a target payload.
   * @return ReturnCode: kSuccess.
   */
  ReturnCode
  Write(  //
      const Key &key,
      const Payload &payload,
      const size_t key_length = sizeof(Key),
      const size_t payload_length = sizeof(Payload))
  {
    const auto key_addr = component::GetAddr(key);
    const auto guard = gc_.CreateEpochGuard();

    // traverse to a target leaf node
    Mapping_t *consol_node = nullptr;
    NodeStack stack = SearchLeafNode(key_addr, true, consol_node);

    // create a delta record to write a key/value pair
    Node_t *delta_node = Node_t::CreateDeltaNode(NodeType::kLeaf, DeltaType::kInsert,  //
                                                 key_addr, key_length, payload, payload_length);

    // insert the delta record
    for (Node_t *prev_head = nullptr; true;) {
      // check whether the target node is valid (containing a target key and no incomplete SMOs)
      Node_t *cur_head = ValidateNode(key_addr, true, prev_head, stack, consol_node);

      // prepare nodes to perform CAS
      delta_node->SetNextNode(cur_head);
      prev_head = cur_head;

      // try to insert the delta record
      if (stack.back()->compare_exchange_weak(cur_head, delta_node, mo_relax)) break;
    }

    if (consol_node != nullptr) {
      TryConsolidation(consol_node, key_addr, true, stack);
    }

    return ReturnCode::kSuccess;
  }

  /**
   * @brief Insert a specified key/payload pair.
   *
   * This function performs a uniqueness check in its processing. If a specified key
   * does not exist, this function insert a target payload into the index. If a
   * specified key exists in the index, this function does nothing and returns kKeyExist
   * as a return code.
   *
   * Note that if a target key/payload is binary data, it is required to specify its
   * length in bytes.
   *
   * @param key a target key to be written.
   * @param payload a target payload to be written.
   * @param key_length the length of a target key.
   * @param payload_length the length of a target payload.
   * @retval.kSuccess if inserted.
   * @retval kKeyExist if a specified key exists.
   */
  ReturnCode
  Insert(  //
      const Key &key,
      const Payload &payload,
      const size_t key_length = sizeof(Key),
      const size_t payload_length = sizeof(Payload))
  {
    const auto key_addr = component::GetAddr(key);
    const auto guard = gc_.CreateEpochGuard();

    // traverse to a target leaf node
    Mapping_t *consol_node = nullptr;
    NodeStack stack = SearchLeafNode(key_addr, true, consol_node);

    // create a delta record to write a key/value pair
    Node_t *delta_node = Node_t::CreateDeltaNode(NodeType::kLeaf, DeltaType::kInsert,  //
                                                 key_addr, key_length, payload, payload_length);
    // insert the delta record
    for (Node_t *prev_head = nullptr; true;) {
      // check whether the target node is valid (containing a target key and no incomplete SMOs)
      Node_t *cur_head = ValidateNode(key_addr, true, prev_head, stack, consol_node);

      // check target key/value existence
      const auto [target_node, meta] = CheckExistence(key_addr, stack, consol_node);
      if (target_node != nullptr) return ReturnCode::kKeyExist;

      // prepare nodes to perform CAS
      delta_node->SetNextNode(cur_head);
      prev_head = cur_head;

      // try to insert the delta record
      if (stack.back()->compare_exchange_weak(cur_head, delta_node, mo_relax)) break;
    }

    if (consol_node != nullptr) {
      TryConsolidation(consol_node, key_addr, true, stack);
    }

    return ReturnCode::kSuccess;
  }

  /**
   * @brief Update a target kay with a specified payload.
   *
   * This function performs a uniqueness check in its processing. If a specified key
   * exist, this function update a target payload. If a specified key does not exist in
   * the index, this function does nothing and returns kKeyNotExist as a return code.
   *
   * Note that if a target key/payload is binary data, it is required to specify its
   * length in bytes.
   *
   * @param key a target key to be written.
   * @param payload a target payload to be written.
   * @param key_length the length of a target key.
   * @param payload_length the length of a target payload.
   * @retval kSuccess if updated.
   * @retval kKeyNotExist if a specified key does not exist.
   */
  ReturnCode
  Update(  //
      const Key &key,
      const Payload &payload,
      const size_t key_length = sizeof(Key),
      const size_t payload_length = sizeof(Payload))
  {
    const auto key_addr = component::GetAddr(key);
    const auto guard = gc_.CreateEpochGuard();

    // traverse to a target leaf node
    Mapping_t *consol_node = nullptr;
    NodeStack stack = SearchLeafNode(key_addr, true, consol_node);

    // create a delta record to write a key/value pair
    Node_t *delta_node = Node_t::CreateDeltaNode(NodeType::kLeaf, DeltaType::kModify,  //
                                                 key_addr, key_length, payload, payload_length);
    // insert the delta record
    for (Node_t *prev_head = nullptr; true;) {
      // check whether the target node is valid (containing a target key and no incomplete SMOs)
      Node_t *cur_head = ValidateNode(key_addr, true, prev_head, stack, consol_node);

      // check target key/value existence
      const auto [target_node, meta] = CheckExistence(key_addr, stack, consol_node);
      if (target_node == nullptr) return ReturnCode::kKeyNotExist;

      // prepare nodes to perform CAS
      delta_node->SetNextNode(cur_head);
      prev_head = cur_head;

      // try to insert the delta record
      if (stack.back()->compare_exchange_weak(cur_head, delta_node, mo_relax)) break;
    }

    if (consol_node != nullptr) {
      TryConsolidation(consol_node, key_addr, true, stack);
    }

    return ReturnCode::kSuccess;
  }

  /*####################################################################################
   * ......Not refactored yet
   *##################################################################################*/

 private:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  static constexpr auto mo_relax = component::mo_relax;

  static constexpr auto kHeaderLength = component::kHeaderLength;

  static constexpr size_t kExpectedTreeHeight = 8;

  static constexpr bool kClosed = component::kClosed;

  static constexpr uintptr_t kNullPtr = component::kNullPtr;

  /*####################################################################################
   * Internal utility functions
   *##################################################################################*/

  /**
   * @brief Create a New Node accordint to a given template paramter.
   *
   * @tparam T a template paramter for indicating whether a new node is a leaf.
   * @retval an empty leaf node if Payload is given as a template.
   * @retval an empty internal node otherwise.
   */
  [[nodiscard]] auto
  GetNodePage(const size_t size)  //
      -> void *
  {
    auto *page = (size > kPageSize) ? (::operator new(size))  //
                                    : gc_->template GetPageIfPossible<Node_t>();
    return (page == nullptr) ? (::operator new(kPageSize)) : page;
  }

  [[nodiscard]] auto
  GetRecordPage()  //
      -> void *
  {
    auto *page = gc_->template GetPageIfPossible<DeltaRecord_t>();
    return (page == nullptr) ? (::operator new(kPageSize)) : page;
  }

  auto
  SearchLeafNode(  //
      const Key &key,
      const bool closed)  //
      -> NodeStack
  {
    NodeStack stack{};
    stack.reserve(kExpectedTreeHeight);

    // get a logical page of a root node
    auto [page_id, height] = root_.load(std::memory_order_relaxed);

    // traverse a Bw-tree
    for (size_t i = 0; i < height; ++i) {
      auto [ptr, rc] = DeltaRecord_t::SearchChildNode(key, closed, page_id);
      switch (rc) {
        case DeltaRC::kRecordFound:
          stack.emplace_back(page_id);
          page_id = reinterpret_cast<std::atomic_uintptr_t *>(ptr);
          break;

        case DeltaRC::kNodeRemoved:
          page_id = stack.back();
          stack.pop_back();
          --i;
          continue;

        case DeltaRC::kReachBase:
        default:
          stack.emplace_back(page_id);
          if (rc >= kMaxDeltaNodeNum) {
            consol_page_ = page_id;
          }

          // search a child node in a base node
          Node_t *base_node = reinterpret_cast<Node_t *>(ptr);
          auto pos = base_node->SearchChild(key, closed);
          auto meta = base_node->GetMetadata(pos);
          page_id = base_node->template GetPayload<std::atomic_uintptr_t *>(meta);
          break;
      }
    }
    stack.emplace_back(page_id);

    return stack;
  }

  auto
  ValidateNode(  //
      const Key &key,
      const bool closed,
      const uintptr_t prev_head,
      NodeStack &stack)  //
      -> uintptr_t
  {
    auto *page_id = stack.back();
    while (true) {
      auto [ptr, rc] = DeltaRecord_t::Validate(key, closed, page_id, prev_head);
      switch (rc) {
        case DeltaRC::kSplitMayIncomplete:
          // complete splitting and traverse to a sibling node
          CompleteSplit(ptr, stack);
          auto *rec = reinterpret_cast<DeltaRecord_t *>(ptr);
          page_id = rec->template GetPayload<std::atomic_uintptr_t *>();

          // insert a new current-level node (an old node was popped in parent-update)
          stack.emplace_back(page_id);
          break;

        case DeltaRC::kMergeMayIncomplete:
          // ...not implemented yet
          break;

        case DeltaRC::kNodeRemoved:
          // ...not implemented yet
          break;

        case DeltaRC::kReachBase:
        default:
          if (rc >= kMaxDeltaNodeNum) {
            consol_page_ = page_id;
          }
          return ptr;
      }
    }
  }

  auto
  CheckExistence(  //
      const Key &key,
      NodeStack &stack)  //
      -> std::pair<uintptr_t, NodeRC>
  {
    auto [ptr, rc] = DeltaRecord_t::SearchRecord(key, stack);
    switch (rc) {
      case DeltaRC::kRecordFound:
        return {ptr, NodeRC::kKeyInDelta};

      case DeltaRC::kRecordDeleted:
        return {uintptr_t{}, NodeRC::kKeyNotExist};

      case DeltaRC::kReachBase:
      default:
        if (rc >= kMaxDeltaNodeNum) {
          consol_page_ = stack.back();
        }
        // search a target key
        auto *node = reinterpret_cast<Node_t *>(ptr);
        auto pos = node->SearchRecord(key);
        return {ptr, pos};
    }
  }

  void
  AddToGC(uintptr_t head_ptr)
  {
  }

  /*####################################################################################
   * Internal structure modifications
   *##################################################################################*/

  void
  TryConsolidation(  //
      const std::atomic_uintptr_t *target_page,
      const Key &key,
      const bool closed,
      NodeStack &stack)
  {
    // remove child nodes from a node stack
    while (!stack.empty() && stack.back() != target_page) stack.pop_back();
    if (stack.empty()) return;

    // check whether the target node is valid (containing a target key and no incomplete SMOs)
    consol_page_ = nullptr;
    auto cur_head = ValidateNode(key, closed, kNullPtr, stack);
    if (consol_page_ != target_page) return;

    // perform consolidation, and split if needed
    auto [consol_node, size] = Consolidate(cur_head);
    if (size > kPageSize) {
      if (HalfSplit(target_page, cur_head, consol_node, stack)) return;
      TryConsolidation(consol_page, key, closed, stack);  // retry from consolidation
      return;
    }

    // install a consolidated node
    auto old_head = cur_head;
    auto new_head = reinterpret_cast<uintptr_t>(consol_node);
    if (!target_page->compare_exchange_strong(old_head, new_head, std::memory_order_release)) {
      // no CAS retry for consolidation
      AddToGC(new_head);
      return;
    }
    // delete consolidated delta records and a base node
    AddToGC(cur_head);
  }

  auto
  Consolidate(const uintptr_t head)  //
      -> std::pair<Node_t *, size_t>
  {
    // sort delta records
    std::vector<std::pair<Key, uintptr_t>> sorted{};
    sorted.reserve(kMaxDeltaNodeNum * 4);
    auto &&[node_ptr, high_key, high_meta, sib_page, diff] = DeltaRecord_t::Sort(head, sorted);
    auto *node = reinterpret_cast<Node_t *>(node_ptr);

    // reserve a page for a consolidated node
    auto [block_size, rec_num] = node->GetPageSize(high_key, high_meta);
    auto size = kHeaderLength + block_size + diff;
    void *page = GetNodePage(size);
    auto *consol_node = new (page) Node_t{sib_page};

    // perform merge sort to copy records to the consolidated node
    consol_node->Consolidate(node, sorted, high_key, high_meta, size, rec_num);

    return {consol_node, size};
  }

  bool
  HalfSplit(  //
      std::atomic_uintptr_t *split_page,
      const uintptr_t cur_head,
      Node_t *split_node,
      NodeStack &stack)
  {
    auto &&[sep_key, key_len] = split_node->Split();

    // create a split-delta record
    std::atomic_uintptr_t *sib_page = mapping_table_.GetNewLogicalID();
    auto split_ptr = reinterpret_cast<uintptr_t>(split_node);
    sib_page->store(split_ptr, std::memory_order_release);
    auto sib_ptr = reinterpret_cast<uintptr_t>(sib_page);
    auto *delta = new (GetRecordPage()) DeltaRecord_t{split_ptr, sib_ptr, cur_head};

    // install the delta record for splitting a child node
    auto old_head = cur_head;
    auto new_head = reinterpret_cast<uintptr_t>(delta);
    if (!split_page->compare_exchange_strong(old_head, new_head, std::memory_order_release)) {
      // retry from consolidation
      delta->SetNextNode(kNullPtr);
      AddToGC(new_head);
      AddToGC(split_ptr);
      sib_page->store(kNullPtr, std::memory_order_relaxed);
      return false;
    }

    // execute parent update
    consol_page_ = nullptr;
    CompleteSplit(delta, stack);

    // execute parent consolidation/split if needed
    if (consol_page_ != nullptr) {
      TryConsolidation(consol_page_, sep_key, kClosed, stack);
    }

    AddToGC(cur_head);
    return true;
  }

  void
  CompleteSplit(  //
      const DeltaRecord_t *split_delta,
      NodeStack &stack)
  {
    if (stack.size() <= 1) {  // a split node is root
      RootSplit(split_delta, stack);
      return;
    }

    // create an index-entry delta record to complete split
    auto *entry_delta = new (GetRecordPage()) DeltaRecord_t{split_delta};
    auto new_head = reinterpret_cast<uintptr_t>(entry_delta);
    auto &&sep_key = split_delta->GetKey();

    // remove a split child node to modify its parent node
    stack.pop_back();
    uintptr_t cur_head{};
    do {  // insert the delta record into a parent node
      cur_head = ValidateNode(sep_key, kClosed, kNullPtr, stack);

      // check whether another thread has already completed this split
      if (CheckExistence(sep_key, stack).second != NodeRC::kKeyNotExist) {
        entry_delta->SetNextNode(kNullPtr);
        AddToGC(entry_delta);
        return;
      }

      // try to insert the index-entry delta record
      entry_delta->SetNextNode(cur_head);
    } while (stack.back()->compare_exchange_weak(cur_head, new_head, std::memory_order_release));
  }

  void
  RootSplit(  //
      const DeltaRecord_t *split_delta,
      NodeStack &stack)
  {
    // create a new root node
    std::atomic_uintptr_t *old_root_p = stack.back();
    auto *new_root = new (GetNodePage(kPageSize)) Node_t{split_delta, old_root_p};
    auto root_ptr = reinterpret_cast<uintptr_t>(new_root);

    // install a new root page
    auto *new_root_p = mapping_table_.GetNewLogicalID();
    new_root_p->store(root_ptr, std::memory_order_release);
    stack.pop_back();
    if (!root_.compare_exchange_strong(old_root_p, new_root_p, std::memory_order_relaxed)) {
      // another thread has already inserted a new root
      new_root_p->store(nullptr, std::memory_order_relaxed);
      AddToGC(new_root);
      stack.emplace_back(old_root_p);
      return;
    }
    stack.emplace_back(new_root_p);
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// a root node of Bw-tree
  std::atomic<std::atomic_uintptr_t *> root_;

  /// a mapping table
  MappingTable_t mapping_table_;

  /// garbage collector
  NodeGC_t gc_;

  /// a page ID to be consolidated
  inline thread_local std::atomic_uintptr_t *consol_page_{};
};

}  // namespace dbgroup::index::bw_tree
