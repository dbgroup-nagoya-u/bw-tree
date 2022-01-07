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

#ifndef BW_TREE_BW_TREE_HPP
#define BW_TREE_BW_TREE_HPP

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

  using NodeType = component::NodeType;
  using DeltaType = component::DeltaType;
  using NodeRC = component::NodeRC;
  using DeltaRC = component::DeltaRC;
  using Metadata = component::Metadata;
  using Node_t = component::Node<Key, Comp>;
  using Delta_t = component::DeltaRecord<Key, Comp>;
  using MappingTable_t = component::MappingTable<Key, Comp>;
  using NodeGC_t = ::dbgroup::memory::EpochBasedGC<Node_t, Delta_t>;
  using NodeStack = std::vector<std::atomic_uintptr_t *>;

 public:
  /*####################################################################################
   * Public internal classes
   *##################################################################################*/

  /**
   * @brief A class to represent a iterator for scan results.
   *
   * @tparam Key a target key class
   * @tparam Payload a target payload class
   * @tparam Comp a key-comparator class
   */
  class RecordIterator
  {
    using BwTree_t = BwTree<Key, Payload, Comp>;

   public:
    /*##################################################################################
     * Public constructors and assignment operators
     *################################################################################*/

    RecordIterator(  //
        BwTree_t *bw_tree,
        NodeGC_t &gc,
        Node_t *node,
        size_t begin_pos,
        size_t end_pos,
        const std::optional<std::pair<const Key &, bool>> end_key)
        : bw_tree_{bw_tree},
          gc_{gc},
          node_{node},
          record_count_{end_pos},
          current_pos_{begin_pos},
          current_meta_{node_->GetMetadata(current_pos_)},
          end_key_{std::move(end_key)}
    {
    }

    RecordIterator(const RecordIterator &) = delete;
    RecordIterator(RecordIterator &&) = delete;

    auto operator=(const RecordIterator &) -> RecordIterator & = delete;

    constexpr auto
    operator=(RecordIterator &&obj) noexcept  //
        -> RecordIterator &
    {
      auto *old_node = node_;
      node_ = obj.node_;
      obj.node_ = old_node;
      record_count_ = obj.record_count_;
      current_pos_ = obj.current_pos_;
      current_meta_ = obj.current_meta_;

      return *this;
    }

    /*##################################################################################
     * Public destructors
     *################################################################################*/

    ~RecordIterator() { gc_.AddGarbage(node_); }

    /*##################################################################################
     * Public operators for iterators
     *################################################################################*/

    /**
     * @return a current key and payload pair.
     */
    auto
    operator*() const  //
        -> std::pair<Key, Payload>
    {
      return {GetKey(), GetPayload()};
    }

    /**
     * @brief Forward this iterator.
     *
     */
    constexpr void
    operator++()
    {
      ++current_pos_;
      current_meta_ = node_->GetMetadata(current_pos_);
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
    [[nodiscard]] auto
    HasNext()  //
        -> bool
    {
      // check whether records remain in this node
      if (current_pos_ < record_count_) return true;

      // check whether this node has a sibling node
      auto *sib_page = node_->GetSiblingNode();
      if (sib_page == nullptr) return false;

      // go to the sibling node and continue scaning
      *this = bw_tree_->SiblingScan(sib_page, end_key_);
      return HasNext();
    }

    /**
     * @return Key: a key of a current record
     */
    [[nodiscard]] auto
    GetKey() const  //
        -> Key
    {
      return node_->GetKey(current_meta_);
    }

    /**
     * @return Payload: a payload of a current record
     */
    [[nodiscard]] auto
    GetPayload() const  //
        -> Payload
    {
      return node_->template GetPayload<Payload>(current_meta_);
    }

   private:
    /*##################################################################################
     * Internal member variables
     *################################################################################*/

    /// a pointer to BwTree to perform continuous scan
    BwTree_t *bw_tree_{nullptr};

    /// garbage collector
    NodeGC_t &gc_{};

    /// the pointer to a node that includes partial scan results
    Node_t *node_{nullptr};

    /// the number of records in this node.
    size_t record_count_{0};

    /// an index of a current record
    size_t current_pos_{0};

    /// the metadata of a current record
    Metadata current_meta_{};

    /// the end key given from a user
    std::optional<std::pair<const Key &, bool>> end_key_{};
  };

  /*####################################################################################
   * Public constants
   *##################################################################################*/

  static constexpr size_t kDefaultGCTime = 100000;

  static constexpr size_t kDefaultGCThreadNum = 1;

  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new BwTree object.
   *
   * @param gc_interval_microsec GC internal [us]
   */
  explicit BwTree(  //
      const size_t gc_interval_microsec,
      const size_t gc_thread_num = kDefaultGCThreadNum)
      : gc_{gc_interval_microsec, gc_thread_num, true}
  {
    // create an empty Bw-tree
    auto *leaf = new (GetNodePage(kPageSize)) Node_t{};
    auto *root_page = mapping_table_.GetNewLogicalID();
    root_page->store(reinterpret_cast<uintptr_t>(leaf), std::memory_order_release);
    root_.store(root_page, std::memory_order_relaxed);
  }

  BwTree(const BwTree &) = delete;
  BwTree(BwTree &&) = delete;

  BwTree &operator=(const BwTree &) = delete;
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
    [[maybe_unused]] const auto &guard = gc_.CreateEpochGuard();

    // check whether the leaf node has a target key
    consol_page_ = nullptr;
    auto &&stack = SearchLeafNode(key, kClosed);
    auto [ptr, rc] = CheckExistence(key, stack);

    // if a long delta-chain is found, consolidate it
    if (consol_page_ != nullptr) {
      TryConsolidation(consol_page_, key, kClosed, stack);
    }

    switch (rc) {
      case NodeRC::kKeyNotExist: {
        return std::nullopt;
      }
      case NodeRC::kKeyInDelta: {
        auto *delta = reinterpret_cast<Delta_t *>(ptr);
        return std::make_optional(delta->template CopyPayload<Payload>());
      }
      case NodeRC::kKeyExist:
      default: {
        auto *node = reinterpret_cast<Node_t *>(ptr);
        return std::make_optional(node->template CopyPayload<Payload>(rc));
      }
    }
  }

  /**
   * @brief Perform a range scan with specified keys.
   *
   * @param begin_key a pair of a begin key and its openness (true=closed).
   * @param end_key a pair of an end key and its openness (true=closed).
   * @return an iterator to access target records.
   */
  auto
  Scan(  //
      const std::optional<std::pair<const Key &, bool>> &begin_key = std::nullopt,
      const std::optional<std::pair<const Key &, bool>> &end_key = std::nullopt)  //
      -> RecordIterator
  {
    [[maybe_unused]] const auto &guard = gc_.CreateEpochGuard();

    Node_t *node{};
    size_t begin_pos = 0;
    if (begin_key) {
      auto &&[b_key, b_closed] = *begin_key;

      // traverse to a leaf node and sort records for scanning
      consol_page_ = nullptr;
      auto &&stack = SearchLeafNode(b_key, b_closed);
      auto cur_head = ValidateNode(b_key, b_closed, kNullPtr, stack);
      node = Consolidate(cur_head).first;
      auto [rc, pos] = node->SearchRecord(b_key);
      begin_pos = (rc == NodeRC::kKeyNotExist || b_closed) ? pos : pos + 1;

      // if a long delta-chain is found, consolidate it
      if (consol_page_ != nullptr) {
        TryConsolidation(consol_page_, b_key, b_closed, stack);
      }
    } else {
      // traverse to the leftmost leaf node directly
      auto &&stack = SearchLeftEdgeLeaf();
      auto cur_head = stack.back()->load(std::memory_order_acquire);
      node = Consolidate(cur_head).first;
    }

    auto end_pos = node->GetRecordCount();
    if (end_key) {
      auto &&[e_key, e_closed] = *end_key;
      if (!Comp{}(node->GetKey(node->GetMetadata(end_pos - 1)), e_key)) {
        auto [rc, pos] = node->SearchRecord(e_key);
        end_pos = (rc == NodeRC::kKeyExist && e_closed) ? pos + 1 : pos;
      }
    }

    return RecordIterator{this, gc_, node, begin_pos, end_pos, end_key};
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
   * @param key_len the length of a target key.
   * @param pay_len the length of a target payload.
   * @return kSuccess.
   */
  auto
  Write(  //
      const Key &key,
      const Payload &payload,
      const size_t key_len = sizeof(Key),
      const size_t pay_len = sizeof(Payload))  //
      -> ReturnCode
  {
    [[maybe_unused]] const auto &guard = gc_.CreateEpochGuard();

    // traverse to a target leaf node
    consol_page_ = nullptr;
    auto &&stack = SearchLeafNode(key, kClosed);

    // insert a delta record
    auto *rec = new (GetRecPage()) Delta_t{DeltaType::kInsert, key, key_len, payload, pay_len};
    auto rec_ptr = reinterpret_cast<uintptr_t>(rec);
    auto prev_head = kNullPtr;
    while (true) {
      // check whether the target node includes incomplete SMOs
      auto head = ValidateNode(key, kClosed, prev_head, stack);

      // try to insert the delta record
      rec->SetNext(head);
      prev_head = head;
      if (stack.back()->compare_exchange_weak(head, rec_ptr, std::memory_order_release)) break;
    }

    if (consol_page_ != nullptr) {
      TryConsolidation(consol_page_, key, kClosed, stack);
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
      const size_t key_len = sizeof(Key),
      const size_t pay_len = sizeof(Payload))
  {
    [[maybe_unused]] const auto &guard = gc_.CreateEpochGuard();

    // traverse to a target leaf node
    consol_page_ = nullptr;
    auto &&stack = SearchLeafNode(key, kClosed);

    // insert a delta record
    auto *rec = new (GetRecPage()) Delta_t{DeltaType::kInsert, key, key_len, payload, pay_len};
    auto rec_ptr = reinterpret_cast<uintptr_t>(rec);
    auto prev_head = kNullPtr;
    while (true) {
      // check whether the target node includes incomplete SMOs
      auto head = ValidateNode(key, kClosed, prev_head, stack);

      // check target key/value existence
      const auto rc = CheckExistence(key, stack).second;
      if (rc != NodeRC::kKeyNotExist) return ReturnCode::kKeyExist;

      // try to insert the delta record
      rec->SetNext(head);
      prev_head = head;
      if (stack.back()->compare_exchange_weak(head, rec_ptr, std::memory_order_release)) break;
    }

    if (consol_page_ != nullptr) {
      TryConsolidation(consol_page_, key, kClosed, stack);
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
      const size_t key_len = sizeof(Key),
      const size_t pay_len = sizeof(Payload))
  {
    [[maybe_unused]] const auto &guard = gc_.CreateEpochGuard();

    // traverse to a target leaf node
    consol_page_ = nullptr;
    auto &&stack = SearchLeafNode(key, kClosed);

    // insert a delta record
    auto *rec = new (GetRecPage()) Delta_t{DeltaType::kModify, key, key_len, payload, pay_len};
    auto rec_ptr = reinterpret_cast<uintptr_t>(rec);
    auto prev_head = kNullPtr;
    while (true) {
      // check whether the target node includes incomplete SMOs
      auto head = ValidateNode(key, kClosed, prev_head, stack);

      // check target key/value existence
      const auto rc = CheckExistence(key, stack).second;
      if (rc == NodeRC::kKeyNotExist) return ReturnCode::kKeyNotExist;

      // try to insert the delta record
      rec->SetNext(head);
      prev_head = head;
      if (stack.back()->compare_exchange_weak(head, rec_ptr, std::memory_order_release)) break;
    }

    if (consol_page_ != nullptr) {
      TryConsolidation(consol_page_, key, kClosed, stack);
    }

    return ReturnCode::kSuccess;
  }

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

 private:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  static constexpr size_t kExpectedTreeHeight = 8;

  static constexpr size_t kMaxDeltaSize = component::GetMaxDeltaSize<Key, Payload>();

  static constexpr auto kHeaderLength = component::kHeaderLength;

  static constexpr auto kClosed = component::kClosed;

  static constexpr auto kNullPtr = component::kNullPtr;

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
    if (size > kPageSize) return ::operator new(size);

    auto *page = gc_.template GetPageIfPossible<Node_t>();
    return (page == nullptr) ? (::operator new(kPageSize)) : page;
  }

  [[nodiscard]] auto
  GetRecPage()  //
      -> void *
  {
    auto *page = gc_.template GetPageIfPossible<Delta_t>();
    return (page == nullptr) ? (::operator new(kMaxDeltaSize)) : page;
  }

  void
  AddToGC(uintptr_t ptr)
  {
    if (ptr == kNullPtr) return;

    // delete delta records
    auto *rec = reinterpret_cast<Delta_t *>(ptr);
    while (!rec->IsBaseNode()) {
      gc_.AddGarbage(rec);
      ptr = rec->GetNext();
      if (ptr == kNullPtr) return;

      rec = reinterpret_cast<Delta_t *>(ptr);
    }

    // delete a base node
    auto *node = reinterpret_cast<Node_t *>(ptr);
    gc_.AddGarbage(node);
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
    auto *page_id = root_.load(std::memory_order_relaxed);
    auto *head = reinterpret_cast<Node_t *>(page_id->load(std::memory_order_relaxed));

    // traverse a Bw-tree
    while (!head->IsLeaf()) {
      auto [ptr, rc] = Delta_t::SearchChildNode(key, closed, page_id);
      switch (rc) {
        case DeltaRC::kRecordFound: {
          stack.emplace_back(page_id);
          page_id = reinterpret_cast<std::atomic_uintptr_t *>(ptr);
          break;
        }
        case DeltaRC::kNodeRemoved: {
          page_id = stack.back();
          stack.pop_back();
          continue;
        }
        case DeltaRC::kReachBase:
        default: {
          stack.emplace_back(page_id);
          if (static_cast<size_t>(rc) >= kMaxDeltaNodeNum) {
            consol_page_ = page_id;
          }

          // search a child node in a base node
          auto *base_node = reinterpret_cast<Node_t *>(ptr);
          auto pos = base_node->SearchChild(key, closed);
          auto meta = base_node->GetMetadata(pos);
          page_id = base_node->template GetPayload<std::atomic_uintptr_t *>(meta);
          break;
        }
      }
      head = reinterpret_cast<Node_t *>(page_id->load(std::memory_order_relaxed));
    }
    stack.emplace_back(page_id);

    return stack;
  }

  auto
  SearchLeftEdgeLeaf()  //
      -> NodeStack
  {
    NodeStack stack{};
    stack.reserve(kExpectedTreeHeight);

    // get a logical page of a root node
    auto *page_id = root_.load(std::memory_order_relaxed);
    stack.emplace_back(page_id);

    // traverse a Bw-tree
    auto ptr = page_id->load(std::memory_order_acquire);
    auto *delta = reinterpret_cast<Delta_t *>(ptr);
    while (!delta->IsLeaf()) {
      while (!delta->IsBaseNode()) {
        // go to the next delta record or base node
        delta = reinterpret_cast<Delta_t *>(delta->GetNext());
      }

      // get a leftmost node
      auto *node = reinterpret_cast<Node_t *>(delta);
      page_id = node->template GetPayload<std::atomic_uintptr_t *>(node->GetMetadata(0));
      stack.emplace_back(page_id);

      // go down to the next level
      ptr = page_id->load(std::memory_order_acquire);
      delta = reinterpret_cast<Delta_t *>(ptr);
    }

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
    while (true) {
      auto [ptr, rc] = Delta_t::Validate(key, closed, prev_head, stack);
      switch (rc) {
        case DeltaRC::kSplitMayIncomplete: {
          // complete splitting and traverse to a sibling node
          CompleteSplit(ptr, stack);

          // insert a new current-level node (an old node was popped in parent-update)
          auto *rec = reinterpret_cast<Delta_t *>(ptr);
          auto *page_id = rec->template GetPayload<std::atomic_uintptr_t *>();
          stack.emplace_back(page_id);
          break;
        }
        case DeltaRC::kMergeMayIncomplete: {
          // ...not implemented yet
          break;
        }
        case DeltaRC::kNodeRemoved: {
          // ...not implemented yet
          break;
        }
        case DeltaRC::kReachBase:
        default: {
          if (static_cast<size_t>(rc) >= kMaxDeltaNodeNum) {
            consol_page_ = stack.back();
          }
          return ptr;
        }
      }
    }
  }

  auto
  CheckExistence(  //
      const Key &key,
      NodeStack &stack)  //
      -> std::pair<uintptr_t, NodeRC>
  {
    auto [ptr, delta_rc] = Delta_t::SearchRecord(key, stack);
    switch (delta_rc) {
      case DeltaRC::kRecordFound: {
        return {ptr, NodeRC::kKeyInDelta};
      }
      case DeltaRC::kRecordDeleted: {
        return {uintptr_t{}, NodeRC::kKeyNotExist};
      }
      case DeltaRC::kReachBase:
      default: {
        if (static_cast<size_t>(delta_rc) >= kMaxDeltaNodeNum) {
          consol_page_ = stack.back();
        }
        // search a target key
        auto *node = reinterpret_cast<Node_t *>(ptr);
        auto [node_rc, pos] = node->SearchRecord(key);

        if (node_rc == NodeRC::kKeyNotExist) return {ptr, NodeRC::kKeyNotExist};
        return {ptr, static_cast<NodeRC>(pos)};
      }
    }
  }

  /*####################################################################################
   * Internal scan utilities
   *##################################################################################*/

  /**
   * @brief Consolidate a leaf node for range scanning.
   *
   * @param node the target node.
   * @retval the pointer to consolidated leaf node
   */
  auto
  SiblingScan(  //
      const std::atomic_uintptr_t *page_id,
      const std::optional<std::pair<const Key &, bool>> &end_key)  //
      -> RecordIterator
  {
    auto cur_head = page_id->load(std::memory_order_acquire);
    auto *node = Consolidate(cur_head).first;

    auto rec_num = node->GetRecordCount();
    if (end_key) {
      auto &&[e_key, e_closed] = *end_key;
      if (!Comp{}(node->GetKey(node->GetMetadata(rec_num - 1)), e_key)) {
        auto [rc, pos] = node->SearchRecord(e_key);
        rec_num = (rc == NodeRC::kKeyExist && e_closed) ? pos + 1 : pos;
      }
    }

    return RecordIterator{this, gc_, node, 0, rec_num, end_key};
  }

  /*####################################################################################
   * Internal structure modifications
   *##################################################################################*/

  void
  TryConsolidation(  //
      std::atomic_uintptr_t *target_page,
      const Key &key,
      const bool closed,
      NodeStack &stack)
  {
    uintptr_t cur_head{};
    uintptr_t new_head{};
    while (true) {
      // remove child nodes from a node stack
      while (!stack.empty() && stack.back() != target_page) stack.pop_back();
      if (stack.empty()) return;

      // check whether the target node is valid (containing a target key and no incomplete SMOs)
      consol_page_ = nullptr;
      cur_head = ValidateNode(key, closed, kNullPtr, stack);
      if (consol_page_ != target_page) return;

      // perform consolidation
      auto [consol_node, size] = Consolidate(cur_head);
      if (size <= kPageSize) {
        new_head = reinterpret_cast<uintptr_t>(consol_node);
        break;
      }

      // perform splitting if needed
      if (HalfSplit(target_page, cur_head, consol_node, stack)) return;
    }

    // install a consolidated node
    auto old_head = cur_head;
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
    auto &&[ptr, high_key, h_meta, sib_ptr, diff] = Delta_t::template Sort<Payload>(head, sorted);
    auto *node = reinterpret_cast<Node_t *>(ptr);

    // reserve a page for a consolidated node
    auto [block_size, rec_num] = node->GetPageSize(high_key, h_meta);
    auto size = kHeaderLength + block_size + diff;
    auto *new_node = new (GetNodePage(size)) Node_t{node, h_meta, sib_ptr};
    if (new_node->IsLeaf()) {
      new_node->LeafConsolidate(node, sorted, high_key, size, rec_num);
    } else {
      new_node->InternalConsolidate(node, sorted, high_key, size, rec_num);
    }

    return {new_node, size};
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
    auto right_ptr = reinterpret_cast<uintptr_t>(split_node);
    sib_page->store(right_ptr, std::memory_order_release);
    auto sib_ptr = reinterpret_cast<uintptr_t>(sib_page);
    auto *delta = new (GetRecPage()) Delta_t{right_ptr, sib_ptr, cur_head};

    // install the delta record for splitting a child node
    auto old_head = cur_head;
    auto delta_ptr = reinterpret_cast<uintptr_t>(delta);
    if (!split_page->compare_exchange_strong(old_head, delta_ptr, std::memory_order_release)) {
      // retry from consolidation
      delta->SetNext(kNullPtr);
      AddToGC(delta_ptr);
      AddToGC(right_ptr);
      sib_page->store(kNullPtr, std::memory_order_relaxed);
      return false;
    }

    // execute parent update
    consol_page_ = nullptr;
    CompleteSplit(delta_ptr, stack);

    // execute parent consolidation/split if needed
    if (consol_page_ != nullptr) {
      TryConsolidation(consol_page_, sep_key, kClosed, stack);
    }

    return true;
  }

  void
  CompleteSplit(  //
      const uintptr_t split_ptr,
      NodeStack &stack)
  {
    if (stack.size() <= 1) {  // a split node is root
      RootSplit(split_ptr, stack);
      return;
    }

    // create an index-entry delta record to complete split
    auto *split_delta = reinterpret_cast<const Delta_t *>(split_ptr);
    auto *entry_delta = new (GetRecPage()) Delta_t{split_delta};
    auto new_head = reinterpret_cast<uintptr_t>(entry_delta);
    auto &&sep_key = split_delta->GetKey();

    // insert the delta record into a parent node
    stack.pop_back();  // remove a split child node to modify its parent node
    while (true) {
      auto head = ValidateNode(sep_key, kClosed, kNullPtr, stack);

      // check whether another thread has already completed this split
      if (CheckExistence(sep_key, stack).second != NodeRC::kKeyNotExist) {
        entry_delta->SetNext(kNullPtr);
        AddToGC(new_head);
        return;
      }

      // try to insert the index-entry delta record
      entry_delta->SetNext(head);
      if (stack.back()->compare_exchange_weak(head, new_head, std::memory_order_release)) return;
    }
  }

  void
  RootSplit(  //
      const uintptr_t split_delta,
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
      new_root_p->store(kNullPtr, std::memory_order_relaxed);
      AddToGC(root_ptr);
      stack.emplace_back(old_root_p);
      return;
    }
    stack.emplace_back(new_root_p);
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// a root node of Bw-tree
  std::atomic<std::atomic_uintptr_t *> root_{nullptr};

  /// a mapping table
  MappingTable_t mapping_table_{};

  /// garbage collector
  NodeGC_t gc_{kDefaultGCTime, kDefaultGCThreadNum, true};

  /// a page ID to be consolidated
  inline static thread_local std::atomic_uintptr_t *consol_page_{};  // NOLINT
};

}  // namespace dbgroup::index::bw_tree

#endif  // BW_TREE_BW_TREE_HPP
