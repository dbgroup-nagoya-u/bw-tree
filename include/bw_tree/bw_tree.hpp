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
  using DeltaRC = component::DeltaRC;
  using SMOStatus = component::SMOStatus;
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
        NodeGC_t *gc,
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

    RecordIterator(  //
        Node_t *node,
        size_t begin_pos,
        size_t end_pos)
        : node_{node}, record_count_{end_pos}, current_pos_{begin_pos}
    {
    }

    RecordIterator(const RecordIterator &) = delete;
    RecordIterator(RecordIterator &&) = delete;

    auto operator=(const RecordIterator &) -> RecordIterator & = delete;

    constexpr auto
    operator=(RecordIterator &&obj) noexcept  //
        -> RecordIterator &
    {
      node_ = obj.node_;
      obj.node_ = nullptr;
      record_count_ = obj.record_count_;
      current_pos_ = obj.current_pos_;
      current_meta_ = node_->GetMetadata(current_pos_);

      return *this;
    }

    /*##################################################################################
     * Public destructors
     *################################################################################*/

    ~RecordIterator()
    {
      if (node_ != nullptr) {
        gc_->AddGarbage(node_);
      }
    }

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
      *this = bw_tree_->SiblingScan(sib_page, node_, end_key_);
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
    NodeGC_t *gc_{nullptr};

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

    ReturnCode rc{};
    Payload payload{};
    while (true) {
      // check whether the node is active and has a target key
      auto head = stack.back()->load(std::memory_order_acquire);
      size_t delta_num = 0;
      switch (uintptr_t out_ptr{}; Delta_t::SearchRecord(key, head, out_ptr, delta_num)) {
        case DeltaRC::kRecordFound:
          rc = kKeyExist;
          payload = reinterpret_cast<Delta_t *>(out_ptr)->template CopyPayload<Payload>();
          break;

        case DeltaRC::kRecordDeleted:
          rc = kKeyNotExist;
          break;

        case DeltaRC::kKeyIsInSibling:
          // swap a current node in a stack and retry
          *stack.rbegin() = reinterpret_cast<std::atomic_uintptr_t *>(out_ptr);
          continue;

        case DeltaRC::kNodeRemoved:
          // retry from the parent node
          stack.pop_back();
          SearchChildNode(key, kClosed, stack);
          continue;

        case DeltaRC::kReachBaseNode:
        default: {
          // search a target key in the base node
          const auto *node = reinterpret_cast<Node_t *>(out_ptr);
          std::tie(rc, out_ptr) = node->SearchRecord(key);
          if (rc == kKeyExist) {
            payload = node->template CopyPayload<Payload>(out_ptr);
          }
        }
      }

      if (delta_num >= kMaxDeltaNodeNum) {
        TryConsolidation(stack.back(), key, kClosed, stack);
      } else if (consol_page_ != nullptr) {
        TryConsolidation(consol_page_, key, kClosed, stack);
      }

      if (rc == kKeyNotExist) return std::nullopt;
      return payload;
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
      auto cur_head = GetHead(b_key, b_closed, stack);
      node = Consolidate(cur_head).first;
      auto [rc, pos] = node->SearchRecord(b_key);
      begin_pos = (rc == kKeyNotExist || b_closed) ? pos : pos + 1;

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
        end_pos = (rc == kKeyExist && e_closed) ? pos + 1 : pos;
      }
    }

    return RecordIterator{this, &gc_, node, begin_pos, end_pos, end_key};
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
    while (true) {
      // check whether the target node includes incomplete SMOs
      auto head = GetHead(key, kClosed, stack);

      // try to insert the delta record
      rec->SetNext(head);
      if (stack.back()->compare_exchange_weak(head, rec_ptr, std::memory_order_release)) break;
    }

    if (consol_page_ != nullptr) {
      TryConsolidation(consol_page_, key, kClosed, stack);
    }

    return kSuccess;
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
    while (true) {
      // check target record's existence and get a head pointer
      auto [head, rc] = GetHeadWithKeyCheck(key, stack);
      if (rc == kKeyExist) return kKeyExist;

      // try to insert the delta record
      rec->SetNext(head);
      if (stack.back()->compare_exchange_weak(head, rec_ptr, std::memory_order_release)) break;
    }

    if (consol_page_ != nullptr) {
      TryConsolidation(consol_page_, key, kClosed, stack);
    }

    return kSuccess;
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
    while (true) {
      // check target record's existence and get a head pointer
      auto [head, rc] = GetHeadWithKeyCheck(key, stack);
      if (rc == kKeyNotExist) return kKeyNotExist;

      // try to insert the delta record
      rec->SetNext(head);
      if (stack.back()->compare_exchange_weak(head, rec_ptr, std::memory_order_release)) break;
    }

    if (consol_page_ != nullptr) {
      TryConsolidation(consol_page_, key, kClosed, stack);
    }

    return kSuccess;
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
   * @param key_len the length of a target key.
   * @retval kSuccess if deleted.
   * @retval kKeyNotExist if a specified key does not exist.
   */
  auto
  Delete(  //
      const Key &key,
      const size_t key_len = sizeof(Key))  //
      -> ReturnCode
  {
    [[maybe_unused]] const auto &guard = gc_.CreateEpochGuard();

    // traverse to a target leaf node
    consol_page_ = nullptr;
    auto &&stack = SearchLeafNode(key, kClosed);

    // insert a delta record
    auto *rec = new (GetRecPage()) Delta_t{key, key_len};
    auto rec_ptr = reinterpret_cast<uintptr_t>(rec);
    while (true) {
      // check target record's existence and get a head pointer
      auto [head, rc] = GetHeadWithKeyCheck(key, stack);
      if (rc == kKeyNotExist) return kKeyNotExist;

      // try to insert the delta record
      rec->SetNext(head);
      if (stack.back()->compare_exchange_weak(head, rec_ptr, std::memory_order_release)) break;
    }

    if (consol_page_ != nullptr) {
      TryConsolidation(consol_page_, key, kClosed, stack);
    }

    return kSuccess;
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

  void
  SearchChildNode(  //
      const Key &key,
      const bool closed,
      NodeStack &stack)
  {
    while (true) {
      size_t delta_num = 0;
      auto out_ptr = stack.back()->load(std::memory_order_acquire);
      switch (Delta_t::SearchChildNode(key, closed, out_ptr, delta_num)) {
        case DeltaRC::kRecordFound:
          stack.emplace_back(reinterpret_cast<std::atomic_uintptr_t *>(out_ptr));
          return;

        case DeltaRC::kKeyIsInSibling:
          // swap a current node in a stack and retry
          *stack.rbegin() = reinterpret_cast<std::atomic_uintptr_t *>(out_ptr);
          continue;

        case DeltaRC::kNodeRemoved:
          // retry from the parent node
          stack.pop_back();
          continue;

        case DeltaRC::kReachBaseNode:
        default: {
          if (delta_num >= kMaxDeltaNodeNum) {
            consol_page_ = stack.back();
          }

          // search a child node in a base node
          const auto *base_node = reinterpret_cast<Node_t *>(out_ptr);
          const auto pos = base_node->SearchChild(key, closed);
          const auto meta = base_node->GetMetadata(pos);
          stack.emplace_back(base_node->template GetPayload<std::atomic_uintptr_t *>(meta));
          return;
        }
      }
    }
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
    stack.emplace_back(page_id);
    auto *head = reinterpret_cast<Node_t *>(page_id->load(std::memory_order_relaxed));

    // traverse a Bw-tree
    while (!head->IsLeaf()) {
      SearchChildNode(key, closed, stack);
      head = reinterpret_cast<Node_t *>(stack.back()->load(std::memory_order_relaxed));
    }

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

  void
  CompletePartialSMOsIfExist(  //
      const uintptr_t head,
      NodeStack &stack)
  {
    switch (Delta_t::GetSMOStatus(head)) {
      case SMOStatus::kSplitMayIncomplete:
        // complete splitting and traverse to a sibling node
        CompleteSplit(head, stack);
        break;

      case SMOStatus::kMergeMayIncomplete:
        // complete merging
        CompleteMerge(reinterpret_cast<Delta_t *>(head), stack);
        break;

      case SMOStatus::kNoPartialSMOs:
      default:
        break;  // do nothing
    }
  }

  auto
  GetHead(  //
      const Key &key,
      const bool closed,
      NodeStack &stack)  //
      -> uintptr_t
  {
    uintptr_t sib_page{};

    while (true) {
      // check whether the node has partial SMOs
      auto head = stack.back()->load(std::memory_order_acquire);
      CompletePartialSMOsIfExist(head, stack);

      // check whether the node is active and can include a target key
      size_t delta_num = 0;
      switch (Delta_t::Validate(key, closed, head, sib_page, delta_num)) {
        case DeltaRC::kKeyIsInSibling:
          // swap a current node in a stack and retry
          *stack.rbegin() = reinterpret_cast<std::atomic_uintptr_t *>(sib_page);
          continue;

        case DeltaRC::kNodeRemoved:
          // retry from the parent node
          stack.pop_back();
          SearchChildNode(key, closed, stack);
          continue;

        case DeltaRC::kReachBaseNode:
        default:
          break;  // do nothing
      }

      if (delta_num >= kMaxDeltaNodeNum) {
        consol_page_ = stack.back();
      }

      return head;
    }
  }

  auto
  GetHeadWithKeyCheck(  //
      const Key &key,
      NodeStack &stack)  //
      -> std::pair<uintptr_t, ReturnCode>
  {
    uintptr_t out_ptr{};
    ReturnCode rc{};

    while (true) {
      // check whether the node has partial SMOs
      auto head = stack.back()->load(std::memory_order_acquire);
      CompletePartialSMOsIfExist(head, stack);

      // check whether the node is active and has a target key
      size_t delta_num = 0;
      switch (Delta_t::SearchRecord(key, head, out_ptr, delta_num)) {
        case DeltaRC::kRecordFound:
          rc = kKeyExist;
          break;

        case DeltaRC::kRecordDeleted:
          rc = kKeyNotExist;
          break;

        case DeltaRC::kKeyIsInSibling:
          // swap a current node in a stack and retry
          *stack.rbegin() = reinterpret_cast<std::atomic_uintptr_t *>(out_ptr);
          continue;

        case DeltaRC::kNodeRemoved:
          // retry from the parent node
          stack.pop_back();
          SearchChildNode(key, kClosed, stack);
          continue;

        case DeltaRC::kReachBaseNode:
        default:
          // search a target key in the base node
          rc = reinterpret_cast<Node_t *>(out_ptr)->SearchRecord(key).first;
      }

      if (delta_num >= kMaxDeltaNodeNum) {
        consol_page_ = stack.back();
      }

      return {head, rc};
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
      Node_t *page,
      const std::optional<std::pair<const Key &, bool>> &end_key)  //
      -> RecordIterator
  {
    auto cur_head = page_id->load(std::memory_order_acquire);
    auto *node = Consolidate(cur_head, page).first;

    auto rec_num = node->GetRecordCount();
    if (end_key) {
      auto &&[e_key, e_closed] = *end_key;
      if (!Comp{}(node->GetKey(node->GetMetadata(rec_num - 1)), e_key)) {
        auto [rc, pos] = node->SearchRecord(e_key);
        rec_num = (rc == kKeyExist && e_closed) ? pos + 1 : pos;
        node->RemoveSideLink();
      }
    }

    return RecordIterator{node, 0, rec_num};
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
      cur_head = GetHead(key, closed, stack);
      if (consol_page_ != target_page) return;

      // prepare a consolidated node
      auto [consol_node, size] = Consolidate(cur_head);

      if (size > kPageSize) {
        // switch to splitting
        if (HalfSplit(target_page, cur_head, consol_node, stack)) return;
        continue;  // retry splitting
      }

      if (size <= kMinNodeSize && !consol_node->IsLeftmostChildIn(stack)) {
        // switch to merging
        TryMerge(cur_head, consol_node, stack);
        return;  // no retry for merging
      }

      // perform consolidation
      new_head = reinterpret_cast<uintptr_t>(consol_node);
      break;
    }

    // install a consolidated node
    if (!target_page->compare_exchange_strong(cur_head, new_head, std::memory_order_release)) {
      // no CAS retry for consolidation
      AddToGC(new_head);
      return;
    }

    // delete consolidated delta records and a base node
    AddToGC(cur_head);
  }

  auto
  Consolidate(  //
      const uintptr_t head,
      void *page = nullptr)  //
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
    if (page == nullptr) {
      page = GetNodePage(size);
    } else if (size > kPageSize) {
      AddToGC(reinterpret_cast<uintptr_t>(page));
      page = GetNodePage(size);
    }
    auto *new_node = new (page) Node_t{node, h_meta, sib_ptr};
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
    sib_page->store(reinterpret_cast<uintptr_t>(split_node), std::memory_order_release);
    auto *delta = new (GetRecPage()) Delta_t{DeltaType::kSplit, split_node, sib_page, cur_head};

    // install the delta record for splitting a child node
    auto old_head = cur_head;
    auto delta_ptr = reinterpret_cast<uintptr_t>(delta);
    if (!split_page->compare_exchange_strong(old_head, delta_ptr, std::memory_order_release)) {
      // retry from consolidation
      delta->SetNext(kNullPtr);
      AddToGC(delta_ptr);
      AddToGC(reinterpret_cast<uintptr_t>(split_node));
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
    stack.pop_back();  // remove the split child node to modify its parent node
    while (true) {
      // check whether another thread has already completed this splitting
      auto [head, rc] = GetHeadWithKeyCheck(sep_key, stack);
      if (rc == kKeyExist) {
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

  void
  TryMerge(  //
      uintptr_t head,
      const Node_t *merged_node,
      NodeStack &stack)
  {
    auto *merged_page = stack.back();

    // insert a remove-node delta to prevent other threads from modifying this node
    auto *remove_delta = new (GetRecPage()) Delta_t{reinterpret_cast<uintptr_t>(merged_node)};
    auto delta_ptr = reinterpret_cast<uintptr_t>(remove_delta);
    if (!merged_page->compare_exchange_strong(head, delta_ptr, std::memory_order_release)) {
      // no retry for merging
      AddToGC(delta_ptr);
      return;
    }

    // search a left sibling node
    stack.pop_back();
    const auto &low_key = merged_node->GetLowKey();
    SearchChildNode(low_key, kClosed, stack);

    // insert a merge delta into the left sibling node
    auto *sib_page = stack.back();
    auto *merge_delta = new (GetRecPage()) Delta_t{DeltaType::kMerge, merged_node, merged_page};
    delta_ptr = reinterpret_cast<uintptr_t>(merge_delta);
    while (true) {  // continue until insertion succeeds
      head = GetHead(low_key, kClosed, stack);
      merge_delta->SetNext(head);
      if (sib_page->compare_exchange_weak(head, delta_ptr, std::memory_order_release)) break;
    }

    CompleteMerge(merge_delta, stack);
  }

  void
  CompleteMerge(  //
      const Delta_t *merge_delta,
      NodeStack &stack)
  {
    // create an index-delete delta record to complete merging
    auto *del_delta = new (GetRecPage()) Delta_t{merge_delta, stack.back()};
    const auto new_head = reinterpret_cast<uintptr_t>(del_delta);
    const auto &del_key = merge_delta->GetKey();

    // insert the delta record into a parent node
    stack.pop_back();  // remove the merged child node to modify its parent node
    while (true) {
      // check whether another thread has already completed this merging
      auto [head, rc] = GetHeadWithKeyCheck(del_key, stack);
      if (rc == kKeyNotExist) {
        // another thread has already deleted the merged node
        del_delta->SetNext(kNullPtr);
        AddToGC(new_head);
        return;
      }

      // check concurrent splitting
      auto *cur_page = stack.back();
      auto sib_head = GetHead(del_key, !kClosed, stack);
      if (cur_page != stack.back()) {
        // the target node was split, so check whether the merged nodes span two parent nodes
        for (auto *delta = reinterpret_cast<Delta_t *>(sib_head); !delta->IsBaseNode();) {
          sib_head = delta->GetNext();
          delta = reinterpret_cast<Delta_t *>(sib_head);
        }
        if (reinterpret_cast<Node_t *>(sib_head)->HasSameLowKeyWith(del_key)) {
          // the merged nodes have different parent nodes, so abort
          AbortMerge(merge_delta);
          return;
        }
        continue;  // the merged nodes have the same parent node, so retry
      }

      // try to insert the index-delete delta record
      del_delta->SetNext(head);
      if (cur_page->compare_exchange_weak(head, new_head, std::memory_order_release)) return;
    }
  }

  void
  AbortMerge(const Delta_t *merge_delta)
  {
    // check this merging is still active
    auto *sib_page = merge_delta->template GetPayload<std::atomic_uintptr_t *>();
    auto head = sib_page->load(std::memory_order_acquire);
    const auto *remove_d = reinterpret_cast<Delta_t *>(head);
    if (!remove_d->IsRemoveNodeDelta()) return;  // merging has been already aborted

    // delete the remove-node delta to allow other threads to modify the node
    sib_page->compare_exchange_strong(head, remove_d->GetNext(), std::memory_order_release);
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
