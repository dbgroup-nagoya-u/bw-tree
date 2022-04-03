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

#ifndef BW_TREE_COMPONENT_BW_TREE_INTERNAL_HPP
#define BW_TREE_COMPONENT_BW_TREE_INTERNAL_HPP

#include <functional>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>

#include "consolidate_info.hpp"
#include "delta_chain.hpp"
#include "logical_id.hpp"
#include "mapping_table.hpp"
#include "memory/epoch_based_gc.hpp"

namespace dbgroup::index::bw_tree::component
{
/**
 * @brief A class to represent Bw-tree.
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 */
template <class Payload, class Node_t, class Delta_t, bool kIsVarLen>
class BwTree
{
 public:
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using Key = typename Delta_t::Key;
  using DeltaType = component::DeltaType;
  using MappingTable_t = MappingTable<Node_t, Delta_t>;
  using DC = DeltaChain<Delta_t>;
  using NodeGC_t = ::dbgroup::memory::EpochBasedGC<Node_t, Delta_t>;

  /*####################################################################################
   * Public sub -classes
   *##################################################################################*/

  /**
   * @brief A class for representing an iterator of scan results.
   *
   */
  class RecordIterator
  {
   public:
    /*##################################################################################
     * Public constructors and assignment operators
     *################################################################################*/

    /**
     * @brief Construct a new object as an initial iterator.
     *
     * @param bw_tree a pointer to an index.
     * @param gc a garbage collector for useless nodes.
     * @param node a copied node for scanning results.
     * @param begin_pos the begin position of a current node.
     * @param end_pos the end position of a current node.
     * @param end_key an optional end-point key.
     * @param is_end a flag for indicating a current node is rightmost in scan-range.
     */
    RecordIterator(  //
        BwTree *bw_tree,
        NodeGC_t *gc,
        Node_t *node,
        size_t begin_pos,
        size_t end_pos,
        const std::optional<std::pair<const Key &, bool>> end_key,
        const bool is_end)
        : bw_tree_{bw_tree},
          gc_{gc},
          node_{node},
          record_count_{end_pos},
          current_pos_{begin_pos},
          end_key_{std::move(end_key)},
          is_end_{is_end}
    {
    }

    /**
     * @brief Construct a new object for sibling scanning.
     *
     * @param node a copied node for scanning results.
     * @param begin_pos the begin position of a current node.
     * @param end_pos the end position of a current node.
     * @param is_end a flag for indicating a current node is rightmost in scan-range.
     */
    RecordIterator(  //
        Node_t *node,
        size_t begin_pos,
        size_t end_pos,
        const bool is_end)
        : node_{node}, record_count_{end_pos}, current_pos_{begin_pos}, is_end_{is_end}
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
      is_end_ = obj.is_end_;

      return *this;
    }

    /*##################################################################################
     * Public destructors
     *################################################################################*/

    /**
     * @brief Destroy the iterator and a retained node if exist.
     *
     */
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
      return node_->template GetRecord<Payload>(current_pos_);
    }

    /**
     * @brief Forward this iterator.
     *
     */
    constexpr void
    operator++()
    {
      ++current_pos_;
    }

    /*##################################################################################
     * Public getters/setters
     *################################################################################*/

    /**
     * @brief Check if there are any records left.
     *
     * NOTE: this may call a scanning function internally to get a sibling node.
     *
     * @retval true if there are any records or next node left.
     * @retval false otherwise.
     */
    [[nodiscard]] auto
    HasNext()  //
        -> bool
    {
      while (true) {
        if (current_pos_ < record_count_) return true;  // records remain in this node
        if (is_end_) return false;                      // this node is the end of range-scan

        // go to the next sibling node and continue scanning
        const auto &next_key = node_->GetHighKey();
        auto *sib_page = node_->template GetNext<LogicalID *>();
        *this = bw_tree_->SiblingScan(sib_page, node_, next_key, end_key_);

        if constexpr (kIsVarLen && IsVariableLengthData<Key>()) {
          // release a dynamically allocated key
          delete next_key;
        }
      }
    }

    /**
     * @return a key of a current record
     */
    [[nodiscard]] auto
    GetKey() const  //
        -> Key
    {
      return node_->GetKey(current_pos_);
    }

    /**
     * @return a payload of a current record
     */
    [[nodiscard]] auto
    GetPayload() const  //
        -> Payload
    {
      return node_->template GetPayload<Payload>(current_pos_);
    }

   private:
    /*##################################################################################
     * Internal member variables
     *################################################################################*/

    /// a pointer to a BwTree for sibling scanning.
    BwTree *bw_tree_{nullptr};

    /// garbage collector.
    NodeGC_t *gc_{nullptr};

    /// the pointer to a node that includes partial scan results.
    Node_t *node_{nullptr};

    /// the number of records in this node.
    size_t record_count_{0};

    /// the position of a current record.
    size_t current_pos_{0};

    /// the end key given from a user.
    std::optional<std::pair<const Key &, bool>> end_key_{};

    /// a flag for indicating a current node is rightmost in scan-range.
    bool is_end_{true};
  };

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
      const size_t gc_thread_num)
      : gc_{gc_interval_microsec, gc_thread_num, true}
  {
    // create an empty Bw-tree
    auto *root_node = new (GetNodePage(kPageSize)) Node_t{};
    auto *root_lid = mapping_table_.GetNewLogicalID();
    root_lid->Store(root_node);
    root_.store(root_lid, std::memory_order_relaxed);
  }

  BwTree(const BwTree &) = delete;
  BwTree(BwTree &&) = delete;

  auto operator=(const BwTree &) -> BwTree & = delete;
  auto operator=(BwTree &&) -> BwTree & = delete;

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
    for (Payload payload{}; true;) {
      // check whether the node is active and has a target key
      const auto *head = LoadValidHead(key, kClosed, stack);
      size_t delta_num = 0;
      switch (uintptr_t out_ptr{}; DC::SearchRecord(head, key, out_ptr, delta_num)) {
        case DeltaRC::kRecordFound:
          rc = kKeyExist;
          payload = reinterpret_cast<Delta_t *>(out_ptr)->template GetPayload<Payload>();
          break;

        case DeltaRC::kRecordDeleted:
          rc = kKeyNotExist;
          break;

        case DeltaRC::kKeyIsInSibling:
          // swap a current node in a stack and retry
          *stack.rbegin() = reinterpret_cast<LogicalID *>(out_ptr);
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
            payload = node->template GetPayload<Payload>(out_ptr);
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

    Node_t *node = nullptr;
    size_t begin_pos{};
    if (begin_key) {
      // traverse to a leaf node and sort records for scanning
      const auto &[b_key, b_closed] = *begin_key;
      auto &&stack = SearchLeafNode(b_key, b_closed);
      begin_pos = ConsolidateForScan(node, b_key, b_closed, stack);
    } else {
      // traverse to the leftmost leaf node directly
      auto &&stack = SearchLeftmostLeaf();
      while (true) {
        const auto *head = stack.back()->template Load<Delta_t *>();
        if (Consolidate<Payload>(head, node, kIsScan) != kAlreadyConsolidated) break;
        // concurrent consolidations may block scanning
      }
      begin_pos = 0;
    }

    // check the end position of scanning
    const auto [is_end, end_pos] = node->SearchEndPositionFor(end_key);

    return RecordIterator{this, &gc_, node, begin_pos, end_pos, end_key, is_end};
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
      [[maybe_unused]] const size_t key_len = sizeof(Key))  //
      -> ReturnCode
  {
    [[maybe_unused]] const auto &guard = gc_.CreateEpochGuard();

    // traverse to a target leaf node
    consol_page_ = nullptr;
    auto &&stack = SearchLeafNode(key, kClosed);

    // insert a delta record
    Delta_t *insert_d{};
    if constexpr (kIsVarLen) {
      insert_d = new (GetRecPage()) Delta_t{DeltaType::kInsert, key, key_len, payload};
    } else {
      insert_d = new (GetRecPage()) Delta_t{DeltaType::kInsert, key, payload};
    }
    while (true) {
      // check whether the target node includes incomplete SMOs
      const auto *head = GetHead(key, kClosed, stack);

      // try to insert the delta record
      insert_d->SetNext(head);
      if (stack.back()->CASWeak(head, insert_d)) break;
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
      [[maybe_unused]] const size_t key_len = sizeof(Key))
  {
    [[maybe_unused]] const auto &guard = gc_.CreateEpochGuard();

    // traverse to a target leaf node
    consol_page_ = nullptr;
    auto &&stack = SearchLeafNode(key, kClosed);

    // insert a delta record
    Delta_t *insert_d{};
    if constexpr (kIsVarLen) {
      insert_d = new (GetRecPage()) Delta_t{DeltaType::kInsert, key, key_len, payload};
    } else {
      insert_d = new (GetRecPage()) Delta_t{DeltaType::kInsert, key, payload};
    }
    auto rc = kSuccess;
    while (true) {
      // check target record's existence and get a head pointer
      const auto [head, existence] = GetHeadWithKeyCheck(key, stack);
      if (existence == kKeyExist) {
        rc = kKeyExist;
        insert_d->Abort();
        AddToGC(insert_d);
        break;
      }

      // try to insert the delta record
      insert_d->SetNext(head);
      if (stack.back()->CASWeak(head, insert_d)) break;
    }

    if (consol_page_ != nullptr) {
      TryConsolidation(consol_page_, key, kClosed, stack);
    }

    return rc;
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
      [[maybe_unused]] const size_t key_len = sizeof(Key))
  {
    [[maybe_unused]] const auto &guard = gc_.CreateEpochGuard();

    // traverse to a target leaf node
    consol_page_ = nullptr;
    auto &&stack = SearchLeafNode(key, kClosed);

    // insert a delta record
    Delta_t *modify_d{};
    if constexpr (kIsVarLen) {
      modify_d = new (GetRecPage()) Delta_t{DeltaType::kModify, key, key_len, payload};
    } else {
      modify_d = new (GetRecPage()) Delta_t{DeltaType::kModify, key, payload};
    }
    auto rc = kSuccess;
    while (true) {
      // check target record's existence and get a head pointer
      const auto [head, existence] = GetHeadWithKeyCheck(key, stack);
      if (existence == kKeyNotExist) {
        rc = kKeyNotExist;
        modify_d->Abort();
        AddToGC(modify_d);
        break;
      }

      // try to insert the delta record
      modify_d->SetNext(head);
      if (stack.back()->CASWeak(head, modify_d)) break;
    }

    if (consol_page_ != nullptr) {
      TryConsolidation(consol_page_, key, kClosed, stack);
    }

    return rc;
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
      [[maybe_unused]] const size_t key_len = sizeof(Key))  //
      -> ReturnCode
  {
    [[maybe_unused]] const auto &guard = gc_.CreateEpochGuard();

    // traverse to a target leaf node
    consol_page_ = nullptr;
    auto &&stack = SearchLeafNode(key, kClosed);

    // insert a delta record
    Delta_t *delete_d{};
    if constexpr (kIsVarLen) {
      delete_d = new (GetRecPage()) Delta_t{key, key_len};
    } else {
      delete_d = new (GetRecPage()) Delta_t{key};
    }
    auto rc = kSuccess;
    while (true) {
      // check target record's existence and get a head pointer
      auto [head, existence] = GetHeadWithKeyCheck(key, stack);
      if (existence == kKeyNotExist) {
        rc = kKeyNotExist;
        delete_d->Abort();
        AddToGC(delete_d);
        break;
      }

      // try to insert the delta record
      delete_d->SetNext(head);
      if (stack.back()->CASWeak(head, delete_d)) break;
    }

    if (consol_page_ != nullptr) {
      TryConsolidation(consol_page_, key, kClosed, stack);
    }

    return rc;
  }

 private:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  static constexpr size_t kExpectedTreeHeight = 8;

  static constexpr size_t kHeaderLength = sizeof(Node_t);

  static constexpr size_t kDeltaRecLength = Delta_t::template GetMaxDeltaSize<Payload>();

  static constexpr bool kIsScan = true;

  enum SMOsRC
  {
    kConsolidate,
    kTrySplit,
    kTryMerge,
    kAlreadyConsolidated
  };

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
    return (page == nullptr) ? (::operator new(kDeltaRecLength)) : page;
  }

  template <class T>
  void
  AddToGC(const T *ptr)
  {
    static_assert(std::is_same_v<T, Node_t> || std::is_same_v<T, Delta_t>);

    // delete delta records
    const auto *garbage = reinterpret_cast<const Delta_t *>(ptr);
    while (garbage->GetDeltaType() != kNotDelta) {
      // register this delta record with GC
      gc_.AddGarbage(garbage);

      // if the delta record is merge-delta, delete the merged sibling node
      if (garbage->GetDeltaType() == kMerge) {
        auto *sib_lid = garbage->template GetPayload<LogicalID *>();
        const auto *remove_d = sib_lid->template Load<Delta_t *>();
        if (remove_d->GetDeltaType() == kRemoveNode) {  // check merging is not aborted
          sib_lid->Clear();
          AddToGC(remove_d);
        }
      }

      // check the next delta record or base node
      garbage = garbage->GetNext();
      if (garbage == nullptr) return;
    }

    // register a base node with GC
    gc_.AddGarbage(reinterpret_cast<const Node_t *>(garbage));
  }

  void
  SearchChildNode(  //
      const Key &key,
      const bool closed,
      std::vector<LogicalID *> &stack) const
  {
    if (stack.empty()) {
      stack.emplace_back(LoadValidRoot());
      return;
    }

    for (uintptr_t out_ptr{}; true;) {
      size_t delta_num = 0;
      const auto *head = LoadValidHead(key, closed, stack);
      switch (DC::SearchChildNode(head, key, closed, out_ptr, delta_num)) {
        case DeltaRC::kRecordFound:
          stack.emplace_back(reinterpret_cast<LogicalID *>(out_ptr));
          return;

        case DeltaRC::kKeyIsInSibling:
          // swap a current node in a stack and retry
          *stack.rbegin() = reinterpret_cast<LogicalID *>(out_ptr);
          continue;

        case DeltaRC::kNodeRemoved:
          // retry from the parent node
          stack.pop_back();
          SearchChildNode(key, closed, stack);
          continue;

        case DeltaRC::kReachBaseNode:
        default: {
          if (delta_num >= kMaxDeltaNodeNum) {
            consol_page_ = stack.back();
          }

          // search a child node in a base node
          const auto *node = reinterpret_cast<Node_t *>(out_ptr);
          stack.emplace_back(node->SearchChild(key, closed));
          return;
        }
      }
    }
  }

  [[nodiscard]] auto
  SearchLeafNode(  //
      const Key &key,
      const bool closed) const  //
      -> std::vector<LogicalID *>
  {
    std::vector<LogicalID *> stack{};
    stack.reserve(kExpectedTreeHeight);

    // traverse a Bw-tree
    while (true) {
      SearchChildNode(key, closed, stack);
      const auto *node = stack.back()->template Load<Node_t *>();
      if (node == nullptr) {
        // the found node is removed, so retry
        stack.pop_back();
        continue;
      }

      if (node->IsLeaf()) return stack;
    }
  }

  [[nodiscard]] auto
  SearchLeftmostLeaf() const  //
      -> std::vector<LogicalID *>
  {
    std::vector<LogicalID *> stack{LoadValidRoot()};
    stack.reserve(kExpectedTreeHeight);

    // traverse a Bw-tree
    while (true) {
      const auto *node = stack.back()->template Load<Node_t *>();
      if (node->IsLeaf()) break;
      stack.emplace_back(node->GetLeftmostChild());
    }

    return stack;
  }

  [[nodiscard]] auto
  LoadValidRoot() const  //
      -> LogicalID *
  {
    while (true) {
      auto *root_lid = root_.load(std::memory_order_relaxed);
      const auto *root_d = root_lid->template Load<const Delta_t *>();
      if (root_d != nullptr && root_d->GetDeltaType() != kRemoveNode) return root_lid;
    }
  }

  auto
  LoadValidHead(  //
      const Key &key,
      const bool closed,
      std::vector<LogicalID *> &stack) const  //
      -> const Delta_t *
  {
    while (true) {
      const auto *head = stack.back()->template Load<Delta_t *>();
      if (head != nullptr) return head;

      // the current node is removed, so restore a new one
      stack.pop_back();
      if (stack.empty()) {
        stack.emplace_back(LoadValidRoot());
      } else {
        SearchChildNode(key, closed, stack);
      }
    }
  }

  void
  CompletePartialSMOsIfExist(  //
      const Delta_t *head,
      std::vector<LogicalID *> &stack)
  {
    switch (DC::GetSMOStatus(head)) {
      case SMOStatus::kSplitMayIncomplete:
        // complete splitting and traverse to a sibling node
        CompleteSplit(head, stack);
        break;

      case SMOStatus::kMergeMayIncomplete:
        // complete merging
        CompleteMerge(head, stack);
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
      std::vector<LogicalID *> &stack,
      const bool read_only = false)  //
      -> const Delta_t *
  {
    for (LogicalID *sib_lid{}; true;) {
      // check whether the node has partial SMOs
      const auto *head = LoadValidHead(key, closed, stack);
      if (!read_only) {
        CompletePartialSMOsIfExist(head, stack);
      }

      // check whether the node is active and can include a target key
      size_t delta_num = 0;
      switch (DC::Validate(head, key, closed, sib_lid, delta_num)) {
        case DeltaRC::kKeyIsInSibling:
          // swap a current node in a stack and retry
          *stack.rbegin() = sib_lid;
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
      std::vector<LogicalID *> &stack)  //
      -> std::pair<const Delta_t *, ReturnCode>
  {
    ReturnCode rc{};
    for (uintptr_t out_ptr{}; true;) {
      // check whether the node has partial SMOs
      const auto *head = LoadValidHead(key, kClosed, stack);
      CompletePartialSMOsIfExist(head, stack);

      // check whether the node is active and has a target key
      size_t delta_num = 0;
      switch (DC::SearchRecord(head, key, out_ptr, delta_num)) {
        case DeltaRC::kRecordFound:
          rc = kKeyExist;
          break;

        case DeltaRC::kRecordDeleted:
          rc = kKeyNotExist;
          break;

        case DeltaRC::kKeyIsInSibling:
          // swap a current node in a stack and retry
          *stack.rbegin() = reinterpret_cast<LogicalID *>(out_ptr);
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

  auto
  ConsolidateForScan(  //
      Node_t *&node,
      const Key &begin_key,
      const bool closed,
      std::vector<LogicalID *> &stack)  //
      -> size_t
  {
    while (true) {
      const auto *head = GetHead(begin_key, closed, stack, kIsScan);
      if (Consolidate<Payload>(head, node, kIsScan) != kAlreadyConsolidated) break;
      // concurrent consolidations may block scanning
    }

    // check the begin position for scanning
    const auto [rc, pos] = node->SearchRecord(begin_key);

    return (rc == kKeyNotExist || closed) ? pos : pos + 1;
  }

  /**
   * @brief Consolidate a leaf node for range scanning.
   *
   * @param node the target node.
   * @retval the pointer to consolidated leaf node
   */
  auto
  SiblingScan(  //
      LogicalID *sib_lid,
      Node_t *node,
      const Key &begin_key,
      const std::optional<std::pair<const Key &, bool>> &end_key)  //
      -> RecordIterator
  {
    // consolidate a sibling node
    std::vector<LogicalID *> stack{sib_lid};
    stack.reserve(kExpectedTreeHeight);
    const auto begin_pos = ConsolidateForScan(node, begin_key, !kClosed, stack);

    // check the end position of scanning
    const auto [is_end, end_pos] = node->SearchEndPositionFor(end_key);

    return RecordIterator{node, begin_pos, end_pos, is_end};
  }

  /*####################################################################################
   * Internal structure modifications
   *##################################################################################*/

  void
  TryConsolidation(  //
      LogicalID *target_lid,
      const Key &key,
      const bool closed,
      std::vector<LogicalID *> &stack)
  {
    while (true) {
      // remove child nodes from a node stack
      while (!stack.empty() && stack.back() != target_lid) stack.pop_back();
      if (stack.empty()) return;

      // check whether the target node is valid (containing a target key and no incomplete SMOs)
      consol_page_ = nullptr;
      const auto *head = GetHead(key, closed, stack);
      if (consol_page_ != target_lid) return;
      if (stack.back() != target_lid) return;

      // prepare a consolidated node
      Node_t *new_node = nullptr;
      const auto rc = (head->IsLeaf()) ? Consolidate<Payload>(head, new_node)
                                       : Consolidate<LogicalID *>(head, new_node);
      switch (rc) {
        case kAlreadyConsolidated:
          // other threads have performed consolidation
          return;

        case kTrySplit:
          // switch to splitting
          if (TrySplit(head, reinterpret_cast<Delta_t *>(new_node), stack)) return;
          continue;  // retry from consolidation

        case kTryMerge:
          if (CanMerge(new_node, stack)) {
            // switch to merging
            if (TryMerge(head, reinterpret_cast<Delta_t *>(new_node), stack)) return;
            continue;  // retry from consolidation
          } else if (stack.size() == 1 && new_node->GetRecordCount() == 1 && !new_node->IsLeaf()) {
            // switch to removing a root node
            if (TryRemoveRoot(head, new_node, target_lid)) return;
            continue;  // retry from consolidation
          }
          break;  // perform consolidation

        case kConsolidate:
        default:
          break;  // perform consolidation
      }

      // install a consolidated node
      if (target_lid->CASStrong(head, new_node)) {
        // delete consolidated delta records and a base node
        AddToGC(head);
        return;
      }
      // if consolidation fails, no retry
      AddToGC(new_node);
      return;
    }
  }

  template <class T>
  auto
  Consolidate(  //
      const Delta_t *head,
      Node_t *&consol_node,
      const bool is_scan = false)  //
      -> SMOsRC
  {
    using Record = typename Delta_t::Record;
    constexpr auto kIsLeaf = !std::is_same_v<T, LogicalID *>;

    // sort delta records
    std::vector<Record> records{};
    std::vector<ConsolidateInfo> consol_info{};
    records.reserve(kMaxDeltaNodeNum * 4);
    consol_info.reserve(kMaxDeltaNodeNum);
    const auto [consolidated, diff] = DC::template Sort<T>(head, records, consol_info);
    if (consolidated) return kAlreadyConsolidated;

    // calculate the size of a consolidated node
    size_t size{};
    if constexpr (kIsVarLen) {
      size = kHeaderLength + Node_t::PreConsolidate(consol_info, kIsLeaf) + diff;
    } else {
      const auto rec_num = Node_t::PreConsolidate(consol_info, kIsLeaf) + diff;
      if constexpr (kIsLeaf) {
        size = kHeaderLength + rec_num * (sizeof(Key) + sizeof(T));
      } else {
        size = (kHeaderLength - sizeof(Key)) + rec_num * (sizeof(Key) + sizeof(T));
      }
    }

    // check whether splitting is needed
    bool do_split = false;
    if (!is_scan && size > kPageSize) {
      do_split = true;
      size = size / 2 + (kHeaderLength / 2);
    }

    // prepare a page for a new node
    void *page{};
    if (consol_node == nullptr) {
      page = GetNodePage(size);
    } else if (size > kPageSize) {
      page = GetNodePage(size);
      AddToGC(consol_node);
    } else {
      page = consol_node;
    }

    // consolidate a target node
    consol_node = new (page) Node_t{kIsLeaf, size};
    if constexpr (kIsLeaf) {
      consol_node->template LeafConsolidate<T>(consol_info, records, do_split);
    } else {
      consol_node->InternalConsolidate(consol_info, records, do_split);
    }

    if (do_split) return kTrySplit;
    if (size <= kMinNodeSize) return kTryMerge;
    return kConsolidate;
  }

  bool
  TrySplit(  //
      const Delta_t *head,
      const Delta_t *split_node,
      std::vector<LogicalID *> &stack)
  {
    // create a split-delta record
    auto *sib_lid = mapping_table_.GetNewLogicalID();
    sib_lid->Store(split_node);
    auto *split_d = new (GetRecPage()) Delta_t{DeltaType::kSplit, split_node, sib_lid, head};

    // install the delta record for splitting a child node
    if (!stack.back()->CASStrong(head, split_d)) {
      // retry from consolidation
      split_d->Abort();
      sib_lid->Clear();
      AddToGC(split_d);
      AddToGC(split_node);
      return false;
    }

    // execute parent update
    consol_page_ = nullptr;
    CompleteSplit(split_d, stack);

    // execute parent consolidation/split if needed
    if (consol_page_ != nullptr) {
      TryConsolidation(consol_page_, split_node->GetKey(), kClosed, stack);
    }

    return true;
  }

  void
  CompleteSplit(  //
      const Delta_t *split_d,
      std::vector<LogicalID *> &stack)
  {
    // check whether this splitting modifies a root node
    if (stack.size() < 2 && RootSplit(reinterpret_cast<const Node_t *>(split_d), stack)) return;

    // create an index-entry delta record to complete split
    auto *entry_d = new (GetRecPage()) Delta_t{split_d};
    const auto &sep_key = split_d->GetKey();

    // insert the delta record into a parent node
    auto *cur_lid = stack.back();  // keep a current logical ID
    stack.pop_back();              // remove the split child node to modify its parent node
    while (true) {
      // check whether another thread has already completed this splitting
      const auto [head, rc] = GetHeadWithKeyCheck(sep_key, stack);
      if (rc == kKeyExist) {
        entry_d->Abort();
        AddToGC(entry_d);
        break;
      }

      // try to insert the index-entry delta record
      entry_d->SetNext(head);
      if (stack.back()->CASWeak(head, entry_d)) break;
    }

    // restore the current logical ID
    stack.emplace_back(cur_lid);
  }

  auto
  RootSplit(  //
      const Node_t *split_d,
      std::vector<LogicalID *> &stack)  //
      -> bool
  {
    // create a new root node
    auto *child_lid = stack.back();
    stack.pop_back();  // remove the current root to push a new one
    const auto *new_root = new (GetNodePage(kPageSize)) Node_t{split_d, child_lid};

    // prepare a new logical ID for the new root
    auto *new_lid = mapping_table_.GetNewLogicalID();
    new_lid->Store(new_root);

    // install a new root page
    auto *cur_lid = child_lid;  // copy a current root LID to prevent CAS from modifing it
    const auto success = root_.compare_exchange_strong(cur_lid, new_lid, std::memory_order_relaxed);
    if (!success) {
      // another thread has already inserted a new root
      new_lid->Clear();
      AddToGC(new_root);
    } else {
      cur_lid = new_lid;
    }

    // prepare a new node stack
    stack.emplace_back(cur_lid);
    stack.emplace_back(child_lid);

    return success;
  }

  [[nodiscard]] auto
  CanMerge(  //
      const Node_t *child,
      const std::vector<LogicalID *> &stack)  //
      -> bool
  {
    const auto &low_key = child->GetLowKey();
    if (!low_key) return false;  // the leftmost node cannot be merged

    // if a parent node has the same lowest-key, the child is the leftmost node in it
    std::vector<LogicalID *> copied_stack{stack};
    copied_stack.pop_back();
    const auto *parent_head = GetHead(*low_key, kClosed, copied_stack);
    return !DC::HasSameLowKeyWith(parent_head, *low_key);
  }

  auto
  TryMerge(  //
      const Delta_t *head,
      const Delta_t *removed_node,
      std::vector<LogicalID *> &stack)  //
      -> bool
  {
    auto *removed_lid = stack.back();

    // insert a remove-node delta to prevent other threads from modifying this node
    const auto *remove_d = new (GetRecPage()) Delta_t{DeltaType::kRemoveNode, removed_node};
    if (!removed_lid->CASStrong(head, remove_d)) {
      // retry from consolidation
      AddToGC(remove_d);
      return false;
    }

    // search a left sibling node
    stack.pop_back();
    const auto &low_key = removed_node->GetKey();
    SearchChildNode(low_key, kClosed, stack);

    // insert a merge delta into the left sibling node
    auto *merge_d = new (GetRecPage()) Delta_t{DeltaType::kMerge, removed_node, removed_lid};
    while (true) {  // continue until insertion succeeds
      const auto *sib_head = GetHead(low_key, kClosed, stack);
      merge_d->SetNext(sib_head);
      if (stack.back()->CASWeak(sib_head, merge_d)) break;
    }

    consol_page_ = nullptr;
    CompleteMerge(merge_d, stack);

    // execute parent consolidation/split if needed
    if (consol_page_ != nullptr) {
      TryConsolidation(consol_page_, low_key, kClosed, stack);
    }

    return true;
  }

  void
  CompleteMerge(  //
      const Delta_t *merge_d,
      std::vector<LogicalID *> &stack)
  {
    if (stack.size() < 2) return;

    auto *child_lid = stack.back();  // keep a current logical ID

    // create an index-delete delta record to complete merging
    auto *delete_d = new (GetRecPage()) Delta_t{merge_d, child_lid};
    const auto &del_key = merge_d->GetKey();

    // insert the delta record into a parent node
    stack.pop_back();  // remove the split child node to modify its parent node
    while (true) {
      // check whether another thread has already completed this merging
      auto [head, rc] = GetHeadWithKeyCheck(del_key, stack);
      if (rc == kKeyNotExist) {
        // another thread has already deleted the merged node
        delete_d->Abort();
        AddToGC(delete_d);
        break;
      }

      // check concurrent splitting
      const auto *cur_lid = stack.back();
      const auto *sib_head = GetHead(del_key, !kClosed, stack);
      if (cur_lid != stack.back()) {
        // the target node was split, so check whether the merged nodes span two parent nodes
        if (DC::HasSameLowKeyWith(sib_head, del_key)) {
          // the merged nodes have different parent nodes, so abort
          AbortMerge(merge_d);
          break;
        }
        continue;  // the merged nodes have the same parent node, so retry
      }

      // try to insert the index-delete delta record
      delete_d->SetNext(head);
      if (stack.back()->CASWeak(head, delete_d)) break;
    }
    stack.emplace_back(child_lid);
  }

  void
  AbortMerge(const Delta_t *merge_d)
  {
    // check this merging is still active
    auto *sib_lid = merge_d->template GetPayload<LogicalID *>();
    const auto *remove_d = sib_lid->template Load<Delta_t *>();
    if (remove_d->GetDeltaType() != kRemoveNode) return;  // merging has been already aborted

    // delete the remove-node delta to allow other threads to modify the node
    sib_lid->CASStrong(remove_d, remove_d->GetNext());
  }

  auto
  TryRemoveRoot(  //
      const Delta_t *head,
      const Node_t *new_root,
      LogicalID *root_lid)  //
      -> bool
  {
    // insert a remove-node delta to prevent other threads from modifying the root node
    const auto *root_d = reinterpret_cast<const Delta_t *>(new_root);
    auto *remove_d = new (GetRecPage()) Delta_t{DeltaType::kRemoveNode, root_d};
    if (!root_lid->CASStrong(head, remove_d)) {
      // retry from consolidation
      AddToGC(remove_d);
      return false;
    }

    // shrink a tree by removing a useless root node
    auto *new_lid = new_root->GetLeftmostChild();
    auto *cur_lid = root_lid;
    if (root_.compare_exchange_strong(cur_lid, new_lid, std::memory_order_relaxed)) {
      // the old root node is removed
      root_lid->Clear();
    } else {
      // removing a root is failed, but consolidation is succeeded
      root_lid->Store(new_root);
      remove_d->Abort();
    }
    AddToGC(remove_d);

    return true;
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// a root node of Bw-tree
  std::atomic<LogicalID *> root_{nullptr};

  /// a mapping table
  MappingTable_t mapping_table_{};

  /// garbage collector
  NodeGC_t gc_{};

  /// a page ID to be consolidated
  inline static thread_local LogicalID *consol_page_{};  // NOLINT
};

}  // namespace dbgroup::index::bw_tree::component

#endif  // BW_TREE_COMPONENT_BW_TREE_INTERNAL_HPP
