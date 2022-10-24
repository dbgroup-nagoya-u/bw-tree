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
#include <future>
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
 * @brief The entity of implementations of Bw-trees.
 *
 * @tparam Payload a class of stored payloads.
 * @tparam Node_t a class of base nodes with variable/fixed-length data.
 * @tparam Delta_t a class of delta records with variable/fixed-length data.
 * @tparam kIsVarLen a flag for indicating this tree deals with variable-length data.
 */
template <class Payload, class Node_t, class Delta_t, bool kIsVarLen>
class BwTree
{
 public:
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using Key = typename Delta_t::Key;
  using Comp = typename Delta_t::Comp;
  using ScanKey = std::optional<std::tuple<const Key &, size_t, bool>>;
  using DeltaType = component::DeltaType;
  using MappingTable_t = MappingTable<Node_t, Delta_t>;
  using DC = DeltaChain<Delta_t>;
  using Record = typename Delta_t::Record;
  using NodeGC_t = ::dbgroup::memory::EpochBasedGC<Node_t, Delta_t>;
  template <class Entry>
  using BulkIter = typename std::vector<Entry>::const_iterator;
  using BulkResult = std::pair<size_t, std::vector<LogicalID *>>;
  using BulkPromise = std::promise<BulkResult>;
  using BulkFuture = std::future<BulkResult>;

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
     * @param node a copied node for scanning results.
     * @param begin_pos the begin position of a current node.
     * @param end_pos the end position of a current node.
     * @param end_key an optional end-point key.
     * @param is_end a flag for indicating a current node is rightmost in scan-range.
     */
    RecordIterator(  //
        BwTree *bw_tree,
        Node_t *node,
        size_t begin_pos,
        size_t end_pos,
        const ScanKey end_key,
        const bool is_end)
        : bw_tree_{bw_tree},
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
    ~RecordIterator() { ::operator delete(node_); }

    /*##################################################################################
     * Public operators for iterators
     *################################################################################*/

    /**
     * @retval true if this iterator indicates a live record.
     * @retval false otherwise.
     */
    explicit operator bool() { return HasRecord(); }

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
    HasRecord()  //
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

    /// the pointer to a node that includes partial scan results.
    Node_t *node_{nullptr};

    /// the number of records in this node.
    size_t record_count_{0};

    /// the position of a current record.
    size_t current_pos_{0};

    /// the end key given from a user.
    ScanKey end_key_{};

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
   * @param gc_thread_num the number of GC threads.
   */
  explicit BwTree(  //
      const size_t gc_interval_microsec,
      const size_t gc_thread_num)
      : gc_{gc_interval_microsec, gc_thread_num, true}
  {
    // create an empty Bw-tree
    auto *root_node = new (GetNodePage()) Node_t{};
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
   * @brief The entity of a function for reading records.
   *
   * @param key a target key.
   * @retval the payload of a given key wrapped with std::optional if it is in this tree.
   * @retval std::nullopt otherwise.
   */
  auto
  Read(const Key &key)  //
      -> std::optional<Payload>
  {
    [[maybe_unused]] const auto &guard = gc_.CreateEpochGuard();

    // check whether the leaf node has a target key
    consol_page_ = nullptr;
    auto &&stack = SearchLeafNode(key, kClosed);

    for (Payload payload{}; true;) {
      // check whether the node is active and has a target key
      const auto *head = LoadValidHead(key, kClosed, stack);

      uintptr_t out_ptr{};
      size_t delta_num = 0;
      auto rc = DC::SearchRecord(head, key, out_ptr, delta_num);
      switch (rc) {
        case DeltaRC::kRecordFound:
          payload = reinterpret_cast<Delta_t *>(out_ptr)->template GetPayload<Payload>();
          break;

        case DeltaRC::kRecordDeleted:
          break;

        case DeltaRC::kKeyIsInSibling:
          // swap a current node in a stack and retry
          stack.back() = reinterpret_cast<LogicalID *>(out_ptr);
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
          if (rc == kRecordFound) {
            payload = node->template GetPayload<Payload>(out_ptr);
          }
          break;
        }
      }

      if (delta_num >= kMaxDeltaRecordNum) {
        consol_page_ = stack.back();
      }

      if (consol_page_ != nullptr) {
        TryConsolidation(consol_page_, stack);
      }

      if (rc == kRecordDeleted) return std::nullopt;
      return payload;
    }
  }

  /**
   * @brief The entity of a function for scanning records.
   *
   * @param begin_key a pair of a begin key and its openness (true=closed).
   * @param end_key a pair of an end key and its openness (true=closed).
   * @return an iterator to access scanned records.
   */
  auto
  Scan(  //
      const ScanKey &begin_key = std::nullopt,
      const ScanKey &end_key = std::nullopt)  //
      -> RecordIterator
  {
    [[maybe_unused]] const auto &guard = gc_.CreateEpochGuard();

    auto *node = new (GetNodePage()) Node_t{};
    size_t begin_pos{};
    if (begin_key) {
      // traverse to a leaf node and sort records for scanning
      const auto &[b_key, b_key_len, b_closed] = *begin_key;
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

    return RecordIterator{this, node, begin_pos, end_pos, end_key, is_end};
  }

  /*####################################################################################
   * Public write APIs
   *##################################################################################*/

  /**
   * @brief The entity of a function for putting records.
   *
   * @param key a target key to be written.
   * @param payload a target payload to be written.
   * @param key_len the length of a target key.
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
    Delta_t *write_d{};
    if constexpr (kIsVarLen) {
      write_d = new (GetRecPage()) Delta_t{DeltaType::kInsert, key, key_len, payload};
    } else {
      write_d = new (GetRecPage()) Delta_t{DeltaType::kInsert, key, payload};
    }
    while (true) {
      // check whether the target node includes incomplete SMOs
      const auto [head, rc] = GetHeadWithKeyCheck(key, stack);

      // try to insert the delta record
      write_d->SetDeltaType((rc == kRecordFound) ? kModify : kInsert);
      write_d->SetNext(head);
      if (stack.back()->CASWeak(head, write_d)) break;
    }

    if (consol_page_ != nullptr) {
      TryConsolidation(consol_page_, stack);
    }

    return kSuccess;
  }

  /**
   * @brief The entity of a function for inserting records.
   *
   * @param key a target key to be inserted.
   * @param payload a target payload to be inserted.
   * @param key_len the length of a target key.
   * @retval.kSuccess if inserted.
   * @retval kKeyExist otherwise.
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
      if (existence == kKeyIsInSibling) {
        stack = SearchLeafNode(key, kClosed);
        continue;
      }
      if (existence == kRecordFound) {
        rc = kKeyExist;
        tls_delta_page_.reset(insert_d);
        break;
      }

      // try to insert the delta record
      insert_d->SetNext(head);
      if (stack.back()->CASWeak(head, insert_d)) break;
    }

    if (consol_page_ != nullptr) {
      TryConsolidation(consol_page_, stack);
    }

    return rc;
  }

  /**
   * @brief The entity of a function for updating records.
   *
   * @param key a target key to be updated.
   * @param payload a payload for updating.
   * @param key_len the length of a target key.
   * @retval kSuccess if updated.
   * @retval kKeyNotExist otherwise.
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
      if (existence == kKeyIsInSibling) {
        stack = SearchLeafNode(key, kClosed);
        continue;
      }
      if (existence == kRecordDeleted) {
        rc = kKeyNotExist;
        tls_delta_page_.reset(modify_d);
        break;
      }

      // try to insert the delta record
      modify_d->SetNext(head);
      if (stack.back()->CASWeak(head, modify_d)) break;
    }

    if (consol_page_ != nullptr) {
      TryConsolidation(consol_page_, stack);
    }

    return rc;
  }

  /**
   * @brief The entity of a function for deleting records.
   *
   * @param key a target key to be deleted.
   * @param key_len the length of a target key.
   * @retval kSuccess if deleted.
   * @retval kKeyNotExist otherwise.
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
      if (existence == kKeyIsInSibling) {
        stack = SearchLeafNode(key, kClosed);
        continue;
      }
      if (existence == kRecordDeleted) {
        rc = kKeyNotExist;
        tls_delta_page_.reset(delete_d);
        break;
      }

      // try to insert the delta record
      delete_d->SetNext(head);
      if (stack.back()->CASWeak(head, delete_d)) break;
    }

    if (consol_page_ != nullptr) {
      TryConsolidation(consol_page_, stack);
    }

    return rc;
  }

  /*####################################################################################
   * Public bulkload API
   *##################################################################################*/

  /**
   * @brief The entity of a function for bulkinserting records.
   *
   * @param entries vector of entries to be bulkloaded.
   * @param thread_num the number of threads to perform bulkloading.
   * @return kSuccess.
   */
  template <class Entry>
  auto
  Bulkload(  //
      const std::vector<Entry> &entries,
      const size_t thread_num)  //
      -> ReturnCode
  {
    assert(thread_num > 0);
    assert(entries.size() >= thread_num);

    if (entries.empty()) return ReturnCode::kSuccess;

    std::vector<LogicalID *> nodes{};
    auto &&iter = entries.cbegin();
    if (thread_num == 1) {
      // bulkloading with a single thread
      nodes = BulkloadWithSingleThread<Entry>(iter, entries.size()).second;
    } else {
      // bulkloading with multi-threads
      std::vector<BulkFuture> futures{};
      futures.reserve(thread_num);

      // a lambda function for bulkloading with multi-threads
      auto loader = [&](BulkPromise p, BulkIter<Entry> iter, size_t n, bool is_rightmost) {
        p.set_value(BulkloadWithSingleThread<Entry>(iter, n, is_rightmost));
      };

      // create threads to construct partial BzTrees
      const auto rec_num = entries.size();
      const auto rightmost_id = thread_num - 1;
      for (size_t i = 0; i < thread_num; ++i) {
        // create a partial BzTree
        BulkPromise p{};
        futures.emplace_back(p.get_future());
        const size_t n = (rec_num + i) / thread_num;
        std::thread{loader, std::move(p), iter, n, i == rightmost_id}.detach();

        // forward the iterator to the next begin position
        iter += n;
      }

      // wait for the worker threads to create partial trees
      std::vector<BulkResult> partial_trees{};
      partial_trees.reserve(thread_num);
      size_t height = 1;
      for (auto &&future : futures) {
        partial_trees.emplace_back(future.get());
        const auto partial_height = partial_trees.back().first;
        height = (partial_height > height) ? partial_height : height;
      }

      // align the height of partial trees
      nodes.reserve(kInnerNodeCap * thread_num);
      const LogicalID *prev_lid = nullptr;
      for (auto &&[p_height, p_nodes] : partial_trees) {
        while (p_height < height) {  // NOLINT
          ConstructUpperLayer(p_nodes);
          ++p_height;
        }
        nodes.insert(nodes.end(), p_nodes.begin(), p_nodes.end());

        // link partial trees
        if (prev_lid != nullptr) {
          Node_t::LinkVerticalBorderNodes(prev_lid, p_nodes.front());
        }
        prev_lid = p_nodes.back();
      }
    }

    // create upper layers until a root node is created
    while (nodes.size() > 1) {
      ConstructUpperLayer(nodes);
    }

    // set a new root
    auto *old_root = root_.exchange(nodes.front(), std::memory_order_release);
    gc_.AddGarbage(old_root->Load<Delta_t *>());
    old_root->Clear();

    return ReturnCode::kSuccess;
  }

 private:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  /// an expected maximum height of a tree.
  static constexpr size_t kExpectedTreeHeight = 8;

  /// Header length in bytes.
  static constexpr size_t kHeaderLength = sizeof(Node_t);

  /// the maximum size of delta records.
  static constexpr size_t kDeltaRecSize = Delta_t::template GetMaxDeltaSize<Payload>();

  /// the expected length of keys for bulkloading.
  static constexpr size_t kBulkKeyLen = (kIsVarLen) ? kWordSize : sizeof(Key);

  /// the expected length of records in leaf nodes for bulkloading.
  static constexpr size_t kLeafRecLen = kBulkKeyLen + sizeof(Payload);

  /// the expected capacity of leaf nodes for bulkloading.
  static constexpr size_t kLeafNodeCap =
      (kPageSize - kHeaderLength - kBulkKeyLen) / (kLeafRecLen + ((kIsVarLen) ? kWordSize : 0));

  /// the expected length of records in internal nodes for bulkloading.
  static constexpr size_t kInnerRecLen = kBulkKeyLen + sizeof(LogicalID *);

  /// the expected capacity of internal nodes for bulkloading.
  static constexpr size_t kInnerNodeCap =
      (kPageSize - kHeaderLength - kBulkKeyLen) / (kInnerRecLen + ((kIsVarLen) ? kWordSize : 0));

  /// a flag for indicating leaf nodes.
  static constexpr bool kIsLeaf = true;

  /// a flag for preventing a consolidate-operation from splitting a node.
  static constexpr bool kIsScan = true;

  /**
   * @brief An internal enum for distinguishing a partial SMO status.
   *
   */
  enum SMOsRC {
    kConsolidate,
    kTrySplit,
    kTryMerge,
    kAlreadyConsolidated,
  };

  /*####################################################################################
   * Internal utility functions
   *##################################################################################*/

  /**
   * @brief Allocate or reuse a memory region for a base node.
   *
   * @param size the desired page size.
   * @returns the reserved memory page.
   */
  [[nodiscard]] auto
  GetNodePage()  //
      -> void *
  {
    if (tls_node_page_) return tls_node_page_.release();

    auto *page = gc_.template GetPageIfPossible<Node_t>();
    return (page == nullptr) ? (::operator new(kPageSize)) : page;
  }

  /**
   * @brief Allocate or reuse a memory region for a delta record.
   *
   * @returns the reserved memory page.
   */
  [[nodiscard]] auto
  GetRecPage()  //
      -> void *
  {
    if (tls_delta_page_) return tls_delta_page_.release();

    auto *page = gc_.template GetPageIfPossible<Delta_t>();
    return (page == nullptr) ? (::operator new(kDeltaRecSize)) : page;
  }

  /**
   * @brief Add a given delta-chain to GC targets.
   *
   * If a given delta-chain has multiple delta records and base nodes, this function
   * adds all of them to GC.
   *
   * @tparam T a templated class for simplicity.
   * @param head the head pointer of a target delta-chain.
   */
  template <class T>
  void
  AddToGC(const T *head)
  {
    static_assert(std::is_same_v<T, Node_t> || std::is_same_v<T, Delta_t>);

    // delete delta records
    const auto *garbage = reinterpret_cast<const Delta_t *>(head);
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

  /**
   * @brief Search a child node of the top node in a given stack.
   *
   * @param key a search key.
   * @param closed a flag for indicating closed/open-interval.
   * @param stack a stack of traversed nodes.
   * @param targe_lid a logical ID for stopping a child search.
   */
  void
  SearchChildNode(  //
      const Key &key,
      const bool closed,
      std::vector<LogicalID *> &stack,
      const LogicalID *target_lid = nullptr) const
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
          stack.back() = reinterpret_cast<LogicalID *>(out_ptr);
          if (target_lid != nullptr && stack.back() == target_lid) return;
          continue;

        case DeltaRC::kNodeRemoved:
          // retry from the parent node
          stack.pop_back();
          SearchChildNode(key, closed, stack, target_lid);
          if (target_lid != nullptr && stack.back() == target_lid) return;
          continue;

        case DeltaRC::kReachBaseNode:
        default: {
          if (delta_num >= kMaxDeltaRecordNum) {
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

  /**
   * @brief Search a leaf node that may have a target key.
   *
   * @param key a search key.
   * @param closed a flag for indicating closed/open-interval.
   * @return a stack of traversed nodes.
   */
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

  /**
   * @brief Search a leftmost leaf node in this tree.
   *
   * @return a stack of traversed nodes.
   */
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

  /**
   * @brief Search a target node to trace a current node path.
   *
   * @param stack a stack of traversed nodes.
   * @param key a search key.
   * @param target_lid the logical ID of a target node.
   */
  void
  SearchTargetNode(  //
      std::vector<LogicalID *> &stack,
      const std::optional<Key> &key,
      const LogicalID *target_lid)
  {
    while (true) {
      auto *cur_lid = LoadValidRoot();
      const auto *cur_node = cur_lid->template Load<Node_t *>();
      if (cur_node->IsLeaf()) continue;
      stack.emplace_back(cur_lid);

      if (key) {
        while (true) {
          if (stack.back() == target_lid) return;
          SearchChildNode(*key, !kClosed, stack, target_lid);
          cur_node = stack.back()->template Load<Node_t *>();
          if (cur_node == nullptr) {
            // the found node is removed, so retry
            stack.pop_back();
            continue;
          }
          if (cur_node->IsLeaf()) break;
        }
        GetHead(*key, !kClosed, stack);  // check sibling nodes
      } else {
        do {
          if (stack.back() == target_lid) return;
          cur_lid = cur_node->GetLeftmostChild();
          cur_node = cur_lid->template Load<Node_t *>();
          stack.emplace_back(cur_lid);
        } while (!cur_node->IsLeaf());
      }

      if (stack.back() == target_lid) return;
      stack.clear();
    }
  }

  /**
   * @return the logical ID of a current root node.
   */
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

  /**
   * @brief Get a valid (i.e., not NULL) head pointer of a logical node.
   *
   * @param key a search key.
   * @param closed a flag for indicating closed/open-interval.
   * @param stack a stack of traversed nodes.
   * @return the head of this logical node.
   */
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

  /**
   * @brief Get the head pointer of a logical node.
   *
   * @param key a search key.
   * @param closed a flag for indicating closed/open-interval.
   * @param stack a stack of traversed nodes.
   * @return the head of this logical node.
   */
  auto
  GetHead(  //
      const Key &key,
      const bool closed,
      std::vector<LogicalID *> &stack)  //
      -> const Delta_t *
  {
    while (true) {
      // check whether the node has partial SMOs
      const auto *head = LoadValidHead(key, closed, stack);

      // check whether the node is active and can include a target key
      uintptr_t out_ptr{};
      size_t delta_num = 0;
      switch (DC::Validate(head, key, closed, out_ptr, delta_num)) {
        case DeltaRC::kKeyIsInSibling:
          // swap a current node in a stack and retry
          stack.back() = reinterpret_cast<LogicalID *>(out_ptr);
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

      if (delta_num >= kMaxDeltaRecordNum) {
        consol_page_ = stack.back();
      }

      return head;
    }
  }

  /**
   * @brief Get the head pointer of a logical node and complete partial SMOs if exist.
   *
   * @param stack a stack of traversed nodes.
   * @retval 1st: the head of this logical node.
   * @retval 2nd: the number of delta records in this node.
   */
  auto
  GetHeadForConsolidation(std::vector<LogicalID *> &stack)  //
      -> std::pair<const Delta_t *, size_t>
  {
    // check whether the node has partial SMOs
    const auto *head = stack.back()->template Load<Delta_t *>();
    if (head == nullptr) return {nullptr, 0};  // abort consolidation

    // check whether the node is active
    uintptr_t out_ptr = 0;
    size_t delta_num = 0;
    switch (DC::CheckPartialSMOs(head, out_ptr, delta_num)) {
      case DeltaRC::kNodeRemoved:
        // abort consolidation
        return {nullptr, 0};

      case DeltaRC::kPartialSplitMayExist: {
        const auto *split_d = reinterpret_cast<Delta_t *>(out_ptr);
        CompleteSplit(split_d, stack);
        break;
      }

      case DeltaRC::kPartialMergeMayExist: {
        const auto *merge_d = reinterpret_cast<Delta_t *>(out_ptr);
        CompleteMerge(merge_d, stack);
        break;
      }

      case DeltaRC::kReachBaseNode:
      default:
        break;  // do nothing
    }

    return {head, delta_num};
  }

  /**
   * @brief Get the head pointer of a logical node and check key existence.
   *
   * @param key a search key.
   * @param stack a stack of traversed nodes.
   * @retval 1st: the head of this logical node.
   * @retval 2nd: key existence.
   */
  auto
  GetHeadWithKeyCheck(  //
      const Key &key,
      std::vector<LogicalID *> &stack)  //
      -> std::pair<const Delta_t *, DeltaRC>
  {
    for (uintptr_t out_ptr{}; true;) {
      // check whether the node has partial SMOs
      const auto *head = LoadValidHead(key, kClosed, stack);

      // check whether the node is active and has a target key
      size_t delta_num = 0;
      auto rc = DC::SearchRecord(head, key, out_ptr, delta_num);
      switch (rc) {
        case DeltaRC::kRecordFound:
        case DeltaRC::kRecordDeleted:
          break;

        case DeltaRC::kKeyIsInSibling:
          // swap a current node in a stack and retry
          stack.back() = reinterpret_cast<LogicalID *>(out_ptr);
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
          break;
      }

      if (delta_num >= kMaxDeltaRecordNum) {
        consol_page_ = stack.back();
      }

      return {head, rc};
    }
  }

  /*####################################################################################
   * Internal scan utilities
   *##################################################################################*/

  /**
   * @brief Perform consolidation for scanning.
   *
   * @param node a node page to store records.
   * @param begin_key a search key.
   * @param closed a flag for indicating closed/open-interval.
   * @param stack a stack of traversed nodes.
   * @return the begin position for scanning.
   */
  auto
  ConsolidateForScan(  //
      Node_t *&node,
      const Key &begin_key,
      const bool closed,
      std::vector<LogicalID *> &stack)  //
      -> size_t
  {
    while (true) {
      const auto *head = GetHead(begin_key, closed, stack);
      if (Consolidate<Payload>(head, node, kIsScan) != kAlreadyConsolidated) break;
      // concurrent consolidations may block scanning
    }

    // check the begin position for scanning
    const auto [rc, pos] = node->SearchRecord(begin_key);

    return (rc == kRecordDeleted || closed) ? pos : pos + 1;
  }

  /**
   * @brief Perform scanning with a given sibling node.
   *
   * @param sib_lid the logical ID of a sibling node.
   * @param node a node page to store records.
   * @param begin_key a begin key (i.e., the highest key of the previous node).
   * @param end_key an optional end key for scanning.
   * @return the next iterator for scanning.
   */
  auto
  SiblingScan(  //
      LogicalID *sib_lid,
      Node_t *node,
      const Key &begin_key,
      const ScanKey &end_key)  //
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

  /**
   * @brief Try consolidation of a given node.
   *
   * This function will perform splitting/merging if needed.
   *
   * @param target_lid the logical ID of a target node.
   * @param stack a stack of traversed nodes.
   */
  void
  TryConsolidation(  //
      LogicalID *target_lid,
      std::vector<LogicalID *> &stack)
  {
    while (true) {
      // remove child nodes from a node stack
      while (!stack.empty() && stack.back() != target_lid) stack.pop_back();
      if (stack.empty()) return;

      // check whether the target node is valid (containing a target key and no incomplete SMOs)
      const auto [head, delta_rec_num] = GetHeadForConsolidation(stack);
      if (head == nullptr || delta_rec_num < kMaxDeltaRecordNum) return;

      // prepare a consolidated node
      auto *new_node = reinterpret_cast<Node_t *>(GetNodePage());
      const auto rc = (head->IsLeaf()) ? Consolidate<Payload>(head, new_node)
                                       : Consolidate<LogicalID *>(head, new_node);
      switch (rc) {
        case kAlreadyConsolidated:
          // other threads have performed consolidation
          return;

        case kTrySplit:
          // switch to splitting
          TrySplit(head, reinterpret_cast<Delta_t *>(new_node), stack);
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

      // if consolidation is failed, keep the allocated page to reuse
      tls_node_page_.reset(new_node);

      // no retry if the number of delta records is sufficiently small
      if (delta_rec_num < 2 * kMaxDeltaRecordNum) return;
    }
  }

  /**
   * @brief Consolidate a given node.
   *
   * @tparam T a class of expected payloads.
   * @param head the head pointer of a terget node.
   * @param consol_node a node page to store consolidated records.
   * @param is_scan a flag to prevent a split-operation.
   * @return the status of a consolidation result.
   */
  template <class T>
  auto
  Consolidate(  //
      const Delta_t *head,
      Node_t *&consol_node,
      const bool is_scan = false)  //
      -> SMOsRC
  {
    constexpr auto kIsLeaf = !std::is_same_v<T, LogicalID *>;

    // sort delta records
    std::vector<Record> records{};
    std::vector<ConsolidateInfo> consol_info{};
    records.reserve(kMaxDeltaRecordNum * 4);
    consol_info.reserve(kMaxDeltaRecordNum);
    const auto [consolidated, diff] = DC::template Sort<T>(head, records, consol_info);
    if (consolidated) return kAlreadyConsolidated;

    // calculate the size of a consolidated node
    size_t size{};
    if constexpr (kIsVarLen) {
      size = kHeaderLength + Node_t::PreConsolidate(consol_info, kIsLeaf) + diff;
    } else {
      const auto rec_num = Node_t::PreConsolidate(consol_info, kIsLeaf) + diff;
      if (kIsLeaf) {
        size = kHeaderLength + rec_num * (sizeof(Key) + sizeof(T));
      } else {
        size = (kHeaderLength - sizeof(Key)) + rec_num * (sizeof(Key) + sizeof(T));
      }
    }

    // check whether splitting is needed
    void *page = consol_node;
    bool do_split = false;
    if (is_scan) {
      // use dynamic page sizes for scanning
      size = (size / kPageSize + 1) * kPageSize;
      if (size > consol_node->GetNodeSize()) {
        ::operator delete(page);
        page = ::operator new(size);
      }
    } else if (size > kPageSize) {
      // perform splitting
      do_split = true;
      size = size / 2 + (kHeaderLength / 2);
      if (size > kPageSize) {
        size += size - kPageSize;
      }
    } else {
      // perform consolidation
      size = kPageSize;
    }

    // consolidate a target node
    consol_node = new (page) Node_t{kIsLeaf, size, do_split};
    if (kIsLeaf) {
      LeafConsolidate<T>(consol_node, consol_info, records);
    } else {
      InternalConsolidate(consol_node, consol_info, records);
    }

    if (do_split) return kTrySplit;
    if (size <= kMinNodeSize) return kTryMerge;
    return kConsolidate;
  }

  /**
   * @brief Consolidate given leaf nodes and delta records.
   *
   * @tparam T a class of expected payloads.
   * @param consol_node a node page to store consolidated records.
   * @param consol_info the set of leaf nodes to be consolidated.
   * @param records insert/modify/delete-delta records.
   */
  template <class T>
  void
  LeafConsolidate(  //
      Node_t *consol_node,
      const std::vector<ConsolidateInfo> &consol_info,
      const std::vector<Record> &records)
  {
    const auto new_rec_num = records.size();

    // copy a lowest key for consolidation or set initial offset for splitting
    auto offset = consol_node->CopyLowKeyOrSetInitialOffset(consol_info.back());

    // perform merge-sort to consolidate a node
    size_t j = 0;
    for (int64_t k = consol_info.size() - 1; k >= 0; --k) {
      const auto *node = reinterpret_cast<const Node_t *>(consol_info[k].node);
      const auto base_rec_num = consol_info[k].rec_num;
      for (size_t i = 0; i < base_rec_num; ++i) {
        // copy new records
        const auto &node_key = node->GetKey(i);
        for (; j < new_rec_num && Node_t::LT(records[j], node_key); ++j) {
          offset = consol_node->template CopyRecordFrom<T>(records[j], offset);
        }

        // check a new record is updated one
        if (j < new_rec_num && Node_t::LE(records[j], node_key)) {
          offset = consol_node->template CopyRecordFrom<T>(records[j], offset);
          ++j;
        } else {
          offset = consol_node->template CopyRecordFrom<T>(node, i, offset);
        }
      }
    }

    // copy remaining new records
    for (; j < new_rec_num; ++j) {
      offset = consol_node->template CopyRecordFrom<T>(records[j], offset);
    }

    // copy a highest key
    consol_node->CopyHighKeyFrom(consol_info.front(), offset);
  }

  /**
   * @brief Consolidate given internal nodes and delta records.
   *
   * @tparam T a class of expected payloads.
   * @param consol_node a node page to store consolidated records.
   * @param consol_info the set of internal nodes to be consolidated.
   * @param records insert/modify/delete-delta records.
   */
  void
  InternalConsolidate(  //
      Node_t *consol_node,
      const std::vector<ConsolidateInfo> &consol_info,
      const std::vector<Record> &records)
  {
    const auto new_rec_num = records.size();

    // copy a lowest key for consolidation or set initial offset for splitting
    auto offset = consol_node->CopyLowKeyOrSetInitialOffset(consol_info.back());

    // perform merge-sort to consolidate a node
    bool payload_is_embedded = false;
    size_t j = 0;
    for (int64_t k = consol_info.size() - 1; k >= 0; --k) {
      const auto *node = reinterpret_cast<const Node_t *>(consol_info[k].node);
      const int64_t end_pos = consol_info[k].rec_num - 1;
      for (int64_t i = 0; i <= end_pos && i >= 0; ++i) {
        // copy a payload of a base node in advance to swap that of a new index entry
        if (!payload_is_embedded) {  // skip a deleted page
          offset = consol_node->template CopyPayloadFrom<LogicalID *>(node, offset, i);
        }

        // get a current key in the base node
        if (i == end_pos) {
          if (k == 0) break;  // the last record does not need a key
          // if nodes are merged, a current key is equivalent with the lowest one in the next node
          node = reinterpret_cast<const Node_t *>(consol_info[k - 1].node);
          i = kCopyLowKey;
        }
        const auto &node_key = (i < 0) ? *(node->GetLowKey()) : node->GetKey(i);

        // insert new index entries
        for (; j < new_rec_num && Node_t::LT(records[j], node_key); ++j) {
          offset = consol_node->CopyIndexEntryFrom(records[j], offset);
        }

        // set a key for the current record
        if (j < new_rec_num && Node_t::LE(records[j], node_key)) {
          // a record is in a base node, but it may be deleted and inserted again
          offset = consol_node->CopyIndexEntryFrom(records[j], offset);
          payload_is_embedded = true;
          ++j;
        } else {
          // copy a key in a base node
          offset = consol_node->CopyKeyFrom(node, offset, i);
          payload_is_embedded = false;
        }
      }
    }

    // copy remaining new records
    for (; j < new_rec_num; ++j) {
      offset = consol_node->CopyIndexEntryFrom(records[j], offset);
    }
    consol_node->SetLastRecordForInternal(offset);

    // copy a highest key
    consol_node->CopyHighKeyFrom(consol_info.front(), offset);
  }

  /**
   * @brief Try splitting a target node.
   *
   * @param head the head pointer of a terget node.
   * @param split_node a split-right node to be inserted to this tree.
   * @param stack a stack of traversed nodes.
   */
  void
  TrySplit(  //
      const Delta_t *head,
      Delta_t *split_node,
      std::vector<LogicalID *> &stack)
  {
    // create a split-delta record
    auto *sib_lid = mapping_table_.GetNewLogicalID();
    sib_lid->Store(split_node);
    auto *split_d = new (GetRecPage()) Delta_t{DeltaType::kSplit, split_node, sib_lid, head};

    // install the delta record for splitting a child node
    if (!stack.back()->CASStrong(head, split_d)) {
      // retry from consolidation
      sib_lid->Clear();
      tls_delta_page_.reset(split_d);
      tls_node_page_.reset(reinterpret_cast<Node_t *>(split_node));
      return;
    }

    // execute parent update and recursive SMOs if needed
    consol_page_ = nullptr;
    CompleteSplit(split_d, stack);
    if (consol_page_ != nullptr) {
      TryConsolidation(consol_page_, stack);
    }
  }

  /**
   * @brief Complete partial splitting by inserting index-entry to this tree.
   *
   * @param split_d a split-delta record.
   * @param stack a copied stack of traversed nodes.
   */
  void
  CompleteSplit(  //
      const Delta_t *split_d,
      std::vector<LogicalID *> stack)
  {
    // check whether this splitting modifies a root node
    if (stack.size() < 2 && RootSplit(reinterpret_cast<const Node_t *>(split_d), stack)) return;

    // create an index-entry delta record to complete split
    auto *entry_d = new (GetRecPage()) Delta_t{split_d};
    const auto &sep_key = split_d->GetKey();

    // insert the delta record into a parent node
    stack.pop_back();  // remove the split child node to modify its parent node
    consol_page_ = nullptr;
    while (true) {
      // check whether another thread has already completed this splitting
      const auto [head, rc] = GetHeadWithKeyCheck(sep_key, stack);
      if (rc == kRecordFound) {
        tls_delta_page_.reset(entry_d);
        break;
      }

      // try to insert the index-entry delta record
      entry_d->SetNext(head);
      if (stack.back()->CASWeak(head, entry_d)) break;
    }
  }

  /**
   * @brief Perform splitting a root node.
   *
   * @param split_d a split-delta record.
   * @param stack a stack of traversed nodes.
   * @retval true if splitting succeeds.
   * @retval false otherwise.
   */
  auto
  RootSplit(  //
      const Node_t *split_d,
      std::vector<LogicalID *> &stack)  //
      -> bool
  {
    // create a new root node
    auto *old_lid = stack.back();
    stack.pop_back();  // remove the current root to push a new one
    auto *new_root = new (GetNodePage()) Node_t{split_d, old_lid};

    // prepare a new logical ID for the new root
    auto *new_lid = mapping_table_.GetNewLogicalID();
    new_lid->Store(new_root);

    // install a new root page
    auto *cur_lid = old_lid;  // copy a current root LID to prevent CAS from modifing it
    if (root_.compare_exchange_strong(cur_lid, new_lid, std::memory_order_relaxed)) return true;

    // another thread has already inserted a new root
    new_lid->Clear();
    tls_node_page_.reset(new_root);
    const auto &low_key = DC::TraverseToGetLowKey(reinterpret_cast<const Delta_t *>(split_d));
    SearchTargetNode(stack, low_key, old_lid);

    return false;
  }

  /**
   * @brief Check a given node can be merged with its left-sibling node.
   *
   * That is, this function checks a given node is not leftmost in its parent node.
   *
   * @param child a node to be merged.
   * @param stack a stack of traversed nodes.
   * @retval true if a target node can be merged with its left-sibling node.
   * @retval false otherwise.
   */
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
    const auto &parent_low_key = DC::TraverseToGetLowKey(parent_head);
    if (!parent_low_key) return true;

    return Comp{}(*parent_low_key, *low_key) || Comp{}(*low_key, *parent_low_key);
  }

  /**
   * @brief
   *
   * @param head the head pointer of a terget node.
   * @param removed_node a removed (i.e., merged) node.
   * @param stack a stack of traversed nodes.
   * @retval true if partial merging succeeds.
   * @retval false otherwise.
   */
  auto
  TryMerge(  //
      const Delta_t *head,
      Delta_t *removed_node,
      std::vector<LogicalID *> &stack)  //
      -> bool
  {
    auto *removed_lid = stack.back();

    // insert a remove-node delta to prevent other threads from modifying this node
    auto *remove_d = new (GetRecPage()) Delta_t{DeltaType::kRemoveNode, removed_node};
    if (!removed_lid->CASStrong(head, remove_d)) {
      // retry from consolidation
      tls_delta_page_.reset(remove_d);
      tls_node_page_.reset(reinterpret_cast<Node_t *>(removed_node));
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

    // execute parent update and recursive SMOs if needed
    consol_page_ = nullptr;
    CompleteMerge(merge_d, stack);
    if (consol_page_ != nullptr) {
      TryConsolidation(consol_page_, stack);
    }

    return true;
  }

  /**
   * @brief Complete partial merging by deleting index-entry from this tree.
   *
   * @param merge_d a merge-delta record.
   * @param stack a copied stack of traversed nodes.
   */
  void
  CompleteMerge(  //
      const Delta_t *merge_d,
      std::vector<LogicalID *> stack)
  {
    if (stack.size() < 2) return;

    // create an index-delete delta record to complete merging
    auto *delete_d = new (GetRecPage()) Delta_t{merge_d, stack.back()};
    const auto &del_key = merge_d->GetKey();

    // insert the delta record into a parent node
    stack.pop_back();  // remove the split child node to modify its parent node
    while (true) {
      // check whether another thread has already completed this merging
      auto [head, rc] = GetHeadWithKeyCheck(del_key, stack);
      if (rc == kRecordDeleted) {
        // another thread has already deleted the merged node
        tls_delta_page_.reset(delete_d);
        break;
      }

      // check concurrent splitting
      const auto *cur_lid = stack.back();
      const auto *sib_head = GetHead(del_key, !kClosed, stack);
      if (cur_lid != stack.back()) {
        // the target node was split, so check whether the merged nodes span two parent nodes
        const auto &low_key = DC::TraverseToGetLowKey(sib_head);
        if (low_key && !Comp{}(*low_key, del_key) && !Comp{}(del_key, *low_key)) {
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
  }

  /**
   * @brief Abort partial merging by deleting a remove-node delta record.
   *
   * @param merge_d a merge-delta record.
   */
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

  /**
   * @brief Remove a root node that has only one child node from this tree.
   *
   * @param head the head pointer of an expected root node.
   * @param new_root a desired root node.
   * @param root_lid the logical ID of an expected root node.
   * @retval true if a root node is removed.
   * @retval false otherwise.
   */
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
      tls_delta_page_.reset(remove_d);
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
   * Internal bulkload utilities
   *##################################################################################*/

  /**
   * @brief Bulkload specified kay/payload pairs with a single thread.
   *
   * Note that this function does not create a root node. The main process must create a
   * root node by using the nodes constructed by this function.
   *
   * @param iter the begin position of target records.
   * @param n the number of entries to be bulkloaded.
   * @param is_rightmost a flag for indicating a constructed tree is rightmost one.
   * @retval 1st: the height of a constructed tree.
   * @retval 2nd: constructed nodes in the top layer.
   */
  template <class Entry>
  auto
  BulkloadWithSingleThread(  //
      BulkIter<Entry> &iter,
      const size_t n,
      const bool is_rightmost = true)  //
      -> BulkResult
  {
    // reserve space for nodes in the leaf layer
    std::vector<LogicalID *> nodes{};
    nodes.reserve((n / kLeafNodeCap) + 1);

    // load records into leaf nodes
    const auto &iter_end = iter + n;
    Node_t *prev_node = nullptr;
    while (iter < iter_end) {
      auto *node = new (GetNodePage()) Node_t{kIsLeaf};
      auto *lid = mapping_table_.GetNewLogicalID();
      lid->Store(node);
      node->template Bulkload<Payload, Entry>(iter, iter_end, prev_node, lid, is_rightmost);
      nodes.emplace_back(lid);
      prev_node = node;
    }

    // construct index layers
    size_t height = 1;
    if (nodes.size() > 1) {
      // continue until the number of internal nodes is sufficiently small
      do {
        ++height;
      } while (ConstructUpperLayer(nodes));
    }

    return {height, std::move(nodes)};
  }

  /**
   * @brief Construct internal nodes based on given child nodes.
   *
   * @param child_nodes child nodes in a lower layer.
   * @retval true if constructed nodes cannot be contained in a single node.
   * @retval false otherwise.
   */
  auto
  ConstructUpperLayer(std::vector<LogicalID *> &child_nodes)  //
      -> bool
  {
    // reserve space for nodes in the upper layer
    std::vector<LogicalID *> nodes{};
    nodes.reserve((child_nodes.size() / kInnerNodeCap) + 1);

    // load child nodes into parent nodes
    auto &&iter = child_nodes.cbegin();
    const auto &iter_end = child_nodes.cend();
    Node_t *prev_node = nullptr;
    while (iter < iter_end) {
      auto *node = new (GetNodePage()) Node_t{!kIsLeaf};
      auto *lid = mapping_table_.GetNewLogicalID();
      lid->Store(node);
      node->Bulkload(iter, iter_end, prev_node, lid);
      nodes.emplace_back(lid);
      prev_node = node;
    }

    child_nodes = std::move(nodes);
    return child_nodes.size() > kInnerNodeCap;
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// a root node of this Bw-tree.
  std::atomic<LogicalID *> root_{nullptr};

  /// a table to map logical IDs with physical pointers.
  MappingTable_t mapping_table_{};

  /// a garbage collector of base nodes and delta records.
  NodeGC_t gc_{};

  /// the logical ID of a node to be consolidated.
  inline static thread_local LogicalID *consol_page_{};  // NOLINT

  /// a thread-local node page to reuse in SMOs
  inline static thread_local std::unique_ptr<Node_t> tls_node_page_{nullptr};  // NOLINT

  /// a thread-local delta-record page to reuse
  inline static thread_local std::unique_ptr<Delta_t> tls_delta_page_{nullptr};  // NOLINT
};

}  // namespace dbgroup::index::bw_tree::component

#endif  // BW_TREE_COMPONENT_BW_TREE_INTERNAL_HPP
