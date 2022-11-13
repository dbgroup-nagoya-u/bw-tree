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
#include <future>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>

// organization libraries
#include "memory/epoch_based_gc.hpp"

// local sources
#include "component/consolidate_info.hpp"
#include "component/delta_chain.hpp"
#include "component/fixlen/delta_record.hpp"
#include "component/fixlen/node.hpp"
#include "component/logical_id.hpp"
#include "component/mapping_table.hpp"
#include "component/varlen/delta_record.hpp"
#include "component/varlen/node.hpp"

namespace dbgroup::index::bw_tree
{
/**
 * @brief A class for representing Bw-trees.
 *
 * @tparam Key a class of stored keys.
 * @tparam Payload a class of stored payloads (only fixed-length data for simplicity).
 * @tparam Comp a class for ordering keys.
 */
template <class Key,
          class Payload,
          class Comp = ::std::less<Key>,
          bool kIsVarLen = IsVarLenData<Key>()>
class BwTree
{
 public:
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using DeltaRC = component::DeltaRC;
  using DeltaType = component::DeltaType;
  using ConsolidateInfo = component::ConsolidateInfo;
  using LogicalID = component::LogicalID;
  using NodeVarLen_t = component::varlen::Node<Key, Comp>;
  using NodeFixLen_t = component::fixlen::Node<Key, Comp>;
  using Node_t = std::conditional_t<kIsVarLen, NodeVarLen_t, NodeFixLen_t>;
  using DeltaVarLen_t = component::varlen::DeltaRecord<Key, Comp>;
  using DeltaFixLen_t = component::fixlen::DeltaRecord<Key, Comp>;
  using Delta_t = std::conditional_t<kIsVarLen, DeltaVarLen_t, DeltaFixLen_t>;
  using Record = typename Delta_t::Record;
  using MappingTable_t = component::MappingTable<Node_t, Delta_t>;
  using DC = component::DeltaChain<Delta_t>;
  using NodeGC_t = ::dbgroup::memory::EpochBasedGC<Node_t, Delta_t>;
  using ScanKey = std::optional<std::tuple<const Key &, size_t, bool>>;

  template <class Entry>
  using BulkIter = typename std::vector<Entry>::const_iterator;
  using NodeEntry = std::tuple<Key, LogicalID *, size_t>;
  using BulkResult = std::pair<size_t, std::vector<NodeEntry>>;
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
          rec_count_{end_pos},
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
        : node_{node}, rec_count_{end_pos}, current_pos_{begin_pos}, is_end_{is_end}
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
      rec_count_ = obj.rec_count_;
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
    ~RecordIterator() { free(node_); }

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
        if (current_pos_ < rec_count_) return true;  // records remain in this node
        if (is_end_) return false;                   // this node is the end of range-scan

        // go to the next sibling node and continue scanning
        const auto &next_key = node_->GetHighKey();
        auto *sib_page = node_->template GetNext<LogicalID *>();
        *this = bw_tree_->SiblingScan(sib_page, node_, next_key, end_key_);

        if constexpr (kIsVarLen && IsVarLenData<Key>()) {
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
    size_t rec_count_{0};

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
      const size_t gc_interval_microsec = kDefaultGCTime,
      const size_t gc_thread_num = kDefaultGCThreadNum)
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
   * @brief Read the payload corresponding to a given key if it exists.
   *
   * @param key a target key.
   * @retval the payload of a given key wrapped with std::optional if it is in this tree.
   * @retval std::nullopt otherwise.
   */
  auto
  Read(  //
      const Key &key,
      [[maybe_unused]] const size_t key_len = sizeof(Key))  //
      -> std::optional<Payload>
  {
    [[maybe_unused]] const auto &guard = gc_.CreateEpochGuard();

    // check whether the leaf node has a target key
    consol_lid_ = nullptr;
    auto &&stack = SearchLeafNode(key, kClosed);

    for (Payload payload{}; true;) {
      // check whether the node is active and has a target key
      const auto *head = stack.back()->template Load<Delta_t *>();

      uintptr_t out_ptr{};
      auto rc = DC::SearchRecord(head, key, out_ptr);
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
          if (rc == DeltaRC::kRecordFound) {
            payload = node->template GetPayload<Payload>(out_ptr);
          }
          break;
        }
      }

      if (rc == DeltaRC::kRecordDeleted) return std::nullopt;
      return payload;
    }
  }

  /**
   * @brief Perform a range scan with given keys.
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
        if (head->GetDeltaType() == DeltaType::kRemoveNode) continue;
        TryConsolidate(head, node, kIsScan);
        break;
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
   * @brief Write (i.e., put) a given key/payload pair.
   *
   * If a given key does not exist in this tree, this function performs an insert
   * operation. If a given key has been already inserted, this function perfroms an
   * update operation. Thus, this function always returns kSuccess as a return code.
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
      const size_t key_len = sizeof(Key))  //
      -> ReturnCode
  {
    [[maybe_unused]] const auto &guard = gc_.CreateEpochGuard();

    // traverse to a target leaf node
    consol_lid_ = nullptr;
    auto &&stack = SearchLeafNode(key, kClosed);

    // insert a delta record
    auto *write_d = new (GetRecPage()) Delta_t{DeltaType::kInsert, key, key_len, payload};
    while (true) {
      // check whether the target node includes incomplete SMOs
      const auto [head, rc] = GetHeadWithKeyCheck(key, stack);

      // try to insert the delta record
      const auto type = (rc == DeltaRC::kRecordFound) ? DeltaType::kModify : DeltaType::kInsert;
      write_d->SetDeltaType(type);
      write_d->SetNext(head);
      if (stack.back()->CASWeak(head, write_d)) break;
    }

    if (consol_lid_ != nullptr) {
      TrySMOs(consol_lid_, stack);
    }

    return kSuccess;
  }

  /**
   * @brief Insert a given key/payload pair.
   *
   * This function performs a uniqueness check in its processing. If a given key does
   * not exist in this tree, this function inserts a target payload to this tree. If
   * there is a given key in this tree, this function does nothing and returns kKeyExist
   * as a return code.
   *
   * @param key a target key to be inserted.
   * @param payload a target payload to be inserted.
   * @param key_len the length of a target key.
   * @retval.kSuccess if inserted.
   * @retval kKeyExist otherwise.
   */
  auto
  Insert(  //
      const Key &key,
      const Payload &payload,
      const size_t key_len = sizeof(Key))  //
  {
    [[maybe_unused]] const auto &guard = gc_.CreateEpochGuard();

    // traverse to a target leaf node
    consol_lid_ = nullptr;
    auto &&stack = SearchLeafNode(key, kClosed);

    // insert a delta record
    auto *insert_d = new (GetRecPage()) Delta_t{DeltaType::kInsert, key, key_len, payload};
    auto rc = kSuccess;
    while (true) {
      // check target record's existence and get a head pointer
      const auto [head, existence] = GetHeadWithKeyCheck(key, stack);
      if (existence == DeltaRC::kRecordFound) {
        rc = kKeyExist;
        tls_delta_page_.reset(insert_d);
        break;
      }

      // try to insert the delta record
      insert_d->SetNext(head);
      if (stack.back()->CASWeak(head, insert_d)) break;
    }

    if (consol_lid_ != nullptr) {
      TrySMOs(consol_lid_, stack);
    }

    return rc;
  }

  /**
   * @brief Update the record corresponding to a given key with a given payload.
   *
   * This function performs a uniqueness check in its processing. If there is a given
   * key in this tree, this function updates the corresponding record. If a given key
   * does not exist in this tree, this function does nothing and returns kKeyNotExist as
   * a return code.
   *
   * @param key a target key to be updated.
   * @param payload a payload for updating.
   * @param key_len the length of a target key.
   * @retval kSuccess if updated.
   * @retval kKeyNotExist otherwise.
   */
  auto
  Update(  //
      const Key &key,
      const Payload &payload,
      const size_t key_len = sizeof(Key))  //
  {
    [[maybe_unused]] const auto &guard = gc_.CreateEpochGuard();

    // traverse to a target leaf node
    consol_lid_ = nullptr;
    auto &&stack = SearchLeafNode(key, kClosed);

    // insert a delta record
    auto *modify_d = new (GetRecPage()) Delta_t{DeltaType::kModify, key, key_len, payload};
    auto rc = kSuccess;
    while (true) {
      // check target record's existence and get a head pointer
      const auto [head, existence] = GetHeadWithKeyCheck(key, stack);
      if (existence == DeltaRC::kRecordDeleted) {
        rc = kKeyNotExist;
        tls_delta_page_.reset(modify_d);
        break;
      }

      // try to insert the delta record
      modify_d->SetNext(head);
      if (stack.back()->CASWeak(head, modify_d)) break;
    }

    if (consol_lid_ != nullptr) {
      TrySMOs(consol_lid_, stack);
    }

    return rc;
  }

  /**
   * @brief Delete the record corresponding to a given key from this tree.
   *
   * This function performs a uniqueness check in its processing. If there is a given
   * key in this tree, this function deletes it. If a given key does not exist in this
   * tree, this function does nothing and returns kKeyNotExist as a return code.
   *
   * @param key a target key to be deleted.
   * @param key_len the length of a target key.
   * @retval kSuccess if deleted.
   * @retval kKeyNotExist otherwise.
   */
  auto
  Delete(const Key &key,
         const size_t key_len = sizeof(Key))  //
      -> ReturnCode
  {
    [[maybe_unused]] const auto &guard = gc_.CreateEpochGuard();

    // traverse to a target leaf node
    consol_lid_ = nullptr;
    auto &&stack = SearchLeafNode(key, kClosed);

    // insert a delta record
    auto *delete_d = new (GetRecPage()) Delta_t{key, key_len};
    auto rc = kSuccess;
    while (true) {
      // check target record's existence and get a head pointer
      auto [head, existence] = GetHeadWithKeyCheck(key, stack);
      if (existence == DeltaRC::kRecordDeleted) {
        rc = kKeyNotExist;
        tls_delta_page_.reset(delete_d);
        break;
      }

      // try to insert the delta record
      delete_d->SetNext(head);
      if (stack.back()->CASWeak(head, delete_d)) break;
    }

    if (consol_lid_ != nullptr) {
      TrySMOs(consol_lid_, stack);
    }

    return rc;
  }

  /*####################################################################################
   * Public bulkload API
   *##################################################################################*/

  /**
   * @brief Bulkload specified kay/payload pairs.
   *
   * This function bulkloads given entries into this index. The entries are assumed to
   * be given as a vector of pairs of Key and Payload (or key/payload/key-length for
   * variable-length keys). Note that keys in records are assumed to be unique and
   * sorted.
   *
   * @param entries vector of entries to be bulkloaded.
   * @param thread_num the number of threads to perform bulkloading.
   * @return kSuccess.
   */
  template <class Entry>
  auto
  Bulkload(  //
      const std::vector<Entry> &entries,
      const size_t thread_num = 1)  //
      -> ReturnCode
  {
    if (entries.empty()) return ReturnCode::kSuccess;

    std::vector<NodeEntry> nodes{};
    auto &&iter = entries.cbegin();
    const auto rec_num = entries.size();
    if (thread_num <= 1 || rec_num < thread_num) {
      // bulkloading with a single thread
      nodes = BulkloadWithSingleThread<Entry>(iter, rec_num).second;
    } else {
      // bulkloading with multi-threads
      std::vector<BulkFuture> futures{};
      futures.reserve(thread_num);

      // a lambda function for bulkloading with multi-threads
      auto loader = [&](BulkPromise p, BulkIter<Entry> iter, size_t n) {
        p.set_value(BulkloadWithSingleThread<Entry>(iter, n));
      };

      // create threads to construct partial BzTrees
      for (size_t i = 0; i < thread_num; ++i) {
        // create a partial BzTree
        BulkPromise p{};
        futures.emplace_back(p.get_future());
        const size_t n = (rec_num + i) / thread_num;
        std::thread{loader, std::move(p), iter, n}.detach();

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
          p_nodes = ConstructSingleLayer<NodeEntry>(p_nodes.cbegin(), p_nodes.size());
          ++p_height;
        }
        nodes.insert(nodes.end(), p_nodes.begin(), p_nodes.end());

        // link partial trees
        Node_t::LinkVerticalBorderNodes(prev_lid, std::get<1>(p_nodes.front()));
        prev_lid = std::get<1>(p_nodes.back());
      }
    }

    // create upper layers until a root node is created
    while (nodes.size() > 1) {
      nodes = ConstructSingleLayer<NodeEntry>(nodes.cbegin(), nodes.size());
    }
    auto *new_root = std::get<1>(nodes.front());
    Node_t::RemoveLeftmostKeys(new_root);

    // set a new root
    auto *old_root = root_.exchange(new_root, std::memory_order_release);
    gc_.AddGarbage(old_root->template Load<Delta_t *>());
    old_root->Clear();

    return ReturnCode::kSuccess;
  }

 private:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  /// an expected maximum height of a tree.
  static constexpr size_t kExpectedTreeHeight = 8;

  /// the maximum length of keys.
  static constexpr size_t kMaxKeyLen = (IsVarLenData<Key>()) ? kMaxVarDataSize : sizeof(Key);

  /// the length of payloads.
  static constexpr size_t kPayLen = sizeof(Payload);

  /// the length of child pointers.
  static constexpr size_t kPtrLen = sizeof(LogicalID *);

  /// the length of record metadata.
  static constexpr size_t kMetaLen = (kIsVarLen) ? sizeof(component::varlen::Metadata) : 0;

  /// Header length in bytes.
  static constexpr size_t kHeaderLen = sizeof(Node_t);

  /// the maximum size of delta records.
  static constexpr size_t kDeltaRecSize = Delta_t::template GetMaxDeltaSize<Payload>();

  /// the expected length of keys for bulkloading.
  static constexpr size_t kBulkKeyLen = sizeof(Key);

  /// the expected length of records in leaf nodes for bulkloading.
  static constexpr size_t kLeafRecLen = kBulkKeyLen + kPayLen;

  /// the expected capacity of leaf nodes for bulkloading.
  static constexpr size_t kLeafNodeCap =
      (kPageSize - kHeaderLen - kBulkKeyLen) / (kLeafRecLen + kMetaLen);

  /// the expected length of records in internal nodes for bulkloading.
  static constexpr size_t kInnerRecLen = kBulkKeyLen + kPtrLen;

  /// the expected capacity of internal nodes for bulkloading.
  static constexpr size_t kInnerNodeCap =
      (kPageSize - kHeaderLen - kBulkKeyLen) / (kInnerRecLen + kMetaLen);

  /// a flag for preventing a consolidate-operation from splitting a node.
  static constexpr bool kIsScan = true;

  /// a flag for indicating leaf nodes.
  static constexpr bool kIsLeaf = false;

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
    return (page == nullptr) ? (aligned_alloc(kCacheLineSize, kPageSize)) : page;
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
    return (page == nullptr) ? (aligned_alloc(kCacheLineSize, kDeltaRecSize)) : page;
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
    while (garbage->GetDeltaType() != DeltaType::kNotDelta) {
      // register this delta record with GC
      gc_.AddGarbage(garbage);

      // if the delta record is merge-delta, delete the merged sibling node
      if (garbage->GetDeltaType() == DeltaType::kMerge) {
        auto *removed_node = garbage->template GetPayload<Node_t *>();
        gc_.AddGarbage(removed_node);
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
      stack.emplace_back(root_.load(std::memory_order_relaxed));
      return;
    }

    for (uintptr_t out_ptr{}; true;) {
      const auto *head = stack.back()->template Load<Delta_t *>();
      switch (DC::SearchChildNode(head, key, closed, out_ptr)) {
        case DeltaRC::kRecordFound:
          stack.emplace_back(reinterpret_cast<LogicalID *>(out_ptr));
          break;

        case DeltaRC::kKeyIsInSibling:
          // swap a current node in a stack and retry
          stack.back() = reinterpret_cast<LogicalID *>(out_ptr);
          continue;

        case DeltaRC::kNodeRemoved:
          // retry from the parent node
          stack.pop_back();
          SearchChildNode(key, closed, stack, target_lid);
          continue;

        case DeltaRC::kReachBaseNode:
        default: {
          // search a child node in a base node
          const auto *node = reinterpret_cast<Node_t *>(out_ptr);
          stack.emplace_back(node->SearchChild(key, closed));
          break;
        }
      }

      if (head->GetRecordCount() >= kMaxDeltaRecordNum) {
        consol_lid_ = stack.back();
      }
      return;
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
    std::vector<LogicalID *> stack{root_.load(std::memory_order_relaxed)};
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
      auto *cur_lid = root_.load(std::memory_order_relaxed);
      const auto *cur_node = cur_lid->template Load<Node_t *>();
      stack.emplace_back(cur_lid);

      if (key) {
        while (!cur_node->IsLeaf() && stack.back() != target_lid) {
          SearchChildNode(*key, kClosed, stack, target_lid);
          cur_node = stack.back()->template Load<Node_t *>();
        }
      } else {
        while (!cur_node->IsLeaf() && stack.back() != target_lid) {
          cur_lid = cur_node->GetLeftmostChild();
          cur_node = cur_lid->template Load<Node_t *>();
          stack.emplace_back(cur_lid);
        }
      }

      if (stack.back() == target_lid && stack.size() > 1) return;
      stack.clear();
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
    for (uintptr_t out_ptr{}; true;) {
      // check whether the node is active and can include a target key
      const auto *head = stack.back()->template Load<Delta_t *>();
      switch (DC::Validate(head, key, closed, out_ptr)) {
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

      if (head->GetRecordCount() >= kMaxDeltaRecordNum) {
        consol_lid_ = stack.back();
      }

      return head;
    }
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
      // check whether the node is active and has a target key
      const auto *head = stack.back()->template Load<Delta_t *>();
      auto rc = DC::SearchRecord(head, key, out_ptr);
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
        default: {
          // search a target key in the base node
          rc = reinterpret_cast<Node_t *>(out_ptr)->SearchRecord(key).first;
          break;
        }
      }

      if (head->GetRecordCount() >= kMaxDeltaRecordNum) {
        consol_lid_ = stack.back();
      }

      return {head, rc};
    }
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
  GetHeadForMerge(  //
      const Key &key,
      const std::optional<Key> &sib_key,
      std::vector<LogicalID *> &stack)  //
      -> std::pair<const Delta_t *, DeltaRC>
  {
    for (uintptr_t out_ptr{}; true;) {
      // check whether the node is active and has a target key
      const auto *head = stack.back()->template Load<Delta_t *>();
      auto key_found = false;
      auto sib_key_found = !sib_key;
      auto rc = DC::SearchForMerge(head, key, sib_key, out_ptr, key_found, sib_key_found);
      switch (rc) {
        case DeltaRC::kRecordFound:
        case DeltaRC::kAbortMerge:
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
          const auto *node = reinterpret_cast<Node_t *>(out_ptr);
          if (!key_found) {
            if (node->SearchRecord(key).first == DeltaRC::kRecordFound) {
              key_found = true;
            }
          }
          if (!sib_key_found) {
            if (node->SearchRecord(*sib_key).first != DeltaRC::kRecordFound) {
              rc = DeltaRC::kAbortMerge;
              break;
            }
            sib_key_found = true;
          }
          rc = (key_found && sib_key_found) ? DeltaRC::kRecordFound : DeltaRC::kAbortMerge;
          break;
        }
      }

      if (head->GetRecordCount() >= kMaxDeltaRecordNum) {
        consol_lid_ = stack.back();
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
      if (head->GetDeltaType() == DeltaType::kRemoveNode) continue;
      TryConsolidate(head, node, kIsScan);
      break;
    }

    // check the begin position for scanning
    const auto [rc, pos] = node->SearchRecord(begin_key);

    return (rc == DeltaRC::kRecordDeleted || closed) ? pos : pos + 1;
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
    const auto begin_pos = ConsolidateForScan(node, begin_key, kClosed, stack);

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
  TrySMOs(  //
      LogicalID *target_lid,
      std::vector<LogicalID *> &stack)
  {
    while (true) {
      // remove child nodes from a node stack
      while (!stack.empty() && stack.back() != target_lid) stack.pop_back();
      if (stack.empty()) return;

      // prepare a consolidated node
      const auto *head = stack.back()->template Load<Delta_t *>();
      if (!head->NeedConsolidation()) return;
      auto *new_node = reinterpret_cast<Node_t *>(GetNodePage());
      switch (TryConsolidate(head, new_node)) {
        case kTrySplit:
          if (TrySplit(head, new_node, stack)) return;
          break;  // retry from consolidation

        case kTryMerge:
          if (TryMerge(head, new_node, stack)) return;
          break;  // retry from consolidation

        case kConsolidate:
        default:
          // install a consolidated node
          if (target_lid->CASStrong(head, new_node)) {
            // delete consolidated delta records and a base node
            AddToGC(head);
            return;
          }
          break;  // retry from consolidation
      }

      // if consolidation is failed, keep the allocated page to reuse
      tls_node_page_.reset(new_node);
      if (head->GetRecordCount() < kMaxDeltaRecordNum * 4) return;
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
  auto
  TryConsolidate(  //
      const Delta_t *head,
      Node_t *&consol_node,
      const bool is_scan = false)  //
      -> SMOsRC
  {
    constexpr auto kSepKeyLen = (kIsVarLen) ? kMaxKeyLen : 0;
    constexpr auto kMaxBlockSize = kPageSize - kHeaderLen - 2 * kSepKeyLen;
    size_t size{};

    // sort delta records
    thread_local std::vector<Record> records(kMaxDeltaRecordNum * 4);
    thread_local std::vector<ConsolidateInfo> consol_info(kMaxDeltaRecordNum);
    records.clear();
    consol_info.clear();
    const auto diff = DC::Sort(head, records, consol_info);

    // calculate the size of a consolidated node
    if constexpr (kIsVarLen) {
      size = Node_t::PreConsolidate(consol_info) + diff;
    } else {
      const auto rec_len = sizeof(Key) + (head->IsLeaf() ? kPayLen : kPtrLen);
      size = (Node_t::PreConsolidate(consol_info) + diff) * rec_len;
    }

    // check whether splitting is needed
    void *page = consol_node;
    bool do_split = false;
    auto page_size = kPageSize;
    if (is_scan) {
      // use dynamic page sizes for scanning
      page_size = ((size + kHeaderLen + 2 * kSepKeyLen) / kPageSize + 1) * kPageSize;
      if (page_size > consol_node->GetNodeSize()) {
        free(page);
        page = aligned_alloc(kCacheLineSize, page_size);
      }
    } else if (size > kMaxBlockSize) {
      // perform splitting
      do_split = true;
      if (auto sep_size = size / 2; sep_size > kMaxBlockSize) {
        page_size = size - kMaxBlockSize;
      } else {
        page_size = sep_size;
      }
    } else {
      // perform consolidation
      page_size = kPageSize;
    }

    // consolidate a target node
    if (head->IsLeaf()) {
      consol_node = new (page) Node_t{kIsLeaf, page_size, is_scan};
      Consolidate<Payload>(consol_node, consol_info, records, do_split, page_size);
    } else {
      consol_node = new (page) Node_t{!kIsLeaf, page_size, is_scan};
      Consolidate<LogicalID *>(consol_node, consol_info, records, do_split, page_size);
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
  Consolidate(  //
      Node_t *consol_node,
      const std::vector<ConsolidateInfo> &consol_info,
      const std::vector<Record> &records,
      const bool do_split,
      const size_t size)
  {
    const auto new_rec_num = records.size();

    // perform merge-sort to consolidate a node
    int64_t offset = (do_split) ? -size : size;
    size_t j = 0;
    for (int64_t k = consol_info.size() - 1; k >= 0; --k) {
      const auto *node = reinterpret_cast<const Node_t *>(consol_info[k].node);
      const auto base_rec_num = consol_info[k].rec_num;
      size_t i = 0;
      if (!node->IsLeaf() && node->IsLeftmost()) {
        offset = consol_node->template CopyRecordFrom<T>(node, i++, offset);
      }
      for (; i < base_rec_num; ++i) {
        // copy new records
        const auto &node_key = node->GetKey(i);
        for (; j < new_rec_num && Node_t::LT(records[j], node_key); ++j) {
          offset = consol_node->template CopyRecordFrom<T>(records[j], offset);
        }

        // check a new record is updated one
        if (j < new_rec_num && Node_t::LE(records[j], node_key)) {
          offset = consol_node->template CopyRecordFrom<T>(records[j++], offset);
        } else {
          offset = consol_node->template CopyRecordFrom<T>(node, i, offset);
        }
      }
    }

    // copy remaining new records
    for (; j < new_rec_num; ++j) {
      offset = consol_node->template CopyRecordFrom<T>(records[j], offset);
    }

    // copy the lowest/highest keys
    offset = consol_node->CopyLowKeyFrom(consol_info.back(), offset, do_split);
    consol_node->CopyHighKeyFrom(consol_info.front(), offset);
  }

  /**
   * @brief Try splitting a target node.
   *
   * @param head the head pointer of a terget node.
   * @param split_node a split-right node to be inserted to this tree.
   * @param stack a stack of traversed nodes.
   */
  auto
  TrySplit(  //
      const Delta_t *head,
      Node_t *split_node,
      std::vector<LogicalID *> &stack)  //
      -> bool
  {
    const auto *split_node_d = reinterpret_cast<Delta_t *>(split_node);

    // create a split-delta record
    auto *sib_lid = mapping_table_.GetNewLogicalID();
    sib_lid->Store(split_node);
    auto *split_d = new (GetRecPage()) Delta_t{DeltaType::kSplit, split_node_d, sib_lid};
    split_d->SetNext(head);

    // install the delta record for splitting a child node
    if (!stack.back()->CASStrong(head, split_d)) {
      // retry from consolidation
      sib_lid->Clear();
      tls_delta_page_.reset(split_d);
      return false;
    }

    // execute parent update and recursive SMOs if needed
    consol_lid_ = nullptr;
    CompleteSplit(split_d, stack);
    if (consol_lid_ != nullptr) {
      TrySMOs(consol_lid_, stack);
    }

    return true;
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
      std::vector<LogicalID *> &stack)
  {
    // check whether this splitting modifies a root node
    if (stack.size() < 2 && RootSplit(split_d, stack)) return;

    // create an index-entry delta record to complete split
    auto *entry_d = new (GetRecPage()) Delta_t{split_d};
    const auto &sep_key = split_d->GetKey();

    // insert the delta record into a parent node
    stack.pop_back();  // remove the split child node to modify its parent node
    while (true) {
      // try to insert the index-entry delta record
      const auto *head = GetHead(sep_key, kClosed, stack);
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
      const Delta_t *split_d,
      std::vector<LogicalID *> &stack)  //
      -> bool
  {
    const auto *split_delta_n = reinterpret_cast<const Node_t *>(split_d);

    // create a new root node
    auto *old_lid = stack.back();
    stack.pop_back();  // remove the current root to push a new one
    auto *new_root = new (GetNodePage()) Node_t{split_delta_n, old_lid};

    // prepare a new logical ID for the new root
    auto *new_lid = mapping_table_.GetNewLogicalID();
    new_lid->Store(new_root);

    // install a new root page
    auto *cur_lid = old_lid;  // copy a current root LID to prevent CAS from modifing it
    if (root_.compare_exchange_strong(cur_lid, new_lid, std::memory_order_relaxed)) return true;

    // another thread has already inserted a new root
    new_lid->Clear();
    tls_node_page_.reset(new_root);
    const auto &low_key = DC::TraverseToGetLowKey(split_d);
    SearchTargetNode(stack, low_key, old_lid);

    return false;
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
      Node_t *removed_node,
      std::vector<LogicalID *> &stack)  //
      -> bool
  {
    auto *removed_node_d = reinterpret_cast<Delta_t *>(removed_node);

    // insert a remove-node delta to prevent other threads from modifying this node
    auto *remove_d = new (GetRecPage()) Delta_t{removed_node->IsLeaf()};
    auto *removed_lid = stack.back();
    if (!removed_lid->CASStrong(head, remove_d)) {
      // retry from consolidation
      tls_delta_page_.reset(remove_d);
      return false;
    }
    AddToGC(head);

    // remove the index entry before merging
    consol_lid_ = nullptr;
    const auto &low_key = removed_node->GetLowKey();
    auto *delete_d = TryDeleteIndexEntry(removed_node_d, low_key, stack);
    if (delete_d == nullptr) {
      // check this tree should be shrinked
      if (TryRemoveRoot(removed_node, removed_lid, stack)) return true;

      // merging has failed, but consolidation succeeds
      removed_lid->Store(removed_node);
      AddToGC(remove_d);
    } else {
      // search a left sibling node
      const auto &sep_key = *low_key;
      SearchChildNode(sep_key, kOpen, stack);

      // insert a merge delta into the left sibling node
      auto *merge_d = new (GetRecPage()) Delta_t{DeltaType::kMerge, removed_node_d, removed_node};
      while (true) {  // continue until insertion succeeds
        const auto *sib_head = GetHead(sep_key, kOpen, stack);
        merge_d->SetNext(sib_head);
        if (stack.back()->CASWeak(sib_head, merge_d)) break;
      }
      delete_d->SetSiblingLID(stack.back());
    }

    // execute recursive SMOs if needed
    if (consol_lid_ != nullptr) {
      TrySMOs(consol_lid_, stack);
    }
    return true;
  }

  /**
   * @brief Complete partial merging by deleting index-entry from this tree.
   *
   * @param merge_d a merge-delta record.
   * @param stack a copied stack of traversed nodes.
   */
  auto
  TryDeleteIndexEntry(  //
      const Delta_t *removed_node,
      const std::optional<Key> &low_key,
      std::vector<LogicalID *> &stack)  //
      -> Delta_t *
  {
    // check a current node can be merged
    stack.pop_back();
    if (stack.empty()) return nullptr;  // a root node cannot be merged
    if (!low_key) return nullptr;       // the leftmost nodes cannot be merged

    // insert the delta record into a parent node
    auto *delete_d = new (GetRecPage()) Delta_t{removed_node, nullptr};
    const auto &key = *low_key;
    const auto &sib_key = removed_node->GetHighKey();
    while (true) {
      // check the removed node is not leftmost in its parent node
      auto [head, rc] = GetHeadForMerge(key, sib_key, stack);
      if (rc == DeltaRC::kAbortMerge) {
        // the leftmost nodes cannot be merged
        tls_delta_page_.reset(delete_d);
        return nullptr;
      }

      // try to insert the index-delete delta record
      delete_d->SetNext(head);
      if (stack.back()->CASWeak(head, delete_d)) return delete_d;
    }
  }

  auto
  TryRemoveRoot(  //
      const Node_t *root,
      LogicalID *old_lid,
      std::vector<LogicalID *> &stack)  //
      -> bool
  {
    // check a given node can be shrinked
    if (!stack.empty() || root->GetRecordCount() > 1 || root->IsLeaf()) return false;

    // shrink the tree by removing a useless root node
    auto *new_lid = root->GetLeftmostChild();
    if (!root_.compare_exchange_strong(old_lid, new_lid, std::memory_order_relaxed)) return false;
    AddToGC(root);
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
   * @retval 1st: the height of a constructed tree.
   * @retval 2nd: constructed nodes in the top layer.
   */
  template <class Entry>
  auto
  BulkloadWithSingleThread(  //
      BulkIter<Entry> iter,
      const size_t n)  //
      -> BulkResult
  {
    // construct a data layer (leaf nodes)
    auto &&nodes = ConstructSingleLayer<Entry>(iter, n);

    // construct index layers (inner nodes)
    size_t height = 1;
    for (auto n = nodes.size(); n > kInnerNodeCap; n = nodes.size(), ++height) {
      // continue until the number of inner nodes is sufficiently small
      nodes = ConstructSingleLayer<NodeEntry>(nodes.cbegin(), n);
    }

    return {height, std::move(nodes)};
  }

  /**
   * @brief Construct nodes based on given entries.
   *
   * @param iter the begin position of target records.
   * @param n the number of entries to be bulkloaded.
   * @return constructed nodes.
   */
  template <class Entry>
  auto
  ConstructSingleLayer(  //
      BulkIter<Entry> iter,
      const size_t n)  //
      -> std::vector<NodeEntry>
  {
    using T = std::tuple_element_t<1, Entry>;
    constexpr auto kIsInner = std::is_same_v<T, LogicalID *>;

    // reserve space for nodes in the upper layer
    std::vector<NodeEntry> nodes{};
    nodes.reserve((n / (kIsInner ? kInnerNodeCap : kLeafNodeCap)) + 1);

    // load child nodes into parent nodes
    const auto &iter_end = iter + n;
    for (Node_t *prev_node = nullptr; iter < iter_end;) {
      auto *node = new (GetNodePage()) Node_t{kIsInner};
      auto *lid = mapping_table_.GetNewLogicalID();
      lid->Store(node);
      node->template Bulkload<Entry>(iter, iter_end, prev_node, lid, nodes);
      prev_node = node;
    }

    return nodes;
  }

  /*####################################################################################
   * Static assertions
   *##################################################################################*/

  /**
   * @retval true if a target key class is trivially copyable.
   * @retval false otherwise.
   */
  [[nodiscard]] static constexpr auto
  KeyIsTriviallyCopyable()  //
      -> bool
  {
    if constexpr (IsVarLenData<Key>()) {
      // check a base type is trivially copyable
      return std::is_trivially_copyable_v<std::remove_pointer_t<Key>>;
    } else {
      // check a given key type is trivially copyable
      return std::is_trivially_copyable_v<Key>;
    }
  }

  // cannot use optimized page layouts with variable-length data
  static_assert(kIsVarLen || !IsVarLenData<Key>());

  // target keys must be trivially copyable.
  static_assert(KeyIsTriviallyCopyable());

  // target payloads must be trivially copyable.
  static_assert(std::is_trivially_copyable_v<Payload>);

  // node pages have sufficient capacity for records.
  static_assert(kMaxKeyLen + kPayLen <= kPageSize / 4);

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
  inline static thread_local LogicalID *consol_lid_{};  // NOLINT

  /// a thread-local node page to reuse in SMOs
  inline static thread_local std::unique_ptr<void, std::function<void(void *)>>  //
      tls_node_page_{nullptr, std::function<void(void *)>(free)};                // NOLINT

  /// a thread-local delta-record page to reuse
  inline static thread_local std::unique_ptr<void, std::function<void(void *)>>  //
      tls_delta_page_{nullptr, std::function<void(void *)>(free)};               // NOLINT
};

/*######################################################################################
 * Aliases for convenience
 *####################################################################################*/

/// a Bw-tree with a general node layout.
template <class Key, class Payload, class Comp = std::less<Key>>
using BwTreeVarLen = BwTree<Key, Payload, Comp, !kOptimizeForFixLenData>;

/// a Bw-tree with an optimized node layout for fixed-length data.
template <class Key, class Payload, class Comp = std::less<Key>>
using BwTreeFixLen = BwTree<Key, Payload, Comp, kOptimizeForFixLenData>;

}  // namespace dbgroup::index::bw_tree

#endif  // BW_TREE_BW_TREE_HPP
