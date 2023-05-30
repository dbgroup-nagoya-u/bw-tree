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

// C++ standard libraries
#include <functional>
#include <future>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>

// external sources
#include "memory/epoch_based_gc.hpp"

// local sources
#include "bw_tree/component/delta_chain.hpp"
#include "bw_tree/component/fixlen/delta_record.hpp"
#include "bw_tree/component/fixlen/node.hpp"
#include "bw_tree/component/logical_id.hpp"
#include "bw_tree/component/mapping_table.hpp"
#include "bw_tree/component/varlen/delta_record.hpp"
#include "bw_tree/component/varlen/node.hpp"

namespace dbgroup::index::bw_tree
{
/**
 * @brief A class for representing Bw-trees.
 *
 * @tparam Key a class of stored keys.
 * @tparam Payload a class of stored payloads (only fixed-length data for simplicity).
 * @tparam Comp a class for ordering keys.
 * @tparam kIsVarLen a flag for using a general node layout.
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

  using NodePage = component::NodePage;
  using DeltaPage = component::DeltaPage;
  using DeltaRC = component::DeltaRC;
  using DeltaType = component::DeltaType;
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
  using ConsolidateInfo = std::pair<const void *, const void *>;
  using NodeGC_t = ::dbgroup::memory::EpochBasedGC<NodePage, DeltaPage>;
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
    ~RecordIterator() = default;

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
   * @param gc_interval_microsec GC internal [us] (default: 10ms).
   * @param gc_thread_num the number of GC threads (default: 1).
   */
  explicit BwTree(  //
      const size_t gc_interval_microsec = kDefaultGCTime,
      const size_t gc_thread_num = kDefaultGCThreadNum)
      : gc_{gc_interval_microsec, gc_thread_num}
  {
    // create an empty Bw-tree
    auto *root_node = new (GetNodePage()) Node_t{};
    auto *root_lid = mapping_table_.GetNewLogicalID();
    root_lid->Store(root_node);
    root_.store(root_lid, std::memory_order_relaxed);

    gc_.StartGC();
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
   * @param key_len the length of the target key.
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

        case DeltaRC::kRecordNotFound:
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

      if (rc == DeltaRC::kRecordNotFound) return std::nullopt;
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
    thread_local std::unique_ptr<void, std::function<void(void *)>>  //
        page{::operator new(2 * kPageSize, component::kCacheAlignVal),
             std::function<void(void *)>{component::DeleteAlignedPtr}};

    auto *node = new (page.get()) Node_t{};
    size_t begin_pos{};
    if (begin_key) {
      // traverse to a leaf node and sort records for scanning
      const auto &[b_key, b_key_len, b_closed] = *begin_key;
      auto &&stack = SearchLeafNode(b_key, b_closed);
      begin_pos = ConsolidateForScan(node, b_key, b_closed, stack);
    } else {
      Node_t *dummy_node = nullptr;
      // traverse to the leftmost leaf node directly
      auto &&stack = SearchLeftmostLeaf();
      while (true) {
        const auto *head = stack.back()->template Load<Delta_t *>();
        if (head->GetDeltaType() == DeltaType::kRemoveNode) continue;
        TryConsolidate(head, node, dummy_node, kIsScan);
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
   * This function always overwrites a payload and can be optimized for that purpose;
   * the procedure may omit the key uniqueness check.
   *
   * @param key a target key.
   * @param payload a target payload.
   * @param key_len the length of the target key.
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
    auto &&stack = SearchLeafNode(key, kClosed);

    // insert a delta record
    const auto rec_len = key_len + kPayLen + kMetaLen;
    auto *write_d = new (GetRecPage()) Delta_t{DeltaType::kInsert, key, key_len, payload};
    while (true) {
      // check whether the target node includes incomplete SMOs
      const auto [head, rc] = GetHeadWithKeyCheck(key, stack);
      if (rc == DeltaRC::kRecordFound) {
        write_d->SetDeltaType(DeltaType::kModify);
        write_d->SetNext(head, 0);
      } else {
        write_d->SetDeltaType(DeltaType::kInsert);
        write_d->SetNext(head, rec_len);
      }

      // try to insert the delta record
      if (stack.back()->CASWeak(head, write_d)) break;
    }

    if (write_d->NeedConsolidation()) {
      TrySMOs(write_d, stack);
    }

    return kSuccess;
  }

  /**
   * @brief Insert a given key/payload pair.
   *
   * This function performs a uniqueness check on its processing. If the given key does
   * not exist in this tree, this function inserts a target payload into this tree. If
   * the given key exists in this tree, this function does nothing and returns kKeyExist.
   *
   * @param key a target key.
   * @param payload a target payload.
   * @param key_len the length of the target key.
   * @retval kSuccess if inserted.
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
    auto &&stack = SearchLeafNode(key, kClosed);

    // insert a delta record
    const auto rec_len = key_len + kPayLen + kMetaLen;
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
      insert_d->SetNext(head, rec_len);
      if (stack.back()->CASWeak(head, insert_d)) {
        if (insert_d->NeedConsolidation()) {
          TrySMOs(insert_d, stack);
        }
        break;
      }
    }

    return rc;
  }

  /**
   * @brief Update the record corresponding to a given key with a given payload.
   *
   * This function performs a uniqueness check on its processing. If the given key
   * exists in this tree, this function updates the corresponding payload. If the given
   * key does not exist in this tree, this function does nothing and returns
   * kKeyNotExist.
   *
   * @param key a target key.
   * @param payload a target payload.
   * @param key_len the length of the target key.
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
    auto &&stack = SearchLeafNode(key, kClosed);

    // insert a delta record
    auto *modify_d = new (GetRecPage()) Delta_t{DeltaType::kModify, key, key_len, payload};
    auto rc = kSuccess;
    while (true) {
      // check target record's existence and get a head pointer
      const auto [head, existence] = GetHeadWithKeyCheck(key, stack);
      if (existence == DeltaRC::kRecordNotFound) {
        rc = kKeyNotExist;
        tls_delta_page_.reset(modify_d);
        break;
      }

      // try to insert the delta record
      modify_d->SetNext(head, 0);
      if (stack.back()->CASWeak(head, modify_d)) {
        if (modify_d->NeedConsolidation()) {
          TrySMOs(modify_d, stack);
        }
        break;
      }
    }

    return rc;
  }

  /**
   * @brief Delete the record corresponding to a given key from this tree.
   *
   * This function performs a uniqueness check on its processing. If the given key
   * exists in this tree, this function deletes it. If the given key does not exist in
   * this tree, this function does nothing and returns kKeyNotExist.
   *
   * @param key a target key.
   * @param key_len the length of the target key.
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
    auto &&stack = SearchLeafNode(key, kClosed);

    // insert a delta record
    const auto rec_len = key_len + kPayLen + kMetaLen;
    auto *delete_d = new (GetRecPage()) Delta_t{key, key_len};
    auto rc = kSuccess;
    while (true) {
      // check target record's existence and get a head pointer
      auto [head, existence] = GetHeadWithKeyCheck(key, stack);
      if (existence == DeltaRC::kRecordNotFound) {
        rc = kKeyNotExist;
        tls_delta_page_.reset(delete_d);
        break;
      }

      // try to insert the delta record
      delete_d->SetNext(head, -rec_len);
      if (stack.back()->CASWeak(head, delete_d)) {
        if (delete_d->NeedConsolidation()) {
          TrySMOs(delete_d, stack);
        }
        break;
      }
    }

    return rc;
  }

  /*####################################################################################
   * Public bulkload API
   *##################################################################################*/

  /**
   * @brief Bulkload specified kay/payload pairs.
   *
   * This function loads the given entries into this index, assuming that the entries
   * are given as a vector of key/payload pairs (or the tuples key/payload/key-length
   * for variable-length keys). Note that keys in records are assumed to be unique and
   * sorted.
   *
   * @tparam Entry a container of a key/payload pair.
   * @param entries the vector of entries to be bulkloaded.
   * @param thread_num the number of threads used for bulk loading.
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
    gc_.AddGarbage<NodePage>(old_root->template Load<Delta_t *>());
    old_root->Clear();

    return ReturnCode::kSuccess;
  }

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  /**
   * @brief Collect statistical data of this tree.
   *
   * @retval 1st: the number of nodes.
   * @retval 2nd: the actual usage in bytes.
   * @retval 3rd: the virtual usage (i.e., reserved memory) in bytes.
   */
  auto
  CollectStatisticalData()  //
      -> std::vector<std::tuple<size_t, size_t, size_t>>
  {
    std::vector<std::tuple<size_t, size_t, size_t>> stat_data{};
    auto *lid = root_.load(std::memory_order_acquire);

    CollectStatisticalData(lid, 0, stat_data);
    stat_data.emplace_back(mapping_table_.CollectStatisticalData());

    return stat_data;
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

  static constexpr auto kIsSMO = true;

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
   * @returns the reserved memory page.
   */
  [[nodiscard]] auto
  GetNodePage()  //
      -> void *
  {
    auto *page = gc_.template GetPageIfPossible<NodePage>();
    return (page == nullptr) ? (::operator new(kPageSize, component::kCacheAlignVal)) : page;
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

    auto *page = gc_.template GetPageIfPossible<DeltaPage>();
    return (page == nullptr) ? (::operator new(kDeltaRecSize, component::kCacheAlignVal)) : page;
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
      gc_.AddGarbage<DeltaPage>(garbage);

      // if the delta record is merge-delta, delete the merged sibling node
      if (garbage->GetDeltaType() == DeltaType::kMerge) {
        auto *removed_node = garbage->template GetPayload<Node_t *>();
        gc_.AddGarbage<NodePage>(removed_node);
      }

      // check the next delta record or base node
      garbage = garbage->GetNext();
      if (garbage == nullptr) return;
    }

    // register a base node with GC
    gc_.AddGarbage<NodePage>(reinterpret_cast<const Node_t *>(garbage));
  }

  /**
   * @brief Collect statistical data recursively.
   *
   * @param lid a target node.
   * @param level the current level in the tree.
   * @param stat_data an output statistical data.
   */
  void
  CollectStatisticalData(  //
      const LogicalID *lid,
      const size_t level,
      std::vector<std::tuple<size_t, size_t, size_t>> &stat_data)
  {
    // add an element for a new level
    if (stat_data.size() <= level) {
      stat_data.emplace_back(0, 0, 0);
    }

    // get the head of the current logical ID
    const auto *head = LoadValidHead(lid);
    while (head->GetDeltaType() == DeltaType::kRemoveNode) {
      head = LoadValidHead(lid);
    }

    // add statistical data of this node
    auto &[node_num, actual_usage, virtual_usage] = stat_data.at(level);
    const auto [node_size, delta_num] = head->GetNodeUsage();
    const auto delta_size = delta_num * kDeltaRecSize;
    ++node_num;
    actual_usage += node_size + delta_size;
    virtual_usage += kPageSize + delta_size;

    // collect data recursively
    if (!head->IsLeaf()) {
      // consolidate the node to traverse child nodes
      auto *page = ::operator new(2 * kPageSize, component::kCacheAlignVal);
      auto *consolidated = new (page) Node_t{!kIsLeaf};
      Node_t *dummy_node = nullptr;
      TryConsolidate(head, consolidated, dummy_node, kIsScan);

      for (size_t i = 0; i < consolidated->GetRecordCount(); ++i) {
        const auto *child = consolidated->template GetPayload<LogicalID *>(i);
        CollectStatisticalData(child, level + 1, stat_data);
      }

      ::operator delete(consolidated, component::kCacheAlignVal);
    }
  }

  /**
   * @brief Search a child node of the top node in a given stack.
   *
   * @param key a search key.
   * @param closed a flag for indicating closed/open-interval.
   * @param stack a stack of traversed nodes.
   * @param target_lid an optional node to prevent this function from searching a child.
   * @retval true if the search for the target node is successful.
   * @retval false otherwise.
   */
  auto
  SearchChildNode(  //
      const Key &key,
      const bool closed,
      std::vector<LogicalID *> &stack,
      const LogicalID *target_lid = nullptr) const  //
      -> bool
  {
    for (uintptr_t out_ptr{}; true;) {
      const auto *head = stack.back()->template Load<Delta_t *>();
      switch (DC::SearchChildNode(head, key, closed, out_ptr)) {
        case DeltaRC::kRecordFound:
          stack.emplace_back(reinterpret_cast<LogicalID *>(out_ptr));
          break;

        case DeltaRC::kKeyIsInSibling: {
          // swap a current node in a stack and retry
          auto *sib_lid = reinterpret_cast<LogicalID *>(out_ptr);
          if (sib_lid == target_lid) return true;
          stack.back() = sib_lid;
          continue;
        }

        case DeltaRC::kNodeRemoved:
          // retry from the parent node
          stack.pop_back();
          if (stack.empty()) {
            if (target_lid != nullptr) return false;
            stack.emplace_back(root_.load(std::memory_order_relaxed));
          } else {
            if (SearchChildNode(key, closed, stack, target_lid)) return true;
            if (stack.empty()) return false;  // the tree structure has modified
          }
          continue;

        case DeltaRC::kReachBaseNode:
        default: {
          // search a child node in a base node
          const auto *node = reinterpret_cast<Node_t *>(out_ptr);
          stack.emplace_back(node->SearchChild(key, closed));
          break;
        }
      }

      return false;
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
    stack.emplace_back(root_.load(std::memory_order_relaxed));

    // traverse a Bw-tree
    while (true) {
      const auto *node = stack.back()->template Load<Node_t *>();
      if (node->IsLeaf()) return stack;
      SearchChildNode(key, closed, stack);
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
    std::vector<LogicalID *> stack{};
    stack.reserve(kExpectedTreeHeight);
    stack.emplace_back(root_.load(std::memory_order_relaxed));

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
      const Key &key,
      const LogicalID *target_lid)
  {
    while (true) {
      auto *cur_lid = root_.load(std::memory_order_relaxed);
      const auto *cur_node = cur_lid->template Load<Node_t *>();
      stack.emplace_back(cur_lid);

      while (!cur_node->IsLeaf()) {
        if (SearchChildNode(key, kClosed, stack, target_lid)) return;
        if (stack.empty()) break;
        cur_node = stack.back()->template Load<Node_t *>();
      }

      if (stack.empty()) continue;
      return;
    }
  }

  /**
   * @brief Load a head of a delta chain in a given logical node.
   *
   * This function waits for other threads if the given logical node in SMOs.
   *
   * @param lid a logical node ID.
   * @return a head of a delta chain.
   */
  auto
  LoadValidHead(const LogicalID *lid)  //
      -> const Delta_t *
  {
    while (true) {
      for (size_t i = 1; true; ++i) {
        const auto *head = lid->template Load<Delta_t *>();
        if (!head->NeedWaitSMOs()) return head;
        if (i >= kRetryNum) break;
        BW_TREE_SPINLOCK_HINT
      }
      std::this_thread::sleep_for(kShortSleep);
    }
  }

  /**
   * @brief Get the head pointer of a logical node.
   *
   * @param key a search key.
   * @param closed a flag for indicating closed/open-interval.
   * @param stack a stack of traversed nodes.
   * @param target_lid an optional node to prevent this function from searching a head.
   * @return the head of this logical node.
   */
  auto
  GetHead(  //
      const Key &key,
      const bool closed,
      std::vector<LogicalID *> &stack,
      const LogicalID *target_lid = nullptr)  //
      -> const Delta_t *
  {
    for (uintptr_t out_ptr{}; true;) {
      // check whether the node is active and can include a target key
      const auto *head = LoadValidHead(stack.back());
      switch (DC::Validate(head, key, closed, out_ptr)) {
        case DeltaRC::kKeyIsInSibling: {
          // swap a current node in a stack and retry
          auto *sib_lid = reinterpret_cast<LogicalID *>(out_ptr);
          if (sib_lid == target_lid) return head;
          stack.back() = sib_lid;
          continue;
        }

        case DeltaRC::kNodeRemoved:
          // retry from the parent node
          stack.pop_back();
          if (stack.empty()) {
            if (target_lid != nullptr) return nullptr;
            stack = SearchLeafNode(key, closed);
          } else {
            SearchChildNode(key, closed, stack, target_lid);
            if (stack.empty()) return nullptr;
          }
          continue;

        case DeltaRC::kReachBaseNode:
        default:
          break;  // do nothing
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
      const auto *head = LoadValidHead(stack.back());
      auto rc = DC::SearchRecord(head, key, out_ptr);
      switch (rc) {
        case DeltaRC::kRecordFound:
        case DeltaRC::kRecordNotFound:
          break;

        case DeltaRC::kKeyIsInSibling:
          // swap a current node in a stack and retry
          stack.back() = reinterpret_cast<LogicalID *>(out_ptr);
          continue;

        case DeltaRC::kNodeRemoved:
          // retry from the parent node
          stack.pop_back();
          if (stack.empty()) {  // the tree structure has modified
            stack = SearchLeafNode(key, kClosed);
          } else {
            SearchChildNode(key, kClosed, stack);
          }
          continue;

        case DeltaRC::kReachBaseNode:
        default: {
          // search a target key in the base node
          rc = reinterpret_cast<Node_t *>(out_ptr)->SearchRecord(key).first;
          break;
        }
      }

      return {head, rc};
    }
  }

  /**
   * @brief Get the head pointer of a logical node and check keys existence.
   *
   * @param key a search key.
   * @param sib_key a separator key of a right-sibling node.
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
      const auto *head = LoadValidHead(stack.back());
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
          rc = DeltaRC::kAbortMerge;
          break;

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
    Node_t *dummy_node = nullptr;

    while (true) {
      const auto *head = GetHead(begin_key, closed, stack);
      if (head->GetDeltaType() == DeltaType::kRemoveNode) continue;
      TryConsolidate(head, node, dummy_node, kIsScan);
      break;
    }

    // check the begin position for scanning
    const auto [rc, pos] = node->SearchRecord(begin_key);

    return (rc == DeltaRC::kRecordNotFound || closed) ? pos : pos + 1;
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
   * @param head a head delta record of a target delta chain.
   * @param stack a stack of traversed nodes.
   */
  void
  TrySMOs(  //
      Delta_t *head,
      std::vector<LogicalID *> &stack)
  {
    thread_local std::unique_ptr<void, std::function<void(void *)>>  //
        tls_node{nullptr, std::function<void(void *)>{component::DeleteAlignedPtr}};
    Node_t *r_node = nullptr;

    // recheck other threads have modifed this delta chain
    if (head != stack.back()->template Load<Delta_t *>()) return;

    // prepare a consolidated node
    auto *new_node = reinterpret_cast<Node_t *>((tls_node) ? tls_node.release() : GetNodePage());
    switch (TryConsolidate(head, new_node, r_node)) {
      case kTrySplit:
        // we use fixed-length pages, and so splitting a node must succeed
        Split(new_node, r_node, stack);
        break;

      case kTryMerge:
        if (!TryMerge(head, new_node, stack)) {
          tls_node.reset(new_node);
          return;
        }
        break;

      case kConsolidate:
      default:
        // install a consolidated node
        if (!stack.back()->CASStrong(head, new_node)) {
          tls_node.reset(new_node);
          return;
        }
        break;
    }
    AddToGC(head);
  }

  /**
   * @brief Consolidate a given node.
   *
   * @param head the head pointer of a terget node.
   * @param new_node a node page to store consolidated records.
   * @param r_node a node page to store split-right records.
   * @param is_scan a flag to prevent a split-operation.
   * @return the status of a consolidation result.
   */
  auto
  TryConsolidate(  //
      const Delta_t *head,
      Node_t *new_node,
      Node_t *&r_node,
      const bool is_scan = false)  //
      -> SMOsRC
  {
    thread_local std::vector<Record> records{};
    thread_local std::vector<const void *> nodes{};
    records.reserve(kMaxDeltaRecordNum);
    nodes.reserve(kDeltaRecordThreshold);
    records.clear();
    nodes.clear();

    // sort delta records
    DC::Sort(head, records, nodes);

    // check whether splitting is needed
    const auto node_size = head->GetNodeSize();
    const auto do_split = !is_scan && node_size > kPageSize;

    // consolidate a target node
    const auto is_inner = !(head->IsLeaf());
    new (new_node) Node_t{is_inner};
    if (do_split) {
      r_node = new (GetNodePage()) Node_t{is_inner};
    }
    if (is_inner) {
      Consolidate<LogicalID *>(new_node, r_node, nodes, records, is_scan);
    } else {
      Consolidate<Payload>(new_node, r_node, nodes, records, is_scan);
    }

    if (do_split) return kTrySplit;
    if (node_size <= kMinNodeSize) return kTryMerge;
    return kConsolidate;
  }

  /**
   * @brief Consolidate given leaf nodes and delta records.
   *
   * @tparam T a class of expected payloads.
   * @param new_node a node page to store consolidated records.
   * @param r_node a node page to store split-right records.
   * @param nodes the set of leaf nodes to be consolidated.
   * @param records insert/modify/delete-delta records.
   * @param is_scan a flag to prevent a split-operation.
   */
  template <class T>
  void
  Consolidate(  //
      Node_t *new_node,
      Node_t *r_node,
      const std::vector<const void *> &nodes,
      const std::vector<Record> &records,
      const bool is_scan)
  {
    constexpr auto kIsSplitLeft = true;
    const auto new_rec_num = records.size();
    auto *l_node = (r_node != nullptr) ? new_node : nullptr;

    // perform merge-sort to consolidate a node
    size_t offset = kPageSize * (is_scan ? 2 : 1);
    size_t j = 0;
    for (int64_t k = nodes.size() - 1; k >= 0; --k) {
      const auto *node = reinterpret_cast<const Node_t *>(nodes[k]);
      const auto node_rec_num = node->GetRecordCount();

      // check a null key for inner nodes
      size_t i = 0;
      if (!node->IsLeaf() && node->IsLeftmost()) {
        offset = Node_t::template CopyRecordFrom<T>(new_node, node, i++, offset, r_node);
      }
      for (; i < node_rec_num; ++i) {
        // copy new records
        const auto &node_key = node->GetKey(i);
        for (; j < new_rec_num && Node_t::LT(records[j], node_key); ++j) {
          offset = Node_t::template CopyRecordFrom<T>(new_node, records[j], offset, r_node);
        }

        // check a new record is updated one
        if (j < new_rec_num && Node_t::LE(records[j], node_key)) {
          offset = Node_t::template CopyRecordFrom<T>(new_node, records[j++], offset, r_node);
        } else {
          offset = Node_t::template CopyRecordFrom<T>(new_node, node, i, offset, r_node);
        }
      }
    }

    // copy remaining new records
    for (; j < new_rec_num; ++j) {
      offset = Node_t::template CopyRecordFrom<T>(new_node, records[j], offset, r_node);
    }

    // copy the lowest/highest keys
    if (l_node == nullptr) {
      // consolidated node
      offset = new_node->CopyLowKeyFrom(nodes.back());
      new_node->CopyHighKeyFrom(nodes.front(), offset);
    } else {
      // split nodes
      offset = l_node->CopyLowKeyFrom(nodes.back());
      l_node->CopyHighKeyFrom(new_node, offset, kIsSplitLeft);
      offset = new_node->CopyLowKeyFrom(new_node);
      new_node->CopyHighKeyFrom(nodes.front(), offset);
    }

    if (is_scan) {
      new_node->SetNodeSizeForScan();
    }
  }

  /**
   * @brief Try splitting a target node.
   *
   * @param l_node a split-left node to be updated.
   * @param r_node a split-right node to be inserted to this tree.
   * @param stack a stack of traversed nodes.
   */
  void
  Split(  //
      Node_t *l_node,
      const Node_t *r_node,
      std::vector<LogicalID *> &stack)
  {
    // install the split nodes
    auto *r_lid = mapping_table_.GetNewLogicalID();
    r_lid->Store(r_node);
    l_node->SetNext(r_lid);
    auto *l_lid = stack.back();
    l_lid->Store(l_node);
    stack.pop_back();  // remove the split child node to modify its parent node

    // create an index-entry delta record to complete split
    const auto *r_node_d = reinterpret_cast<const Delta_t *>(r_node);
    auto *entry_d = new (GetRecPage()) Delta_t{DeltaType::kInsert, r_node_d, r_lid};
    const auto &key = r_node_d->GetKey();
    const auto rec_len = r_node_d->GetKeyLength() + kPtrLen + kMetaLen;

    while (true) {
      // check the current node is a root node
      if (stack.empty()) {
        if (TryRootSplit(entry_d, l_lid)) {
          tls_delta_page_.reset(entry_d);
          return;
        }
        SearchTargetNode(stack, key, r_lid);
        stack.pop_back();  // remove the split node
        continue;
      }

      // insert the delta record into a parent node
      while (true) {
        const auto *head = GetHead(key, kClosed, stack, r_lid);
        if (head == nullptr) break;  // the tree structure has modified, so retry

        // try to insert the index-entry delta record
        entry_d->SetNext(head, rec_len);
        if (stack.back()->CASWeak(head, entry_d)) {
          if (entry_d->NeedConsolidation()) {
            TrySMOs(entry_d, stack);
          }
          return;
        }
      }
    }
  }

  /**
   * @brief Perform splitting a root node.
   *
   * @param entry_d a insert-entry delta record.
   * @param old_lid a logical node ID of an old root node.
   * @retval true if splitting succeeds.
   * @retval false otherwise.
   */
  auto
  TryRootSplit(  //
      const Delta_t *entry_d,
      const LogicalID *old_lid)  //
      -> bool
  {
    if (root_.load(std::memory_order_relaxed) != old_lid) return false;

    // create a new root node
    const auto *entry_delta_n = reinterpret_cast<const Node_t *>(entry_d);
    auto *new_root = new (GetNodePage()) Node_t{entry_delta_n, old_lid};
    auto *new_lid = mapping_table_.GetNewLogicalID();
    new_lid->Store(new_root);

    // install a new root page
    root_.store(new_lid, std::memory_order_relaxed);
    return true;
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
      tls_delta_page_.reset(remove_d);
      return false;
    }
    stack.pop_back();  // remove the child node

    // remove the index entry before merging
    const auto &low_key = removed_node->GetLowKey();
    auto *delete_d = TryDeleteIndexEntry(removed_node_d, low_key, stack);
    if (delete_d == nullptr) {
      // check this tree should be shrinked
      if (!TryRemoveRoot(removed_node, removed_lid, stack)) {
        // merging has failed, but consolidation succeeds
        removed_lid->Store(removed_node);
        AddToGC(remove_d);
      }
      return true;
    }

    // insert a merge delta into the left sibling node
    auto *merge_d = new (GetRecPage()) Delta_t{DeltaType::kMerge, removed_node_d, removed_node};
    const auto diff = removed_node->GetNodeDiff();
    const auto &sep_key = *low_key;
    while (true) {
      if (stack.empty()) {
        // concurrent SMOs have modified the tree structure, so reconstruct a stack
        SearchTargetNode(stack, sep_key, removed_lid);
      } else {
        SearchChildNode(sep_key, kOpen, stack, removed_lid);
        if (stack.empty()) continue;
      }

      while (true) {  // continue until insertion succeeds
        const auto *sib_head = GetHead(sep_key, kOpen, stack, removed_lid);
        if (sib_head == nullptr) break;  // retry from searching the left sibling node

        // try to insert the merge-delta record
        merge_d->SetNext(sib_head, diff);
        if (stack.back()->CASWeak(sib_head, merge_d)) {
          delete_d->SetSiblingLID(stack.back());  // set a shortcut
          if (merge_d->NeedConsolidation()) {
            TrySMOs(merge_d, stack);
          }
          return true;
        }
      }
    }
  }

  /**
   * @brief Complete partial merging by deleting index-entry from this tree.
   *
   * @param removed_node a consolidated node to be removed.
   * @param low_key a lowest key of a removed node.
   * @param stack a copied stack of traversed nodes.
   * @retval the delete-delta record if successful.
   * @retval nullptr otherwise.
   */
  auto
  TryDeleteIndexEntry(  //
      const Delta_t *removed_node,
      const std::optional<Key> &low_key,
      std::vector<LogicalID *> stack)  //
      -> Delta_t *
  {
    // check a current node can be merged
    if (stack.empty()) return nullptr;  // a root node cannot be merged
    if (!low_key) return nullptr;       // the leftmost nodes cannot be merged

    // insert the delta record into a parent node
    auto *delete_d = new (GetRecPage()) Delta_t{removed_node};
    const auto rec_len = delete_d->GetKeyLength() + kPtrLen + kMetaLen;
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
      delete_d->SetNext(head, -rec_len);
      if (stack.back()->CASWeak(head, delete_d)) break;
    }

    if (delete_d->NeedConsolidation()) {
      TrySMOs(delete_d, stack);
    }
    return delete_d;
  }

  /**
   * @brief Remove a root node and shrink a tree.
   *
   * @param root an old root node to be removed.
   * @param old_lid a logical node ID of an old root node.
   * @param stack a stack of ancestor nodes.
   * @retval true if a root node is removed.
   * @return false otherwise.
   */
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
    if (new_lid->template Load<Node_t *>()->IsLeaf()
        || !root_.compare_exchange_strong(old_lid, new_lid, std::memory_order_relaxed)) {
      return false;
    }
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
   * @tparam Entry a container of a key/payload pair.
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
   * @tparam Entry a container of a key/payload pair.
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

  /// a thread-local delta-record page to reuse
  inline static thread_local std::unique_ptr<void, std::function<void(void *)>>            //
      tls_delta_page_{nullptr, std::function<void(void *)>{component::DeleteAlignedPtr}};  // NOLINT
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
