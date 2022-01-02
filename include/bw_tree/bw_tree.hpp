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
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new BwTree object.
   *
   * @param gc_interval_microsec GC internal [us]
   */
  explicit BwTree(  //
      const size_t gc_interval_microsec = 100000,
      const size_t gc_thread_num = 1)
      : root_{nullptr}, mapping_table_{}, gc_{gc_interval_microsec, gc_thread_num, true}
  {
    // create an empty Bw-tree
    auto *leaf = new (GetNodePage(kPageSize)) Node_t{};
    auto *root_page = mapping_table_.GetNewLogicalID();
    root_page->store(reinterpret_cast<uintptr_t>(leaf), std::memory_order_release);
    root_.store(root_page, std::memory_order_relaxed);
  }

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

    // check whether the leaf node has a target key
    consol_page_ = nullptr;
    auto &&stack = SearchLeafNode(key, kClosed);
    auto [ptr, rc] = CheckExistence(key, stack);

    // if a long delta-chain is found, consolidate it
    if (consol_page_ != nullptr) {
      TryConsolidation(consol_page_, key, kClosed, stack);
    }

    switch (rc) {
      case NodeRC::kKeyNotExist:
        return std::nullopt;

      case NodeRC::kKeyInDelta:
        auto *delta = reinterpret_cast<Delta_t *>(ptr);
        return std::make_optional(delta->CopyPayload());

      case NodeRC::kKeyExist:
      default:
        auto *node = reinterpret_cast<Node_t *>(ptr);
        return std::make_optional(node->CopyPayload(rc));
    }
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
    const auto &guard = gc_.CreateEpochGuard();

    // traverse to a target leaf node
    consol_page_ = nullptr;
    auto &&stack = SearchLeafNode(key, kClosed);

    // insert a delta record
    auto *rec = new (GetRecPage()) Delta_t{DeltaType::kInsert, key, key_len, payload, pay_len};
    auto rec_ptr = reinterpret_cast<uintptr_t>(rec);
    auto prev_head = kNullPtr;
    do {
      // check whether the target node includes incomplete SMOs
      auto cur_head = ValidateNode(key, kClosed, prev_head, stack);

      // prepare nodes to perform CAS
      rec->SetNext(cur_head);
      prev_head = cur_head;
    } while (stack.back()->compare_exchange_weak(cur_head, rec_ptr, std::memory_order_release));

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
      const size_t key_length = sizeof(Key),
      const size_t payload_length = sizeof(Payload))
  {
    // const auto key_addr = component::GetAddr(key);
    // const auto guard = gc_.CreateEpochGuard();

    // // traverse to a target leaf node
    // Mapping_t *consol_node = nullptr;
    // NodeStack stack = SearchLeafNode(key_addr, true, consol_node);

    // // create a delta record to write a key/value pair
    // Node_t *delta_node = Node_t::CreateDeltaNode(NodeType::kLeaf, DeltaType::kInsert,  //
    //                                              key_addr, key_length, payload, payload_length);
    // // insert the delta record
    // for (Node_t *prev_head = nullptr; true;) {
    //   // check whether the target node is valid (containing a target key and no incomplete SMOs)
    //   Node_t *cur_head = ValidateNode(key_addr, true, prev_head, stack, consol_node);

    //   // check target key/value existence
    //   const auto [target_node, meta] = CheckExistence(key_addr, stack, consol_node);
    //   if (target_node != nullptr) return ReturnCode::kKeyExist;

    //   // prepare nodes to perform CAS
    //   delta_node->SetNextNode(cur_head);
    //   prev_head = cur_head;

    //   // try to insert the delta record
    //   if (stack.back()->compare_exchange_weak(cur_head, delta_node, mo_relax)) break;
    // }

    // if (consol_node != nullptr) {
    //   TryConsolidation(consol_node, key_addr, true, stack);
    // }

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
    // const auto key_addr = component::GetAddr(key);
    // const auto guard = gc_.CreateEpochGuard();

    // // traverse to a target leaf node
    // Mapping_t *consol_node = nullptr;
    // NodeStack stack = SearchLeafNode(key_addr, true, consol_node);

    // // create a delta record to write a key/value pair
    // Node_t *delta_node = Node_t::CreateDeltaNode(NodeType::kLeaf, DeltaType::kModify,  //
    //                                              key_addr, key_length, payload, payload_length);
    // // insert the delta record
    // for (Node_t *prev_head = nullptr; true;) {
    //   // check whether the target node is valid (containing a target key and no incomplete SMOs)
    //   Node_t *cur_head = ValidateNode(key_addr, true, prev_head, stack, consol_node);

    //   // check target key/value existence
    //   const auto [target_node, meta] = CheckExistence(key_addr, stack, consol_node);
    //   if (target_node == nullptr) return ReturnCode::kKeyNotExist;

    //   // prepare nodes to perform CAS
    //   delta_node->SetNextNode(cur_head);
    //   prev_head = cur_head;

    //   // try to insert the delta record
    //   if (stack.back()->compare_exchange_weak(cur_head, delta_node, mo_relax)) break;
    // }

    // if (consol_node != nullptr) {
    //   TryConsolidation(consol_node, key_addr, true, stack);
    // }

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

    auto *page = gc_->template GetPageIfPossible<Node_t>();
    return (page == nullptr) ? (::operator new(kPageSize)) : page;
  }

  [[nodiscard]] auto
  GetRecPage()  //
      -> void *
  {
    auto *page = gc_->template GetPageIfPossible<Delta_t>();
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
    auto [page_id, height] = root_.load(std::memory_order_relaxed);

    // traverse a Bw-tree
    for (size_t i = 0; i < height; ++i) {
      auto [ptr, rc] = Delta_t::SearchChildNode(key, closed, page_id);
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
      auto [ptr, rc] = Delta_t::Validate(key, closed, page_id, prev_head);
      switch (rc) {
        case DeltaRC::kSplitMayIncomplete:
          // complete splitting and traverse to a sibling node
          CompleteSplit(ptr, stack);
          auto *rec = reinterpret_cast<Delta_t *>(ptr);
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
    auto [ptr, rc] = Delta_t::SearchRecord(key, stack);
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
    auto &&[node_ptr, high_key, high_meta, sib_page, diff] = Delta_t::Sort(head, sorted);
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
    auto *delta = new (GetRecPage()) Delta_t{split_ptr, sib_ptr, cur_head};

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
      const Delta_t *split_delta,
      NodeStack &stack)
  {
    if (stack.size() <= 1) {  // a split node is root
      RootSplit(split_delta, stack);
      return;
    }

    // create an index-entry delta record to complete split
    auto *entry_delta = new (GetRecPage()) Delta_t{split_delta};
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
      const Delta_t *split_delta,
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
