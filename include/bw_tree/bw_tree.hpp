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
#include <utility>
#include <vector>

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
 * @tparam Compare a comparetor class for keys.
 */
template <class Key, class Payload, class Compare = ::std::less<Key>>
class BwTree
{
  using DeltaNodeType = component::DeltaNodeType;
  using Metadata = component::Metadata;
  using NodeReturnCode = component::NodeReturnCode;
  using NodeType = component::NodeType;
  using Node_t = component::Node<Key, Compare>;
  using Mapping_t = std::atomic<Node_t *>;
  using MappingTable_t = component::MappingTable<Key, Compare>;
  using NodeGC_t = ::dbgroup::memory::EpochBasedGC<Node_t>;
  using NodeStack_t = std::vector<Mapping_t *, ::dbgroup::memory::STLAlloc<Mapping_t *>>;
  using NodeVec_t = std::vector<Node_t *, ::dbgroup::memory::STLAlloc<Node_t *>>;
  using RecordVec_t = std::vector<std::pair<Node_t *, Metadata>,
                                  ::dbgroup::memory::STLAlloc<std::pair<Node_t *, Metadata>>>;
  using Binary_p = std::unique_ptr<std::remove_pointer_t<Payload>,
                                   ::dbgroup::memory::Deleter<std::remove_pointer_t<Payload>>>;

 private:
  /*################################################################################################
   * Internal constants
   *##############################################################################################*/

  static constexpr auto mo_relax = component::mo_relax;

  static constexpr size_t kExpectedTreeHeight = 8;

  /*################################################################################################
   * Internal classes
   *##############################################################################################*/

  struct RecordComp {
    constexpr bool
    operator()(  //
        const std::pair<Node_t *, Metadata> a,
        const std::pair<Node_t *, Metadata> b) const noexcept
    {
      assert(a.first != nullptr && b.first != nullptr);

      const auto key_a = a.first->GetKey(a.second), key_b = b.first->GetKey(b.second);
      return Compare{}(key_a, key_b);
    }
  };

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// a root node of Bw-tree
  Mapping_t *root_;

  /// a mapping table
  MappingTable_t mapping_table_;

  /// garbage collector
  NodeGC_t gc_;

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  void
  SearchLeafNode(  //
      const Key &key,
      const bool range_closed,
      Mapping_t *page_id,
      NodeStack_t &stack,
      Mapping_t *&consol_node)
  {
    Node_t *cur_head = page_id->load(mo_relax);
    stack.emplace_back(page_id);

    // traverse a Bw-tree
    while (!cur_head->IsLeaf()) {
      size_t delta_chain_length = 0;

      // traverse a delta chain
      while (true) {
        switch (cur_head->GetDeltaNodeType()) {
          case DeltaNodeType::kInsert:
          case DeltaNodeType::kDelete: {
            // check whether this delta record includes a target key
            const auto meta0 = cur_head->GetMetadata(0), meta1 = cur_head->GetMetadata(1);
            const auto key0 = cur_head->GetKey(meta0), key1 = cur_head->GetKey(meta1);
            if (component::IsInRange<Compare>(key, &key0, !range_closed, &key1, range_closed)) {
              page_id = cur_head->template GetPayload<Mapping_t *>(meta0);
              ++delta_chain_length;
              goto found_child_node;
            }
            break;
          }
          case DeltaNodeType::kRemoveNode: {
            // this node is deleted, retry until a delete-index-entry delta is inserted
            page_id = stack.back();
            stack.pop_back();
            cur_head = page_id->load(mo_relax);
            delta_chain_length = 0;
            continue;
          }
          case DeltaNodeType::kSplit: {
            // check whether a split right (i.e., sibling) node includes a target key
            const auto meta = cur_head->GetMetadata(0);
            const auto sep_key = cur_head->GetKey(meta);
            if (Compare{}(sep_key, key) || (!range_closed && !Compare{}(key, sep_key))) {
              // traverse to a split right node
              page_id = cur_head->template GetPayload<Mapping_t *>(meta);
              cur_head = page_id->load(mo_relax);
              delta_chain_length = 0;
              continue;
            }
            break;
          }
          case DeltaNodeType::kMerge: {
            // check whether a merged node includes a target key
            const auto meta = cur_head->GetMetadata(0);
            const auto sep_key = cur_head->GetKey(meta);
            if (Compare{}(sep_key, key) || (!range_closed && !Compare{}(key, sep_key))) {
              // traverse to a merged node
              cur_head = cur_head->template GetPayload<Node_t *>(meta);
              ++delta_chain_length;
              continue;
            }
            break;
          }
          case DeltaNodeType::kNotDelta: {
            // reach a base page
            const auto idx = cur_head->SearchRecord(key, range_closed).second;
            const auto meta = cur_head->GetMetadata(idx);
            page_id = cur_head->template GetPayload<Mapping_t *>(meta);
            goto found_child_node;
          }
        }

        // go to the next delta record or base node
        cur_head = cur_head->GetNextNode();
        ++delta_chain_length;
      }

    found_child_node:
      if (delta_chain_length > kMaxDeltaNodeNum) {
        consol_node = stack.back();
      }
      stack.emplace_back(page_id);
      cur_head = page_id->load(mo_relax);
    }
  }

  void
  ValidateNode(  //
      const Key &key,
      const bool range_closed,
      Mapping_t *&page_id,
      Node_t *&cur_head,
      const Node_t *prev_head,
      NodeStack_t &stack,
      Mapping_t *&consol_node)
  {
    Node_t *cur_node = cur_head;
    size_t delta_chain_length = 0;

    // check whether there are incomplete SMOs
    while (cur_node != prev_head) {
      switch (cur_node->GetDeltaNodeType()) {
        case DeltaNodeType::kSplit: {
          const auto meta = cur_node->GetMetadata(0);
          const auto sep_key = cur_node->GetKey(meta);
          if (Compare{}(sep_key, key) || (!range_closed && !Compare{}(key, sep_key))) {
            // there may be incomplete split
            // CompleteSplit();

            // traverse to a split right node
            page_id = cur_node->template GetPayload<Mapping_t *>(meta);
            cur_head = page_id->load(mo_relax);
            return ValidateNode(key, range_closed, page_id, cur_head, nullptr, stack, consol_node);
          }
          // although this split may be incomplete, the other SMOs in this chain must be complete
          ++delta_chain_length;
          goto no_incomplete_smo;
        }
        case DeltaNodeType::kRemoveNode: {
          // there may be incomplete merging
          // CompleteMerge();

          // traverse to a merged node
          const auto parent_node = stack.back();
          stack.pop_back();
          SearchLeafNode(key, range_closed, parent_node, stack, consol_node);
          page_id = stack.back();
          cur_head = page_id->load(mo_relax);
          return ValidateNode(key, range_closed, page_id, cur_head, nullptr, stack, consol_node);
        }
        case DeltaNodeType::kMerge: {
          // there are no incomplete SMOs
          ++delta_chain_length;
          goto no_incomplete_smo;
        }
        case DeltaNodeType::kNotDelta: {
          // there are no incomplete SMOs
          goto no_incomplete_smo;
        }
        default:
          break;
      }

      // go to the next delta record or base node
      cur_node = cur_node->GetNextNode();
      ++delta_chain_length;
    }

  no_incomplete_smo:
    if (delta_chain_length > kMaxDeltaNodeNum) {
      consol_node = page_id;
    }
  }

  ReturnCode
  CheckExistence(  //
      const Key &key,
      const bool range_closed,
      Mapping_t *page_id,
      NodeStack_t &stack,
      Mapping_t *&consol_node,
      Payload *payload = nullptr)
  {
    // prepare variables to read a record
    Node_t *cur_head = page_id->load(mo_relax);
    ReturnCode existence;
    size_t delta_chain_length = 0;

    // traverse a delta chain and a base node
    while (true) {
      switch (cur_head->GetDeltaNodeType()) {
        case DeltaNodeType::kInsert: {
          // check whether this delta record includes a target key
          const auto meta = cur_head->GetMetadata(0);
          const auto rec_key = cur_head->GetKey(meta);
          if (component::IsEqual<Compare>(key, rec_key)) {
            existence = kKeyExist;
            ++delta_chain_length;
            if (payload != nullptr) {
              cur_head->CopyPayload(meta, *payload);
            }
            goto found_record;
          }
          break;
        }
        case DeltaNodeType::kDelete: {
          // check whether this delta record includes a target key
          const auto meta = cur_head->GetMetadata(0);
          const auto rec_key = cur_head->GetKey(meta);
          if (component::IsEqual<Compare>(key, rec_key)) {
            existence = kKeyNotExist;
            ++delta_chain_length;
            goto found_record;
          }
          break;
        }
        case DeltaNodeType::kRemoveNode: {
          // this node is deleted, retry until a delete-index-entry delta is inserted
          stack.pop_back();
          SearchLeafNode(key, range_closed, stack.back(), stack, consol_node);
          page_id = stack.back();
          cur_head = page_id->load(mo_relax);
          delta_chain_length = 0;
          continue;
        }
        case DeltaNodeType::kSplit: {
          // check whether a split right (i.e., sibling) node includes a target key
          const auto meta = cur_head->GetMetadata(0);
          const auto sep_key = cur_head->GetKey(meta);
          if (Compare{}(sep_key, key)) {
            // traverse to a split right node
            page_id = cur_head->template GetPayload<Mapping_t *>(meta);
            cur_head = page_id->load(mo_relax);
            delta_chain_length = 0;
            continue;
          }
          break;
        }
        case DeltaNodeType::kMerge: {
          // check whether a merged node includes a target key
          const auto meta = cur_head->GetMetadata(0);
          const auto sep_key = cur_head->GetKey(meta);
          if (Compare{}(sep_key, key)) {
            // traverse to a merged right node
            cur_head = cur_head->template GetPayload<Node_t *>(meta);
            ++delta_chain_length;
            continue;
          }
          break;
        }
        case DeltaNodeType::kNotDelta: {
          // reach a base page
          size_t idx;
          std::tie(existence, idx) = cur_head->SearchRecord(key, range_closed);
          if (existence == kKeyExist && payload != nullptr) {
            const auto meta = cur_head->GetMetadata(idx);
            cur_head->CopyPayload(meta, *payload);
          }
          goto found_record;
        }
      }

      // go to the next delta record or base node
      cur_head = cur_head->GetNextNode();
      ++delta_chain_length;
    }

  found_record:
    if (delta_chain_length > kMaxDeltaNodeNum) {
      consol_node = page_id;
    }

    return existence;
  }

  Key
  SortDeltaRecords(  //
      Node_t *cur_node,
      RecordVec_t &records,
      NodeVec_t &base_nodes,
      Mapping_t *&sib_node)
  {
    Node_t *last_smo_delta = nullptr;
    Key high_key;

    while (true) {
      switch (cur_node->GetDeltaNodeType()) {
        case DeltaNodeType::kInsert:
        case DeltaNodeType::kDelete: {
          // check whether this delta record has a new key
          const auto rec = std::make_pair(cur_node, cur_node->GetMetadata(0));
          auto it = std::lower_bound(records.begin(), records.end(), rec, RecordComp{});
          if (it == records.end()) {
            records.emplace_back(rec);
          } else if (RecordComp{}(rec, *it)) {
            records.insert(it, rec);
          }

          break;
        }
        case DeltaNodeType::kSplit: {
          const auto meta = cur_node->GetMetadata(0);
          if (last_smo_delta == nullptr) {
            // this delta record has a highest key
            last_smo_delta = cur_node;
            high_key = cur_node->GetKey(meta);
          }
          if (sib_node == nullptr) {
            sib_node = cur_node->template GetPayload<Mapping_t *>(meta);
          }

          break;
        }
        case DeltaNodeType::kMerge: {
          const auto meta = cur_node->GetMetadata(0);
          if (last_smo_delta == nullptr) {
            // this delta record has a highest key
            last_smo_delta = cur_node;
            high_key = cur_node->GetKey(meta);
          }

          // traverse a merged delta chain recursively
          auto merged_node = cur_node->template GetPayload<Node_t *>(meta);
          SortDeltaRecords(merged_node, records, base_nodes, sib_node);

          break;
        }
        case DeltaNodeType::kNotDelta: {
          // keep a base node for later merge sort
          base_nodes.emplace_back(cur_node);

          // get a highest key if needed
          if (last_smo_delta == nullptr) {
            const auto meta = cur_node->GetMetadata(cur_node->GetRecordCount() - 1);
            high_key = cur_node->GetKey(meta);
          }

          // get a sibling node if needed
          if (sib_node == nullptr) {
            sib_node = cur_node->GetSiblingNode();
          }

          goto sort_finished;
        }
        default:
          // ignore remove-node deltas because consolidated delta chains do not have them
          break;
      }

      // go to the next delta record or base node
      cur_node = cur_node->GetNextNode();
    }

  sort_finished:
    return high_key;
  }

  void
  MergeRecords(  //
      const Key &high_key,
      RecordVec_t &records,
      Node_t *base_node)
  {
    // get the number of records to be merged
    auto [existence, rec_num] = base_node->SearchRecord(high_key, true);
    if (existence == ReturnCode::kKeyExist) ++rec_num;

    // insert records in a merged node
    for (size_t i = 0; i < rec_num; ++i) {
      const auto rec = std::make_pair(base_node, base_node->GetMetadata(i));
      const auto it = std::lower_bound(records.begin(), records.end(), rec, RecordComp{});
      if (it == records.end()) {
        records.emplace_back(rec);
      } else if (RecordComp{}(rec, *it)) {
        records.insert(it, rec);
      }
    }
  }

  size_t
  CalculatePageSize(  //
      const Key &high_key,
      Node_t *base_node,
      RecordVec_t &records)
  {
    size_t page_size = component::kHeaderLength;

    // add the size of delta records
    int64_t rec_num = records.size();
    for (auto &&rec : records) {
      if (rec.first->GetDeltaType() != DeltaNodeType::kDelete) {
        page_size += rec.second.GetTotalLength();
      } else {
        rec_num -= 2;
      }
    }

    // add the size of metadata
    auto [existence, base_rec_num] = base_node->SearchRecord(high_key, true);
    if (existence == ReturnCode::kKeyExist) ++base_rec_num;
    rec_num += base_rec_num;
    page_size += rec_num * sizeof(Metadata);

    // add the size of records in a base node
    const auto begin_offset = base_node->GetMetadata(0).GetOffset();
    const auto end_meta = base_node->GetMetadata(base_rec_num - 1);
    const auto end_offset = end_meta.GetOffset() + end_meta.GetTotalLength();
    page_size += end_offset - begin_offset;

    return page_size;
  }

  /*################################################################################################
   * Internal structure modification functoins
   *##############################################################################################*/

  void
  Consolidate(  //
      Mapping_t *page_id,
      Node_t *cur_head)
  {
    // reserve vectors for delta nodes and base nodes to be consolidated
    NodeVec_t delta_nodes, base_nodes;
    delta_nodes.reserve(kMaxDeltaNodeNum * 4);
    base_nodes.reserve(kMaxDeltaNodeNum);

    // collect and sort delta records
    size_t page_size = component::kHeaderLength;
    SortDeltaRecords(cur_head, delta_nodes, base_nodes, page_size);

    // create a consolidated node
    const auto node_type = (cur_head->IsLeaf()) ? NodeType::kLeaf : NodeType::kInternal;
    Node_t *consolidated_node =
        ::dbgroup::memory::MallocNew<Node_t>(page_size, node_type, 0UL, nullptr);

    // no retry for consolidation
    page_id->compare_exchange_weak(cur_head, consolidated_node, mo_relax);
  }

 public:
  /*################################################################################################
   * Public constructor/destructor
   *##############################################################################################*/

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
    Node_t *empty_leaf = ::dbgroup::memory::MallocNew<Node_t>(component::kHeaderLength,
                                                              NodeType::kLeaf, 0UL, nullptr);
    child_page_id->store(empty_leaf, mo_relax);

    // create an empty Bw-tree
    root_ = mapping_table_.GetNewLogicalID();
    auto offset = component::kHeaderLength + sizeof(Metadata) + sizeof(Mapping_t *);
    Node_t *initial_root =
        ::dbgroup::memory::MallocNew<Node_t>(offset, NodeType::kInternal, 1UL, nullptr);
    initial_root->template SetPayload<Mapping_t *>(offset, child_page_id, sizeof(Mapping_t *));
    initial_root->SetMetadata(0, offset, 0, sizeof(Mapping_t *));
    root_->store(initial_root, mo_relax);

    // start garbage collector for removed nodes
    gc_.StartGC();
  }

  /**
   * @brief Destroy the BwTree object.
   *
   */
  ~BwTree() {}

  BwTree(const BwTree &) = delete;
  BwTree &operator=(const BwTree &) = delete;
  BwTree(BwTree &&) = delete;
  BwTree &operator=(BwTree &&) = delete;

  /*################################################################################################
   * Public read APIs
   *##############################################################################################*/

  /**
   * @brief Read a payload of a specified key if it exists.
   *
   * This function returns two return codes: kSuccess and kKeyNotExist. If a return code
   * is kSuccess, a returned pair contains a target payload. If a return code is
   * kKeyNotExist, the value of a returned payload is undefined.
   *
   * @param key a target key.
   * @return std::pair<ReturnCode, Payload>: a return code and payload pair.
   */
  auto
  Read(const Key &key)
  {
    const auto guard = gc_.CreateEpochGuard();

    // traverse to a target leaf node
    NodeStack_t stack;
    stack.reserve(kExpectedTreeHeight);
    Mapping_t *consol_node = nullptr;
    SearchLeafNode(key, true, root_, stack, consol_node);

    // prepare variables to read a record
    Mapping_t *page_id = stack.back();
    Payload payload;
    const auto existence = CheckExistence(key, true, page_id, stack, consol_node, &payload);

    if (consol_node != nullptr) {
      // execute consolidation
      // Consolidate(consol_node);
    }

    if (existence == kKeyExist) {
      if constexpr (IsVariableLengthData<Payload>()) {
        return std::make_pair(ReturnCode::kSuccess, Binary_p{payload});
      } else {
        return std::make_pair(ReturnCode::kSuccess, std::move(payload));
      }
    }
    if constexpr (IsVariableLengthData<Payload>()) {
      return std::make_pair(ReturnCode::kKeyNotExist, Binary_p{});
    } else {
      return std::make_pair(ReturnCode::kKeyNotExist, Payload{});
    }
  }

  /**
   * @brief Perform a range scan with specified keys.
   *
   * If a begin/end key is nullptr, it is treated as negative or positive infinite.
   *
   * @param begin_key the pointer of a begin key of a range scan.
   * @param begin_closed a flag to indicate whether the begin side of a range is closed.
   * @param end_key the pointer of an end key of a range scan.
   * @param end_closed a flag to indicate whether the end side of a range is closed.
   * @return RecordIterator_t: an iterator to access target records.
   */
  void
  Scan(  //
      const Key *begin_key = nullptr,
      const bool begin_closed = false,
      const Key *end_key = nullptr,
      const bool end_closed = false)
  {
    // not implemented yet
  }

  /*################################################################################################
   * Public write APIs
   *##############################################################################################*/

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
    const auto guard = gc_.CreateEpochGuard();

    // traverse to a target leaf node
    NodeStack_t stack;
    Mapping_t *consol_node = nullptr;
    SearchLeafNode(key, true, root_, stack, consol_node);

    // prepare variables to insert a new delta record
    Mapping_t *page_id = stack.back();
    Node_t *cur_head = page_id->load(mo_relax);
    Node_t *prev_head = nullptr;
    Node_t *delta_node = Node_t::CreateDeltaNode(NodeType::kLeaf, DeltaNodeType::kInsert,  //
                                                 key, key_length, payload, payload_length);

    while (true) {
      // check whether the target node is valid (containing a target key and no incomplete SMOs)
      ValidateNode(key, true, page_id, cur_head, prev_head, stack, consol_node);

      // prepare nodes to perform CAS
      delta_node->SetNextNode(cur_head);
      prev_head = cur_head;

      // insert the delta record
      if (page_id->compare_exchange_weak(cur_head, delta_node, mo_relax)) break;
    }

    if (consol_node != nullptr) {
      // execute consolidation
      // Consolidate(consol_node);
    }

    return ReturnCode::kSuccess;
  }

  /**
   * @brief Insert a specified kay/payload pair.
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
    // not implemented yet

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
    // not implemented yet

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
  ReturnCode
  Delete(  //
      const Key &key,
      const size_t key_length = sizeof(Key))
  {
    // not implemented yet

    return ReturnCode::kSuccess;
  }
};

}  // namespace dbgroup::index::bw_tree
