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
  using NodeStack_t = std::vector<Mapping_t *>;
  using Binary_t = std::remove_pointer_t<Payload>;
  using Binary_p = std::unique_ptr<Binary_t, component::PayloadDeleter<Binary_t>>;

  struct KeyAddrComp {
    constexpr bool
    operator()(const void *a, const void *b) const noexcept
    {
      if constexpr (IsVariableLengthData<Key>()) {
        return Compare{}(reinterpret_cast<Key>(const_cast<void *>(a)),
                         reinterpret_cast<Key>(const_cast<void *>(b)));
      } else {
        return Compare{}(*reinterpret_cast<const Key *>(a), *reinterpret_cast<const Key *>(b));
      }
    }
  };

  struct Record {
    Node_t *node;
    Metadata meta;
    Key *key;

    constexpr bool
    operator<(const Record &comp) const noexcept
    {
      return KeyAddrComp{}(this->key, comp.key);
    }
  };

 private:
  /*################################################################################################
   * Internal constants
   *##############################################################################################*/

  static constexpr auto mo_relax = component::mo_relax;

  static constexpr auto kHeaderLength = component::kHeaderLength;

  static constexpr size_t kExpectedTreeHeight = 8;

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

  Mapping_t *
  SearchChildNode(  //
      const Key &key,
      const bool range_closed,
      Mapping_t *page_id,
      Node_t *cur_head,
      NodeStack_t &stack,
      Mapping_t *&consol_node)
  {
    size_t delta_chain_length = 0;

    // traverse a delta chain
    while (true) {
      switch (cur_head->GetDeltaNodeType()) {
        case DeltaNodeType::kInsert:
        case DeltaNodeType::kModify:
        case DeltaNodeType::kDelete: {
          // check whether this delta record includes a target key
          const auto low_key = cur_head->GetLowKeyAddr(), high_key = cur_head->GetHighKeyAddr();
          if (component::IsInRange<Compare>(key, low_key, !range_closed, high_key, range_closed)) {
            page_id = cur_head->template GetPayload<Mapping_t *>(cur_head->GetFirstMeta());
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
          const auto meta = cur_head->GetFirstMeta();
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
          const auto meta = cur_head->GetFirstMeta();
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
    if (delta_chain_length >= kMaxDeltaNodeNum) {
      consol_node = stack.back();
    }

    return page_id;
  }

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
      page_id = SearchChildNode(key, range_closed, page_id, cur_head, stack, consol_node);
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
          const auto meta = cur_node->GetFirstMeta();
          const auto sep_key = cur_node->GetKey(meta);
          if (Compare{}(sep_key, key) || (!range_closed && !Compare{}(key, sep_key))) {
            // there may be incomplete split
            CompleteSplit(cur_node, stack);

            // traverse to a split right node
            page_id = cur_node->template GetPayload<Mapping_t *>(meta);
            cur_head = page_id->load(mo_relax);
            cur_node = cur_head;
            delta_chain_length = 0;
            continue;
          }
          break;
        }
        case DeltaNodeType::kRemoveNode: {
          // there may be incomplete merging
          // CompleteMerge();

          // traverse to a merged node
          const auto parent_node = stack.back();
          stack.pop_back();
          page_id = SearchChildNode(key, range_closed, parent_node, cur_head, stack, consol_node);
          cur_head = page_id->load(mo_relax);
          cur_node = cur_head;
          delta_chain_length = 0;
          continue;
        }
        case DeltaNodeType::kMerge: {
          // there may be incomplete merging
          // CompleteMerge();
          break;
        }
        case DeltaNodeType::kNotDelta: {
          // check whether a target key is in this node
          const auto high_meta = cur_head->GetSecondMeta();
          if (high_meta.GetKeyLength() != 0) {
            const Key *high_key = cur_head->GetKeyAddr(high_meta);
            if (Compare{}(*high_key, key) || (!range_closed && !Compare{}(key, *high_key))) {
              // traverse to a sibling node
              page_id = cur_node->GetSiblingNode();
              cur_head = page_id->load(mo_relax);
              cur_node = cur_head;
              delta_chain_length = 0;
              continue;
            }
          }

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
    if (delta_chain_length >= kMaxDeltaNodeNum) {
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
        case DeltaNodeType::kInsert:
        case DeltaNodeType::kModify: {
          // check whether this delta record includes a target key
          const auto meta = cur_head->GetFirstMeta();
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
          const auto meta = cur_head->GetFirstMeta();
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
          const auto parent_node = stack.back();
          stack.pop_back();
          page_id = SearchChildNode(key, range_closed, parent_node, cur_head, stack, consol_node);
          cur_head = page_id->load(mo_relax);
          delta_chain_length = 0;
          continue;
        }
        case DeltaNodeType::kSplit: {
          // check whether a split right (i.e., sibling) node includes a target key
          const auto meta = cur_head->GetFirstMeta();
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
          const auto meta = cur_head->GetFirstMeta();
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
    if (delta_chain_length >= kMaxDeltaNodeNum) {
      consol_node = page_id;
    }

    return existence;
  }

  std::tuple<Key *, Node_t *, Node_t *, Mapping_t *>
  SortDeltaRecords(  //
      Node_t *cur_node,
      std::vector<Record> &records)
  {
    Mapping_t *sib_node;
    Node_t *last_smo_delta = nullptr;
    Key *sep_key = nullptr;

    while (true) {
      switch (cur_node->GetDeltaNodeType()) {
        case DeltaNodeType::kInsert:
        case DeltaNodeType::kModify:
        case DeltaNodeType::kDelete: {
          // check whether this delta record is in current key-range
          const auto meta = cur_node->GetFirstMeta();
          const Record rec{cur_node, meta, cur_node->GetKeyAddr(meta)};
          if (sep_key == nullptr || KeyAddrComp{}(rec.key, sep_key)) {
            // check whether this delta record has a new key
            const auto it = std::lower_bound(records.begin(), records.end(), rec);
            if (it == records.end()) {
              records.emplace_back(rec);
            } else if (KeyAddrComp{}(rec.key, (*it).key)) {
              records.insert(it, rec);
            }
          }

          break;
        }
        case DeltaNodeType::kSplit: {
          // get a separator key to ignore out-of-range keys
          const auto meta = cur_node->GetFirstMeta();
          sep_key = cur_node->GetKeyAddr(meta);

          if (last_smo_delta == nullptr) {
            // this split delta has a sibling node
            last_smo_delta = cur_node;
            sib_node = cur_node->template GetPayload<Mapping_t *>(meta);
          }

          break;
        }
        case DeltaNodeType::kMerge: {
          const auto meta = cur_node->GetFirstMeta();
          auto merged_node = cur_node->template GetPayload<Node_t *>(meta);

          // traverse a merged delta chain recursively
          Key *key;
          Node_t *dummy_node;
          if (last_smo_delta == nullptr) {
            // a merged right node has a sibling node
            last_smo_delta = cur_node;
            std::tie(key, merged_node, dummy_node, sib_node) =
                SortDeltaRecords(merged_node, records);
          } else {
            Mapping_t *dummy;
            std::tie(key, merged_node, dummy_node, dummy) = SortDeltaRecords(merged_node, records);
          }

          // add records in a merged base node
          MergeRecords(key, records, merged_node);

          break;
        }
        case DeltaNodeType::kNotDelta: {
          if (last_smo_delta == nullptr) {
            // if there are no SMOs, a base node has a sibling node
            sib_node = cur_node->GetSiblingNode();
          }

          return {sep_key, cur_node, last_smo_delta, sib_node};
        }
        default:
          // ignore remove-node deltas because consolidated delta chains do not have them
          break;
      }

      // go to the next delta record or base node
      cur_node = cur_node->GetNextNode();
    }
  }

  void
  MergeRecords(  //
      const Key *sep_key,
      std::vector<Record> &records,
      Node_t *base_node)
  {
    // get the number of records to be merged
    size_t rec_num;
    if (sep_key == nullptr) {
      rec_num = base_node->GetRecordCount();
    } else {
      ReturnCode existence;
      std::tie(existence, rec_num) = base_node->SearchRecord(*sep_key, true);
      if (existence == ReturnCode::kKeyExist) ++rec_num;
    }

    // insert records in a merged node
    for (size_t i = 0; i < rec_num; ++i) {
      const auto meta = base_node->GetMetadata(i);
      const Record rec{base_node, meta, base_node->GetKeyAddr(meta)};
      const auto it = std::lower_bound(records.begin(), records.end(), rec);
      if (it == records.end()) {
        records.emplace_back(rec);
      } else if (KeyAddrComp{}(rec.key, (*it).key)) {
        records.insert(it, rec);
      }
    }
  }

  size_t
  CalculatePageSize(  //
      Node_t *base_node,
      const size_t base_rec_num,
      std::vector<Record> &records,
      const Node_t *last_smo_delta)
  {
    size_t page_size = kHeaderLength;

    // add the size of delta records
    int64_t rec_num = records.size();
    for (auto &&rec : records) {
      const auto delta_type = rec.node->GetDeltaNodeType();
      if (delta_type == DeltaNodeType::kInsert) {
        page_size += rec.meta.GetTotalLength();
      } else if (delta_type == DeltaNodeType::kModify) {
        page_size += rec.meta.GetPayloadLength();
        --rec_num;
      } else {
        rec_num -= 2;
      }
    }

    // add the size of metadata
    rec_num += base_rec_num;
    page_size += rec_num * sizeof(Metadata);

    // add the size of records in a base node
    page_size += base_node->GetFirstMeta().GetTotalLength();
    if (last_smo_delta == nullptr) {
      page_size += base_node->GetSecondMeta().GetTotalLength();
    } else {
      page_size += last_smo_delta->GetFirstMeta().GetTotalLength();
    }
    if (base_rec_num > 0) {
      const auto begin_offset = base_node->GetMetadata(base_rec_num - 1).GetOffset();
      const auto end_meta = base_node->GetMetadata(0);
      const auto end_offset = end_meta.GetOffset() + end_meta.GetTotalLength();
      page_size += end_offset - begin_offset;
    }

    return page_size;
  }

  /*################################################################################################
   * Internal structure modification functoins
   *##############################################################################################*/

  void
  Consolidate(  //
      Mapping_t *page_id,
      const Key &key,
      const bool range_closed,
      NodeStack_t &stack)
  {
    // remove child nodes from a node stack
    while (stack.back() != page_id) {
      stack.pop_back();
    }

    // reserve vectors for delta nodes and base nodes to be consolidated
    std::vector<Record> records;
    records.reserve(kMaxDeltaNodeNum * 4);

    // check whether the target node is valid (containing a target key and no incomplete SMOs)
    Mapping_t *dummy;
    Node_t *cur_head = page_id->load(mo_relax);
    ValidateNode(key, range_closed, page_id, cur_head, nullptr, stack, dummy);

    // collect and sort delta records
    const auto [sep_key, base_node, last_smo_delta, sib_node] = SortDeltaRecords(cur_head, records);

    // get a base node and the position of a highest key
    size_t base_rec_num;
    if (sep_key == nullptr) {
      base_rec_num = base_node->GetRecordCount();
    } else {
      ReturnCode existence;
      std::tie(existence, base_rec_num) = base_node->SearchRecord(*sep_key, true);
      if (existence == ReturnCode::kKeyExist) ++base_rec_num;
    }

    // reserve a page for a consolidated node
    auto offset = CalculatePageSize(base_node, base_rec_num, records, last_smo_delta);
    const bool need_split = (offset > kPageSize) ? true : false;
    const NodeType node_type = static_cast<NodeType>(cur_head->IsLeaf());
    Node_t *consol_node = Node_t::CreateNode(offset, node_type, 0UL, sib_node);

    // copy the lowest/highest keys
    const Metadata low_meta = base_node->GetFirstMeta();
    const auto low_key_len = low_meta.GetKeyLength();
    if (low_key_len == 0) {
      consol_node->SetLowMeta(low_meta);
    } else {
      const Key *low_key = base_node->GetKeyAddr(low_meta);
      consol_node->SetKey(offset, *low_key, low_key_len);
      consol_node->SetLowMeta(Metadata{offset, low_key_len, low_key_len});
    }
    if (last_smo_delta == nullptr) {
      const Metadata high_meta = base_node->GetSecondMeta();
      const auto high_key_len = high_meta.GetKeyLength();
      if (high_key_len == 0) {
        consol_node->SetHighMeta(high_meta);
      } else {
        const Key *high_key = base_node->GetKeyAddr(high_meta);
        consol_node->SetKey(offset, *high_key, high_key_len);
        consol_node->SetHighMeta(Metadata{offset, high_key_len, high_key_len});
      }
    } else if (last_smo_delta->GetDeltaNodeType() == DeltaNodeType::kSplit) {
      const Metadata high_meta = last_smo_delta->GetFirstMeta();
      const auto high_key_len = high_meta.GetKeyLength();
      const Key *high_key = last_smo_delta->GetKeyAddr(high_meta);
      consol_node->SetKey(offset, *high_key, high_key_len);
      consol_node->SetHighMeta(Metadata{offset, high_key_len, high_key_len});
    }

    // copy records from a delta chain and base node
    Metadata meta;
    Key *base_key{};
    size_t rec_num = 0, j = 0;
    const auto delta_rec_num = records.size();
    for (size_t i = 0; i < delta_rec_num; ++i) {
      const auto [delta, delta_meta, delta_key] = records[i];

      // copy records in a base node
      for (; j < base_rec_num; ++j) {
        meta = base_node->GetMetadata(j);
        base_key = base_node->GetKeyAddr(meta);
        if (KeyAddrComp{}(base_key, delta_key)) {
          offset = base_node->CopyRecordTo(consol_node, rec_num++, offset, meta);
        } else {
          break;
        }
      }

      // copy a delta record
      if (delta->GetDeltaNodeType() != DeltaNodeType::kDelete) {
        offset = delta->CopyRecordTo(consol_node, rec_num++, offset, delta_meta);
      }
      if (j < base_rec_num && !KeyAddrComp{}(delta_key, base_key)) {
        ++j;  // a base node has the same key, so skip it
      }
    }
    for (; j < base_rec_num; ++j) {  // copy remaining records
      offset = base_node->CopyRecordTo(consol_node, rec_num++, offset, base_node->GetMetadata(j));
    }
    consol_node->SetRecordCount(rec_num);

    if (need_split) {
      Split(page_id, cur_head, consol_node, stack);
      return;
    }

    // install a consolidated node
    auto tmp_node = cur_head;
    while (!page_id->compare_exchange_weak(tmp_node, consol_node, mo_relax)) {
      if (tmp_node == cur_head) continue;  // weak CAS may fail even if it can execute

      // no CAS retry for consolidation
      Node_t::DeleteNode(consol_node);
      return;
    }
    gc_.AddGarbage(cur_head);
  }

  void
  Split(  //
      Mapping_t *page_id,
      Node_t *cur_head,
      Node_t *split_node,
      [[maybe_unused]] NodeStack_t &stack)
  {
    // get the number of records and metadata of a separator key
    const size_t rec_num = split_node->GetRecordCount();
    const size_t left_num = rec_num >> 1;
    const size_t right_num = rec_num - left_num;
    const auto sep_meta = split_node->GetMetadata(left_num - 1);

    // shift metadata to use a consolidated node as a split-right node
    auto dest_addr = component::ShiftAddress(split_node, kHeaderLength);
    auto src_addr = component::ShiftAddress(dest_addr, sizeof(Metadata) * left_num);
    memcpy(dest_addr, src_addr, sizeof(Metadata) * right_num);

    // set a separator key as the highest key
    split_node->SetLowMeta(sep_meta);
    split_node->SetRecordCount(right_num);

    // create a split-delta record
    const auto node_type = static_cast<NodeType>(split_node->IsLeaf());
    const Key *sep_key = split_node->GetKeyAddr(sep_meta);
    Mapping_t *split_page_id = mapping_table_.GetNewLogicalID();
    Node_t *split_delta =
        Node_t::CreateDeltaNode(node_type, DeltaNodeType::kSplit, *sep_key, sep_meta.GetKeyLength(),
                                split_page_id, sizeof(Mapping_t *));
    split_page_id->store(split_node, mo_relax);
    split_delta->SetNextNode(cur_head);

    // install the delta record for splitting a child node
    auto *tmp_node = cur_head;
    while (!page_id->compare_exchange_weak(tmp_node, split_delta, mo_relax)) {
      if (tmp_node == cur_head) continue;  // weak CAS may fail even if it can execute

      // no CAS retry for split
      split_delta->SetNextNode(nullptr);
      Node_t::DeleteNode(split_delta);
      Node_t::DeleteNode(split_node);
      split_page_id->store(nullptr, mo_relax);
      return;
    }

    // execute parent split
    stack.pop_back();
    CompleteSplit(split_delta, stack);
  }

  void
  CompleteSplit(  //
      Node_t *split_delta,
      NodeStack_t &stack)
  {
    // create an index-entry delta record
    const auto split_meta = split_delta->GetFirstMeta();
    const Key *sep_key = split_delta->GetKeyAddr(split_meta);
    const Mapping_t *new_page = split_delta->template GetPayload<Mapping_t *>(split_meta);
    Node_t *entry_delta =
        Node_t::CreateIndexEntryDelta(*sep_key, split_meta.GetKeyLength(), new_page);

    Mapping_t *page_id = stack.back(), *dummy = nullptr;
    Node_t *cur_head = page_id->load(mo_relax), *prev_node = nullptr;

    while (true) {
      // check whether there are no incomplete SMOs
      ValidateNode(*sep_key, true, page_id, cur_head, prev_node, stack, dummy);

      // check whether another thread has already completed this split
      if (CheckExistence(*sep_key, true, page_id, stack, dummy) == ReturnCode::kKeyExist) {
        entry_delta->SetNextNode(nullptr);
        Node_t::DeleteNode(entry_delta);
        return;
      }

      // insert the index-entry delta record
      entry_delta->SetNextNode(cur_head);
      prev_node = cur_head;
      if (page_id->compare_exchange_weak(cur_head, entry_delta, mo_relax)) return;
    }
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
    Node_t *empty_leaf = Node_t::CreateNode(kHeaderLength, NodeType::kLeaf, 0UL, nullptr);
    empty_leaf->SetLowMeta(Metadata{0, 0, 0});
    empty_leaf->SetHighMeta(Metadata{0, 0, 0});
    child_page_id->store(empty_leaf, mo_relax);

    // create an empty Bw-tree
    root_ = mapping_table_.GetNewLogicalID();
    auto offset = kHeaderLength + sizeof(Metadata) + sizeof(Mapping_t *);
    Node_t *initial_root = Node_t::CreateNode(offset, NodeType::kInternal, 1UL, nullptr);
    initial_root->SetLowMeta(Metadata{0, 0, 0});
    initial_root->SetHighMeta(Metadata{0, 0, 0});
    initial_root->template SetPayload<Mapping_t *>(offset, child_page_id, sizeof(Mapping_t *));
    initial_root->SetMetadata(0, Metadata{offset, 0, sizeof(Mapping_t *)});
    root_->store(initial_root, mo_relax);

    // start garbage collector for removed nodes
    gc_.StartGC();
  }

  /**
   * @brief Destroy the BwTree object.
   *
   */
  ~BwTree() = default;

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
    Payload payload{};
    const auto existence = CheckExistence(key, true, page_id, stack, consol_node, &payload);

    if (consol_node != nullptr) {
      Consolidate(consol_node, key, true, stack);
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
      Consolidate(consol_node, key, true, stack);
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
