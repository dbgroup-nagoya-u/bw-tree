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
 * @tparam Comp a comparetor class for keys.
 */
template <class Key, class Payload, class Comp = ::std::less<Key>>
class BwTree
{
  using DeltaNodeType = component::DeltaNodeType;
  using Metadata = component::Metadata;
  using NodeReturnCode = component::NodeReturnCode;
  using NodeType = component::NodeType;
  using Node_t = component::Node<Key, Comp>;
  using Mapping_t = std::atomic<Node_t *>;
  using MappingTable_t = component::MappingTable<Key, Comp>;
  using NodeGC_t = ::dbgroup::memory::EpochBasedGC<Node_t>;
  using NodeStack_t = std::vector<Mapping_t *>;
  using Binary_t = std::remove_pointer_t<Payload>;
  using Binary_p = std::unique_ptr<Binary_t, component::PayloadDeleter<Binary_t>>;

  struct Record {
    Node_t *node;
    Metadata meta;
    void *key;

    constexpr bool
    operator<(const Record &comp) const noexcept
    {
      return component::LT<Key, Comp>(this->key, comp.key);
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
  std::atomic<Mapping_t *> root_;

  /// a mapping table
  MappingTable_t mapping_table_;

  /// garbage collector
  NodeGC_t gc_;

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  NodeStack_t
  SearchLeafNode(  //
      const void *key,
      const bool closed,
      Mapping_t *&consol_node)
  {
    NodeStack_t stack;
    stack.reserve(kExpectedTreeHeight);

    // get a logical page of a root node
    Mapping_t *page_id = root_.load(mo_relax);
    stack.emplace_back(page_id);

    // traverse a Bw-tree
    Node_t *cur_head = page_id->load(mo_relax);
    while (!cur_head->IsLeaf()) {
      page_id = SearchChildNode(key, closed, page_id, cur_head, stack, consol_node);
      stack.emplace_back(page_id);
      cur_head = page_id->load(mo_relax);
    }

    return stack;
  }

  Mapping_t *
  SearchChildNode(  //
      const void *key,
      const bool closed,
      Mapping_t *page_id,
      Node_t *cur_node,
      NodeStack_t &stack,
      Mapping_t *&consol_node)
  {
    size_t delta_chain_length = 0;
    Mapping_t *child_page;

    // traverse a delta chain
    while (true) {
      if (const auto delta_type = cur_node->GetDeltaNodeType();
          delta_type == DeltaNodeType::kInsert ||  //
          delta_type == DeltaNodeType::kModify ||  //
          delta_type == DeltaNodeType::kDelete) {
        // check whether this delta record includes a target key
        const auto low_key = cur_node->GetLowKeyAddr(), high_key = cur_node->GetHighKeyAddr();
        if (component::IsInRange<Key, Comp>(key, low_key, !closed, high_key, closed)) {
          child_page = cur_node->template GetPayload<Mapping_t *>(cur_node->GetLowMeta());
          ++delta_chain_length;
          break;
        }
      } else if (delta_type == DeltaNodeType::kNotDelta) {
        // reach a base page
        const auto idx = cur_node->SearchRecord(key, closed).second;
        child_page = cur_node->template GetPayload<Mapping_t *>(cur_node->GetMetadata(idx));
        break;
      } else if (delta_type == DeltaNodeType::kSplit) {
        // check whether a split right (i.e., sibling) node includes a target key
        const auto meta = cur_node->GetLowMeta();
        const auto sep_key = cur_node->GetKeyAddr(meta);
        if (component::LT<Key, Comp>(sep_key, key)
            || (!closed && !component::LT<Key, Comp>(key, sep_key))) {
          // traverse to a split right node
          page_id = cur_node->template GetPayload<Mapping_t *>(meta);
          cur_node = page_id->load(mo_relax);
          delta_chain_length = 0;
          continue;
        }
      } else if (delta_type == DeltaNodeType::kMerge) {
        // check whether a merged node includes a target key
        const auto meta = cur_node->GetLowMeta();
        const auto sep_key = cur_node->GetKeyAddr(meta);
        if (component::LT<Key, Comp>(sep_key, key)
            || (!closed && !component::LT<Key, Comp>(key, sep_key))) {
          // traverse to a merged node
          cur_node = cur_node->template GetPayload<Node_t *>(meta);
          ++delta_chain_length;
          continue;
        }
      } else {  // delta_type == DeltaNodeType::kRemoveNode
        // this node is deleted, so retry until a delete-index-entry delta is inserted
        page_id = stack.back();
        stack.pop_back();
        cur_node = page_id->load(mo_relax);
        delta_chain_length = 0;
        continue;
      }

      // go to the next delta record or base node
      cur_node = cur_node->GetNextNode();
      ++delta_chain_length;
    }

    if (delta_chain_length >= kMaxDeltaNodeNum) {
      consol_node = page_id;
    }

    return child_page;
  }

  Node_t *
  ValidateNode(  //
      const void *key,
      const bool closed,
      const Node_t *prev_head,
      NodeStack_t &stack,
      Mapping_t *&consol_node)
  {
    Mapping_t *page_id = stack.back();
    Node_t *cur_head = page_id->load(mo_relax);
    size_t delta_chain_length = 0;

    // check whether there are incomplete SMOs and a target key in this logical page
    for (Node_t *cur_node = cur_head; cur_node != prev_head;) {
      if (const auto delta_type = cur_node->GetDeltaNodeType();
          delta_type == DeltaNodeType::kNotDelta) {
        // check whether a target key is in this node
        const auto high_key = cur_node->GetHighKeyAddr();
        if (high_key != nullptr
            && (component::LT<Key, Comp>(high_key, key)
                || (!closed && component::LT<Key, Comp>(key, high_key)))) {
          // traverse to a sibling node
          page_id = cur_node->GetSiblingNode();
          cur_head = page_id->load(mo_relax);
          cur_node = cur_head;
          delta_chain_length = 0;

          // swap a current node in a stack
          stack.pop_back();
          stack.emplace_back(page_id);
          continue;
        }
        break;
      } else if (delta_type == DeltaNodeType::kSplit) {
        // check whether a target key is in a split-right node
        const auto meta = cur_node->GetLowMeta();
        const auto sep_key = cur_node->GetKeyAddr(meta);
        if (component::LT<Key, Comp>(sep_key, key)
            || (!closed && !component::LT<Key, Comp>(key, sep_key))) {
          // there may be incomplete split
          CompleteSplit(cur_node, stack, consol_node);

          // traverse to a split right node
          page_id = cur_node->template GetPayload<Mapping_t *>(meta);
          cur_head = page_id->load(mo_relax);
          cur_node = cur_head;
          delta_chain_length = 0;

          // insert a new current-level node (an old node was popped in parent-update)
          stack.emplace_back(page_id);
          continue;
        }
      } else if (delta_type == DeltaNodeType::kMerge) {
        // there may be incomplete merging
        // CompleteMerge();
      } else if (delta_type == DeltaNodeType::kRemoveNode) {
        // there may be incomplete merging
        // CompleteMerge();

        // traverse to a merged node
        const auto parent_node = stack.back();
        stack.pop_back();
        page_id = SearchChildNode(key, closed, parent_node, cur_head, stack, consol_node);
        cur_head = page_id->load(mo_relax);
        cur_node = cur_head;
        delta_chain_length = 0;
        continue;
      }

      // go to the next delta record or base node
      cur_node = cur_node->GetNextNode();
      ++delta_chain_length;
    }

    if (delta_chain_length >= kMaxDeltaNodeNum) {
      consol_node = page_id;
    }

    return cur_head;
  }

  std::pair<Node_t *, Metadata>
  CheckExistence(  //
      const void *key,
      NodeStack_t &stack,
      Mapping_t *&consol_node)
  {
    size_t delta_chain_length = 0;
    Metadata meta;

    // traverse a delta chain and a base node
    Mapping_t *page_id = stack.back();
    Node_t *cur_node = page_id->load(mo_relax);
    while (true) {
      if (const auto delta_type = cur_node->GetDeltaNodeType();
          delta_type == DeltaNodeType::kInsert || delta_type == DeltaNodeType::kModify) {
        // check whether this delta record includes a target key
        meta = cur_node->GetLowMeta();
        if (component::IsEqual<Key, Comp>(key, cur_node->GetKeyAddr(meta))) {
          ++delta_chain_length;
          break;
        }
      } else if (delta_type == DeltaNodeType::kNotDelta) {
        // check whether a target key is in this node
        const auto high_key = cur_node->GetHighKeyAddr();
        if (high_key != nullptr && component::LT<Key, Comp>(high_key, key)) {
          // traverse to a sibling node
          page_id = cur_node->GetSiblingNode();
          cur_node = page_id->load(mo_relax);
          delta_chain_length = 0;

          // swap a current node in a stack
          stack.pop_back();
          stack.emplace_back(page_id);
          continue;
        }

        // search a target key
        const auto [existence, idx] = cur_node->SearchRecord(key, true);
        if (existence == kKeyExist) {
          meta = cur_node->GetMetadata(idx);
        } else {
          cur_node = nullptr;
        }
        break;
      } else if (delta_type == DeltaNodeType::kDelete) {
        // check whether a target key is deleted
        if (component::IsEqual<Key, Comp>(key, cur_node->GetLowKeyAddr())) {
          cur_node = nullptr;
          ++delta_chain_length;
          break;
        }
      } else if (delta_type == DeltaNodeType::kSplit) {
        // check whether a split right (i.e., sibling) node includes a target key
        meta = cur_node->GetLowMeta();
        if (component::LT<Key, Comp>(cur_node->GetKeyAddr(meta), key)) {
          // traverse to a split right node
          page_id = cur_node->template GetPayload<Mapping_t *>(meta);
          cur_node = page_id->load(mo_relax);
          delta_chain_length = 0;

          // swap a current node in a stack
          stack.pop_back();
          stack.emplace_back(page_id);
          continue;
        }
      } else if (delta_type == DeltaNodeType::kMerge) {
        // check whether a merged node includes a target key
        meta = cur_node->GetLowMeta();
        if (component::LT<Key, Comp>(cur_node->GetKeyAddr(meta), key)) {
          // traverse to a merged right node
          cur_node = cur_node->template GetPayload<Node_t *>(meta);
          ++delta_chain_length;
          continue;
        }
      } else {  // delta_type == DeltaNodeType::kRemoveNode
        // this node is deleted, retry until a delete-index-entry delta is inserted
        stack.pop_back();
        const auto parent_node = stack.back();
        page_id = SearchChildNode(key, true, parent_node, cur_node, stack, consol_node);
        stack.emplace_back(page_id);
        cur_node = page_id->load(mo_relax);
        delta_chain_length = 0;
        continue;
      }

      // go to the next delta record or base node
      cur_node = cur_node->GetNextNode();
      ++delta_chain_length;
    }

    if (delta_chain_length >= kMaxDeltaNodeNum) {
      consol_node = page_id;
    }

    return {cur_node, meta};
  }

  std::tuple<void *, Node_t *, Node_t *, Mapping_t *>
  SortDeltaRecords(  //
      Node_t *cur_node,
      std::vector<Record> &records)
  {
    Mapping_t *sib_node;
    Node_t *last_smo_delta = nullptr;
    void *sep_key = nullptr;

    while (true) {
      switch (cur_node->GetDeltaNodeType()) {
        case DeltaNodeType::kInsert:
        case DeltaNodeType::kModify:
        case DeltaNodeType::kDelete: {
          // check whether this delta record is in current key-range
          const auto meta = cur_node->GetLowMeta();
          const Record rec{cur_node, meta, cur_node->GetKeyAddr(meta)};
          if (sep_key == nullptr || component::LT<Key, Comp>(rec.key, sep_key)) {
            // check whether this delta record has a new key
            const auto it = std::lower_bound(records.begin(), records.end(), rec);
            if (it == records.end()) {
              records.emplace_back(rec);
            } else if (component::LT<Key, Comp>(rec.key, (*it).key)) {
              records.insert(it, rec);
            }
          }

          break;
        }
        case DeltaNodeType::kSplit: {
          // get a separator key to ignore out-of-range keys
          const auto meta = cur_node->GetLowMeta();
          sep_key = cur_node->GetKeyAddr(meta);

          if (last_smo_delta == nullptr) {
            // this split delta has a sibling node
            last_smo_delta = cur_node;
            sib_node = cur_node->template GetPayload<Mapping_t *>(meta);
          }

          break;
        }
        case DeltaNodeType::kMerge: {
          const auto meta = cur_node->GetLowMeta();
          auto merged_node = cur_node->template GetPayload<Node_t *>(meta);

          // traverse a merged delta chain recursively
          void *key;
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
      const void *sep_key,
      std::vector<Record> &records,
      Node_t *base_node)
  {
    // get the number of records to be merged
    size_t rec_num;
    if (sep_key == nullptr) {
      rec_num = base_node->GetRecordCount();
    } else {
      ReturnCode existence;
      std::tie(existence, rec_num) = base_node->SearchRecord(sep_key, true);
      if (existence == ReturnCode::kKeyExist) ++rec_num;
    }

    // insert records in a merged node
    for (size_t i = 0; i < rec_num; ++i) {
      const auto meta = base_node->GetMetadata(i);
      const Record rec{base_node, meta, base_node->GetKeyAddr(meta)};
      const auto it = std::lower_bound(records.begin(), records.end(), rec);
      if (it == records.end()) {
        records.emplace_back(rec);
      } else if (component::LT<Key, Comp>(rec.key, (*it).key)) {
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
    page_size += base_node->GetLowMeta().GetTotalLength();
    if (last_smo_delta == nullptr) {
      page_size += base_node->GetHighMeta().GetTotalLength();
    } else {
      page_size += last_smo_delta->GetLowMeta().GetTotalLength();
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
      const Mapping_t *target_page,
      const void *key,
      const bool closed,
      NodeStack_t &stack)
  {
    // remove child nodes from a node stack
    while (stack.back() != target_page) stack.pop_back();

    // check whether the target node is valid (containing a target key and no incomplete SMOs)
    Mapping_t *consol_page = nullptr;
    Node_t *cur_head = ValidateNode(key, closed, nullptr, stack, consol_page);
    if (consol_page != target_page) return;

    // collect and sort delta records
    std::vector<Record> records;
    records.reserve(kMaxDeltaNodeNum * 4);
    const auto [sep_key, base_node, last_smo_delta, sib_page] = SortDeltaRecords(cur_head, records);

    // get a base node and the position of a highest key
    size_t base_rec_num;
    if (sep_key == nullptr) {
      base_rec_num = base_node->GetRecordCount();
    } else {
      ReturnCode existence;
      std::tie(existence, base_rec_num) = base_node->SearchRecord(sep_key, true);
      if (existence == ReturnCode::kKeyExist) ++base_rec_num;
    }

    // reserve a page for a consolidated node
    auto offset = CalculatePageSize(base_node, base_rec_num, records, last_smo_delta);
    const auto need_split = (offset > kPageSize) ? true : false;
    const auto node_type = static_cast<NodeType>(cur_head->IsLeaf());
    Node_t *consol_node = Node_t::CreateNode(offset, node_type, 0UL, sib_page);

    // copy the lowest/highest keys
    const auto low_meta = base_node->GetLowMeta();
    const auto low_key_len = low_meta.GetKeyLength();
    if (low_key_len == 0) {
      consol_node->SetLowMeta(low_meta);
    } else {
      const auto low_key = base_node->GetKeyAddr(low_meta);
      consol_node->SetKey(offset, low_key, low_key_len);
      consol_node->SetLowMeta(Metadata{offset, low_key_len, low_key_len});
    }
    if (last_smo_delta == nullptr) {
      const auto high_meta = base_node->GetHighMeta();
      const auto high_key_len = high_meta.GetKeyLength();
      if (high_key_len == 0) {
        consol_node->SetHighMeta(high_meta);
      } else {
        const auto high_key = base_node->GetKeyAddr(high_meta);
        consol_node->SetKey(offset, high_key, high_key_len);
        consol_node->SetHighMeta(Metadata{offset, high_key_len, high_key_len});
      }
    } else if (last_smo_delta->GetDeltaNodeType() == DeltaNodeType::kSplit) {
      const auto high_meta = last_smo_delta->GetLowMeta();
      const auto high_key_len = high_meta.GetKeyLength();
      const auto high_key = last_smo_delta->GetKeyAddr(high_meta);
      consol_node->SetKey(offset, high_key, high_key_len);
      consol_node->SetHighMeta(Metadata{offset, high_key_len, high_key_len});
    }

    // copy records from a delta chain and base node
    Metadata meta;
    void *base_key{};
    size_t rec_num = 0, j = 0;
    const auto delta_rec_num = records.size();
    for (size_t i = 0; i < delta_rec_num; ++i) {
      auto [delta, delta_meta, delta_key] = records[i];

      // copy records in a base node
      for (; j < base_rec_num; ++j) {
        meta = base_node->GetMetadata(j);
        base_key = base_node->GetKeyAddr(meta);
        if (meta.GetKeyLength() != 0 && component::LT<Key, Comp>(base_key, delta_key)) {
          offset = base_node->CopyRecordTo(consol_node, rec_num++, offset, meta);
        } else {
          break;
        }
      }

      // copy a delta record
      if (delta->GetDeltaNodeType() != DeltaNodeType::kDelete) {
        if (delta->IsLeaf()) {
          offset = delta->CopyRecordTo(consol_node, rec_num++, offset, delta_meta);
        } else {
          Node_t *prev_node = base_node;
          Metadata prev_meta = meta;
          do {
            // insert a new index-entry
            const void *cur_key = delta->GetLowKeyAddr();
            const size_t cur_key_len = delta_meta.GetKeyLength();
            const Mapping_t *ins_page = prev_node->template GetPayload<Mapping_t *>(prev_meta);
            consol_node->SetPayload(offset, ins_page, sizeof(Mapping_t *));
            consol_node->SetKey(offset, cur_key, cur_key_len);
            consol_node->SetMetadata(
                rec_num++, Metadata{offset, cur_key_len, cur_key_len + sizeof(Mapping_t *)});

            // keep a current delta record for multiple splitting
            prev_node = delta;
            prev_meta = delta->GetLowMeta();

            // check the next delta record
            if (++i >= delta_rec_num) break;
            delta = records[i].node;
            delta_meta = records[i].meta;
            delta_key = records[i].key;
          } while (meta.GetKeyLength() == 0 || component::LT<Key, Comp>(delta_key, base_key));

          // insert the originally inserted index-entry
          const auto orig_key = base_node->GetKeyAddr(meta);
          const auto orig_key_len = meta.GetKeyLength();
          const auto ins_page = prev_node->template GetPayload<Mapping_t *>(prev_meta);
          consol_node->SetPayload(offset, ins_page, sizeof(Mapping_t *));
          consol_node->SetKey(offset, orig_key, orig_key_len);
          consol_node->SetMetadata(
              rec_num++, Metadata{offset, orig_key_len, orig_key_len + sizeof(Mapping_t *)});
          ++j;
        }
      }
      if (j < base_rec_num && !component::LT<Key, Comp>(delta_key, base_key)) {
        ++j;  // a base node has the same key, so skip it
      }
    }
    for (; j < base_rec_num; ++j) {  // copy remaining records
      offset = base_node->CopyRecordTo(consol_node, rec_num++, offset, base_node->GetMetadata(j));
    }
    consol_node->SetRecordCount(rec_num);

    if (need_split) {
      if (HalfSplit(consol_page, cur_head, consol_node, stack)) return;
      Consolidate(consol_page, key, closed, stack);  // retry split
      return;
    }

    // install a consolidated node
    auto old_head = cur_head;
    while (!consol_page->compare_exchange_weak(old_head, consol_node, mo_relax)) {
      if (old_head == cur_head) continue;  // weak CAS may fail even if it can execute

      // no CAS retry for consolidation
      Node_t::DeleteNode(consol_node);
      return;
    }
    gc_.AddGarbage(cur_head);
  }

  bool
  HalfSplit(  //
      Mapping_t *split_page,
      Node_t *cur_head,
      Node_t *split_node,
      NodeStack_t &stack)
  {
    // get the number of records and metadata of a separator key
    const auto total_num = split_node->GetRecordCount();
    const auto left_num = total_num >> 1;
    const auto right_num = total_num - left_num;
    const auto sep_meta = split_node->GetMetadata(left_num - 1);

    // shift metadata to use a consolidated node as a split-right node
    auto dest_addr = component::ShiftAddress(split_node, kHeaderLength);
    const auto src_addr = component::ShiftAddress(dest_addr, sizeof(Metadata) * left_num);
    memmove(dest_addr, src_addr, sizeof(Metadata) * right_num);

    // set a separator key as the lowest key
    split_node->SetLowMeta(sep_meta);
    split_node->SetRecordCount(right_num);

    // create a split-delta record
    const auto node_type = static_cast<NodeType>(split_node->IsLeaf());
    const auto sep_key = split_node->GetKeyAddr(sep_meta);
    Mapping_t *right_page_id = mapping_table_.GetNewLogicalID();
    Node_t *split_delta = Node_t::CreateDeltaNode(node_type, DeltaNodeType::kSplit,  //
                                                  sep_key, sep_meta.GetKeyLength(),  //
                                                  right_page_id, sizeof(Mapping_t *));
    split_delta->SetNextNode(cur_head);
    right_page_id->store(split_node, mo_relax);

    // install the delta record for splitting a child node
    for (auto old_head = cur_head;
         !split_page->compare_exchange_weak(old_head, split_delta, mo_relax);) {
      if (old_head == cur_head) continue;  // weak CAS may fail even if it can execute

      // no CAS retry for split
      split_delta->SetNextNode(nullptr);
      Node_t::DeleteNode(split_delta);
      Node_t::DeleteNode(split_node);
      right_page_id->store(nullptr, mo_relax);
      return false;
    }

    // execute parent update
    Mapping_t *consol_page = nullptr;
    CompleteSplit(split_delta, stack, consol_page);

    // execute parent consolidation/split if needed
    if (consol_page != nullptr) {
      Consolidate(consol_page, sep_key, true, stack);
    }

    return true;
  }

  void
  CompleteSplit(  //
      Node_t *split_delta,
      NodeStack_t &stack,
      Mapping_t *&consol_page)
  {
    // create an index-entry delta record
    const auto sep_meta = split_delta->GetLowMeta();
    const auto sep_key_len = sep_meta.GetKeyLength();
    const auto sep_key = split_delta->GetKeyAddr(sep_meta);
    Mapping_t *right_page = split_delta->template GetPayload<Mapping_t *>(sep_meta);

    if (stack.size() <= 1) {
      // a split node is a root node
      SplitRoot(sep_key, sep_key_len, right_page, stack);
      return;
    }

    // create an index-entry delta record to complete split
    Node_t *entry_delta = Node_t::CreateIndexEntryDelta(sep_key, sep_key_len, right_page);

    // insert the delta record into a parent node
    stack.pop_back();  // remove a split child node to modify its parent node
    for (Node_t *prev_node = nullptr; true;) {
      // check whether there are no incomplete SMOs
      Node_t *cur_head = ValidateNode(sep_key, true, prev_node, stack, consol_page);

      // check whether another thread has already completed this split
      if (CheckExistence(sep_key, stack, consol_page).first != nullptr) {
        entry_delta->SetNextNode(nullptr);
        Node_t::DeleteNode(entry_delta);
        return;
      }

      // try to insert the index-entry delta record
      entry_delta->SetNextNode(cur_head);
      prev_node = cur_head;
      if (stack.back()->compare_exchange_weak(cur_head, entry_delta, mo_relax)) return;
    }
  }

  void
  SplitRoot(  //
      const void *sep_key,
      const size_t sep_key_len,
      Mapping_t *right_page,
      NodeStack_t &stack)
  {
    // create a new root node
    Mapping_t *left_page = stack.back();  // i.e., the old root node
    const auto total_len = sep_key_len + sizeof(Mapping_t *);
    auto offset = kHeaderLength + (2 * sizeof(Metadata)) + sizeof(Mapping_t *) + total_len;
    Node_t *new_root = Node_t::CreateNode(offset, NodeType::kInternal, 2, nullptr);
    new_root->SetLowMeta(Metadata{0, 0, 0});
    new_root->SetHighMeta(Metadata{0, 0, 0});

    // set a split-left page
    new_root->SetPayload(offset, left_page, sizeof(Mapping_t *));
    new_root->SetKey(offset, sep_key, sep_key_len);
    new_root->SetMetadata(0, Metadata{offset, sep_key_len, total_len});

    // set a split-right page
    new_root->SetPayload(offset, right_page, sizeof(Mapping_t *));
    new_root->SetMetadata(1, Metadata{offset, 0, sizeof(Mapping_t *)});

    // install a new root page
    Mapping_t *new_root_page = mapping_table_.GetNewLogicalID();
    new_root_page->store(new_root, mo_relax);
    for (auto old_root_page = left_page;
         !root_.compare_exchange_weak(old_root_page, new_root_page, mo_relax);) {
      if (old_root_page == left_page) continue;  // weak CAS may fail even if it can execute

      // another thread has already inserted a new root
      new_root_page->store(nullptr, mo_relax);
      Node_t::DeleteNode(new_root);
      return;
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
    const auto key_addr = component::GetAddr(key);
    const auto guard = gc_.CreateEpochGuard();

    // traverse to a target leaf node
    Mapping_t *consol_node = nullptr;
    NodeStack_t stack = SearchLeafNode(key_addr, true, consol_node);

    // check whether the leaf node has a target key
    const auto [target_node, meta] = CheckExistence(key_addr, stack, consol_node);

    if (consol_node != nullptr) {
      Consolidate(consol_node, key_addr, true, stack);
    }

    if (target_node != nullptr) {
      // get a target payload
      Payload payload{};
      target_node->CopyPayload(meta, payload);

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
    const auto key_addr = component::GetAddr(key);
    const auto guard = gc_.CreateEpochGuard();

    // traverse to a target leaf node
    Mapping_t *consol_node = nullptr;
    NodeStack_t stack = SearchLeafNode(key_addr, true, consol_node);

    // create a delta record to write a key/value pair
    Node_t *delta_node = Node_t::CreateDeltaNode(NodeType::kLeaf, DeltaNodeType::kInsert,  //
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
      Consolidate(consol_node, key_addr, true, stack);
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
