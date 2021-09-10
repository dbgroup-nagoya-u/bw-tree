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
  using Node_t = component::Node<Key, Payload, Compare>;
  using Mapping_t = std::atomic<Node_t *>;
  using MappingTable_t = component::MappingTable<Key, Payload, Compare>;
  using NodeGC_t = ::dbgroup::memory::EpochBasedGC<Node_t>;
  using NodeStack_t = std::vector<Node_t *, ::dbgroup::memory::STLAlloc<Node_t *>>;
  using Binary_p = std::unique_ptr<std::remove_pointer_t<Payload>,
                                   ::dbgroup::memory::Deleter<std::remove_pointer_t<Payload>>>;

 private:
  /*################################################################################################
   * Internal constants
   *##############################################################################################*/

  static constexpr auto mo_relax = component::mo_relax;

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
          case DeltaNodeType::kDelete:
            // check whether this delta record includes a target key
            const auto meta0 = cur_head->GetMetadata(0), meta1 = cur_head->GetMetadata(1);
            const auto key0 = cur_head->GetKey(meta0), key1 = cur_head->GetKey(meta1);
            if (component::IsInRange(key, key0, !range_closed, key1, range_closed)) {
              page_id = cur_head->template GetPayload<Mapping_t *>(meta0);
              ++delta_chain_length;
              goto found_child_node;
            }
            break;

          case DeltaNodeType::kRemoveNode:
            // this node is deleted, retry until a delete-index-entry delta is inserted
            page_id = stack.back();
            stack.pop_back();
            cur_head = page_id->load(mo_relax);
            delta_chain_length = 0;
            continue;

          case DeltaNodeType::kSplit:
            // check whether a split right (i.e., sibling) node includes a target key
            auto meta = cur_head->GetMetadata(0);
            auto sep_key = cur_head->GetKey(meta);
            if (Compare{}(sep_key, key) || (!range_closed && !Compare{}(key, sep_key))) {
              // traverse to a split right node
              page_id = cur_head->template GetPayload<Mapping_t *>(meta);
              cur_head = page_id->load(mo_relax);
              delta_chain_length = 0;
              continue;
            }
            break;

          case DeltaNodeType::kMerge:
            // check whether a merged node includes a target key
            meta = cur_head->GetMetadata(0);
            sep_key = cur_head->GetKey(meta);
            if (Compare{}(sep_key, key) || (!range_closed && !Compare{}(key, sep_key))) {
              // traverse to a merged node
              cur_head = cur_head->template GetPayload<Node_t *>(meta);
              ++delta_chain_length;
              continue;
            }
            break;

          case DeltaNodeType::kNotDelta:
            // reach a base page
            auto idx = cur_head->SearchRecord(key, range_closed);
            meta = cur_head->GetMetadata(idx);
            page_id = cur_head->template GetPayload<Mapping_t *>(meta);
            goto found_child_node;
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
        case DeltaNodeType::kSplit:
          auto meta = cur_node->GetMetadata(0);
          auto sep_key = cur_node->GetKey(meta);
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

        case DeltaNodeType::kRemoveNode:
          // there may be incomplete merging
          // CompleteMerge();

          // traverse to a merged node
          const auto parent_node = stack.back();
          stack.pop_back();
          SearchLeafNode(key, range_closed, parent_node, stack, consol_node);
          page_id = stack.back();
          cur_head = page_id->load(mo_relax);
          return ValidateNode(key, range_closed, page_id, cur_head, nullptr, stack, consol_node);

        case DeltaNodeType::kMerge:
          // there are no incomplete SMOs
          ++delta_chain_length;
          goto no_incomplete_smo;

        case DeltaNodeType::kNotDelta:
          // there are no incomplete SMOs
          goto no_incomplete_smo;

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

  /*################################################################################################
   * Internal structure modification functoins
   *##############################################################################################*/

  void
  Consolidate(  //
      Mapping_t *page_id,
      Node_t *cur_head)
  {
    // not implemented yet

    return;
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
    root_ = mapping_table_.GetNewLogicalID();
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
      Consolidate(consol_node, delta_node);
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
