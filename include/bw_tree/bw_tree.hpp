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
      Mapping_t *&consolidate_node)
  {
    Node_t *current_head = page_id->load(mo_relax);
    stack.emplace_back(page_id);

    // traverse a Bw-tree
    while (!current_head->IsLeaf()) {
      size_t delta_chain_length = 0;

      // traverse a delta chain
      while (true) {
        switch (current_head->GetDeltaNodeType()) {
          case DeltaNodeType::kInsert:
          case DeltaNodeType::kDelete:
            // check whether this delta record includes a target key
            const auto meta0 = current_head->GetMetadata(0), meta1 = current_head->GetMetadata(1);
            const auto key0 = current_head->GetKey(meta0), key1 = current_head->GetKey(meta1);
            if (component::IsInRange(key, key0, !range_closed, key1, range_closed)) {
              page_id = current_head->template GetPayload<Mapping_t *>(meta0);
              ++delta_chain_length;
              goto found_child_node;
            }
            break;

          case DeltaNodeType::kRemoveNode:
            // this node is deleted, retry until a delete-index-entry delta is inserted
            page_id = stack.back();
            stack.pop_back();
            current_head = page_id->load(mo_relax);
            delta_chain_length = 0;
            continue;

          case DeltaNodeType::kSplit:
            // check whether a split right (i.e., sibling) node includes a target key
            auto meta = current_head->GetMetadata(0);
            auto sep_key = current_head->GetKey(meta);
            if (Compare{}(sep_key, key) || (!range_closed && !Compare{}(key, sep_key))) {
              // traverse to a split right node
              page_id = current_head->template GetPayload<Mapping_t *>(meta);
              current_head = page_id->load(mo_relax);
              delta_chain_length = 0;
              continue;
            }
            break;

          case DeltaNodeType::kMerge:
            // check whether a merged node includes a target key
            meta = current_head->GetMetadata(0);
            sep_key = current_head->GetKey(meta);
            if (Compare{}(sep_key, key) || (!range_closed && !Compare{}(key, sep_key))) {
              // traverse to a merged node
              current_head = current_head->template GetPayload<Node_t *>(meta);
              ++delta_chain_length;
              continue;
            }
            break;

          case DeltaNodeType::kNotDelta:
            // reach a base page
            auto idx = current_head->SearchRecord(key, range_closed);
            meta = current_head->GetMetadata(idx);
            page_id = current_head->template GetPayload<Mapping_t *>(meta);
            goto found_child_node;
        }

        // go to the next delta record or base node
        current_head = current_head->GetNextNode();
        ++delta_chain_length;
      }

    found_child_node:
      if (delta_chain_length > kMaxDeltaNodeNum) {
        consolidate_node = stack.back();
      }
      stack.emplace_back(page_id);
      current_head = page_id->load(mo_relax);
    }
  }

  /*################################################################################################
   * Internal structure modification functoins
   *##############################################################################################*/

  void
 public:
  /*################################################################################################
   * Public constructor/destructor
   *##############################################################################################*/

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

};

}  // namespace dbgroup::index::bw_tree
