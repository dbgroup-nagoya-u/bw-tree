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

#ifndef BW_TREE_COMPONENT_DELTA_CHAIN_HPP
#define BW_TREE_COMPONENT_DELTA_CHAIN_HPP

#include <optional>
#include <utility>
#include <vector>

#include "bw_tree/component/logical_id.hpp"

namespace dbgroup::index::bw_tree::component
{
/**
 * @brief A class for managing procedures for delta-chains.
 *
 * @tparam DeltaRecord a class of delta records.
 */
template <class DeltaRecord>
class DeltaChain
{
 public:
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using Key = typename DeltaRecord::Key;
  using Comp = typename DeltaRecord::Comp;
  using Record = typename DeltaRecord::Record;
  using ConsolidateInfo = std::pair<const void *, const void *>;

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  /**
   * @brief Check the lowest key of this node is equivalent with a given key.
   *
   * Note that this function traverses a delta-chain and use a base node for check.
   *
   * @param delta the head record in a delta-chain.
   * @param key a target key to be compared.
   * @retval true if the keys are same.
   * @retval false otherwise.
   */
  [[nodiscard]] static auto
  TraverseToGetLowKey(const DeltaRecord *delta)  //
      -> std::optional<Key>
  {
    // traverse to a base node
    while (delta->GetDeltaType() != kNotDelta) {
      delta = delta->GetNext();
    }
    return delta->GetLowKey();
  }

  /**
   * @brief Traverse a delta-chain to search a child node with a given key.
   *
   * @param delta the head record in a delta-chain.
   * @param key a target key to be searched.
   * @param closed a flag for including the same key.
   * @param out_ptr an output pointer if needed.
   * @retval kRecordFound if a delta record (in out_ptr) has a corresponding child.
   * @retval kReachBaseNode if a base node (in out_ptr) has a corresponding child.
   * @retval kKeyIsInSibling if the target key is not in this node due to other SMOs.
   * @retval kNodeRemoved if this node is removed by other SMOs.
   */
  static auto
  SearchChildNode(  //
      const DeltaRecord *delta,
      const Key &key,
      const bool closed,
      uintptr_t &out_ptr)  //
      -> DeltaRC
  {
    auto has_smo = false;

    // traverse a delta chain
    for (; true; delta = delta->GetNext()) {
      switch (delta->GetDeltaType()) {
        case kInsert:
          if (delta->LowKeyIsLE(key, closed) && delta->HighKeyIsGE(key, !closed)) {
            // this index-entry delta directly indicates a child node
            out_ptr = delta->template GetPayload<uintptr_t>();
            return kRecordFound;
          }
          break;

        case kDelete:
          if (delta->LowKeyIsLE(key, closed) && delta->HighKeyIsGE(key, !closed)) {
            // this index-entry delta directly indicates a child node
            out_ptr = delta->GetPayloadAtomically();
            return kRecordFound;
          }
          break;

        case kRemoveNode:
          return kNodeRemoved;

        case kMerge:
          // check whether the merged node contains a target key
          if (delta->LowKeyIsLE(key, closed)) {
            // check whether the node contains a target key
            const auto *merged_node = delta->template GetPayload<DeltaRecord *>();
            if (!has_smo && !delta->HighKeyIsGE(key, !closed)) {
              out_ptr = merged_node->template GetNext<uintptr_t>();
              return kKeyIsInSibling;
            }

            // a target record may be in the merged node
            out_ptr = reinterpret_cast<uintptr_t>(merged_node);
            return kReachBaseNode;
          }
          has_smo = true;
          break;

        case kNotDelta:
        default:
          if (!has_smo && !delta->HighKeyIsGE(key, !closed)) {
            // a sibling node includes a target key
            out_ptr = delta->template GetNext<uintptr_t>();
            return kKeyIsInSibling;
          }

          // reach a base page
          out_ptr = reinterpret_cast<uintptr_t>(delta);
          return kReachBaseNode;
      }
    }
  }

  /**
   * @brief Traverse a delta-chain to search a record with a given key.
   *
   * @param delta the head record in a delta-chain.
   * @param key a target key to be searched.
   * @param out_ptr an output pointer if needed.
   * @retval kRecordFound if a delta record (in out_ptr) has the given key.
   * @retval kReachBaseNode if a base node (in out_ptr) may have the given key.
   * @retval kKeyIsInSibling if the target key is not in this node due to other SMOs.
   * @retval kNodeRemoved if this node is removed by other SMOs.
   */
  static auto
  SearchRecord(  //
      const DeltaRecord *delta,
      const Key &key,
      uintptr_t &out_ptr)  //
      -> DeltaRC
  {
    auto has_smo = false;

    // traverse a delta chain
    for (; true; delta = delta->GetNext()) {
      switch (delta->GetDeltaType()) {
        case kInsert:
        case kModify:
          // check whether a target record is inserted
          if (delta->HasSameKey(key)) {
            out_ptr = reinterpret_cast<uintptr_t>(delta);
            return kRecordFound;
          }
          break;

        case kDelete:
          // check whether a target record is deleted
          if (delta->HasSameKey(key)) return kRecordDeleted;
          break;

        case kRemoveNode:
          return kNodeRemoved;

        case kMerge:
          // check whether the merged node contains a target key
          if (delta->LowKeyIsLE(key, kClosed)) {
            // check whether the node contains a target key
            const auto *merged_node = delta->template GetPayload<DeltaRecord *>();
            if (!has_smo && !delta->HighKeyIsGE(key, kOpen)) {
              out_ptr = merged_node->template GetNext<uintptr_t>();
              return kKeyIsInSibling;
            }

            // a target record may be in the merged node
            out_ptr = reinterpret_cast<uintptr_t>(merged_node);
            return kReachBaseNode;
          }
          has_smo = true;
          break;

        case kNotDelta:
        default:
          // check whether the node contains a target key
          if (!has_smo && !delta->HighKeyIsGE(key, kOpen)) {
            out_ptr = delta->template GetNext<uintptr_t>();
            return kKeyIsInSibling;
          }

          // a target record may be in the base node
          out_ptr = reinterpret_cast<uintptr_t>(delta);
          return kReachBaseNode;
      }
    }
  }

  /**
   * @brief Traverse a delta-chain to search a record with a given key.
   *
   * @param delta the head record in a delta-chain.
   * @param key a target key to be searched.
   * @param out_ptr an output pointer if needed.
   * @retval kRecordFound if a delta record (in out_ptr) has the given key.
   * @retval kReachBaseNode if a base node (in out_ptr) may have the given key.
   * @retval kKeyIsInSibling if the target key is not in this node due to other SMOs.
   * @retval kNodeRemoved if this node is removed by other SMOs.
   */
  static auto
  SearchForMerge(  //
      const DeltaRecord *delta,
      const Key &key,
      const std::optional<Key> &sib_key,
      uintptr_t &out_ptr,
      bool &key_found,
      bool &sib_key_found)  //
      -> DeltaRC
  {
    auto has_smo = false;

    // traverse a delta chain
    for (; true; delta = delta->GetNext()) {
      switch (delta->GetDeltaType()) {
        case kInsert:
          // check whether a target record is inserted
          if (!key_found && delta->HasSameKey(key)) {
            key_found = true;
            if (sib_key_found) return kRecordFound;
          }
          if (!sib_key_found && delta->HasSameKey(*sib_key)) {
            sib_key_found = true;
            if (key_found) return kRecordFound;
          }
          break;

        case kDelete:
          // check whether a target record is deleted
          if (!key_found && delta->HasSameKey(key)) return kAbortMerge;
          if (!sib_key_found && delta->HasSameKey(*sib_key)) return kAbortMerge;  // merged node
          break;

        case kRemoveNode:
          return kNodeRemoved;

        case kMerge:
          // check whether the merged node contains a target key
          if (delta->LowKeyIsLE(key, kClosed)) {
            // check whether the node contains a target key
            const auto *merged_node = delta->template GetPayload<DeltaRecord *>();
            if (!has_smo && !key_found && !delta->HighKeyIsGE(key, kOpen)) {
              out_ptr = merged_node->template GetNext<uintptr_t>();
              return kKeyIsInSibling;
            }

            // a target record may be in the merged node
            out_ptr = reinterpret_cast<uintptr_t>(merged_node);
            return kReachBaseNode;
          }
          if (!sib_key_found && delta->HasSameKey(*sib_key)) {
            sib_key_found = true;
            if (key_found) return kRecordFound;
          }
          has_smo = true;
          break;

        case kNotDelta:
        default:
          // check whether the node contains a target key
          if (!key_found) {
            if (!delta->IsLeftmost() && delta->HasSameKey(key)) return kAbortMerge;
            if (!has_smo && !delta->HighKeyIsGE(key, kOpen)) {
              out_ptr = delta->template GetNext<uintptr_t>();
              return kKeyIsInSibling;
            }
          }

          // a target record may be in the base node
          out_ptr = reinterpret_cast<uintptr_t>(delta);
          return kReachBaseNode;
      }
    }
  }

  /**
   * @brief Traverse a delta-chain to check this node is valid for modifying this tree.
   *
   * @param delta the head record in a delta-chain.
   * @param key a target key to be searched.
   * @param closed a flag for including the same key.
   * @param out_ptr an output pointer if needed.
   * @retval kReachBaseNode if this node is valid for the given key.
   * @retval kKeyIsInSibling if the target key is not in this node due to other SMOs.
   * @retval kNodeRemoved if this node is removed by other SMOs.
   */
  static auto
  Validate(  //
      const DeltaRecord *delta,
      const Key &key,
      const bool closed,
      uintptr_t &out_ptr)  //
      -> DeltaRC
  {
    // traverse a delta chain
    for (; true; delta = delta->GetNext()) {
      switch (delta->GetDeltaType()) {
        case kRemoveNode:
          return kNodeRemoved;

        case kMerge:
          // check whether the node contains a target key
          if (!delta->HighKeyIsGE(key, !closed)) {
            const auto *merged_node = delta->template GetPayload<DeltaRecord *>();
            out_ptr = merged_node->template GetNext<uintptr_t>();
            return kKeyIsInSibling;
          }
          return kReachBaseNode;

        case kNotDelta:
          // check whether the node contains a target key
          if (!delta->HighKeyIsGE(key, !closed)) {
            out_ptr = delta->template GetNext<uintptr_t>();
            return kKeyIsInSibling;
          }
          return kReachBaseNode;

        default:
          break;  // do nothing
      }
    }
  }

  /**
   * @brief Sort delta records for consolidation.
   *
   * @tparam T a class of expected payloads.
   * @param delta the head record in a delta-chain.
   * @param records a vector for storing sorted records.
   * @param nodes a vector for storing base nodes and corresponding separator keys.
   */
  static void
  Sort(  //
      const DeltaRecord *delta,
      std::vector<Record> &records,
      std::vector<const void *> &nodes)
  {
    // traverse and sort a delta chain
    for (; true; delta = delta->GetNext()) {
      switch (delta->GetDeltaType()) {
        case kInsert:
        case kModify:
        case kDelete:
          delta->AddByInsertionSortTo(records);
          break;

        case kMerge: {
          // keep the merged node and the corresponding separator key
          nodes.emplace_back(delta->GetPayload());
          break;
        }

        case kNotDelta:
        default:
          nodes.emplace_back(delta);
          return;
      }
    }
  }
};

}  // namespace dbgroup::index::bw_tree::component

#endif  // BW_TREE_COMPONENT_DELTA_CHAIN_HPP
