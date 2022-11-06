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

#include "bw_tree/component/consolidate_info.hpp"
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
   * @param out_delta_num the number of records in this delta-chain.
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
      uintptr_t &out_ptr,
      size_t &out_delta_num)  //
      -> DeltaRC
  {
    auto has_smo = false;

    // traverse a delta chain
    for (; true; delta = delta->GetNext(), ++out_delta_num) {
      switch (delta->GetDeltaType()) {
        case kInsert:
        case kDelete: {
          if (delta->LowKeyIsLE(key, closed) && delta->HighKeyIsGE(key, !closed)) {
            // this index-entry delta directly indicates a child node
            out_ptr = delta->template GetPayload<uintptr_t>();
            return kRecordFound;
          }
          break;
        }

        case kSplit: {
          if (delta->LowKeyIsLE(key, closed)) {
            // a sibling node includes a target key
            out_ptr = delta->template GetPayload<uintptr_t>();
            return kKeyIsInSibling;
          }

          has_smo = true;
          break;
        }

        case kRemoveNode:
          return kNodeRemoved;

        case kMerge: {
          // check whether the merged node contains a target key
          if (delta->LowKeyIsLE(key, closed)) {
            // check whether the merging is aborted
            const auto *sib_lid = delta->template GetPayload<LogicalID *>();
            const auto *remove_d = sib_lid->template Load<DeltaRecord *>();
            if (remove_d == nullptr) return kNodeRemoved;  // the node is consolidated
            if (remove_d->GetDeltaType() == kRemoveNode) {
              // a target record may be in the merged node
              out_ptr = remove_d->template GetNext<uintptr_t>();
              return kReachBaseNode;
            }
            // merging was aborted, so check the sibling node
            out_ptr = reinterpret_cast<uintptr_t>(sib_lid);
            return kKeyIsInSibling;
          }

          has_smo = true;
          break;
        }

        case kNotDelta:
        default: {
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
  }

  /**
   * @brief Traverse a delta-chain to search a record with a given key.
   *
   * @param delta the head record in a delta-chain.
   * @param key a target key to be searched.
   * @param out_ptr an output pointer if needed.
   * @param out_delta_num the number of records in this delta-chain.
   * @retval kRecordFound if a delta record (in out_ptr) has the given key.
   * @retval kReachBaseNode if a base node (in out_ptr) may have the given key.
   * @retval kKeyIsInSibling if the target key is not in this node due to other SMOs.
   * @retval kNodeRemoved if this node is removed by other SMOs.
   */
  static auto
  SearchRecord(  //
      const DeltaRecord *delta,
      const Key &key,
      uintptr_t &out_ptr,
      size_t &out_delta_num)  //
      -> DeltaRC
  {
    auto has_smo = false;

    // traverse a delta chain
    for (; true; delta = delta->GetNext(), ++out_delta_num) {
      switch (delta->GetDeltaType()) {
        case kInsert:
        case kModify: {
          // check whether a target record is inserted
          if (delta->HasSameKey(key)) {
            out_ptr = reinterpret_cast<uintptr_t>(delta);
            return kRecordFound;
          }
          break;
        }

        case kDelete: {
          // check whether a target record is deleted
          if (delta->HasSameKey(key)) return kRecordDeleted;
          break;
        }

        case kSplit: {
          // check whether the right-sibling node contains a target key
          if (delta->LowKeyIsLE(key, kClosed)) {
            out_ptr = delta->template GetPayload<uintptr_t>();
            return kKeyIsInSibling;
          }

          has_smo = true;
          break;
        }

        case kRemoveNode:
          return kNodeRemoved;

        case kMerge: {
          // check whether the merged node contains a target key
          if (delta->LowKeyIsLE(key, kClosed)) {
            // check whether the merging is aborted
            const auto *sib_lid = delta->template GetPayload<LogicalID *>();
            const auto *remove_d = sib_lid->template Load<DeltaRecord *>();
            if (remove_d == nullptr) return kNodeRemoved;  // the node is consolidated
            if (remove_d->GetDeltaType() == kRemoveNode) {
              // a target record may be in the merged node
              out_ptr = remove_d->template GetNext<uintptr_t>();
              return kReachBaseNode;
            }
            // merging was aborted, so check the sibling node
            out_ptr = reinterpret_cast<uintptr_t>(sib_lid);
            return kKeyIsInSibling;
          }

          has_smo = true;
          break;
        }

        case kNotDelta:
        default: {
          // check whether the node contains a target key
          if (!has_smo && !delta->HighKeyIsGE(key, !kClosed)) {
            out_ptr = delta->template GetNext<uintptr_t>();
            return kKeyIsInSibling;
          }

          // a target record may be in the base node
          out_ptr = reinterpret_cast<uintptr_t>(delta);
          return kReachBaseNode;
        }
      }
    }
  }

  /**
   * @brief Traverse a delta-chain to check this node is valid for modifying this tree.
   *
   * @param delta the head record in a delta-chain.
   * @param out_ptr an output pointer if needed.
   * @param out_delta_num the number of records in this delta-chain.
   * @retval kReachBaseNode if this node does not have partial SMOs.
   * @retval kNodeRemoved if this node is removed by other SMOs.
   * @retval kPartialSplitMayExist if this node may be in splitting.
   * @retval kPartialMergeMayExist if this node may be in merging.
   */
  static auto
  CheckPartialSMOs(  //
      const DeltaRecord *delta,
      uintptr_t &out_ptr,
      size_t &out_delta_num)  //
      -> DeltaRC
  {
    auto rc = kReachBaseNode;

    // traverse a delta chain
    for (; true; delta = delta->GetNext(), ++out_delta_num) {
      switch (delta->GetDeltaType()) {
        case kSplit:
          if (rc == kReachBaseNode) {
            rc = kPartialSplitMayExist;
            out_ptr = reinterpret_cast<uintptr_t>(delta);
          }
          break;

        case kRemoveNode:
          return kNodeRemoved;

        case kMerge: {
          // check whether this merging is in processing
          auto tmp_ptr = delta->template GetPayload<uintptr_t>();
          const auto *merged_lid = reinterpret_cast<LogicalID *>(tmp_ptr);
          const auto *remove_d = merged_lid->template Load<DeltaRecord *>();
          if (remove_d == nullptr) return kNodeRemoved;        // the node is consolidated
          if (remove_d->GetDeltaType() != kRemoveNode) break;  // merging was aborted

          if (rc == kReachBaseNode) {
            rc = kPartialMergeMayExist;
            out_ptr = reinterpret_cast<uintptr_t>(delta);
          }
          break;
        }

        case kNotDelta:
          return rc;

        default:
          break;  // do nothing
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
   * @param out_delta_num the number of records in this delta-chain.
   * @retval kReachBaseNode if this node is valid for the given key.
   * @retval kKeyIsInSibling if the target key is not in this node due to other SMOs.
   * @retval kNodeRemoved if this node is removed by other SMOs.
   */
  static auto
  Validate(  //
      const DeltaRecord *delta,
      const Key &key,
      const bool closed,
      uintptr_t &out_ptr,
      size_t &out_delta_num)  //
      -> DeltaRC
  {
    auto has_smo = false;

    // traverse a delta chain
    for (; true; delta = delta->GetNext(), ++out_delta_num) {
      switch (delta->GetDeltaType()) {
        case kSplit: {
          // check whether the right-sibling node contains a target key
          if (delta->LowKeyIsLE(key, closed)) {
            out_ptr = delta->template GetPayload<uintptr_t>();
            return kKeyIsInSibling;
          }

          has_smo = true;
          break;
        }

        case kRemoveNode:
          return kNodeRemoved;

        case kMerge: {
          // check whether the merged node contains a target key
          if (delta->LowKeyIsLE(key, closed)) {
            // check whether the merging is aborted and the sibling node includes a target key
            out_ptr = delta->template GetPayload<uintptr_t>();
            const auto *merged_lid = reinterpret_cast<LogicalID *>(out_ptr);
            const auto *remove_d = merged_lid->template Load<DeltaRecord *>();
            if (remove_d == nullptr) return kNodeRemoved;  // the node is consolidated
            if (remove_d->GetDeltaType() != kRemoveNode) {
              // merging was aborted, so check the sibling node
              return kKeyIsInSibling;
            }
          }

          return kReachBaseNode;
        }

        case kNotDelta: {
          // check whether the node contains a target key
          if (!has_smo && !delta->HighKeyIsGE(key, !closed)) {
            out_ptr = delta->template GetNext<uintptr_t>();
            return kKeyIsInSibling;
          }

          return kReachBaseNode;
        }

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
   * @param consol_info a vector for storing base nodes and corresponding separator keys.
   * @retval 1st: true if this node has been already consolidated.
   * @retval 2nd: the difference of node size in bytes.
   */
  template <class T>
  static auto
  Sort(  //
      const DeltaRecord *delta,
      std::vector<Record> &records,
      std::vector<ConsolidateInfo> &consol_info)  //
      -> std::pair<bool, int64_t>
  {
    std::optional<Key> sep_key = std::nullopt;
    const DeltaRecord *split_d = nullptr;

    // traverse and sort a delta chain
    int64_t size_diff = 0;
    for (; true; delta = delta->GetNext()) {
      switch (delta->GetDeltaType()) {
        case kInsert:
        case kModify:
        case kDelete: {
          size_diff += delta->template AddByInsertionSortTo<T>(sep_key, records);
          break;
        }

        case kSplit: {
          const auto &cur_key = delta->GetKey();
          if (!sep_key || Comp{}(cur_key, *sep_key)) {
            // keep a separator key to exclude out-of-range records
            sep_key = cur_key;
            split_d = delta;
          }
          break;
        }

        case kMerge: {
          // check whether the merging was aborted
          const auto *sib_lid = delta->template GetPayload<LogicalID *>();
          const auto *remove_d = sib_lid->template Load<DeltaRecord *>();
          if (remove_d == nullptr) return {true, 0};           // the node is consolidated
          if (remove_d->GetDeltaType() != kRemoveNode) break;  // merging was aborted

          // keep the merged node and the corresponding separator key
          consol_info.emplace_back(remove_d->GetNext(), split_d);
          break;
        }

        case kNotDelta:
        default:
          consol_info.emplace_back(delta, split_d);
          return {false, size_diff};
      }
    }
  }
};

}  // namespace dbgroup::index::bw_tree::component

#endif  // BW_TREE_COMPONENT_DELTA_CHAIN_HPP
