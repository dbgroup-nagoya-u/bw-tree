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

#ifndef BW_TREE_FIXLEN_DELTA_RECORD_HPP
#define BW_TREE_FIXLEN_DELTA_RECORD_HPP

#include <optional>
#include <utility>
#include <vector>

#include "bw_tree/common/common.hpp"
#include "bw_tree/common/consolidate_info.hpp"
#include "bw_tree/common/logical_id.hpp"

namespace dbgroup::index::bw_tree::component::fixlen
{
/**
 * @brief A class to represent delta records in Bw-tree.
 *
 * @tparam Key a target key class.
 * @tparam Comp a comparetor class for keys.
 */
template <class Key, class Comp>
class DeltaRecord
{
 public:
  /*####################################################################################
   * Public constructors for inserting/deleting records in leaf nodes
   *##################################################################################*/

  /**
   * @brief Construct a new delta record for inserting/modifying a record.
   *
   * @tparam T a target payload class.
   * @param delta_type
   * @param key
   * @param payload
   */
  template <class T>
  DeltaRecord(  //
      const DeltaType delta_type,
      const Key &key,
      const T &payload)
      : node_type_{kLeaf}, delta_type_{delta_type}, has_low_key_{1}, has_high_key_{0}, key_{key}
  {
    SetPayload(payload);
  }

  /**
   * @brief Construct a new delta record for deleting a record.
   *
   * @param key
   */
  explicit DeltaRecord(const Key &key)
      : node_type_{kLeaf}, delta_type_{kDelete}, has_low_key_{1}, has_high_key_{0}, key_{key}
  {
  }

  /*####################################################################################
   * Public constructors for inserting/deleting records in internal nodes
   *##################################################################################*/

  /**
   * @brief Construct a new delta record for inserting an index-entry.
   *
   * @param split_d
   */
  explicit DeltaRecord(const DeltaRecord *split_d)
      : node_type_{kInternal},
        delta_type_{kInsert},
        has_low_key_{1},
        has_high_key_{split_d->has_high_key_},
        key_{split_d->key_},
        high_key_{split_d->high_key_}
  {
    // copy contents of a split delta
    memcpy(payload_, (split_d->payload_), kWordSize);
  }

  /**
   * @brief Construct a new delta record for deleting an index-entry.
   *
   * @param merge_d
   * @param left_lid
   */
  DeltaRecord(  //
      const DeltaRecord *merge_d,
      const LogicalID *left_lid)
      : node_type_{kInternal},
        delta_type_{kDelete},
        has_low_key_{1},
        has_high_key_{merge_d->has_high_key_},
        key_{merge_d->key_},
        high_key_{merge_d->high_key_}
  {
    SetPayload(left_lid);
  }

  /*####################################################################################
   * Public constructors for performing SMOs
   *##################################################################################*/

  /**
   * @brief Construct a new delta record for splitting/merging a node.
   *
   * @param split_delta
   */
  DeltaRecord(  //
      const DeltaType delta_type,
      const DeltaRecord *right_node,
      const LogicalID *right_lid,
      const DeltaRecord *next = nullptr)
      : node_type_{right_node->node_type_},
        delta_type_{delta_type},
        has_low_key_{1},
        has_high_key_{right_node->has_high_key_},
        next_{reinterpret_cast<uintptr_t>(next)},
        key_{right_node->key_},
        high_key_{right_node->high_key_}
  {
    // set a sibling node
    SetPayload(right_lid);
  }

  /**
   * @brief Construct a new delta record for removing a node.
   *
   * @param next a pointer to the removed node.
   */
  DeltaRecord(  //
      [[maybe_unused]] const DeltaType dummy,
      const DeltaRecord *removed_node)
      : node_type_{removed_node->node_type_},
        delta_type_{kRemoveNode},
        has_low_key_{0},
        has_high_key_{0},
        next_{reinterpret_cast<uintptr_t>(removed_node)}
  {
  }

  /*####################################################################################
   * Public assignment operators
   *##################################################################################*/

  DeltaRecord(const DeltaRecord &) = delete;
  DeltaRecord(DeltaRecord &&) noexcept = delete;

  auto operator=(const DeltaRecord &) -> DeltaRecord & = delete;
  auto operator=(DeltaRecord &&) noexcept -> DeltaRecord & = delete;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  ~DeltaRecord() = default;

  /*####################################################################################
   * Public getters/setters
   *##################################################################################*/

  /**
   * @retval true if this is a leaf node.
   * @retval false if this is an internal node.
   */
  [[nodiscard]] constexpr auto
  IsLeaf() const  //
      -> bool
  {
    return node_type_ == kLeaf;
  }

  /**
   * @retval true if this is a base node.
   * @retval false if this is a delta record.
   */
  [[nodiscard]] constexpr auto
  IsBaseNode() const  //
      -> bool
  {
    return delta_type_ == kNotDelta;
  }

  [[nodiscard]] constexpr auto
  IsRemoveNodeDelta() const  //
      -> bool
  {
    return delta_type_ == kRemoveNode;
  }

  [[nodiscard]] constexpr auto
  IsMergeDelta() const  //
      -> bool
  {
    return delta_type_ == kMerge;
  }

  template <class T = DeltaRecord *>
  [[nodiscard]] constexpr auto
  GetNext() const  //
      -> T
  {
    return reinterpret_cast<T>(next_);
  }

  /**
   * @param meta metadata of a corresponding record.
   * @return a key in a target record.
   */
  [[nodiscard]] auto
  GetKey() const  //
      -> Key
  {
    return key_;
  }

  /**
   * @tparam Payload a class of payload.
   * @return a payload in this record.
   */
  template <class T>
  [[nodiscard]] auto
  GetPayload() const  //
      -> T
  {
    T payload{};
    memcpy(&payload, reinterpret_cast<const T *>(payload_), sizeof(T));
    return payload;
  }

  void
  SetNext(const DeltaRecord *next)
  {
    next_ = reinterpret_cast<uintptr_t>(next);
  }

  void
  Abort()
  {
    next_ = kNullPtr;
  }

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  [[nodiscard]] auto
  HasSameLowKeyWith(const Key &key) const  //
      -> bool
  {
    // traverse to a base node
    const auto *head = this;
    while (!head->IsBaseNode()) {
      head = head->GetNext();
    }

    if (!head->has_low_key_) return false;
    return IsEqual<Comp>(head->key_, key);
  }

  [[nodiscard]] auto
  GetSMOStatus() const  //
      -> SMOStatus
  {
    switch (delta_type_) {
      case kSplit:
        return kSplitMayIncomplete;

      case kMerge:
        return kMergeMayIncomplete;

      default:
        return kNoPartialSMOs;
    }
  }

  auto
  SearchChildNode(  //
      const Key &key,
      const bool closed,
      uintptr_t &out_ptr,
      size_t &delta_num) const  //
      -> DeltaRC
  {
    auto has_smo = false;

    // traverse a delta chain
    for (const auto *cur_rec = this; true; cur_rec = cur_rec->GetNext(), ++delta_num) {
      switch (cur_rec->delta_type_) {
        case kInsert:
        case kDelete: {
          const auto &sep_key = cur_rec->key_;
          const auto &high_key = cur_rec->GetHighKey();
          if ((Comp{}(sep_key, key) || (!closed && !Comp{}(key, sep_key)))
              && (!high_key || Comp{}(key, *high_key) || (closed && !Comp{}(*high_key, key)))) {
            // this index-entry delta directly indicates a child node
            out_ptr = cur_rec->template GetPayload<uintptr_t>();
            return kRecordFound;
          }
          break;
        }

        case kSplit: {
          const auto &sep_key = cur_rec->key_;
          if (Comp{}(sep_key, key) || (!closed && !Comp{}(key, sep_key))) {
            // a sibling node includes a target key
            out_ptr = cur_rec->template GetPayload<uintptr_t>();
            return kKeyIsInSibling;
          }

          has_smo = true;
          break;
        }

        case kRemoveNode:
          return kNodeRemoved;

        case kMerge: {
          // check whether the merged node contains a target key
          const auto &sep_key = cur_rec->key_;
          if (Comp{}(sep_key, key) || (!closed && !Comp{}(key, sep_key))) {
            // check whether the merging is aborted
            const auto *sib_lid = cur_rec->template GetPayload<LogicalID *>();
            const auto *remove_d = sib_lid->template Load<DeltaRecord *>();
            if (remove_d == nullptr) return kNodeRemoved;  // the node is consolidated
            if (remove_d->delta_type_ == kRemoveNode) {
              // a target record may be in the merged node
              out_ptr = remove_d->next_;
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
          if (!has_smo) {
            const auto &high_key = cur_rec->GetHighKey();
            if (high_key && (Comp{}(*high_key, key) || (!closed && !Comp{}(key, *high_key)))) {
              // a sibling node includes a target key
              out_ptr = cur_rec->next_;
              return kKeyIsInSibling;
            }
          }

          // reach a base page
          out_ptr = reinterpret_cast<uintptr_t>(cur_rec);
          return kReachBaseNode;
        }
      }
    }
  }

  auto
  SearchRecord(  //
      const Key &key,
      uintptr_t &out_ptr,
      size_t &delta_num) const  //
      -> DeltaRC
  {
    auto has_smo = false;

    // traverse a delta chain
    for (const auto *cur_rec = this; true; cur_rec = cur_rec->GetNext(), ++delta_num) {
      switch (cur_rec->delta_type_) {
        case kInsert:
        case kModify: {
          // check whether a target record is inserted
          const auto &rec_key = cur_rec->key_;
          if (!Comp{}(key, rec_key) && !Comp{}(rec_key, key)) {
            out_ptr = reinterpret_cast<uintptr_t>(cur_rec);
            return kRecordFound;
          }
          break;
        }

        case kDelete: {
          // check whether a target record is deleted
          const auto &rec_key = cur_rec->key_;
          if (!Comp{}(key, rec_key) && !Comp{}(rec_key, key)) return kRecordDeleted;
          break;
        }

        case kSplit: {
          // check whether the right-sibling node contains a target key
          if (Comp{}(cur_rec->key_, key)) {
            out_ptr = cur_rec->template GetPayload<uintptr_t>();
            return kKeyIsInSibling;
          }

          has_smo = true;
          break;
        }

        case kRemoveNode:
          return kNodeRemoved;

        case kMerge: {
          // check whether the merged node contains a target key
          if (Comp{}(cur_rec->key_, key)) {
            // check whether the merging is aborted
            const auto *sib_lid = cur_rec->template GetPayload<LogicalID *>();
            const auto *remove_d = sib_lid->template Load<DeltaRecord *>();
            if (remove_d == nullptr) return kNodeRemoved;  // the node is consolidated
            if (remove_d->delta_type_ == kRemoveNode) {
              // a target record may be in the merged node
              out_ptr = remove_d->next_;
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
          if (!has_smo) {
            const auto &high_key = cur_rec->GetHighKey();
            if (high_key && Comp{}(*high_key, key)) {
              out_ptr = cur_rec->next_;
              return kKeyIsInSibling;
            }
          }

          // a target record may be in the base node
          out_ptr = reinterpret_cast<uintptr_t>(cur_rec);
          return kReachBaseNode;
        }
      }
    }
  }

  auto
  Validate(  //
      const Key &key,
      const bool closed,
      LogicalID *&sib_lid,
      size_t &delta_num) const  //
      -> DeltaRC
  {
    auto has_smo = false;

    // traverse a delta chain
    for (const auto *cur_rec = this; true; cur_rec = cur_rec->GetNext(), ++delta_num) {
      switch (cur_rec->delta_type_) {
        case kSplit: {
          // check whether the right-sibling node contains a target key
          const auto &sep_key = cur_rec->key_;
          if (Comp{}(sep_key, key) || (!closed && !Comp{}(key, sep_key))) {
            sib_lid = cur_rec->template GetPayload<LogicalID *>();
            return kKeyIsInSibling;
          }

          has_smo = true;
          break;
        }

        case kRemoveNode:
          return kNodeRemoved;

        case kMerge: {
          // check whether the merging is aborted and the sibling node includes a target key
          const auto &sep_key = cur_rec->key_;
          sib_lid = cur_rec->template GetPayload<LogicalID *>();
          const auto *remove_d = sib_lid->template Load<DeltaRecord *>();
          if (remove_d == nullptr) return kNodeRemoved;  // the node is consolidated
          if (remove_d->delta_type_ != kRemoveNode
              && (Comp{}(sep_key, key) || (!closed && !Comp{}(key, sep_key)))) {
            // merging was aborted, so check the sibling node
            return kKeyIsInSibling;
          }

          has_smo = true;
          break;
        }

        case kNotDelta: {
          // check whether the node contains a target key
          if (!has_smo) {
            const auto &high_key = cur_rec->GetHighKey();
            if (high_key && (Comp{}(*high_key, key) || (!closed && !Comp{}(key, *high_key)))) {
              sib_lid = cur_rec->template GetNext<LogicalID *>();
              return kKeyIsInSibling;
            }
          }

          return kReachBaseNode;
        }

        default:
          break;  // do nothing
      }
    }
  }

  template <class T>
  auto
  Sort(  //
      std::vector<std::pair<Key, const void *>> &records,
      std::vector<ConsolidateInfo> &consol_info) const  //
      -> std::pair<bool, int64_t>
  {
    std::optional<Key> sep_key = std::nullopt;
    const DeltaRecord *split_d = nullptr;

    // traverse and sort a delta chain
    int64_t count_diff = 0;
    for (const auto *cur_rec = this; true; cur_rec = cur_rec->GetNext()) {
      switch (const auto delta_type = cur_rec->delta_type_; delta_type) {
        case kInsert:
        case kModify:
        case kDelete: {
          // check whether this record is in a target node
          const auto &rec_key = cur_rec->key_;
          if (!sep_key || Comp{}(rec_key, *sep_key)
              || (!Comp{}(*sep_key, rec_key) && (cur_rec->IsLeaf() || delta_type != kInsert))) {
            // check uniqueness
            auto it = records.cbegin();
            const auto it_end = records.cend();
            for (; it != it_end && Comp{}(it->first, rec_key); ++it) {
              // skip smaller keys
            }
            if (it == it_end) {
              records.emplace_back(rec_key, cur_rec);
            } else if (Comp{}(rec_key, it->first)) {
              records.insert(it, std::make_pair(rec_key, cur_rec));
            }

            // update the page size
            if (delta_type == kInsert) {
              ++count_diff;
            } else if (delta_type == kDelete) {
              --count_diff;
            }
          }
          break;
        }

        case kSplit: {
          if (!sep_key || Comp{}(cur_rec->key_, *sep_key)) {
            // keep a separator key to exclude out-of-range records
            sep_key = cur_rec->key_;
            split_d = cur_rec;
          }
          break;
        }

        case kMerge: {
          // check whether the merging was aborted
          const auto *sib_lid = cur_rec->template GetPayload<LogicalID *>();
          const auto *remove_d = sib_lid->template Load<DeltaRecord *>();
          if (remove_d == nullptr) return {true, 0};        // the node is consolidated
          if (remove_d->delta_type_ != kRemoveNode) break;  // merging was aborted

          // keep the merged node and the corresponding separator key
          consol_info.emplace_back(remove_d->GetNext(), split_d);
          break;
        }

        case kNotDelta:
        default:
          consol_info.emplace_back(cur_rec, split_d);
          return {false, count_diff};
      }
    }
  }

 private:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  static constexpr size_t kHeaderLength = sizeof(DeltaRecord);

  /*####################################################################################
   * Internal getters/setters
   *##################################################################################*/

  /**
   * @return a highest key in a target record.
   */
  [[nodiscard]] constexpr auto
  GetHighKey() const  //
      -> std::optional<Key>
  {
    if (!has_high_key_) return std::nullopt;
    return high_key_;
  }

  template <class T>
  void
  SetPayload(const T &payload)
  {
    memcpy(payload_, &payload, sizeof(T));
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// a flag for indicating whether this node is a leaf or internal node.
  uint16_t node_type_ : 1;

  /// a flag for indicating the types of a delta record.
  uint16_t delta_type_ : 3;

  /// a flag for indicating whether this delta record has a lowest-key.
  uint16_t has_low_key_ : 1;

  /// a flag for indicating whether this delta record has a highest-key.
  uint16_t has_high_key_ : 1;

  /// a blank block for alignment.
  uint64_t : 0;

  /// the pointer to the next node.
  uintptr_t next_{kNullPtr};

  /// metadata of an embedded record
  Key key_{};

  /// metadata of a highest key
  Key high_key_{};

  /// an actual data block for records
  std::byte payload_[0]{};
};

}  // namespace dbgroup::index::bw_tree::component::fixlen

#endif  // BW_TREE_FIXLEN_DELTA_RECORD_HPP
