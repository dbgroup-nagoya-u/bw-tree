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

#ifndef BW_TREE_VARLEN_DELTA_RECORD_HPP
#define BW_TREE_VARLEN_DELTA_RECORD_HPP

#include <optional>
#include <utility>
#include <vector>

#include "bw_tree/common/common.hpp"
#include "bw_tree/common/consolidate_info.hpp"
#include "bw_tree/common/logical_id.hpp"
#include "metadata.hpp"

namespace dbgroup::index::bw_tree::component::varlen
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
   * @param key_length
   * @param payload
   */
  template <class T>
  DeltaRecord(  //
      const DeltaType delta_type,
      const Key &key,
      const size_t key_len,
      const T &payload)
      : node_type_{kLeaf},
        delta_type_{delta_type},
        meta_{kHeaderLength, key_len, key_len + sizeof(T)}
  {
    const auto offset = SetKey(kHeaderLength, key, key_len);
    SetPayload(offset, payload);
  }

  /**
   * @brief Construct a new delta record for deleting a record.
   *
   * @param key
   * @param key_length
   */
  DeltaRecord(  //
      const Key &key,
      const size_t key_len)
      : node_type_{kLeaf}, delta_type_{kDelete}, meta_{kHeaderLength, key_len, key_len}
  {
    SetKey(kHeaderLength, key, key_len);
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
        meta_{split_d->meta_},
        high_key_meta_{split_d->high_key_meta_}
  {
    // copy contents of a split delta
    const auto rec_len = meta_.GetTotalLength() + high_key_meta_.GetKeyLength();
    memcpy(&data_block_, &(split_d->data_block_), rec_len);
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
        meta_{merge_d->meta_},
        high_key_meta_{merge_d->high_key_meta_}
  {
    // copy contents of a merge delta
    const auto rec_len = meta_.GetTotalLength() + high_key_meta_.GetKeyLength();
    memcpy(&data_block_, &(merge_d->data_block_), rec_len);

    // update logical ID of a sibling node
    SetPayload(kHeaderLength + meta_.GetKeyLength(), left_lid);
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
        next_{reinterpret_cast<uintptr_t>(next)}
  {
    // copy a lowest key
    auto key_len = right_node->meta_.GetKeyLength();
    meta_ = Metadata{kHeaderLength, key_len, key_len + kWordSize};
    memcpy(&data_block_, right_node->GetKeyAddr(right_node->meta_), key_len);

    // set a sibling node
    const auto offset = SetPayload(kHeaderLength + key_len, right_lid);

    // copy a highest key
    key_len = right_node->high_key_meta_.GetKeyLength();
    high_key_meta_ = Metadata{offset, key_len, key_len};
    memcpy(ShiftAddr(this, offset), right_node->GetKeyAddr(right_node->high_key_meta_), key_len);
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
    if constexpr (IsVariableLengthData<Key>()) {
      return reinterpret_cast<Key>(GetKeyAddr(meta_));
    } else {
      Key key{};
      memcpy(&key, GetKeyAddr(meta_), sizeof(Key));
      return key;
    }
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
    memcpy(&payload, GetPayloadAddr(), sizeof(T));
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

    if (head->meta_.GetKeyLength() == 0) return false;
    return IsEqual<Comp>(head->GetKey(), key);
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
          const auto &sep_key = cur_rec->GetKey();
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
          const auto &sep_key = cur_rec->GetKey();
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
          const auto &sep_key = cur_rec->GetKey();
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
          const auto &rec_key = cur_rec->GetKey();
          if (!Comp{}(key, rec_key) && !Comp{}(rec_key, key)) {
            out_ptr = reinterpret_cast<uintptr_t>(cur_rec);
            return kRecordFound;
          }
          break;
        }

        case kDelete: {
          // check whether a target record is deleted
          const auto &rec_key = cur_rec->GetKey();
          if (!Comp{}(key, rec_key) && !Comp{}(rec_key, key)) return kRecordDeleted;
          break;
        }

        case kSplit: {
          // check whether the right-sibling node contains a target key
          const auto &sep_key = cur_rec->GetKey();
          if (Comp{}(sep_key, key)) {
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
          const auto &sep_key = cur_rec->GetKey();
          if (Comp{}(sep_key, key)) {
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
          const auto &sep_key = cur_rec->GetKey();
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
          const auto &sep_key = cur_rec->GetKey();
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
    int64_t size_diff = 0;
    for (const auto *cur_rec = this; true; cur_rec = cur_rec->GetNext()) {
      switch (const auto delta_type = cur_rec->delta_type_; delta_type) {
        case kInsert:
        case kModify:
        case kDelete: {
          // check whether this record is in a target node
          const auto &rec_key = cur_rec->GetKey();
          if (!sep_key || Comp{}(rec_key, *sep_key)
              || (!Comp{}(*sep_key, rec_key) && (cur_rec->IsLeaf() || delta_type != kInsert))) {
            // check uniqueness
            auto it = records.cbegin();
            const auto it_end = records.cend();
            for (; it != it_end && Comp{}(it->first, rec_key); ++it) {
              // skip smaller keys
            }
            if (it == it_end) {
              records.emplace_back(std::move(rec_key), cur_rec);
            } else if (Comp{}(rec_key, it->first)) {
              records.insert(it, std::make_pair(std::move(rec_key), cur_rec));
            }

            // update the page size
            const auto rec_size = cur_rec->meta_.GetKeyLength() + sizeof(T) + kWordSize;
            if (delta_type == kInsert) {
              size_diff += rec_size;
            } else if (delta_type == kDelete) {
              size_diff -= rec_size;
            }
          }
          break;
        }

        case kSplit: {
          const auto &cur_key = cur_rec->GetKey();
          if (!sep_key || Comp{}(cur_key, *sep_key)) {
            // keep a separator key to exclude out-of-range records
            sep_key = cur_key;
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
          return {false, size_diff};
      }
    }
  }

 private:
  /*####################################################################################
   * Internal getters/setters
   *##################################################################################*/

  /**
   * @param meta metadata of a corresponding record.
   * @return an address of a target key.
   */
  [[nodiscard]] constexpr auto
  GetKeyAddr(const Metadata meta) const  //
      -> void *
  {
    return ShiftAddr(this, meta.GetOffset());
  }

  /**
   * @return a highest key in a target record.
   */
  [[nodiscard]] constexpr auto
  GetHighKey() const  //
      -> std::optional<Key>
  {
    const auto key_len = high_key_meta_.GetKeyLength();
    if (key_len == 0) return std::nullopt;

    if constexpr (IsVariableLengthData<Key>()) {
      return reinterpret_cast<Key>(GetKeyAddr(high_key_meta_));
    } else {
      Key key{};
      memcpy(&key, GetKeyAddr(high_key_meta_), sizeof(Key));
      return key;
    }
  }

  /**
   * @return an address of a target payload.
   */
  [[nodiscard]] constexpr auto
  GetPayloadAddr() const  //
      -> void *
  {
    return ShiftAddr(this, meta_.GetOffset() + meta_.GetKeyLength());
  }

  /**
   * @brief Set a target data directly.
   *
   * @tparam T a class of data.
   * @param offset an offset to set a target data.
   * @param payload a target payload to be set.
   * @param payload_length the length of a target payload.
   */
  template <class T>
  auto
  SetKey(  //
      size_t offset,
      const T &key,
      [[maybe_unused]] const size_t key_len)  //
      -> size_t
  {
    if constexpr (IsVariableLengthData<T>()) {
      memcpy(ShiftAddr(this, offset), key, key_len);
      offset += key_len;
    } else {
      memcpy(ShiftAddr(this, offset), &key, sizeof(T));
      offset += sizeof(T);
    }

    return offset;
  }

  template <class T>
  auto
  SetPayload(  //
      size_t offset,
      const T &payload)  //
      -> size_t
  {
    memcpy(ShiftAddr(this, offset), &payload, sizeof(T));
    offset += sizeof(T);
    return offset;
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// a flag to indicate whether this node is a leaf or internal node.
  uint16_t node_type_ : 1;

  /// a flag for indicating the types of a delta record.
  uint16_t delta_type_ : 3;

  /// a blank block for alignment.
  uint64_t : 0;

  /// the pointer to the next node.
  uintptr_t next_{kNullPtr};

  /// metadata of an embedded record
  Metadata meta_{};

  /// metadata of a highest key
  Metadata high_key_meta_{};

  /// an actual data block for records
  std::byte data_block_[0]{};
};

}  // namespace dbgroup::index::bw_tree::component::varlen

#endif  // BW_TREE_VARLEN_DELTA_RECORD_HPP
