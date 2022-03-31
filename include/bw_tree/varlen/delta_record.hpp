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

#include "bw_tree/common/consolidate_info.hpp"
#include "bw_tree/common/logical_id.hpp"
#include "metadata.hpp"

namespace dbgroup::index::bw_tree::component::varlen
{
/**
 * @brief A class for representing delta records in Bw-tree.
 *
 * NOTE: this class fill a page from top to bottom (low_key => payload => high_key).
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
   * @param delta_type an insert or modify delta.
   * @param key a key to be inserted.
   * @param key_len the length of a key.
   * @param payload a payload to be inserted.
   */
  template <class T>
  DeltaRecord(  //
      const DeltaType delta_type,
      const Key &key,
      const size_t key_len,
      const T &payload)
      : is_leaf_{kLeaf}, delta_type_{delta_type}, meta_{kHeaderLength, key_len, key_len + sizeof(T)}
  {
    const auto offset = SetKey(kHeaderLength, key, key_len);
    SetPayload(offset, payload);
  }

  /**
   * @brief Construct a new delta record for deleting a record.
   *
   * @param key a key to be deleted.
   * @param key_len the length of a key.
   */
  DeltaRecord(  //
      const Key &key,
      const size_t key_len)
      : is_leaf_{kLeaf}, delta_type_{kDelete}, meta_{kHeaderLength, key_len, key_len}
  {
    SetKey(kHeaderLength, key, key_len);
  }

  /*####################################################################################
   * Public constructors for inserting/deleting records in internal nodes
   *##################################################################################*/

  /**
   * @brief Construct a new delta record for inserting an index-entry.
   *
   * @param split_d a child split-delta record.
   */
  explicit DeltaRecord(const DeltaRecord *split_d)
      : is_leaf_{kInternal},
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
   * @param merge_d a child merge-delta record.
   * @param left_lid the logical ID of a merged-left child.
   */
  DeltaRecord(  //
      const DeltaRecord *merge_d,
      const LogicalID *left_lid)
      : is_leaf_{kInternal},
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
   * @param delta_type a split or merge delta.
   * @param right_node a split/merged right node.
   * @param right_lid the logical ID of a split/merged right node.
   * @param next a pointer to the next delta record or base node.
   */
  DeltaRecord(  //
      const DeltaType delta_type,
      const DeltaRecord *right_node,
      const LogicalID *right_lid,
      const DeltaRecord *next = nullptr)
      : is_leaf_{right_node->is_leaf_},
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
   * @param dummy a dummy delta type for distinguishing constructors.
   * @param removed_node a remove node.
   */
  DeltaRecord(  //
      [[maybe_unused]] const DeltaType dummy,
      const DeltaRecord *removed_node)
      : is_leaf_{removed_node->is_leaf_},
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
   * @retval false otherwise.
   */
  [[nodiscard]] constexpr auto
  IsLeaf() const  //
      -> bool
  {
    return is_leaf_;
  }

  /**
   * @retval true if this is a base node.
   * @retval false otherwise.
   */
  [[nodiscard]] constexpr auto
  IsBaseNode() const  //
      -> bool
  {
    return delta_type_ == kNotDelta;
  }

  /**
   * @retval true if this is a remove-node-delta record.
   * @retval false otherwise.
   */
  [[nodiscard]] constexpr auto
  IsRemoveNodeDelta() const  //
      -> bool
  {
    return delta_type_ == kRemoveNode;
  }

  /**
   * @retval true if this is a merge-delta record.
   * @retval false otherwise.
   */
  [[nodiscard]] constexpr auto
  IsMergeDelta() const  //
      -> bool
  {
    return delta_type_ == kMerge;
  }

  /**
   * @brief Get the next pointer of a delta record or a base node.
   *
   * @tparam T a expected class to be loaded.
   * @return a pointer to the next object.
   */
  template <class T = DeltaRecord *>
  [[nodiscard]] constexpr auto
  GetNext() const  //
      -> T
  {
    return reinterpret_cast<T>(next_);
  }

  /**
   * @return a key in this record.
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
   * @tparam T a class of expected payloads.
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

  /**
   * @brief Set a given pointer as the next one.
   *
   * @param next a pointer to be set as the next one.
   */
  void
  SetNext(const DeltaRecord *next)
  {
    next_ = reinterpret_cast<uintptr_t>(next);
  }

  /**
   * @brief Remove the next pointer from this record.
   *
   */
  void
  Abort()
  {
    next_ = kNullPtr;
  }

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  /**
   * @brief Compute the maximum size of delta records with given template classes.
   *
   * @tparam Payload a class of payloads.
   * @return the maximum size of delta records in bytes.
   */
  template <class Payload>
  static constexpr auto
  GetMaxDeltaSize()  //
      -> size_t
  {
    constexpr auto kKeyLen = (IsVariableLengthData<Key>()) ? kMaxVarDataSize : sizeof(Key);
    constexpr auto kPayLen = (sizeof(Payload) > kWordSize) ? sizeof(Payload) : kWordSize;
    return kHeaderLength + 2 * kKeyLen + kPayLen;
  }

  /**
   * @brief Check the lowest key of this node is equivalent with a given key.
   *
   * Note that this function traverses a delta-chain and use a base node for check.
   *
   * @param key a target key to be compared.
   * @retval true if the keys are same.
   * @retval false otherwise.
   */
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

  /**
   * @retval kSplitMayIncomplete if this delta-chain may includes a partial splitting.
   * @retval kMergeMayIncomplete if this delta-chain may includes a partial merging.
   * @retval kNoPartialSMOs otherwise.
   */
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

  /**
   * @brief Traverse a delta-chain to search a child node with a given key.
   *
   * @param key a target key to be searched.
   * @param closed a flag for indicating closed/open-interval.
   * @param out_ptr an output pointer if needed.
   * @param out_delta_num the number of records in this delta-chain.
   * @retval kRecordFound if a delta record (in out_ptr) has a corresponding child.
   * @retval kReachBaseNode if a base node (in out_ptr) has a corresponding child.
   * @retval kKeyIsInSibling if the target key is not in this node due to other SMOs.
   * @retval kNodeRemoved if this node is removed by other SMOs.
   */
  auto
  SearchChildNode(  //
      const Key &key,
      const bool closed,
      uintptr_t &out_ptr,
      size_t &out_delta_num) const  //
      -> DeltaRC
  {
    auto has_smo = false;

    // traverse a delta chain
    for (const auto *cur_rec = this; true; cur_rec = cur_rec->GetNext(), ++out_delta_num) {
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

  /**
   * @brief Traverse a delta-chain to search a record with a given key.
   *
   * @param key a target key to be searched.
   * @param out_ptr an output pointer if needed.
   * @param out_delta_num the number of records in this delta-chain.
   * @retval kRecordFound if a delta record (in out_ptr) has the given key.
   * @retval kReachBaseNode if a base node (in out_ptr) may have the given key.
   * @retval kKeyIsInSibling if the target key is not in this node due to other SMOs.
   * @retval kNodeRemoved if this node is removed by other SMOs.
   */
  auto
  SearchRecord(  //
      const Key &key,
      uintptr_t &out_ptr,
      size_t &out_delta_num) const  //
      -> DeltaRC
  {
    auto has_smo = false;

    // traverse a delta chain
    for (const auto *cur_rec = this; true; cur_rec = cur_rec->GetNext(), ++out_delta_num) {
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

  /**
   * @brief Traverse a delta-chain to check this node is valid for modifying this tree.
   *
   * @param key a target key to be searched.
   * @param closed a flag for indicating closed/open-interval.
   * @param out_sib_lid the logical ID of a sibling node.
   * @param out_delta_num the number of records in this delta-chain.
   * @retval kReachBaseNode if this node is valid for the given key.
   * @retval kKeyIsInSibling if the target key is not in this node due to other SMOs.
   * @retval kNodeRemoved if this node is removed by other SMOs.
   */
  auto
  Validate(  //
      const Key &key,
      const bool closed,
      LogicalID *&out_sib_lid,
      size_t &out_delta_num) const  //
      -> DeltaRC
  {
    auto has_smo = false;

    // traverse a delta chain
    for (const auto *cur_rec = this; true; cur_rec = cur_rec->GetNext(), ++out_delta_num) {
      switch (cur_rec->delta_type_) {
        case kSplit: {
          // check whether the right-sibling node contains a target key
          const auto &sep_key = cur_rec->GetKey();
          if (Comp{}(sep_key, key) || (!closed && !Comp{}(key, sep_key))) {
            out_sib_lid = cur_rec->template GetPayload<LogicalID *>();
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
          out_sib_lid = cur_rec->template GetPayload<LogicalID *>();
          const auto *remove_d = out_sib_lid->template Load<DeltaRecord *>();
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
              out_sib_lid = cur_rec->template GetNext<LogicalID *>();
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

  /**
   * @brief Sort delta records for consolidation.
   *
   * @tparam T a class of expected payloads.
   * @param records a vector for storing sorted records.
   * @param consol_info a vector for storing base nodes and corresponding separator keys.
   * @retval 1st: true if this node has been already consolidated.
   * @retval 2nd: the difference of node size in bytes.
   */
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
   * Internal constants
   *##################################################################################*/

  /// Header length in bytes.
  static constexpr size_t kHeaderLength = sizeof(DeltaRecord);

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
   * @return a highest key in a target record if exist.
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
   * @brief Set a target key directly.
   *
   * @param offset an offset to set a target key.
   * @param key a target key to be set.
   * @param key_len the length of a target key.
   * @return an offset to the top of free space.
   */
  auto
  SetKey(  //
      size_t offset,
      const Key &key,
      [[maybe_unused]] const size_t key_len)  //
      -> size_t
  {
    if constexpr (IsVariableLengthData<Key>()) {
      memcpy(ShiftAddr(this, offset), key, key_len);
      offset += key_len;
    } else {
      memcpy(ShiftAddr(this, offset), &key, sizeof(Key));
      offset += sizeof(Key);
    }

    return offset;
  }

  /**
   * @brief Set a target payload directly.
   *
   * @tparam T a class of expected payloads.
   * @param offset an offset to set a target payload.
   * @param payload a target payload to be set.
   * @return an offset to the top of free space.
   */
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

  /// a flag for indicating whether this node is a leaf or internal node.
  uint16_t is_leaf_ : 1;

  /// a flag for indicating the types of delta records.
  uint16_t delta_type_ : 3;

  /// a blank block for alignment.
  uint64_t : 0;

  /// the pointer to the next node or delta record.
  uintptr_t next_{kNullPtr};

  /// metadata of an embedded record.
  Metadata meta_{};

  /// metadata of a highest key.
  Metadata high_key_meta_{};

  /// an actual data block for records.
  std::byte data_block_[0]{};
};

}  // namespace dbgroup::index::bw_tree::component::varlen

#endif  // BW_TREE_VARLEN_DELTA_RECORD_HPP
