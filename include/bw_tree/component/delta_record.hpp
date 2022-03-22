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

#ifndef BW_TREE_COMPONENT_DELTA_RECORD_HPP
#define BW_TREE_COMPONENT_DELTA_RECORD_HPP

#include <atomic>
#include <optional>
#include <utility>
#include <vector>

#include "common.hpp"
#include "metadata.hpp"
#include "node_info.hpp"

namespace dbgroup::index::bw_tree::component
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
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using NodeStack = std::vector<std::atomic_uintptr_t *>;

 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new delta record for leaf-insert/modify.
   *
   * @tparam T a target payload class.
   * @param delta_type
   * @param key
   * @param key_length
   * @param payload
   * @param payload_length
   */
  template <class T>
  DeltaRecord(  //
      const DeltaType delta_type,
      const Key &key,
      const size_t key_len,
      const T &payload,
      const size_t pay_len)
      : node_type_{NodeType::kLeaf},
        delta_type_{delta_type},
        meta_{kHeaderLength, key_len, key_len + pay_len}
  {
    auto offset = SetKey(kHeaderLength, key, key_len);
    SetPayload(offset, payload);
  }

  /**
   * @brief Construct a new delta record for leaf-delete.
   *
   * @param key
   * @param key_length
   */
  DeltaRecord(  //
      const Key &key,
      const size_t key_len)
      : node_type_{NodeType::kLeaf},
        delta_type_{DeltaType::kDelete},
        meta_{kHeaderLength, key_len, key_len}
  {
    SetKey(kHeaderLength, key, key_len);
  }

  /**
   * @brief Construct a new delta record for split/merge.
   *
   * @param split_delta
   */
  DeltaRecord(  //
      const DeltaType delta_type,
      const void *right_ptr,
      const std::atomic_uintptr_t *sib_page_id,
      const uintptr_t next = kNullPtr)
      : delta_type_{delta_type}, next_{next}
  {
    const auto *right_node = reinterpret_cast<const DeltaRecord *>(right_ptr);
    node_type_ = right_node->node_type_;

    // copy a lowest key
    auto key_len = right_node->meta_.GetKeyLength();
    meta_ = Metadata{kHeaderLength, key_len, key_len + kWordSize};
    memcpy(&data_block_, right_node->GetKeyAddr(right_node->meta_), key_len);

    // set a sibling node
    const auto offset = SetPayload(kHeaderLength + key_len, sib_page_id);

    // copy a highest key
    key_len = right_node->high_key_meta_.GetKeyLength();
    high_key_meta_ = Metadata{offset, key_len, key_len};
    memcpy(ShiftAddr(this, offset), right_node->GetKeyAddr(right_node->high_key_meta_), key_len);
  }

  /**
   * @brief Construct a new delta record for index-entries.
   *
   * @param split_delta
   */
  explicit DeltaRecord(const DeltaRecord *delta)
      : node_type_{NodeType::kInternal}, delta_type_{DeltaType::kInsert}
  {
    // copy contents of a split delta
    meta_ = delta->meta_;
    high_key_meta_ = delta->high_key_meta_;
    auto rec_len = meta_.GetTotalLength() + high_key_meta_.GetTotalLength();
    memcpy(&data_block_, &(delta->data_block_), rec_len);
  }

  /**
   * @brief Construct a new delta record for removing-node.
   *
   * @param next a pointer to the removed node.
   */
  explicit DeltaRecord(const uintptr_t next) : delta_type_{DeltaType::kRemoveNode}, next_{next}
  {
    auto *removed_node = reinterpret_cast<DeltaRecord *>(next);
    node_type_ = removed_node->node_type_;
  }

  explicit DeltaRecord(  //
      const DeltaRecord *delta,
      const std::atomic_uintptr_t *sib_page)
      : node_type_{NodeType::kInternal}, delta_type_{DeltaType::kDelete}
  {
    // copy contents of a merge delta
    meta_ = delta->meta_;
    high_key_meta_ = delta->high_key_meta_;
    auto rec_len = meta_.GetTotalLength() + high_key_meta_.GetTotalLength();
    memcpy(&data_block_, &(delta->data_block_), rec_len);

    // update a sibling node ID
    SetPayload(kHeaderLength + meta_.GetKeyLength(), sib_page);
  }

  constexpr DeltaRecord(const DeltaRecord &) = default;
  constexpr DeltaRecord(DeltaRecord &&) noexcept = default;

  constexpr auto operator=(const DeltaRecord &) -> DeltaRecord & = default;
  constexpr auto operator=(DeltaRecord &&) noexcept -> DeltaRecord & = default;

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

  [[nodiscard]] constexpr auto
  GetNext() const  //
      -> uintptr_t
  {
    return next_;
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
  SetNext(uintptr_t next)
  {
    next_ = next;
  }

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  static auto
  GetSMOStatus(const uintptr_t ptr)  //
      -> SMOStatus
  {
    const auto *delta = reinterpret_cast<const DeltaRecord *>(ptr);
    switch (delta->delta_type_) {
      case kSplit:
        return kSplitMayIncomplete;

      case kMerge:
        return kMergeMayIncomplete;

      default:
        return kNoPartialSMOs;
    }
  }

  static auto
  SearchChildNode(  //
      const Key &key,
      const bool closed,
      uintptr_t &out_ptr,
      size_t &delta_num)  //
      -> DeltaRC
  {
    auto has_smo = false;

    // traverse a delta chain
    auto *cur_rec = reinterpret_cast<DeltaRecord *>(out_ptr);
    while (true) {
      switch (cur_rec->delta_type_) {
        case kInsert:
        case kDelete: {
          const auto &sep_key = cur_rec->GetKey();
          const auto &high_key = cur_rec->GetHighKey();
          if ((Comp{}(sep_key, key) || (!closed && !Comp{}(key, sep_key)))
              && (!high_key || Comp{}(key, *high_key) || (closed && !Comp{}(*high_key, key)))) {
            // this index-entry delta directly indicates a child node
            out_ptr = cur_rec->template GetPayload<uintptr_t>();
            assert(out_ptr != kNullPtr);
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
            out_ptr = cur_rec->template GetPayload<uintptr_t>();
            const auto *sib_page = reinterpret_cast<std::atomic_uintptr_t *>(out_ptr);
            const auto sib_ptr = sib_page->load(std::memory_order_acquire);
            if (sib_ptr == kNullPtr) return kNodeRemoved;  // the node is consolidated
            const auto *remove_d = reinterpret_cast<DeltaRecord *>(sib_ptr);
            if (remove_d->delta_type_ == kRemoveNode) {
              // a target record may be in the merged node
              out_ptr = remove_d->next_;
              return kReachBaseNode;
            }
            // merging was aborted, so check the sibling node
            out_ptr = reinterpret_cast<uintptr_t>(sib_page);
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
          return kReachBaseNode;
        }
      }

      // go to the next delta record or base node
      out_ptr = cur_rec->next_;
      cur_rec = reinterpret_cast<DeltaRecord *>(out_ptr);
      ++delta_num;
    }
  }

  static auto
  SearchRecord(  //
      const Key &key,
      uintptr_t ptr,
      uintptr_t &out_ptr,
      size_t &delta_num)  //
      -> DeltaRC
  {
    auto has_smo = false;

    // traverse a delta chain
    auto *cur_rec = reinterpret_cast<DeltaRecord *>(ptr);
    while (true) {
      switch (cur_rec->delta_type_) {
        case kInsert:
        case kModify: {
          // check whether a target record is inserted
          const auto &rec_key = cur_rec->GetKey();
          if (!Comp{}(key, rec_key) && !Comp{}(rec_key, key)) {
            out_ptr = ptr;
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

        case kRemoveNode: {
          return kNodeRemoved;
        }

        case kMerge: {
          // check whether the merged node contains a target key
          const auto &sep_key = cur_rec->GetKey();
          if (Comp{}(sep_key, key)) {
            // check whether the merging is aborted
            const auto *sib_page = cur_rec->template GetPayload<std::atomic_uintptr_t *>();
            const auto sib_ptr = sib_page->load(std::memory_order_acquire);
            if (sib_ptr == kNullPtr) return kNodeRemoved;  // the node is consolidated
            const auto *remove_d = reinterpret_cast<DeltaRecord *>(sib_ptr);
            if (remove_d->delta_type_ == kRemoveNode) {
              // a target record may be in the merged node
              out_ptr = remove_d->next_;
              return kReachBaseNode;
            }
            // merging was aborted, so check the sibling node
            out_ptr = reinterpret_cast<uintptr_t>(sib_page);
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
          out_ptr = ptr;
          return kReachBaseNode;
        }
      }

      // go to the next delta record or base node
      ptr = cur_rec->next_;
      cur_rec = reinterpret_cast<DeltaRecord *>(ptr);
      ++delta_num;
    }
  }

  static auto
  Validate(  //
      const Key &key,
      const bool closed,
      uintptr_t ptr,
      uintptr_t &out_ptr,
      size_t &delta_num)  //
      -> DeltaRC
  {
    auto has_smo = false;

    // traverse a delta chain
    auto *cur_rec = reinterpret_cast<DeltaRecord *>(ptr);
    while (true) {
      switch (cur_rec->delta_type_) {
        case kSplit: {
          // check whether the right-sibling node contains a target key
          const auto &sep_key = cur_rec->GetKey();
          if (Comp{}(sep_key, key) || (!closed && !Comp{}(key, sep_key))) {
            out_ptr = cur_rec->template GetPayload<uintptr_t>();
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
          const auto *sib_page = cur_rec->template GetPayload<std::atomic_uintptr_t *>();
          const auto sib_ptr = sib_page->load(std::memory_order_acquire);
          if (sib_ptr == kNullPtr) return kNodeRemoved;  // the node is consolidated
          const auto *remove_d = reinterpret_cast<DeltaRecord *>(sib_ptr);
          if (remove_d->delta_type_ != kRemoveNode
              && (Comp{}(sep_key, key) || (!closed && !Comp{}(key, sep_key)))) {
            // merging was aborted, so check the sibling node
            out_ptr = reinterpret_cast<uintptr_t>(sib_page);
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
              out_ptr = cur_rec->next_;
              return kKeyIsInSibling;
            }
          }

          return kReachBaseNode;
        }

        default:
          break;  // do nothing
      }

      // go to the next delta record or base node
      ptr = cur_rec->next_;
      cur_rec = reinterpret_cast<DeltaRecord *>(ptr);
      ++delta_num;
    }
  }

  template <class T>
  static auto
  Sort(  //
      uintptr_t ptr,
      std::vector<std::pair<Key, uintptr_t>> &records,
      std::vector<NodeInfo> &nodes)  //
      -> std::pair<bool, int64_t>
  {
    std::optional<Key> sep_key{std::nullopt};
    uintptr_t split_d_ptr{kNullPtr};

    // traverse and sort a delta chain
    auto *cur_rec = reinterpret_cast<DeltaRecord *>(ptr);
    int64_t size_diff = 0;
    while (true) {
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
              records.emplace_back(std::move(rec_key), ptr);
            } else if (Comp{}(rec_key, it->first)) {
              records.insert(it, std::make_pair(std::move(rec_key), ptr));
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
            split_d_ptr = ptr;
          }
          break;
        }

        case kMerge: {
          // check whether the merging was aborted
          const auto *sib_page = cur_rec->template GetPayload<std::atomic_uintptr_t *>();
          const auto sib_ptr = sib_page->load(std::memory_order_acquire);
          if (sib_ptr == kNullPtr) return {true, 0};  // the node is consolidated
          const auto *remove_d = reinterpret_cast<DeltaRecord *>(sib_ptr);
          if (remove_d->delta_type_ != kRemoveNode) break;  // merging was aborted

          // keep the merged node and the corresponding separator key
          nodes.emplace_back(remove_d->next_, split_d_ptr);
          break;
        }

        case kNotDelta:
        default:
          nodes.emplace_back(ptr, split_d_ptr);
          return {false, size_diff};
      }

      // go to the next delta record or base node
      ptr = cur_rec->next_;
      cur_rec = reinterpret_cast<DeltaRecord *>(ptr);
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

}  // namespace dbgroup::index::bw_tree::component

#endif  // BW_TREE_COMPONENT_DELTA_RECORD_HPP
