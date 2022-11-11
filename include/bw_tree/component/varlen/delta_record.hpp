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

#ifndef BW_TREE_COMPONENT_VARLEN_DELTA_RECORD_HPP
#define BW_TREE_COMPONENT_VARLEN_DELTA_RECORD_HPP

#include <optional>
#include <thread>
#include <utility>
#include <vector>

#include "bw_tree/component/consolidate_info.hpp"
#include "bw_tree/component/logical_id.hpp"
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
template <class Key_t, class Comp_t>
class alignas(kWordSize) DeltaRecord
{
 public:
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using Key = Key_t;
  using Comp = Comp_t;
  using Record = std::pair<Key, const void *>;

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
      : is_inner_{kLeaf}, delta_type_{delta_type}, meta_{kHeaderLen, key_len, key_len + sizeof(T)}
  {
    const auto offset = SetKey(kHeaderLen, key, key_len);
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
      : is_inner_{kLeaf}, delta_type_{kDelete}, meta_{kHeaderLen, key_len, key_len}
  {
    SetKey(kHeaderLen, key, key_len);
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
      : is_inner_{kInternal},
        delta_type_{kInsert},
        meta_{split_d->meta_},
        high_key_meta_{split_d->high_key_meta_}
  {
    // copy contents of a split delta
    const auto rec_len = meta_.rec_len + high_key_meta_.key_len;
    memcpy(&data_block_, &(split_d->data_block_), rec_len);
  }

  /**
   * @brief Construct a new delta record for deleting an index-entry.
   *
   * @param removed_child a removed child node.
   * @param left_lid the logical ID of a merged-left child (dummy nullptr).
   */
  DeltaRecord(  //
      const DeltaRecord *removed_node,
      [[maybe_unused]] const LogicalID *left_lid)
      : is_inner_{kInternal}, delta_type_{kDelete}
  {
    constexpr size_t kAlignMask = 0b111;

    // copy a lowest key
    const auto low_meta = removed_node->meta_;
    const auto low_key_len = low_meta.key_len;
    auto offset = ((kHeaderLen + kAlignMask + low_key_len) & ~kAlignMask) - low_key_len;
    meta_ = Metadata{offset, low_key_len, low_key_len + kPtrLen};
    memcpy(ShiftAddr(this, offset), removed_node->GetKeyAddr(low_meta), low_key_len);

    // set a sibling node
    offset += low_key_len;
    auto *payload = reinterpret_cast<std::atomic<LogicalID *> *>(ShiftAddr(this, offset));
    payload->store(nullptr, std::memory_order_relaxed);
    offset += kPtrLen;

    // copy a highest key
    const auto high_meta = removed_node->high_key_meta_;
    const auto high_key_len = high_meta.key_len;
    high_key_meta_ = Metadata{offset, high_key_len, high_key_len};
    memcpy(ShiftAddr(this, offset), removed_node->GetKeyAddr(high_meta), high_key_len);
  }

  /*####################################################################################
   * Public constructors for performing SMOs
   *##################################################################################*/

  /**
   * @brief Construct a new delta record for splitting/merging a node.
   *
   * @param delta_type a split or merge delta.
   * @param right_node a split/merged right node.
   * @param right_lid the address of a split/merged right node.
   * @param next a pointer to the next delta record or base node.
   */
  DeltaRecord(  //
      const DeltaType delta_type,
      const DeltaRecord *right_node,
      const void *right_lid,
      const DeltaRecord *next = nullptr)
      : is_inner_{right_node->is_inner_},
        delta_type_{delta_type},
        next_{reinterpret_cast<uintptr_t>(next)}
  {
    // copy a lowest key
    auto key_len = right_node->meta_.key_len;
    meta_ = Metadata{kHeaderLen, key_len, key_len + kPtrLen};
    memcpy(&data_block_, right_node->GetKeyAddr(right_node->meta_), key_len);

    // set a sibling node
    const auto offset = SetPayload(kHeaderLen + key_len, right_lid);

    // copy a highest key
    key_len = right_node->high_key_meta_.key_len;
    high_key_meta_ = Metadata{offset, key_len, key_len};
    memcpy(ShiftAddr(this, offset), right_node->GetKeyAddr(right_node->high_key_meta_), key_len);
  }

  /**
   * @brief Construct a new delta record for removing a node.
   *
   * @param removed_node a removed node.
   */
  explicit DeltaRecord(const bool is_leaf)
      : is_inner_{static_cast<uint16_t>(!is_leaf)}, delta_type_{kRemoveNode}
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
    return is_inner_ == kLeaf;
  }

  /**
   * @retval true if this node is leftmost in its tree level.
   * @retval false otherwise.
   */
  [[nodiscard]] constexpr auto
  IsLeftmost() const  //
      -> bool
  {
    return meta_.key_len == 0;
  }

  /**
   * @param key a target key to be compared.
   * @retval true if this record has the same key with a given one.
   * @retval false otherwise.
   */
  [[nodiscard]] constexpr auto
  HasSameKey(const Key &key) const  //
      -> bool
  {
    const auto &rec_key = GetKey();
    return !Comp{}(key, rec_key) && !Comp{}(rec_key, key);
  }

  /**
   * @param key a target key to be compared.
   * @param closed a flag for including the same key.
   * @retval true if the lowest key is less than or equal to a given key.
   * @retval false otherwise.
   */
  [[nodiscard]] constexpr auto
  LowKeyIsLE(  //
      const Key &key,
      const bool closed) const  //
      -> bool
  {
    const auto &low_key = GetKey();
    return Comp{}(low_key, key) || (closed && !Comp{}(key, low_key));
  }

  /**
   * @param key a target key to be compared.
   * @param closed a flag for including the same key.
   * @retval true if the highest key is greater than a given key.
   * @retval false otherwise.
   */
  [[nodiscard]] constexpr auto
  HighKeyIsGE(  //
      const Key &key,
      const bool closed) const  //
      -> bool
  {
    const auto &high_key = GetHighKey();
    return !high_key || Comp{}(key, *high_key) || (closed && !Comp{}(*high_key, key));
  }

  /**
   * @return the modification type of this delta record.
   */
  [[nodiscard]] constexpr auto
  GetDeltaType() const  //
      -> DeltaType
  {
    return DeltaType{delta_type_};
  }

  /**
   * @brief Get the next pointer of a delta record or a base node.
   *
   * @tparam T an expected class to be loaded.
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
    if constexpr (IsVarLenData<Key>()) {
      return reinterpret_cast<Key>(GetKeyAddr(meta_));
    } else {
      Key key{};
      memcpy(&key, GetKeyAddr(meta_), sizeof(Key));
      return key;
    }
  }

  /**
   * @return a lowest key in a target record if exist.
   */
  [[nodiscard]] constexpr auto
  GetLowKey() const  //
      -> std::optional<Key>
  {
    if (meta_.key_len > 0) return GetKey();
    return std::nullopt;
  }

  /**
   * @return a highest key in a target record if exist.
   */
  [[nodiscard]] constexpr auto
  GetHighKey() const  //
      -> std::optional<Key>
  {
    const auto key_len = high_key_meta_.key_len;
    if (key_len == 0) return std::nullopt;

    if constexpr (IsVarLenData<Key>()) {
      return reinterpret_cast<Key>(GetKeyAddr(high_key_meta_));
    } else {
      Key key{};
      memcpy(&key, GetKeyAddr(high_key_meta_), sizeof(Key));
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
   * @tparam T a class of expected payloads.
   * @return a payload in this record.
   */
  [[nodiscard]] auto
  GetPayloadAtomically() const  //
      -> uintptr_t
  {
    const auto *payload_addr = reinterpret_cast<std::atomic_uintptr_t *>(GetPayloadAddr());
    while (true) {
      for (size_t i = 1; true; ++i) {
        const auto payload = payload_addr->load(std::memory_order_relaxed);
        if (payload != kNullPtr) return payload;
        if (i >= kRetryNum) break;
        BW_TREE_SPINLOCK_HINT
      }
      std::this_thread::sleep_for(kShortSleep);
    }
  }

  /**
   * @brief Update the delta-modification type of this record with a given one.
   *
   * @param type a modification type to be updated.
   */
  void
  SetDeltaType(const DeltaType type)
  {
    delta_type_ = type;
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

  /**
   * @brief Set a merged-left child node to complete deleting an index-entry.
   *
   * @param left_lid the LID of a mereged-left child node.
   */
  void
  SetSiblingLID(LogicalID *left_lid)
  {
    auto *payload = reinterpret_cast<std::atomic<LogicalID *> *>(GetPayloadAddr());
    payload->store(left_lid, std::memory_order_relaxed);
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
  [[nodiscard]] static constexpr auto
  GetMaxDeltaSize()  //
      -> size_t
  {
    constexpr auto kKeyLen = (IsVarLenData<Key>()) ? kMaxVarDataSize : sizeof(Key);
    constexpr auto kPayLen = (sizeof(Payload) > kPtrLen) ? sizeof(Payload) : kPtrLen;
    return kHeaderLen + 2 * kKeyLen + kPayLen + kWordSize;
  }

  /**
   * @brief Insert this delta record to a given container.
   *
   * @tparam T a class of payloads.
   * @param sep_key an optional separator key.
   * @param records a set of records to be inserted this delta record.
   * @return the difference of a node size.
   */
  [[nodiscard]] auto
  AddByInsertionSortTo(  //
      const std::optional<Key> &sep_key,
      std::vector<std::pair<Key, const void *>> &records) const  //
      -> int64_t
  {
    // check whether this record is in a target node
    const auto &rec_key = GetKey();
    if (!sep_key || Comp{}(rec_key, *sep_key)) {
      // check uniqueness
      auto it = records.cbegin();
      const auto it_end = records.cend();
      for (; it != it_end && Comp{}(it->first, rec_key); ++it) {
        // skip smaller keys
      }
      if (it == it_end) {
        records.emplace_back(std::move(rec_key), this);
      } else if (Comp{}(rec_key, it->first)) {
        records.insert(it, std::make_pair(std::move(rec_key), this));
      }

      // update the page size
      const auto rec_size = meta_.rec_len + kMetaLen;
      if (delta_type_ == kInsert) return rec_size;
      if (delta_type_ == kDelete) return -rec_size;
    }

    return 0;
  }

 private:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  /// Header length in bytes.
  static constexpr size_t kHeaderLen = sizeof(DeltaRecord);

  /// the length of child pointers.
  static constexpr size_t kPtrLen = sizeof(LogicalID *);

  /// the length of record metadata.
  static constexpr size_t kMetaLen = sizeof(Metadata);

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
    return ShiftAddr(this, meta.offset);
  }

  /**
   * @return an address of a target payload.
   */
  [[nodiscard]] constexpr auto
  GetPayloadAddr() const  //
      -> void *
  {
    return ShiftAddr(this, meta_.offset + meta_.key_len);
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
    if constexpr (IsVarLenData<Key>()) {
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
  uint16_t is_inner_ : 1;

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

#endif  // BW_TREE_COMPONENT_VARLEN_DELTA_RECORD_HPP
