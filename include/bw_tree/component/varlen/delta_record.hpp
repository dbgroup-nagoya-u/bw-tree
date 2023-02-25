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

// C++ standard libraries
#include <optional>
#include <thread>
#include <utility>
#include <vector>

// local sources
#include "bw_tree/component/logical_id.hpp"
#include "bw_tree/component/varlen/metadata.hpp"

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
class DeltaRecord
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
   * Public constructors for performing SMOs
   *##################################################################################*/

  /**
   * @brief Construct a new delta record for insert-entry/merging a node.
   *
   * @param d_type a insert-entry or merge delta.
   * @param r_node a split/merged right node.
   * @param r_addr the address of a split/merged right node.
   */
  DeltaRecord(  //
      const DeltaType d_type,
      const DeltaRecord *r_node,
      const void *r_addr)
      : is_inner_{d_type == kInsert ? static_cast<uint16_t>(kInner) : r_node->is_inner_},
        delta_type_{d_type}
  {
    // copy a lowest key
    auto key_len = r_node->meta_.key_len;
    meta_ = Metadata{kHeaderLen, key_len, key_len + kPtrLen};
    memcpy(&data_block_, r_node->GetKeyAddr(r_node->meta_), key_len);

    // set a sibling node
    const auto offset = SetPayload(kHeaderLen + key_len, r_addr);

    // copy a highest key
    key_len = r_node->high_key_meta_.key_len;
    high_key_meta_ = Metadata{offset, key_len, key_len};
    memcpy(ShiftAddr(this, offset), r_node->GetKeyAddr(r_node->high_key_meta_), key_len);
  }

  /**
   * @brief Construct a new delta record for deleting an index-entry.
   *
   * @param removed_child a removed child node.
   * @param left_lid the logical ID of a merged-left child (dummy nullptr).
   */
  explicit DeltaRecord(const DeltaRecord *removed_node) : is_inner_{kInner}, delta_type_{kDelete}
  {
    // copy a lowest key
    const auto low_meta = removed_node->meta_;
    const auto low_key_len = low_meta.key_len;
    auto offset = ((kHeaderLen + kWordAlign + low_key_len) & ~kWordAlign) - low_key_len;
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
   * Public getters/setters for a header
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
   * @retval true if there is a delta chain.
   * @retval false otherwise.
   */
  [[nodiscard]] constexpr auto
  NeedConsolidation() const  //
      -> bool
  {
    return delta_type_ != kNotDelta
           && (rec_count_ >= kDeltaRecordThreshold || node_size_ > kPageSize);
  }

  /**
   * @retval true if there is a delta chain.
   * @retval false otherwise.
   */
  [[nodiscard]] constexpr auto
  NeedWaitSMOs() const  //
      -> bool
  {
    return delta_type_ != kNotDelta && (rec_count_ >= kMaxDeltaRecordNum || node_size_ > kPageSize);
  }

  /**
   * @return the modification type of this delta record.
   */
  [[nodiscard]] constexpr auto
  GetDeltaType() const  //
      -> DeltaType
  {
    return static_cast<DeltaType>(delta_type_);
  }

  /**
   * @return the byte length of this node.
   */
  [[nodiscard]] constexpr auto
  GetNodeSize() const  //
      -> size_t
  {
    return node_size_;
  }

  /**
   * @return the number of delta records in this chain.
   */
  [[nodiscard]] constexpr auto
  GetRecordCount() const  //
      -> size_t
  {
    return rec_count_;
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
  SetNext(  //
      const DeltaRecord *next,
      const int64_t diff)
  {
    rec_count_ = (next->delta_type_ == kNotDelta) ? 1 : next->rec_count_ + 1;
    node_size_ = next->node_size_ + diff;
    next_ = reinterpret_cast<uintptr_t>(next);
  }

  /*####################################################################################
   * Public getters/setters for records
   *##################################################################################*/

  /**
   * @return the length of a key in this record.
   */
  [[nodiscard]] constexpr auto
  GetKeyLength() const  //
      -> size_t
  {
    return meta_.key_len;
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
   * @tparam T a class of expected payloads.
   * @return a payload in this record.
   */
  template <class T = void *>
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
    return (kHeaderLen + 2 * kKeyLen + kPayLen + kCacheAlign) & ~kCacheAlign;
  }

  /**
   * @brief Insert this delta record to a given container.
   *
   * @tparam T a class of payloads.
   * @param sep_key an optional separator key.
   * @param records a set of records to be inserted this delta record.
   * @return the difference of a node size.
   */
  void
  AddByInsertionSortTo(std::vector<Record> &records) const
  {
    // check uniqueness
    const auto &rec_key = GetKey();
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
  }

 private:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  /// Header length in bytes.
  static constexpr size_t kHeaderLen = sizeof(DeltaRecord);

  /// the length of keys.
  static constexpr size_t kKeyLen = sizeof(Key);

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
  uint16_t : 0;

  /// the number of delta records in this chain.
  uint16_t rec_count_{0};

  /// the size of this logical node in bytes.
  uint32_t node_size_{0};

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
