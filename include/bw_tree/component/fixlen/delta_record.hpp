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

#ifndef BW_TREE_COMPONENT_FIXLEN_DELTA_RECORD_HPP
#define BW_TREE_COMPONENT_FIXLEN_DELTA_RECORD_HPP

// C++ standard libraries
#include <optional>
#include <thread>
#include <utility>
#include <vector>

// local sources
#include "bw_tree/component/common.hpp"
#include "bw_tree/component/logical_id.hpp"

namespace dbgroup::index::bw_tree::component::fixlen
{
/**
 * @brief A class to represent delta records in Bw-tree.
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
  using Record = const void *;

  /*####################################################################################
   * Public constructors for inserting/deleting records in leaf nodes
   *##################################################################################*/

  /**
   * @brief Construct a new delta record for inserting/modifying a record.
   *
   * @tparam T a target payload class.
   * @param delta_type an insert or modify delta.
   * @param key a key to be inserted.
   * @param key_len the length of a target key.
   * @param payload a payload to be inserted.
   */
  template <class T>
  DeltaRecord(  //
      const DeltaType delta_type,
      const Key &key,
      [[maybe_unused]] const size_t key_len,
      const T &payload)
      : is_inner_{kLeaf}, delta_type_{delta_type}, has_low_key_{1}, has_high_key_{0}, key_{key}
  {
    SetPayload(payload);
  }

  /**
   * @brief Construct a new delta record for deleting a record.
   *
   * @param key a key to be deleted.
   * @param key_len the length of a target key.
   */
  explicit DeltaRecord(  //
      const Key &key,
      [[maybe_unused]] const size_t key_len)
      : is_inner_{kLeaf}, delta_type_{kDelete}, has_low_key_{1}, has_high_key_{0}, key_{key}
  {
  }

  /*####################################################################################
   * Public constructors for performing SMOs
   *##################################################################################*/

  /**
   * @brief Construct a new delta record for splitting/merging a node.
   *
   * @param d_type a split or merge delta.
   * @param r_node a split/merged right node.
   * @param r_addr the address of a split/merged right node.
   */
  DeltaRecord(  //
      const DeltaType d_type,
      const DeltaRecord *r_node,
      const void *r_addr)
      : is_inner_{d_type == kInsert ? static_cast<uint16_t>(kInner) : r_node->is_inner_},
        delta_type_{d_type},
        has_low_key_{1},
        has_high_key_{r_node->has_high_key_},
        key_{r_node->key_},
        high_key_{r_node->high_key_}
  {
    // set a sibling node
    SetPayload(r_addr);
  }

  /**
   * @brief Construct a new delta record for deleting an index-entry.
   *
   * @param removed_node a removed node.
   */
  explicit DeltaRecord(const DeltaRecord *removed_node)
      : is_inner_{kInner},
        delta_type_{kDelete},
        has_low_key_{1},
        has_high_key_{removed_node->has_high_key_},
        key_{removed_node->key_},
        high_key_{removed_node->high_key_}
  {
    // set a sibling node
    auto *payload = reinterpret_cast<std::atomic<LogicalID *> *>(ShiftAddr(this, kPayOffset));
    payload->store(nullptr, std::memory_order_relaxed);
  }

  /**
   * @brief Construct a new delta record for removing a node.
   *
   * @param is_leaf a flag for indicating leaf nodes.
   */
  explicit DeltaRecord(const bool is_leaf)
      : is_inner_{static_cast<uint16_t>(!is_leaf)},
        delta_type_{kRemoveNode},
        has_low_key_{0},
        has_high_key_{0}
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
    return has_low_key_ == 0;
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
    return !Comp{}(key, key_) && !Comp{}(key_, key);
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
    return Comp{}(key_, key) || (closed && !Comp{}(key, key_));
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
    return !has_high_key_ || Comp{}(key, high_key_) || (closed && !Comp{}(high_key_, key));
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
   * @retval 1st: the data usage of this node.
   * @retval 2nd: the number of delta records in this node.
   */
  [[nodiscard]] constexpr auto
  GetNodeUsage() const  //
      -> std::pair<size_t, size_t>
  {
    size_t delta_num = 0;
    const auto *cur = this;
    for (; cur->delta_type_ != kNotDelta; cur = cur->GetNext()) {
      ++delta_num;
    }

    return {cur->node_size_, delta_num};
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
    if (!has_low_key_) return std::nullopt;
    return key_;
  }

  /**
   * @return a highest key in a target record if exist.
   */
  [[nodiscard]] constexpr auto
  GetHighKey() const  //
      -> std::optional<Key>
  {
    if (!has_high_key_) return std::nullopt;
    return high_key_;
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
   * @param diff the difference in node sizes.
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
    return kKeyLen;
  }

  /**
   * @return a key in this record.
   */
  [[nodiscard]] auto
  GetKey() const  //
      -> Key
  {
    return key_;
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
    memcpy(&payload, reinterpret_cast<const T *>(payload_), sizeof(T));
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
    const auto *pay_addr = reinterpret_cast<std::atomic_uintptr_t *>(ShiftAddr(this, kPayOffset));
    while (true) {
      for (size_t i = 1; true; ++i) {
        const auto payload = pay_addr->load(std::memory_order_relaxed);
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
    auto *payload = reinterpret_cast<std::atomic<LogicalID *> *>(ShiftAddr(this, kPayOffset));
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
    constexpr auto kPayLen = (sizeof(Payload) > kPtrLen) ? sizeof(Payload) : kPtrLen;
    return (kHeaderLen + kPayLen + kCacheAlign) & ~kCacheAlign;
  }

  /**
   * @brief Insert this delta record to a given container.
   *
   * @tparam T a class of payloads.
   * @param sep_key an optional separator key.
   * @param records a set of records to be inserted this delta record.
   */
  void
  AddByInsertionSortTo(std::vector<Record> &records) const
  {
    // check uniqueness
    auto it = records.cbegin();
    const auto it_end = records.cend();
    Key rec_key{};
    while (it != it_end) {
      // skip smaller keys
      rec_key = reinterpret_cast<const DeltaRecord *>(*it)->key_;
      if (!Comp{}(rec_key, key_)) break;
      ++it;
    }
    if (it == it_end) {
      records.emplace_back(this);
    } else if (Comp{}(key_, rec_key)) {
      records.insert(it, this);
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

  /// an offset value for atomic operations.
  static constexpr size_t kPayOffset = (kHeaderLen + kWordAlign) & ~kWordAlign;

  /*####################################################################################
   * Internal getters/setters
   *##################################################################################*/

  /**
   * @brief Set a target payload directly.
   *
   * @tparam T a class of expected payloads.
   * @param payload a target payload to be set.
   */
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
  uint16_t is_inner_ : 1;

  /// a flag for indicating the types of a delta record.
  uint16_t delta_type_ : 3;

  /// a flag for indicating whether this delta record has a lowest-key.
  uint16_t has_low_key_ : 1;

  /// a flag for indicating whether this delta record has a highest-key.
  uint16_t has_high_key_ : 1;

  /// a blank block for alignment.
  uint16_t : 0;

  /// the number of delta records in this chain.
  uint16_t rec_count_{0};

  /// the size of this logical node in bytes.
  uint32_t node_size_{0};

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

#endif  // BW_TREE_COMPONENT_FIXLEN_DELTA_RECORD_HPP
