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

#include <optional>
#include <utility>
#include <vector>

#include "bw_tree/component/common.hpp"
#include "bw_tree/component/consolidate_info.hpp"
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
        has_low_key_{1},
        has_high_key_{split_d->has_high_key_},
        key_{split_d->key_},
        high_key_{split_d->high_key_}
  {
    // copy contents of a split delta
    memcpy(payload_, (split_d->payload_), kPtrLen);
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
      : is_inner_{kInternal},
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
      : is_inner_{right_node->is_inner_},
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
   * @param dummy a dummy delta type for distinguishing constructors.
   * @param removed_node a removed node.
   */
  DeltaRecord(  //
      [[maybe_unused]] const DeltaType dummy,
      const DeltaRecord *removed_node)
      : is_inner_{removed_node->is_inner_},
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
   * @retval false otherwise.
   */
  [[nodiscard]] constexpr auto
  IsLeaf() const  //
      -> bool
  {
    return is_inner_ == kLeaf;
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
    return key_;
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
   * @tparam T a class of expected payloads.
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
    return kHeaderLen + kPayLen;
  }

  /**
   * @brief Insert this delta record to a given container.
   *
   * @tparam T a class of payloads.
   * @param sep_key an optional separator key.
   * @param records a set of records to be inserted this delta record.
   * @return the difference of a record count.
   */
  template <class T>
  [[nodiscard]] auto
  AddByInsertionSortTo(  //
      const std::optional<Key> &sep_key,
      std::vector<const void *> &records) const  //
      -> int64_t
  {
    // check whether this record is in a target node
    if (!sep_key || Comp{}(key_, *sep_key)) {
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

      // update the page size
      if (delta_type_ == kInsert) return 1;
      if (delta_type_ == kDelete) return -1;
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

#endif  // BW_TREE_COMPONENT_FIXLEN_DELTA_RECORD_HPP
