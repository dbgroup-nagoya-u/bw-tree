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

#include <optional>
#include <utility>

#include "common.hpp"
#include "metadata.hpp"

namespace dbgroup::index::bw_tree::component
{
/**
 * @brief A class to represent delta records in Bw-tree.
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Comp a comparetor class for keys.
 */
template <class Key, class Payload, class Comp>
class DeltaRecord
{
 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new delta record for leaf-insert/modify/delete or split.
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
      const DeltaNodeType delta_type,
      const Key &key,
      const size_t key_length,
      const T &payload,
      const size_t payload_length)
      : delta_type_{delta_type}
  {
    auto [key_len, pay_len, rec_len] = Align<Key, T>(key_length, payload_length);
    auto offset = sizeof(DeltaRecord);

    offset = SetPayload(offset, payload, pay_len);
    offset = SetKey(offset, key, key_len);
    meta_ = Metadata{offset, key_len, rec_len};
  }

  /**
   * @brief Construct a new delta record for internal-insert/delete or merging.
   *
   * @param delta_type
   * @param low_key
   * @param high_key
   * @param sib_node
   */
  DeltaRecord(  //
      const DeltaNodeType delta_type,
      const Key &low_key,
      const size_t low_key_len,
      const std::optional<std::pair<const Key &, size_t>> &high_key,
      const uintptr_t sib_node)
      : delta_type_{delta_type}
  {
    constexpr auto kPtrLength = sizeof(uintptr_t);
    auto offset = sizeof(DeltaRecord);

    if (high_key) {
      const auto &[h_key, h_key_len] = high_key.value();
      auto [key_len, pay_len, rec_len] = Align<Key, uintptr_t>(h_key_len, kPtrLength);

      auto tmp_offset = SetKey(offset, h_key, key_len);
      high_key_meta_ = Metadata{tmp_offset, key_len, rec_len};

      offset -= rec_len;
    } else {
      high_key_meta_ = Metadata{0, 0, 0};
    }

    auto [key_len, pay_len, rec_len] = Align<Key, uintptr_t>(low_key_len, kPtrLength);

    offset = SetPayload(offset, sib_node, pay_len);
    offset = SetKey(offset, low_key, key_len);
    meta_ = Metadata{offset, key_len, rec_len};
  }

  constexpr DeltaRecord(const DeltaRecord &) = default;
  constexpr auto operator=(const DeltaRecord &) -> DeltaRecord & = default;
  constexpr DeltaRecord(DeltaRecord &&) = default;
  constexpr auto operator=(DeltaRecord &&) -> DeltaRecord & = default;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  constexpr ~DeltaRecord() = default;

 private:
  /*####################################################################################
   * Internal getters/setters
   *##################################################################################*/

  /**
   * @brief Set a target key.
   *
   * @param offset an offset to set a target key.
   * @param key a target key to be set.
   * @param key_length the length of a target key.
   */
  auto
  SetKey(  //
      size_t offset,
      const Key &key,
      const size_t key_length)  //
      -> size_t
  {
    offset -= key_length;
    if constexpr (IsVariableLengthData<Key>()) {
      memcpy(ShiftAddr(this, offset), key, key_length);
    } else {
      memcpy(ShiftAddr(this, offset), &key, sizeof(Key));
    }

    return offset;
  }

  /**
   * @brief Set a target payload directly.
   *
   * @tparam Payload a class of payload.
   * @param offset an offset to set a target payload.
   * @param payload a target payload to be set.
   * @param payload_length the length of a target payload.
   */
  template <class T>
  auto
  SetPayload(  //
      size_t offset,
      const T &payload,
      const size_t payload_length)  //
      -> size_t
  {
    offset -= payload_length;
    if constexpr (IsVariableLengthData<T>()) {
      memcpy(ShiftAddr(this, offset), payload, payload_length);
    } else {
      memcpy(ShiftAddr(this, offset), &payload, sizeof(T));
    }

    return offset;
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// a flag for indicating the types of a delta record.
  uint16_t delta_type_ : 3;

  /// a blank block for alignment.
  uint64_t : 0;

  /// the pointer to the next node.
  uintptr_t next_node_{0};

  /// metadata of an embedded record
  Metadata meta_{};

  /// metadata of a highest key
  Metadata high_key_meta_{};

  /// an actual data block for records
  std::byte data_block_[GetMaxDeltaSize<Key, Payload>()]{};
};

}  // namespace dbgroup::index::bw_tree::component

#endif  // BW_TREE_COMPONENT_DELTA_RECORD_HPP
