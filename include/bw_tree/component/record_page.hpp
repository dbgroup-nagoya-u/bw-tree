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

#pragma once

#include <memory>
#include <utility>

#include "common.hpp"

namespace dbgroup::index::bw_tree::component
{
/**
 * @brief A class to represent scan results.
 *
 * @tparam Key a target key class
 * @tparam Payload a target payload class
 */
template <class Key, class Payload>
class RecordPage
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// an address of the end of this page
  std::byte *end_addr_;

  /// an address of the last record's key
  std::byte *last_key_addr_;

  /// scan result records
  std::byte record_block_[kPageSize - kHeaderLength];

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  /**
   * @brief Construct a new instance.
   *
   */
  constexpr RecordPage()
  {
    // static_assert(sizeof(RecordPage) ==  kPageSize);
  }

  ~RecordPage() = default;

  RecordPage(const RecordPage &) = delete;
  RecordPage &operator=(const RecordPage &) = delete;
  RecordPage(RecordPage &&) = delete;
  RecordPage &operator=(RecordPage &&) = delete;

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  /**
   * @retval true if this page does not have records.
   * @retval false if this page has any records.
   */
  constexpr bool
  Empty() const
  {
    return end_addr_ == record_block_;
  }

  /**
   * @return Key: the last key of this copied node.
   */
  constexpr Key
  GetLastKey() const
  {
    if constexpr (IsVariableLengthData<Key>()) {
      return Cast<Key>(last_key_addr_);
    } else {
      return *Cast<Key *>(last_key_addr_);
    }
  }

  /**
   * @brief Set the end address of copied records.
   *
   * @param end_addr the end address of copied records.
   */
  constexpr void
  SetEndAddress(const std::byte *end_addr)
  {
    end_addr_ = const_cast<std::byte *>(end_addr);
  }

  /**
   * @brief Set the address of the last key of copied records.
   *
   * @param last_key_addr the address of the last key of copied records.
   */
  constexpr void
  SetLastKeyAddress(const std::byte *last_key_addr)
  {
    last_key_addr_ = const_cast<std::byte *>(last_key_addr);
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  /**
   * @return std::byte*: the begin address of copied records.
   */
  constexpr std::byte *
  GetBeginAddr()
  {
    if constexpr (IsVariableLengthData<Key>() && IsVariableLengthData<Payload>()) {
      return record_block_ + (sizeof(uint32_t) + sizeof(uint32_t));
    } else if constexpr (IsVariableLengthData<Key>() || IsVariableLengthData<Payload>()) {
      return record_block_ + sizeof(uint32_t);
    } else {
      return record_block_;
    }
  }

  /**
   * @return std::byte*: the end address of copied records.
   */
  constexpr std::byte *
  GetEndAddr() const
  {
    return end_addr_;
  }

  /**
   * @return uint32_t: the length of the begin key of copied records.
   */
  constexpr uint32_t
  GetBeginKeyLength() const
  {
    if constexpr (IsVariableLengthData<Key>()) {
      return *Cast<uint32_t *>(record_block_);
    } else {
      return sizeof(Key);
    }
  }

  /**
   * @return uint32_t: the length of the begin payload of copied records.
   */
  constexpr uint32_t
  GetBeginPayloadLength() const
  {
    if constexpr (IsVariableLengthData<Key>() && IsVariableLengthData<Payload>()) {
      return *Cast<uint32_t *>(record_block_ + sizeof(uint32_t));
    } else if constexpr (IsVariableLengthData<Payload>()) {
      return *Cast<uint32_t *>(record_block_);
    } else {
      return sizeof(Payload);
    }
  }
};

}  // namespace dbgroup::index::bw_tree::component