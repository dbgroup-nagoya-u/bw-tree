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

#include "common.hpp"

namespace dbgroup::index::bw_tree::component
{
/**
 * @brief A class to represent record metadata.
 *
 */
class alignas(sizeof(uint16_t)) Metadata
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// an offset to a corresponding record.
  uint16_t offset_;

  /// the length of a key in a corresponding record.
  const uint16_t key_length_;

  /// the total length of a corresponding record.
  const uint16_t total_length_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  /**
   * @brief Construct a new metadata object.
   *
   */
  constexpr Metadata() : offset_{}, key_length_{}, total_length_{} {}

  /**
   * @brief Construct a new metadata object.
   *
   */
  constexpr Metadata(  //
      const size_t offset,
      const size_t key_length,
      const size_t total_length)
      : offset_{offset}, key_length_{key_length}, total_length_{total_length}
  {
  }

  /**
   * @brief Destroy the metadata object.
   *
   */
  ~Metadata() = default;

  constexpr Metadata(const Metadata &) = default;
  constexpr Metadata &operator=(const Metadata &) = default;
  constexpr Metadata(Metadata &&) = default;
  constexpr Metadata &operator=(Metadata &&) = default;

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  /**
   * @return size_t: an offset to a corresponding record.
   */
  constexpr size_t
  GetOffset() const
  {
    return offset_;
  }

  /**
   * @return size_t: the length of a key in a corresponding record.
   */
  constexpr size_t
  GetKeyLength() const
  {
    return key_length_;
  }

  /**
   * @return size_t: the total length of a corresponding record.
   */
  constexpr size_t
  GetTotalLength() const
  {
    return total_length_;
  }

  /**
   * @return size_t: the length of a payload in a corresponding record.
   */
  constexpr size_t
  GetPayloadLength() const
  {
    return GetTotalLength() - GetKeyLength();
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  /**
   * @param offset a new offset to be set.
   * @return Metadata: a new metadata object.
   */
  constexpr Metadata
  UpdateOffset(const size_t offset) const
  {
    auto updated_meta = *this;
    updated_meta.offset_ = offset;
    return updated_meta;
  }
}

}  // namespace dbgroup::index::bw_tree::component
