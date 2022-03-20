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

#ifndef BW_TREE_COMPONENT_METADATA_HPP
#define BW_TREE_COMPONENT_METADATA_HPP

#include "common.hpp"

namespace dbgroup::index::bw_tree::component
{
/**
 * @brief A class to represent record metadata.
 *
 */
class Metadata
{
 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new metadata object.
   *
   */
  constexpr Metadata() = default;

  /**
   * @brief Construct a new metadata object.
   *
   */
  constexpr Metadata(  //
      const size_t offset,
      const size_t key_length,
      const size_t total_length)
      : offset_{static_cast<uint16_t>(offset)},
        key_length_{static_cast<uint16_t>(key_length)},
        total_length_{static_cast<uint16_t>(total_length)}
  {
  }

  constexpr Metadata(const Metadata &) = default;
  constexpr Metadata(Metadata &&) = default;

  constexpr auto operator=(const Metadata &) -> Metadata & = default;
  constexpr auto operator=(Metadata &&) -> Metadata & = default;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  /**
   * @brief Destroy the metadata object.
   *
   */
  ~Metadata() = default;

  /*####################################################################################
   * Public getters/setters
   *##################################################################################*/

  /**
   * @return an offset to a corresponding record.
   */
  [[nodiscard]] constexpr auto
  GetOffset() const  //
      -> size_t
  {
    return offset_;
  }

  /**
   * @return the length of a key in a corresponding record.
   */
  [[nodiscard]] constexpr auto
  GetKeyLength() const  //
      -> size_t
  {
    return key_length_;
  }

  /**
   * @return the total length of a corresponding record.
   */
  [[nodiscard]] constexpr auto
  GetTotalLength() const  //
      -> size_t
  {
    return total_length_;
  }

  /**
   * @return the length of a payload in a corresponding record.
   */
  [[nodiscard]] constexpr auto
  GetPayloadLength() const  //
      -> size_t
  {
    return GetTotalLength() - GetKeyLength();
  }

  /**
   * @param offset the offset to a corresponding record to be set.
   */
  [[nodiscard]] constexpr auto
  UpdateForLeaf(const size_t offset) const  //
      -> Metadata
  {
    return Metadata{offset, key_length_, total_length_};
  }

  /**
   * @param offset the offset to a corresponding record to be set.
   */
  [[nodiscard]] constexpr auto
  UpdateForInternal(const size_t offset) const  //
      -> Metadata
  {
    return Metadata{offset, key_length_, key_length_ + kWordSize};
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// an offset to a corresponding record.
  uint32_t offset_{};

  /// the length of a key in a corresponding record.
  uint16_t key_length_{};

  /// the total length of a corresponding record.
  uint16_t total_length_{};
};

}  // namespace dbgroup::index::bw_tree::component

#endif  // BW_TREE_COMPONENT_METADATA_HPP
