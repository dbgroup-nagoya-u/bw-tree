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

#ifndef BW_TREE_COMPONENT_VARLEN_METADATA_HPP
#define BW_TREE_COMPONENT_VARLEN_METADATA_HPP

// local sources
#include "bw_tree/component/common.hpp"

namespace dbgroup::index::bw_tree::component::varlen
{
/**
 * @brief A class for representing record metadata.
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
      : offset{static_cast<uint16_t>(offset)},
        key_len{static_cast<uint16_t>(key_length)},
        rec_len{static_cast<uint16_t>(total_length)}
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
   * @return the length of a payload in a corresponding record.
   */
  [[nodiscard]] constexpr auto
  GetPayloadLength() const  //
      -> size_t
  {
    return rec_len - key_len;
  }

  /*####################################################################################
   * Public member variables
   *##################################################################################*/

  /// an offset to a corresponding record.
  uint32_t offset{};

  /// the length of a key in a corresponding record.
  uint16_t key_len{};

  /// the total length of a corresponding record.
  uint16_t rec_len{};
};

}  // namespace dbgroup::index::bw_tree::component::varlen

#endif  // BW_TREE_COMPONENT_VARLEN_METADATA_HPP
