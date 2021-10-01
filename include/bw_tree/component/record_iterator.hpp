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

#include <utility>

namespace dbgroup::index::bw_tree::component
{
/**
 * @brief A class to represent a iterator for scan results.
 *
 * @tparam Key a target key class
 * @tparam Payload a target payload class
 * @tparam Compare a key-comparator class
 */
template <class Key, class Payload, class Compare>
class RecordIterator
{
 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  constexpr RecordIterator() {}

  ~RecordIterator() = default;

  /*################################################################################################
   * Public operators for iterators
   *##############################################################################################*/

  /**
   * @return std::pair<Key, Payload>: a current key and payload pair
   */
  constexpr std::pair<Key, Payload>
  operator*() const
  {
    return {GetKey(), GetPayload()};
  }

  /**
   * @brief Forward an iterator.
   *
   */
  void
  operator++()
  {
    // not implemented yet
  }

  /**
   * @brief Check if there are any records left.
   *
   * Note that a BzTree's scan function copies a target leaf node one by one, so this
   * function may call a scan function internally to get a next leaf node.
   *
   * @retval true if there are any records left.
   * @retval false if there are no records left.
   */
  bool
  HasNext()
  {
    // not implemented yet

    return true;
  }

  /**
   * @return Key: a key of a current record
   */
  constexpr Key
  GetKey() const
  {
    // not implemented yet

    return Key{};
  }

  /**
   * @return Payload: a payload of a current record
   */
  constexpr Payload
  GetPayload() const
  {
    // not implemented yet

    return Payload{};
  }

  /**
   * @return size_t: the length of a current kay
   */
  constexpr size_t
  GetKeyLength() const
  {
    // not implemented yet

    return sizeof(Key);
  }

  /**
   * @return size_t: the length of a current payload
   */
  constexpr size_t
  GetPayloadLength() const
  {
    // not implemented yet

    return sizeof(Payload);
  }
};

}  // namespace dbgroup::index::bw_tree::component
