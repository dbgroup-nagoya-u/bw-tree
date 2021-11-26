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
#include "node.hpp"

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
  using BwTree_t = BwTree<Key, Payload, Compare>;
  using Node_t = Node<Key, Compare>

 private :
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// a pointer to BwTree to perform continuous scan
  BwTree_t* bwtree_;

  /// the end of range scan
  const Key* end_key_;

  /// a flag to specify whether the end of range is closed
  bool end_is_closed_;

  /// the number of records in this node.
  uint16_t record_count_;

  /// metadata
  Metadata meta_;

  /// an index of a current record
  size_t current_idx_;

  /// node
  Node_t* node_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  constexpr RecordIterator(
    BwTree_t* bwtree,
    const Key* end_key,
    const bool end_is_closed,
    Node_t* node)
    : bwtree_{bwtree},
      end_key_{end_key},
      end_is_closed_{end_is_closed},
      record_count_{node->GetRecordCount()},
      current_idx_{node->GetLowKeyAddr()},
      meta_{node->GetMetadata(current_idx_)}
  {
  }

  ~RecordIterator() = default;

  RecordIterator(const RecordIterator&) = delete;
  RecordIterator& operator=(const RecordIterator&) = delete;
  constexpr RecordIterator(RecordIterator&&) = default;
  constexpr RecordIterator& operator=(RecordIterator&&) = default;

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
    current_idx_++;
    current_addr_ += GetKeyLength() + GetPayloadLength();
    if constexpr (IsVariableLengthData<Key>()) {
      if (current_addr_ != end_addr_) {
        memcpy(&key_length_, current_addr_, sizeof(uint32_t));
        current_addr_ += sizeof(uint32_t);
      }
    }
    if constexpr (IsVariableLengthData<Payload>()) {
      if (current_addr_ != end_addr_) {
        memcpy(&payload_length_, current_addr_, sizeof(uint32_t));
        current_addr_ += sizeof(uint32_t);
      }
    }
  }

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

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
    if (current_idx_ < meta_.GetKeyLength()) return true;
    if (node_->GetNextNode() == NULL) return false;

    // search a next leaf node to continue scanning
    const auto begin_key = page_->GetLastKey();
    const auto begin_key = node_->GetHighKeyAddr();
    auto node = node_->GetNextNode();

    *this = bwtree_->Scan(&begin_key, false, end_key_, end_is_closed_, node);

    return HasNext();
  }

  /**
   * @return Key: a key of a current record
   */
  constexpr Key
  GetKey() const
  {
    if constexpr (IsVariableLengthData<Key>()) {
      return Cast<Key>(current_addr_);
    } else {
      return *Cast<Key*>(current_addr_);
    }
  }

  /**
   * @return Payload: a payload of a current record
   */
  constexpr Payload
  GetPayload() const
  {
    if constexpr (IsVariableLengthData<Payload>()) {
      return Cast<Payload>(current_addr_ += GetKeyLength());
    } else {
      return *Cast<Payload*>(current_addr_ + GetKeyLength());
    }
  }

  /**
   * @return size_t: the length of a current kay
   */
  constexpr size_t
  GetKeyLength() const
  {
    if constexpr (IsVariableLengthData<Key>()) {
      return key_length_;
    } else {
      return sizeof(Key);
    }
  }

  /**
   * @return size_t: the length of a current payload
   */
  constexpr size_t
  GetPayloadLength() const
  {
    if constexpr (IsVariableLengthData<Payload>()) {
      return payload_length_;
    } else {
      return sizeof(Payload);
    }
  }
};

}  // namespace dbgroup::index::bw_tree::component
