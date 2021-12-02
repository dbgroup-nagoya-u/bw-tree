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

namespace dbgroup::index::bw_tree
{
template <class Key, class Payload, class Compare>
class BwTree;
namespace component
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
  using Node_t = Node<Key, Compare>;

 private :
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// a pointer to BwTree to perform continuous scan
  BwTree_t* bwtree_;

  /// the number of records in this node.
  uint16_t record_count_;

  /// an index of a current record
  size_t current_idx_;

  /// node
  Node_t* node_;

  const Key* begin_key_;

  bool begin_closed_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  constexpr RecordIterator(
    BwTree_t* bwtree,
    const Key* begin_key,
    const bool begin_closed,
    Node_t* node)
    : bwtree_{bwtree},
      node_{node},
      record_count_{node_->GetRecordCount()},
      current_idx_{0},
      begin_key_{begin_key},
      begin_closed_{begin_closed}
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
  }

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  /**
   * @brief Check if there are any records left.
   *
   * function may call a scan function internally to get a next leaf node.
   *
   * @retval true if there are any records or next node left.
   * @retval false if there are no records and node left.
   */
  bool
  HasNext()
  {
    if (current_idx_ < record_count_) return true;
    else if (node_->GetNextNode() != nullptr) {
      node_ = bwtree_->LeafScan(node_->GetNextNode(), begin_key_, begin_closed_);
      record_count_ = node_->GetRecordCount();
      current_idx_ = 0;
      return HasNext();
    }
    else return false;
  }

  /**
   * @return Key: a key of a current record
   */
  constexpr Key
  GetKey() const
  {
    if constexpr (IsVariableLengthData<Key>()) {
      return Cast<Key>(node_->GetKeyAddr(node_->GetMetadata(current_idx_)));
    } else {
      return *Cast<Key*>(node_->GetKeyAddr(node_->GetMetadata(current_idx_)));
    }
  }

  /**
   * @return Payload: a payload of a current record
   */
  constexpr Payload
  GetPayload() const
  {
    Payload payload{};
    node_->CopyPayload(node_->GetMetadata(current_idx_), payload);
    return payload;
  }
};
}
}  // namespace dbgroup::index::bw_tree::component