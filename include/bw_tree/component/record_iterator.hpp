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
#include "record_page.hpp"

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
  using Node_t = component::Node<Key, Compare>;
  /// a pointer to BzTree to perform continuous scan
  BwTree_t* bwtree_;

  /// the begin of range scan
  const Key* begin_key_;

  /// a flag to specify whether the begin of range is closed
  bool begin_closed_;

  /// the position of iterator cursol
  int64_t cur_position_;

  /// a flag to indicate the end of range scan
  bool scan_finished_;

  /// copied keys and payloads
  Node_t *page_;

  /// a key of cursol points
  Key cur_key_;

  /// a payload of cursol points
  Payload cur_payload_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/
  constexpr RecordIterator() {}
  RecordIterator(  //
      BwTree_t* bwtree,
      const Key* begin_key,
      const bool begin_closed,
      Node_t* page,
      const bool scan_finished)
      : bwtree_{bwtree},
        begin_key_{begin_key},
        begin_closed_{begin_closed},
        cur_position_{0},
        scan_finished_{scan_finished},
        page_{page}
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
    cur_position_++;
  }

  /**
   * @brief Check if there are any records left.
   *
   * Note that a BwTree's scan function copies a target leaf node one by one, so this
   * function may call a scan function internally to get a next leaf node.
   *
   * @retval true if there are any records left.
   * @retval false if there are no records left.
   */
  bool
  HasNext()
  {
    if (((int64_t)page_->GetRecordCount() - cur_position_) > 0) return true;
    if (scan_finished_) return false;

    auto sib_node = page_->GetSiblingNode()->load(mo_relax);
    delete page_;

    scan_finished_ = bwtree_->LeafScan(sib_node, begin_key_,
                                         begin_closed_, &page_);
    cur_position_ = 0;
    return HasNext();
  }

  void
  SetCurrentKeyValue()
  {
    cur_key_ = *reinterpret_cast<Key*>(page_->GetKeyAddr(page_->GetMetadata(cur_position_)));
    page_->CopyPayload(page_->GetMetadata(cur_position_), cur_payload_);
  }

  size_t
  GetRecordCount()
  {
    return page_->GetRecordCount();
  }

  /**
   * @return Key: a key of a current record
   */
  constexpr Key
  GetKey() const
  {
    if constexpr (IsVariableLengthData<Key>()) {
      return reinterpret_cast<const Key>(page_->GetKeyAddr(page_->GetMetadata(cur_position_)));
    } else {
      return *reinterpret_cast<const Key *>(page_->GetKeyAddr(page_->GetMetadata(cur_position_)));
    }
  }

  /**
   * @return Payload: a payload of a current record
   */
  constexpr Payload
  GetPayload() const
  {
    Payload payload{};
    page_->CopyPayload(page_->GetMetadata(cur_position_), payload);
    return payload;
  }
};
}  // namespace component
}  // namespace dbgroup::index::bw_tree