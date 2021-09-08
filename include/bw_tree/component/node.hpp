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
#include "metadata.hpp"

namespace dbgroup::index::bw_tree::component
{
/**
 * @brief A class to represent nodes in Bw-tree.
 *
 * Note that this class represents both base nodes and delta nodes.
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Compare a comparetor class for keys.
 */
template <class Key, class Payload, class Compare>
class Node
{
 private:
  /*################################################################################################
   * Internal variables
   *##############################################################################################*/

  /// a flag to indicate whether this node is a leaf or internal node.
  uint16_t node_type_ : 1;

  /// a flag to indicate the types of a delta node.
  uint16_t delta_type_ : 3;

  /// a blank block for alignment.
  uint16_t : 0;

  /// the number of records in this node.
  uint16_t record_count_;

  /// a blank block for alignment.
  uint64_t : 0;

  /// the pointer to the next node.
  Node *next_node_;

  /// an actual data block (it starts with record metadata).
  Metadata meta_array_[0];

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  /**
   * @brief Construct a new base node object.
   *
   * @param node_type a flag to indicate whether a leaf or internal node is constructed.
   * @param record_count the number of records in this node.
   * @param next_node the pointer to a sibling node.
   */
  Node(  //
      const NodeType node_type,
      const size_t record_count,
      const Node *next_node)
      : node_type_{node_type},
        delta_type_{DeltaNodeType::kNotDelta},
        record_count_{static_cast<uint16_t>(record_count)},
        next_node_{const_cast<Node *>(next_node)}
  {
  }

  /**
   * @brief Construct a new delta node object.
   *
   * @param node_type a flag to indicate whether a leaf or internal node is constructed.
   * @param delta_type a flag to indicate the type of a constructed delta node.
   * @param next_node the pointer to a next delta/base node.
   */
  Node(  //
      const NodeType node_type,
      const DeltaNodeType delta_type,
      const Node *next_node)
      : node_type_{node_type}, delta_type_{delta_type}, next_node_{const_cast<Node *>(next_node)}
  {
  }

  /**
   * @brief Destroy the node object.
   *
   */
  ~Node() = default;

  Node(const Node &) = delete;
  Node &operator=(const Node &) = delete;
  Node(Node &&) = delete;
  Node &operator=(Node &&) = delete;

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  /**
   * @retval true if this is a leaf node.
   * @retval false if this is an internal node.
   */
  constexpr bool
  IsLeaf() const
  {
    return node_type_;
  }

  /**
   * @return DeltaNodeType: the type of a delta node.
   */
  constexpr DeltaNodeType
  GetDeltaNodeType() const
  {
    return static_cast<DeltaNodeType>(delta_type_);
  }

  /**
   * @return size_t: the number of records in this node.
   */
  constexpr size_t
  GetRecordCount() const
  {
    return record_count_;
  }

  constexpr Node *
  GetNextNode() const
  {
    return next_node_;
  }

  /**
   * @param position the position of record metadata to be get.
   * @return Metadata: record metadata.
   */
  constexpr Metadata
  GetMetadata(const size_t position) const
  {
    return meta_array_[position];
  }

  /**
   * @param meta metadata of a corresponding record.
   * @return Key: a target key.
   */
  constexpr Key
  GetKey(const Metadata meta) const
  {
    if constexpr (IsVariableLengthData<Key>()) {
      return reinterpret_cast<Key>(ShiftAddress(this, meta.GetOffset()));
    } else {
      return *reinterpret_cast<Key *>(ShiftAddress(this, meta.GetOffset()));
    }
  }

  /**
   * @tparam T a class of a target payload.
   * @param meta metadata of a corresponding record.
   * @return T: a target payload.
   */
  template <class T>
  constexpr T
  GetPayload(const Metadata meta) const
  {
    const auto offset = meta.GetOffset() + meta.GetKeyLength();
    if constexpr (IsVariableLengthData<T>()) {
      return reinterpret_cast<T>(ShiftAddress(this, offset));
    } else {
      return *reinterpret_cast<T *>(ShiftAddress(this, offset));
    }
  }

  /**
   * @brief Copy a target payload to a specified reference.
   *
   * @param meta metadata of a corresponding record.
   * @param out_payload a reference to be copied a target payload.
   */
  void
  CopyPayload(  //
      const Metadata meta,
      Payload &out_payload) const
  {
    const auto offset = meta.GetOffset() + meta.GetKeyLength();
    if constexpr (IsVariableLengthData<Payload>()) {
      const auto payload_length = meta.GetPayloadLength();
      out_payload = ::dbgroup::memory::MallocNew<std::remove_pointer_t<Payload>>(payload_length);
      memcpy(out_payload, ShiftAddress(this, offset), payload_length);
    } else {
      memcpy(&out_payload, ShiftAddress(this, offset), sizeof(Payload));
    }
  }

  /**
   * @brief Set record metadata.
   *
   * @param position the position of metadata to be set.
   * @param new_meta metadata to be set.
   */
  constexpr void
  SetMetadata(  //
      const size_t position,
      const size_t offset,
      const size_t key_length,
      const size_t total_length)
  {
    meta_array_[position] = Metadata{offset, key_length, total_length};
  }

  /**
   * @brief Set a target key.
   *
   * @param offset an offset to set a target key.
   * @param key a target key to be set.
   * @param key_length the length of a target key.
   */
  void
  SetKey(  //
      size_t &offset,
      const Key &key,
      const size_t key_length)
  {
    if constexpr (IsVariableLengthData<Key>()) {
      offset -= key_length;
      memcpy(ShiftAddress(this, offset), key, key_length);
    } else {
      offset -= sizeof(Key);
      memcpy(ShiftAddress(this, offset), &key, sizeof(Key));
    }
  }

  /**
   * @brief Set a target payload.
   *
   * @tparam T a class of a target payload.
   * @param offset an offset to set a target payload.
   * @param payload a target payload to be set.
   * @param payload_length the length of a target payload.
   */
  template <class T>
  void
  SetPayload(  //
      size_t &offset,
      const T &payload,
      const size_t payload_length)
  {
    if constexpr (IsVariableLengthData<T>()) {
      offset -= payload_length;
      memcpy(ShiftAddress(this, offset), payload, payload_length);
    } else {
      offset -= sizeof(T);
      memcpy(ShiftAddress(this, offset), &payload, sizeof(T));
    }
  }
  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  /**
   * @brief Get the position of a specified key by using binary search. If there is no
   * specified key, this returns the minimum metadata position that is greater than the
   * specified key
   *
   * @param key a target key.
   * @param range_is_closed a flag to indicate that a target key is included.
   * @return size_t: the position of a specified key.
   */
  size_t
  SearchRecord(  //
      const Key &key,
      const bool range_is_closed) const
  {
    int64_t begin_idx = 0;
    int64_t end_idx = GetRecordCount() - 1;
    int64_t idx = (begin_idx + end_idx) >> 1;

    while (begin_idx <= end_idx) {
      const auto meta = GetMetadata(idx);
      const auto idx_key = GetKey(meta);

      if (meta.GetKeyLength() == 0 || Compare{}(key, idx_key)) {
        // a target key is in a left side
        end_idx = idx - 1;
      } else if (Compare{}(idx_key, key)) {
        // a target key is in a right side
        begin_idx = idx + 1;
      } else {
        // find an equivalent key
        if (!range_is_closed) ++idx;
        begin_idx = idx;
        break;
      }

      idx = (begin_idx + end_idx) >> 1;
    }

    return begin_idx;
  }

  /*################################################################################################
};

}  // namespace dbgroup::index::bw_tree::component
