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

#include "common.hpp"
#include "memory/utility.hpp"
#include "metadata.hpp"

namespace dbgroup::index::bw_tree::component
{
/**
 * @brief A class to represent nodes in Bw-tree.
 *
 * Note that this class represents both base nodes and delta nodes.
 *
 * @tparam Key a target key class.
 * @tparam Comp a comparetor class for keys.
 */
template <class Key, class Comp>
class Node
{
 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new base node object.
   *
   */
  constexpr Node() : node_type_{}, delta_type_{}, record_count_{}, next_node_{} {}

  Node(const Node &) = delete;
  Node &operator=(const Node &) = delete;
  Node(Node &&) = delete;
  Node &operator=(Node &&) = delete;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  /**
   * @brief Destroy the node object.
   *
   */
  ~Node() = default;

  /*####################################################################################
   * Public node builders
   *##################################################################################*/

  static Node *
  CreateNode(  //
      const size_t node_size,
      const NodeType node_type,
      const size_t record_count,
      const Mapping_t *sib_node)
  {
    return new (::operator new(node_size)) Node{node_type, record_count, sib_node};
  }

  static void
  DeleteNode(Node *node)
  {
    // release nodes recursively until it reaches a base node
    while (node != nullptr && node->GetDeltaNodeType() != DeltaType::kNotDelta) {
      auto cur_node = node;
      node = cur_node->GetNextNode();

      cur_node->~Node();
      ::operator delete(cur_node);
    }

    if (node != nullptr) {
      node->~Node();
      ::operator delete(node);
    }
  }

  /*####################################################################################
   * Public getters/setters
   *##################################################################################*/

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
   * @return DeltaType: the type of a delta node.
   */
  constexpr DeltaType
  GetDeltaNodeType() const
  {
    return static_cast<DeltaType>(delta_type_);
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
    return const_cast<Node *>(reinterpret_cast<const Node *>(next_node_));
  }

  constexpr Mapping_t *
  GetSiblingNode() const
  {
    return const_cast<Mapping_t *>(reinterpret_cast<const Mapping_t *>(next_node_));
  }

  constexpr Metadata
  GetLowMeta() const
  {
    return low_meta_;
  }

  constexpr Metadata
  GetHighMeta() const
  {
    return high_meta_;
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

  constexpr void *
  GetLowKeyAddr() const
  {
    if (low_meta_.GetKeyLength() == 0) return nullptr;

    return ShiftAddress(this, low_meta_.GetOffset());
  }

  constexpr void *
  GetHighKeyAddr() const
  {
    if (high_meta_.GetKeyLength() == 0) return nullptr;

    return ShiftAddress(this, high_meta_.GetOffset());
  }

  /**
   * @param meta metadata of a corresponding record.
   * @return auto: an address of a target key.
   */
  constexpr void *
  GetKeyAddr(const Metadata meta) const
  {
    return ShiftAddress(this, meta.GetOffset());
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
  template <class Payload>
  void
  CopyPayload(  //
      const Metadata meta,
      Payload &out_payload) const
  {
    const auto offset = meta.GetOffset() + meta.GetKeyLength();
    if constexpr (IsVariableLengthData<Payload>()) {
      const auto payload_length = meta.GetPayloadLength();
      out_payload = reinterpret_cast<Payload>(::operator new(payload_length));
      memcpy(out_payload, ShiftAddress(this, offset), payload_length);
    } else {
      memcpy(&out_payload, ShiftAddress(this, offset), sizeof(Payload));
    }
  }

  void
  SetNextNode(const Node *next_node)
  {
    next_node_ = reinterpret_cast<const uintptr_t>(next_node);
  }

  void
  SetRecordCount(const size_t rec_num)
  {
    record_count_ = rec_num;
  }

  constexpr void
  SetLowMeta(const Metadata meta)
  {
    low_meta_ = meta;
  }

  constexpr void
  SetHighMeta(const Metadata meta)
  {
    high_meta_ = meta;
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
      const Metadata meta)
  {
    meta_array_[position] = meta;
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
      const void *key,
      const size_t key_length)
  {
    offset -= key_length;
    memcpy(ShiftAddress(this, offset), key, key_length);
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

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/

  /**
   * @brief Get the position of a specified key by using binary search. If there is no
   * specified key, this returns the minimum metadata position that is greater than the
   * specified key
   *
   * @param key a target key.
   * @param range_is_closed a flag to indicate that a target key is included.
   * @return std::pair<ReturnCode, size_t>: record's existence and the position of a
   * specified key if exist.
   */
  [[nodiscard]] auto
  SearchRecord(const Key &key) const  //
      -> NodeRC
  {
    int64_t begin_pos = 0;
    int64_t end_pos = record_count_ - 1;
    auto rc = NodeRC::kKeyNotExist;
    while (begin_pos <= end_pos) {
      size_t pos = (begin_pos + end_pos) >> 1UL;  // NOLINT

      const auto &index_key = GetKey(meta_array_[pos]);

      if (Comp{}(key, index_key)) {  // a target key is in a left side
        end_pos = pos - 1;
      } else if (Comp{}(index_key, key)) {  // a target key is in a right side
        begin_pos = pos + 1;
      } else {  // find an equivalent key
        rc = pos;
      }
    }

    return rc;
  }

  /**
   * @brief Get the position of a specified key by using binary search. If there is no
   * specified key, this returns the minimum metadata index that is greater than the
   * specified key
   *
   * @param key a target key.
   * @param range_is_closed a flag to indicate that a target key is included.
   * @return the position of a specified key.
   */
  [[nodiscard]] auto
  SearchChild(  //
      const Key &key,
      const bool range_is_closed) const  //
      -> size_t
  {
    int64_t begin_pos = 0;
    int64_t end_pos = record_count_ - 2;
    while (begin_pos <= end_pos) {
      size_t pos = (begin_pos + end_pos) >> 1UL;  // NOLINT

      const auto &index_key = GetKey(meta_array_[pos]);

      if (Comp{}(key, index_key)) {  // a target key is in a left side
        end_pos = pos - 1;
      } else if (Comp{}(index_key, key)) {  // a target key is in a right side
        begin_pos = pos + 1;
      } else {  // find an equivalent key
        if (!range_is_closed) ++pos;
        begin_pos = pos;
        break;
      }
    }

    return begin_pos;
  }

  void
  CopyRecordFrom(  //
      const size_t position,
      size_t &offset,
      const Node *orig_node,
      const Metadata meta)
  {
    const auto total_length = meta.GetTotalLength();
    offset -= total_length;

    // copy a record
    auto src_addr = ShiftAddress(orig_node, meta.GetOffset());
    auto dest_addr = ShiftAddress(this, offset);
    memcpy(dest_addr, src_addr, total_length);

    // set record metadata
    SetMetadata(position, Metadata{offset, meta.GetKeyLength(), total_length});
  }

  void
  CopyRecordFrom(  //
      const size_t position,
      size_t &offset,
      const Node *key_node,
      const Metadata key_meta,
      const Node *payload_node,
      const Metadata payload_meta)
  {
    const auto key_len = key_meta.GetKeyLength();
    const Mapping_t *ins_page = payload_node->template GetPayload<Mapping_t *>(payload_meta);

    SetPayload(offset, ins_page, sizeof(Mapping_t *));
    SetKey(offset, key_node->GetKeyAddr(key_meta), key_len);
    SetMetadata(position, Metadata{offset, key_len, key_len + sizeof(Mapping_t *)});
  }

 private:
  /*####################################################################################
   * Internal constructors/destructors
   *##################################################################################*/

  /**
   * @brief Construct a new base node object.
   *
   * @param node_type a flag to indicate whether a leaf or internal node is constructed.
   * @param record_count the number of records in this node.
   * @param sib_node the pointer to a sibling node.
   */
  Node(  //
      const NodeType node_type,
      const size_t record_count,
      const Mapping_t *sib_node)
      : node_type_{node_type},
        delta_type_{DeltaType::kNotDelta},
        record_count_{static_cast<uint16_t>(record_count)},
        next_node_{reinterpret_cast<const uintptr_t>(sib_node)}
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
      const DeltaType delta_type)
      : node_type_{node_type}, delta_type_{delta_type}, next_node_{0}
  {
  }

  /*####################################################################################
   * Internal variables
   *##################################################################################*/

  /// a flag to indicate the types of a delta node.
  uint16_t delta_type_ : 3;

  /// a flag to indicate whether this node is a leaf or internal node.
  uint16_t node_type_ : 1;

  /// a blank block for alignment.
  uint16_t : 0;

  /// the number of records in this node.
  uint16_t record_count_;

  /// a blank block for alignment.
  uint64_t : 0;

  /// the pointer to the next node.
  uintptr_t next_node_;

  /// metadata of a lowest key or a first record in a delta node
  Metadata low_meta_;

  /// metadata of a highest key or a second record in a delta node
  Metadata high_meta_;

  /// an actual data block (it starts with record metadata).
  Metadata meta_array_[0];
};

}  // namespace dbgroup::index::bw_tree::component

namespace dbgroup::memory
{
template <class Key, class Comp>
void
Delete(::dbgroup::index::bw_tree::component::Node<Key, Comp> *obj)
{
  ::dbgroup::index::bw_tree::component::Node<Key, Comp>::DeleteNode(obj);
}

}  // namespace dbgroup::memory
