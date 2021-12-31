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
#include "delta_record.hpp"
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
   * @brief Construct an initial root node.
   *
   */
  constexpr Node()
      : node_type_{NodeType::kLeaf}, delta_type_{DeltaType::kNotDelta}, record_count_{0}
  {
  }

  /**
   * @brief Construct a new base node object.
   *
   * @param node_type a flag to indicate whether a leaf or internal node is constructed.
   * @param record_count the number of records in this node.
   * @param sib_node the pointer to a sibling node.
   */
  Node(uintptr_t sib_page) : delta_type_{DeltaType::kNotDelta}, next_node_{sib_page} {}

  /**
   * @brief Construct a new root node.
   *
   * @param split_ptr
   * @param left_page
   */
  Node(  //
      uintptr_t split_ptr,
      std::atomic_uintptr_t *left_page)
      : node_type_{NodeType::kInternal}, delta_type_{DeltaType::kNotDelta}, record_count_{2},
  {
    auto *split_delta = reinterpret_cast<Node *>(split_ptr);

    // set a split-left page
    auto meta = split_delta->low_meta_;
    auto &&sep_key = split_delta->GetKey(meta);
    auto key_len = meta.GetKeyLength();
    auto offset = SetData(kPageSize, left_page, kWordSize);
    offset = SetData(offset, sep_key, key_len);
    meta_array_[0] = Metadata{offset, key_len, key_len + kWordSize};

    // set a split-right page
    auto right_page = split_delta->template GetPayload<uintptr_t>(meta);
    offset = SetData(offset, right_page, kWordSize);
    meta_array_[1] = Metadata{offset, 0, kWordSize};
  }

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
   * Public getters/setters
   *##################################################################################*/

  /**
   * @retval true if this is a leaf node.
   * @retval false if this is an internal node.
   */
  [[nodiscard]] constexpr auto
  IsLeaf() const  //
      -> bool
  {
    return node_type_;
  }

  /**
   * @retval true if this is a base node.
   * @retval false if this is a delta record.
   */
  [[nodiscard]] constexpr auto
  IsBaseNode() const  //
      -> bool
  {
    return delta_type_ == DeltaType::kNotDelta;
  }

  /**
   * @return the number of records in this node.
   */
  [[nodiscard]] constexpr auto
  GetRecordCount() const  //
      -> size_t
  {
    return record_count_;
  }

  [[nodiscard]] constexpr auto
  GetSiblingNode() const  //
      -> std::atomic_uintptr_t *
  {
    return reinterpret_cast<std::atomic_uintptr_t *>(next_);
  }

  /**
   * @param position the position of record metadata to be get.
   * @return Metadata: record metadata.
   */
  [[nodiscard]] constexpr auto
  GetMetadata(const size_t position) const  //
      -> Metadata
  {
    return meta_array_[position];
  }

  /**
   * @param meta metadata of a corresponding record.
   * @return a target key.
   */
  [[nodiscard]] auto
  GetKey(const Metadata meta) const  //
      -> Key
  {
    if constexpr (IsVariableLengthData<Key>()) {
      return reinterpret_cast<Key>(GetKeyAddr(meta));
    } else {
      Key key{};
      memcpy(&key, GetKeyAddr(meta), sizeof(Key));
      return key;
    }
  }

  /**
   * @tparam T a class of a target payload.
   * @param meta metadata of a corresponding record.
   * @return a target payload.
   */
  template <class T>
  [[nodiscard]] auto
  GetPayload(const Metadata meta) const  //
      -> T
  {
    if constexpr (IsVariableLengthData<T>()) {
      return reinterpret_cast<T>(GetPayloadAddr(meta));
    } else {
      T payload{};
      memcpy(&payload, GetPayloadAddr(meta), sizeof(T));
      return payload;
    }
  }

  /**
   * @brief Copy a target payload to a specified reference.
   *
   * @param meta metadata of a corresponding record.
   */
  template <class T>
  [[nodiscard]] auto
  CopyPayload(const size_t pos) const  //
      -> T
  {
    if constexpr (IsVariableLengthData<T>()) {
      const auto pay_len = meta_array_[pos].GetPayloadLength();
      auto payload = reinterpret_cast<T>(::operator new(pay_len));
      memcpy(payload, GetPayloadAddr(meta_array_[pos]), pay_len);
      return payload;
    } else {
      T payload{};
      memcpy(&payload, GetPayloadAddr(meta_array_[pos]), sizeof(T));
      return payload;
    }
  }

  [[nodiscard]] auto
  GetPageSize(  //
      const std::optional<Key> &high_key,
      const Metadata high_meta) const  //
      -> std::pair<size_t, size_t>
  {
    if (record_count_ == 0) return {0, 0};

    auto rec_num = (high_key) ? SearchChild(*high_key, kClosed) : record_count_;
    auto end_offset = meta_array_[0].GetOffset() + meta_array_[0].GetTotalLength();
    auto begin_offset = meta_array_[rec_num - 1].GetOffset();

    auto size = sizeof(Metadata) * rec_num                              // metadata
                + end_offset - begin_offset                             // records
                + low_meta_.GetKeyLength() + high_meta.GetKeyLength();  // low/high keys

    return {size, rec_num};
  }

  /*####################################################################################
   * Public utilities
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

  template <class T>
  void
  Consolidate(  //
      const Node *node,
      const std::vector<std::pair<Key, uintptr_t>> &records,
      const std::optional<Key> &high_key,
      const Metadata high_meta,
      size_t offset,
      const size_t base_rec_num)
  {
    // copy the lowest key
    low_meta_ = node->low_meta_;
    if (low_meta_.GetKeyLength() > 0) {
      offset = CopyKeyFrom(node, low_meta_, offset);
    }
    low_meta_.SetOffset(offset);

    // copy the lowest key
    high_meta_ = high_meta;
    if (high_key) {
      offset = SetData(offset, *high_key, high_meta_.GetKeyLength());
    }
    high_meta_.SetOffset(offset);

    // perform merge-sort to consolidate a node
    const auto new_rec_num = records.size();
    size_t rec_count = 0;
    size_t j = 0;
    Key &rec_key{};
    uintptr_t rec_ptr{};
    Node *rec{};
    for (size_t i = 0; i < base_rec_num; ++i) {
      // copy new records
      auto base_meta = node->meta_array_[i];
      const auto &base_key = node->GetKey(base_meta);
      for (; j < new_rec_num; ++j) {
        std::tie(rec_key, rec_ptr) = records[j];
        rec = reinterpret_cast<Node *>(rec_ptr);
        if (!Comp{}(rec_key, base_key)) break;

        // check a new record has any payload
        if (rec->HasPayload()) {
          offset = CopyRecordFrom(rec, rec->low_meta_, rec_count++, offset);
        }
      }

      // check a new record is updated one
      if (j < new_rec_num && !Comp{}(base_key, rec_key)) {
        if (rec->HasPayload()) {
          offset = CopyRecordFrom(rec, rec->low_meta_, rec_count++, offset);
        }
        ++j;
      } else {
        offset = CopyRecordFrom(node, base_meta, rec_count++, offset);
      }
    }

    // copy remaining new records
    for (; j < new_rec_num; ++j) {
      rec = records[j].second;
      if (rec->HasPayload()) {
        offset = CopyRecordFrom(rec, rec->low_meta_, rec_count++, offset);
      }
    }

    // set header information
    record_count_ = rec_count;
  }

  void
  Split()  //
      ->std::pair<Key, size_t>
  {
    // get the number of records and metadata of a separator key
    const auto l_num = record_count_ >> 1UL;
    record_count_ -= l_num;
    low_meta_ = meta_array_[l_num - 1];

    // shift metadata to use a consolidated node as a split-right node
    memmove(&meta_array_[0], &meta_array_[l_num], sizeof(Metadata) * record_count_);

    return {GetKey(low_meta_), low_meta_.GetKeyLength()};
  }

 private:
  /*####################################################################################
   * Internal getters setters
   *##################################################################################*/

  [[nodescard]] constexpr auto
  HasPayload() const  //
      -> bool
  {
    return delta_type_ == DeltaType::kInsert || delta_type_ == DeltaType::kModify;
  }

  /**
   * @param meta metadata of a corresponding record.
   * @return an address of a target key.
   */
  constexpr auto
  GetKeyAddr(const Metadata meta) const  //
      -> void *
  {
    return ShiftAddr(this, meta.GetOffset());
  }

  /**
   * @param meta metadata of a corresponding record.
   * @return an address of a target payload.
   */
  constexpr auto
  GetPayloadAddr(const Metadata meta) const  //
      -> void *
  {
    return ShiftAddr(this, meta.GetOffset() + meta.GetKeyLength());
  }

  /**
   * @brief Set a target data directly.
   *
   * @tparam T a class of data.
   * @param offset an offset to set a target data.
   * @param payload a target payload to be set.
   * @param payload_length the length of a target payload.
   */
  template <class T>
  auto
  SetData(  //
      size_t offset,
      const T &data,
      [[maybe_unused]] const size_t data_len)  //
      -> size_t
  {
    if constexpr (IsVariableLengthData<T>()) {
      offset -= data_len;
      memcpy(ShiftAddr(this, offset), data, data_len);
    } else {
      offset -= sizeof(T);
      memcpy(ShiftAddr(this, offset), &data, sizeof(T));
    }

    return offset;
  }

  /*####################################################################################
   * Internal utilities
   *##################################################################################*/

  /**
   * @brief Copy a record from a base node.
   *
   * @param node an original node that has a target record.
   * @param meta the corresponding metadata of a target record.
   * @param offset the current offset of this node.
   * @return the updated offset value.
   */
  auto
  CopyKeyFrom(  //
      const Node *node,
      const Metadata meta,
      size_t offset)  //
      -> size_t
  {
    // copy a record from the given node
    const auto key_len = meta.GetKeyLength();
    offset -= key_len;
    memcpy(ShiftAddr(this, offset), node->GetKeyAddr(meta), key_len);

    return offset;
  }

  /**
   * @brief Copy a record from a delta record.
   *
   * @param rec an original delta record.
   * @param rec_count the current number of records in this node.
   * @param offset the current offset of this node.
   * @return the updated offset value.
   */
  auto
  CopyRecordFrom(  //
      const Node *node,
      const Metadata meta,
      const size_t rec_num,
      size_t offset)  //
      -> size_t
  {
    // copy a record from the given delta record
    const auto rec_len = meta.GetTotalLength();
    offset -= rec_len;
    memcpy(ShiftAddr(this, offset), node->GetKeyAddr(meta), rec_len);

    // update metadata
    meta.SetOffset(offset);
    meta_array_[rec_num] = meta;

    return offset;
  }

  /*####################################################################################
   * Internal variables
   *##################################################################################*/

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
  uintptr_t next_node_{kNullPtr};

  /// metadata of a lowest key or a first record in a delta node
  Metadata low_meta_{kPageSize, 0, 0};

  /// metadata of a highest key or a second record in a delta node
  Metadata high_meta_{kPageSize, 0, 0};

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
