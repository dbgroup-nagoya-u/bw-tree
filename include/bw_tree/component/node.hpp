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

#ifndef BW_TREE_COMPONENT_NODE_HPP
#define BW_TREE_COMPONENT_NODE_HPP

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
  constexpr Node() : node_type_{NodeType::kLeaf}, delta_type_{DeltaType::kNotDelta} {}

  /**
   * @brief Construct a new root node.
   *
   * @param split_ptr
   * @param left_page
   */
  Node(  //
      const uintptr_t split_ptr,
      std::atomic_uintptr_t *left_page)
      : node_type_{NodeType::kInternal}, delta_type_{DeltaType::kNotDelta}, record_count_{2}
  {
    auto *split_delta = reinterpret_cast<const Node *>(split_ptr);

    // set a split-left page
    auto meta = split_delta->low_meta_;
    auto &&sep_key = split_delta->GetKey(meta);
    auto key_len = meta.GetKeyLength();
    auto offset = SetPayload(kPageSize, left_page);
    offset = SetKey(offset, sep_key, key_len);
    meta_array_[0] = Metadata{offset, key_len, key_len + kWordSize};

    // set a split-right page
    auto right_page = split_delta->template GetPayload<uintptr_t>(meta);
    offset = SetPayload(offset, right_page);
    meta_array_[1] = Metadata{offset, 0, kWordSize};
  }

  /**
   * @brief Construct a consolidated base node object.
   *
   * @param node_type a flag to indicate whether a leaf or internal node is constructed.
   * @param record_count the number of records in this node.
   * @param sib_node the pointer to a sibling node.
   */
  Node(  //
      const uintptr_t low_ptr,
      const uintptr_t high_ptr,
      const Metadata high_meta)
      : delta_type_{kNotDelta}, high_meta_{high_meta}
  {
    const auto *low_node = reinterpret_cast<Node *>(low_ptr);
    node_type_ = low_node->node_type_;
    low_meta_ = low_node->low_meta_;
    const auto *high_node = reinterpret_cast<Node *>(high_ptr);
    if (high_node->delta_type_ == kNotDelta) {
      next_ = high_node->next_;
    } else {  // a split-delta record
      next_ = high_node->template GetPayload<uintptr_t>(high_meta);
    }
  }

  Node(const Node &) = delete;
  Node(Node &&) = delete;

  Node &operator=(const Node &) = delete;
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
   * @retval true if this node has a left sibling node.
   * @retval false otherwise.
   */
  [[nodiscard]] auto
  IsLeftmostChildIn(const std::vector<std::atomic_uintptr_t *> &stack) const  //
      -> bool
  {
    const auto depth = stack.size();
    if (depth <= 1) return true;

    const auto child_len = low_meta_.GetKeyLength();
    if (child_len == 0) return true;

    const auto *parent_page = stack.at(depth - 2);
    auto *parent = reinterpret_cast<Node *>(parent_page->load(std::memory_order_acquire));
    while (parent->delta_type_ != kNotDelta) {
      parent = reinterpret_cast<Node *>(parent->next_);
    }
    const auto parent_len = parent->low_meta_.GetKeyLength();
    if (parent_len == 0) return false;

    return IsEqual<Comp>(GetLowKey(), parent->GetLowKey());
  }

  [[nodiscard]] auto
  HasSameLowKeyWith(const Key &key)  //
      -> bool
  {
    if (low_meta_.GetKeyLength() == 0) return false;

    return IsEqual<Comp>(GetLowKey(), key);
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
   * @brief Get the lowest key in this node.
   *
   * This function assumes that the node has the lowest key (i.e., has a left sibling
   * node) and does not check its existence.
   *
   * @return the lowest key.
   */
  [[nodiscard]] auto
  GetLowKey() const  //
      -> Key
  {
    return GetKey(low_meta_);
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
    T payload{};
    memcpy(&payload, GetPayloadAddr(meta), sizeof(T));
    return payload;
  }

  [[nodiscard]] static auto
  GetPageSize(  //
      std::vector<std::tuple<uintptr_t, uintptr_t, Metadata>> &nodes,
      const Metadata h_meta)  //
      -> size_t
  {
    size_t rec_num{};
    auto size = reinterpret_cast<Node *>(std::get<0>(nodes.back()))->low_meta_.GetKeyLength()
                + h_meta.GetKeyLength();

    for (auto &[node_ptr, sep_ptr, sep_meta] : nodes) {
      const auto *node = reinterpret_cast<Node *>(node_ptr);
      if (node->record_count_ == 0) {
        // if a base node does not have any record, skip it
        sep_ptr = 0;  // update the 2nd member as the number of records
        continue;
      }

      // check the number of records to be consolidated
      if (sep_meta.GetKeyLength() > 0) {
        const auto *sep_node = reinterpret_cast<Node *>(sep_ptr);
        const auto &sep_key = sep_node->GetKey(sep_meta);
        auto [rc, pos] = node->SearchRecord(sep_key);
        rec_num = (rc == kKeyNotExist) ? pos : pos + 1;
      } else {  //
        rec_num = node->record_count_;
      }
      sep_ptr = rec_num;  // update the 2nd member as the number of records
      if (rec_num == 0) continue;

      // compute the size of records
      auto end_offset = node->meta_array_[0].GetOffset() + node->meta_array_[0].GetTotalLength();
      auto begin_offset = node->meta_array_[rec_num - 1].GetOffset();
      size += sizeof(Metadata) * rec_num + (end_offset - begin_offset);
    }

    return size;
  }

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  void
  RemoveSideLink()
  {
    next_ = kNullPtr;
  }

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
      -> std::pair<ReturnCode, size_t>
  {
    int64_t begin_pos = 0;
    int64_t end_pos = record_count_ - 1;
    if (meta_array_[end_pos].GetKeyLength() == 0) --end_pos;
    while (begin_pos <= end_pos) {
      size_t pos = (begin_pos + end_pos) >> 1UL;  // NOLINT

      const auto &index_key = GetKey(meta_array_[pos]);

      if (Comp{}(key, index_key)) {  // a target key is in a left side
        end_pos = pos - 1;
      } else if (Comp{}(index_key, key)) {  // a target key is in a right side
        begin_pos = pos + 1;
      } else {  // find an equivalent key
        return {kKeyExist, pos};
      }
    }

    return {kKeyNotExist, begin_pos};
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
  LeafConsolidate(  //
      const std::vector<std::tuple<uintptr_t, uintptr_t, Metadata>> &nodes,
      const std::vector<std::pair<Key, uintptr_t>> &records,
      const uintptr_t high_ptr,
      size_t offset)
  {
    // copy low/high keys
    const auto *node = reinterpret_cast<Node *>(std::get<0>(nodes.back()));
    offset = CopyLowKeyFrom(node, low_meta_, offset);
    node = reinterpret_cast<Node *>(high_ptr);
    offset = CopyHighKeyFrom(node, high_meta_, offset);

    // perform merge-sort to consolidate a node
    const auto new_rec_num = records.size();
    size_t j = 0;
    for (int64_t k = nodes.size() - 1; k >= 0; --k) {
      const auto [node_ptr, base_rec_num, dummy] = nodes.at(k);
      node = reinterpret_cast<Node *>(node_ptr);
      for (size_t i = 0; i < base_rec_num; ++i) {
        // copy new records
        const auto meta = node->meta_array_[i];
        const auto &key = node->GetKey(meta);
        for (; j < new_rec_num && Comp{}(records[j].first, key); ++j) {
          // check a new record has any payload
          auto *rec = reinterpret_cast<Node *>(records[j].second);
          if (rec->delta_type_ != kDelete) {
            offset = CopyRecordFrom(rec, rec->low_meta_, record_count_++, offset);
          }
        }

        // check a new record is updated one
        if (j < new_rec_num && !Comp{}(key, records[j].first)) {
          auto *rec = reinterpret_cast<Node *>(records[j].second);
          if (rec->delta_type_ != kDelete) {
            offset = CopyRecordFrom(rec, rec->low_meta_, record_count_++, offset);
          }
          ++j;
        } else {
          offset = CopyRecordFrom(node, meta, record_count_++, offset);
        }
      }
    }

    // copy remaining new records
    for (; j < new_rec_num; ++j) {
      auto *rec = reinterpret_cast<Node *>(records[j].second);
      if (rec->delta_type_ != kDelete) {
        offset = CopyRecordFrom(rec, rec->low_meta_, record_count_++, offset);
      }
    }
  }

  void
  InternalConsolidate(  //
      const std::vector<std::tuple<uintptr_t, uintptr_t, Metadata>> &nodes,
      const std::vector<std::pair<Key, uintptr_t>> &records,
      const uintptr_t high_ptr,
      size_t offset)
  {
    if (low_meta_.GetKeyLength() > 0) {
      const auto *node = reinterpret_cast<Node *>(std::get<0>(nodes.back()));
      offset = CopyKeyFrom(node, low_meta_, offset);
    }
    low_meta_.SetOffset(offset);

    // copy the lowest key
    if (high_meta_.GetKeyLength() > 0) {
      const auto *high_node = reinterpret_cast<Node *>(high_ptr);
      offset = CopyKeyFrom(high_node, high_meta_, offset);
    }
    high_meta_.SetOffset(offset);

    // perform merge-sort to consolidate a node
    const auto new_rec_num = records.size();
    size_t rec_count = 0;
    size_t j = 0;
    Node *rec{};
    for (int64_t k = nodes.size() - 1; k >= 0; --k) {
      const auto [node_ptr, base_rec_num, dummy] = nodes.at(k);
      const auto *node = reinterpret_cast<Node *>(node_ptr);
      bool prev_rec_deleted = false;
      for (size_t i = 0; i < base_rec_num; ++i) {
        // copy a payload of a base node in advance to swap that of a new index entry
        auto meta = node->meta_array_[i];
        if (!prev_rec_deleted) {
          offset = CopyPayloadFrom(node, meta, offset);
        }

        // insert new index entries
        const auto &key = (meta.GetKeyLength() > 0) ? std::make_optional(node->GetKey(meta))  //
                                                    : std::nullopt;
        for (; j < new_rec_num; ++j) {
          const auto &[rec_key, rec_ptr] = records[j];
          rec = reinterpret_cast<Node *>(rec_ptr);
          if (key && !Comp{}(rec_key, *key)) break;

          // check a new record has any payload
          if (rec->HasPayload()) {
            auto rec_meta = rec->low_meta_;
            offset = CopyKeyFrom(rec, rec_meta, offset);
            SetMetadata(rec_meta, rec_count++, offset);
            offset = CopyPayloadFrom(rec, rec_meta, offset);
          }
        }

        if (j < new_rec_num && !Comp{}(*key, records[j].first)) {
          // the record is deleted
          ++j;
          prev_rec_deleted = true;
        } else {
          // finalize setting a record
          offset = CopyKeyFrom(node, meta, offset);
          SetMetadata(meta, rec_count++, offset);
          prev_rec_deleted = false;
        }
      }
    }

    // copy remaining delta records
    if (j < new_rec_num) {
      // copy a payload of a base node in advance to swap that of a new index entry
      const auto [node_ptr, base_rec_num, dummy] = nodes.front();
      const auto *node = reinterpret_cast<Node *>(node_ptr);
      offset = CopyPayloadFrom(node, node->meta_array_[base_rec_num], offset);

      // insert new index entries
      const auto tmp_rec_num = new_rec_num - 1;
      for (; j < tmp_rec_num; ++j) {
        rec = reinterpret_cast<Node *>(records[j].second);

        // check a new record has any payload
        if (rec->HasPayload()) {
          auto rec_meta = rec->low_meta_;
          offset = CopyKeyFrom(rec, rec_meta, offset);
          SetMetadata(rec_meta, rec_count++, offset);
          offset = CopyPayloadFrom(rec, rec_meta, offset);
        }
      }

      rec = reinterpret_cast<Node *>(records[j].second);
      auto rec_meta = rec->low_meta_;
      offset = CopyKeyFrom(rec, rec_meta, offset);
      SetMetadata(rec_meta, rec_count++, offset);
    }

    // set header information
    record_count_ = rec_count;
  }

  auto
  Split()  //
      -> std::pair<Key, size_t>
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

  [[nodiscard]] constexpr auto
  HasPayload() const  //
      -> bool
  {
    return delta_type_ == DeltaType::kInsert || delta_type_ == DeltaType::kModify;
  }

  /**
   * @param meta metadata of a corresponding record.
   * @return an address of a target key.
   */
  [[nodiscard]] constexpr auto
  GetKeyAddr(const Metadata meta) const  //
      -> void *
  {
    return ShiftAddr(this, meta.GetOffset());
  }

  /**
   * @param meta metadata of a corresponding record.
   * @return an address of a target payload.
   */
  [[nodiscard]] constexpr auto
  GetPayloadAddr(const Metadata meta) const  //
      -> void *
  {
    return ShiftAddr(this, meta.GetOffset() + meta.GetKeyLength());
  }

  void
  SetMetadata(  //
      Metadata meta,
      const size_t rec_num,
      const size_t offset)
  {
    meta.SetOffset(offset);
    meta_array_[rec_num] = meta;
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
  SetKey(  //
      size_t offset,
      const T &key,
      [[maybe_unused]] const size_t key_len)  //
      -> size_t
  {
    if constexpr (IsVariableLengthData<T>()) {
      offset -= key_len;
      memcpy(ShiftAddr(this, offset), key, key_len);
    } else {
      offset -= sizeof(T);
      memcpy(ShiftAddr(this, offset), &key, sizeof(T));
    }

    return offset;
  }

  template <class T>
  auto
  SetPayload(  //
      size_t offset,
      const T &payload)  //
      -> size_t
  {
    offset -= sizeof(T);
    memcpy(ShiftAddr(this, offset), &payload, sizeof(T));
    return offset;
  }

  /*####################################################################################
   * Internal utilities
   *##################################################################################*/

  auto
  CopyLowKeyFrom(  //
      const Node *node,
      const Metadata meta,
      size_t offset)  //
      -> size_t
  {
    const auto key_len = meta.GetKeyLength();

    if (key_len > 0) {
      if constexpr (IsVariableLengthData<Key>()) {
        offset -= key_len;
        memcpy(ShiftAddr(this, offset), node->GetKeyAddr(meta), key_len);
        low_meta_ = Metadata(offset, key_len, key_len);
      } else {
        offset -= sizeof(Key);
        memcpy(ShiftAddr(this, offset), node->GetKeyAddr(meta), sizeof(Key));
        low_meta_ = Metadata(offset, sizeof(Key), sizeof(Key));
      }
    } else {
      low_meta_ = Metadata{offset, 0, 0};
    }

    return offset;
  }

  auto
  CopyHighKeyFrom(  //
      const Node *node,
      const Metadata meta,
      size_t offset)  //
      -> size_t
  {
    const auto key_len = meta.GetKeyLength();

    if (key_len > 0) {
      if constexpr (IsVariableLengthData<Key>()) {
        offset -= key_len;
        memcpy(ShiftAddr(this, offset), node->GetKeyAddr(meta), key_len);
        high_meta_ = Metadata(offset, key_len, key_len);
      } else {
        offset -= sizeof(Key);
        memcpy(ShiftAddr(this, offset), node->GetKeyAddr(meta), sizeof(Key));
        high_meta_ = Metadata(offset, sizeof(Key), sizeof(Key));
      }
    } else {
      high_meta_ = Metadata{offset, 0, 0};
    }

    return offset;
  }

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
   * @brief Copy a record from a base node.
   *
   * @param node an original node that has a target record.
   * @param meta the corresponding metadata of a target record.
   * @param offset the current offset of this node.
   * @return the updated offset value.
   */
  auto
  CopyPayloadFrom(  //
      const Node *node,
      const Metadata meta,
      size_t offset)  //
      -> size_t
  {
    // copy a record from the given node
    const auto pay_len = meta.GetPayloadLength();
    offset -= pay_len;
    memcpy(ShiftAddr(this, offset), node->GetPayloadAddr(meta), pay_len);

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
      Metadata meta,
      const size_t rec_num,
      size_t offset)  //
      -> size_t
  {
    // copy a record from the given delta record
    const auto rec_len = meta.GetTotalLength();
    offset -= rec_len;
    memcpy(ShiftAddr(this, offset), node->GetKeyAddr(meta), rec_len);

    // update metadata
    SetMetadata(meta, rec_num, offset);

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
  uint16_t record_count_{0};

  /// a blank block for alignment.
  uint64_t : 0;

  /// the pointer to the next node.
  uintptr_t next_{kNullPtr};

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

#endif  // BW_TREE_COMPONENT_NODE_HPP
