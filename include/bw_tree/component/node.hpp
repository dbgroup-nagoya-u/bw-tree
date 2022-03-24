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
#include "logical_id.hpp"
#include "memory/utility.hpp"
#include "metadata.hpp"
#include "node_info.hpp"

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
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using LogicalID_t = LogicalID<Key, Comp>;

 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct an initial root node.
   *
   */
  constexpr Node() : node_type_{kLeaf}, delta_type_{kNotDelta} {}

  /**
   * @brief Construct a new root node.
   *
   * @param split_ptr
   * @param left_lid
   */
  Node(  //
      const Node *split_d,
      const LogicalID_t *left_lid)
      : node_type_{kInternal}, delta_type_{kNotDelta}, record_count_{2}
  {
    // set a split-left page
    const auto meta = split_d->low_meta_;
    auto offset = SetPayload(kPageSize, left_lid);
    offset = CopyKeyFrom(split_d, meta, offset);
    meta_array_[0] = meta.UpdateForInternal(offset);

    // set a split-right page
    const auto *right_lid = split_d->template GetPayload<LogicalID_t *>(meta);
    offset = SetPayload(offset, right_lid);
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
      const bool node_type,
      const std::vector<NodeInfo> &nodes,
      size_t &offset)
      : node_type_{static_cast<NodeType>(node_type)}, delta_type_{kNotDelta}
  {
    // copy the lowest key
    const auto *low_node = reinterpret_cast<const Node *>(nodes.back().node_ptr);
    const auto low_meta = low_node->low_meta_;
    const auto low_key_len = low_meta.GetKeyLength();
    if (low_key_len > 0) {
      offset -= low_key_len;
      memcpy(ShiftAddr(this, offset), low_node->GetKeyAddr(low_meta), low_key_len);
      low_meta_ = Metadata{offset, low_key_len, low_key_len};
    } else {
      low_meta_ = Metadata{offset, 0, 0};
    }

    // prepare a node that has the highest key, and copy the next logical ID
    const Node *high_node{};
    Metadata high_meta{};
    if (nodes.front().sep_ptr == nullptr) {
      // an original or merged base node
      high_node = reinterpret_cast<const Node *>(nodes.front().node_ptr);
      high_meta = high_node->high_meta_;
      next_ = high_node->next_;
    } else {
      // a split-delta record
      high_node = reinterpret_cast<const Node *>(nodes.front().sep_ptr);
      high_meta = high_node->low_meta_;
      next_ = high_node->template GetPayload<uintptr_t>(high_meta);
    }

    // copy the highest key
    const auto high_key_len = high_meta.GetKeyLength();
    if (high_key_len > 0) {
      offset -= high_key_len;
      memcpy(ShiftAddr(this, offset), high_node->GetKeyAddr(high_meta), high_key_len);
      high_meta_ = Metadata{offset, high_key_len, high_key_len};
    } else {
      high_meta_ = Metadata{offset, 0, 0};
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
    return node_type_ == kLeaf;
  }

  /**
   * @retval true if this is a base node.
   * @retval false if this is a delta record.
   */
  [[nodiscard]] constexpr auto
  IsBaseNode() const  //
      -> bool
  {
    return delta_type_ == kNotDelta;
  }

  /**
   * @retval true if this node has a left sibling node.
   * @retval false otherwise.
   */
  [[nodiscard]] auto
  IsLeftmostChildIn(const std::vector<LogicalID_t *> &stack) const  //
      -> bool
  {
    const auto depth = stack.size();
    if (depth <= 1) return true;

    const auto child_len = low_meta_.GetKeyLength();
    if (child_len == 0) return true;

    const auto *parent_lid = stack.at(depth - 2);
    auto *parent = parent_lid->template Load<Node *>();
    while (true) {
      if (parent == nullptr || parent->delta_type_ == kNodeRemoved) {
        // the parent node is removed, so abort
        return true;
      }
      if (parent->delta_type_ == kNotDelta) break;
      parent = reinterpret_cast<Node *>(parent->next_);
    }
    const auto parent_len = parent->low_meta_.GetKeyLength();
    if (parent_len == 0) return false;

    return IsEqual<Comp>(GetLowKey(), parent->GetLowKey());
  }

  [[nodiscard]] auto
  HasSameLowKeyWith(const Key &key) const  //
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
      -> LogicalID_t *
  {
    return reinterpret_cast<LogicalID_t *>(next_);
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
    int64_t end_pos{};
    if (node_type_ == kLeaf) {
      end_pos = record_count_ - 1;
    } else if (high_meta_.GetKeyLength() == 0 || Comp{}(key, GetKey(high_meta_))) {
      end_pos = record_count_ - 2;
    } else {
      return {kKeyExist, record_count_ - 1};
    }

    int64_t begin_pos = 0;
    while (begin_pos <= end_pos) {
      const size_t pos = (begin_pos + end_pos) >> 1UL;  // NOLINT
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
      -> LogicalID_t *
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

    return GetPayload<LogicalID_t *>(meta_array_[begin_pos]);
  }

  template <bool kIsInternal>
  [[nodiscard]] static auto
  PrepareConsolidation(std::vector<NodeInfo> &nodes)  //
      -> size_t
  {
    const auto end_pos = nodes.size() - 1;
    size_t size = 0;

    for (size_t i = 0; i <= end_pos; ++i) {
      auto &[node_ptr, sep_ptr, rec_num] = nodes.at(i);
      const auto *node = reinterpret_cast<const Node *>(node_ptr);

      // add the length of the lowest key
      if (kIsInternal || i == end_pos) {
        size += node->low_meta_.GetKeyLength();
      }

      // check the number of records to be consolidated
      if (sep_ptr == nullptr) {
        rec_num = node->record_count_;

        if (i == 0) {  // add the length of the highest key
          size += node->high_meta_.GetKeyLength();
        }
      } else {
        const auto *sep_node = reinterpret_cast<const Node *>(sep_ptr);
        const auto sep_meta = sep_node->low_meta_;
        const auto &sep_key = sep_node->GetKey(sep_meta);
        auto [rc, pos] = node->SearchRecord(sep_key);
        rec_num = (kIsInternal || rc == kKeyExist) ? pos + 1 : pos;

        if (i == 0) {  // add the length of the highest key
          size += sep_meta.GetKeyLength();
        }
      }

      // compute the length of a data block
      if (rec_num > 0) {
        const auto end_meta = node->meta_array_[0];
        const auto end_offset = end_meta.GetOffset() + end_meta.GetTotalLength();
        const auto begin_offset = node->meta_array_[rec_num - 1].GetOffset();
        size += sizeof(Metadata) * rec_num + (end_offset - begin_offset);
      }
    }

    return size;
  }

  template <class T>
  void
  LeafConsolidate(  //
      const std::vector<NodeInfo> &nodes,
      const std::vector<std::pair<Key, const void *>> &records,
      size_t offset)
  {
    const auto new_rec_num = records.size();

    // perform merge-sort to consolidate a node
    size_t j = 0;
    for (int64_t k = nodes.size() - 1; k >= 0; --k) {
      const auto *node = reinterpret_cast<const Node *>(nodes.at(k).node_ptr);
      const auto base_rec_num = nodes.at(k).rec_num;
      for (size_t i = 0; i < base_rec_num; ++i) {
        // copy new records
        const auto meta = node->meta_array_[i];
        const auto &node_key = node->GetKey(meta);
        for (; j < new_rec_num && Comp{}(records[j].first, node_key); ++j) {
          offset = CopyRecordFrom<T>(records[j].second, offset);
        }

        // check a new record is updated one
        if (j < new_rec_num && !Comp{}(node_key, records[j].first)) {
          offset = CopyRecordFrom<T>(records[j].second, offset);
          ++j;
        } else {
          offset = CopyRecordFrom<T>(node, meta, offset);
        }
      }
    }

    // copy remaining new records
    for (; j < new_rec_num; ++j) {
      offset = CopyRecordFrom<T>(records[j].second, offset);
    }
  }

  void
  InternalConsolidate(  //
      const std::vector<NodeInfo> &nodes,
      const std::vector<std::pair<Key, const void *>> &records,
      size_t offset)
  {
    const auto new_rec_num = records.size();

    // perform merge-sort to consolidate a node
    bool payload_is_embedded = false;
    size_t j = 0;
    for (int64_t k = nodes.size() - 1; k >= 0; --k) {
      const auto *node = reinterpret_cast<const Node *>(nodes.at(k).node_ptr);
      const auto end_pos = nodes.at(k).rec_num - 1;
      for (size_t i = 0; i <= end_pos; ++i) {
        // copy a payload of a base node in advance to swap that of a new index entry
        auto meta = node->meta_array_[i];
        if (!payload_is_embedded) {  // skip a deleted page
          offset = CopyPayloadFrom<LogicalID_t *>(node, meta, offset);
        }

        // get a current key in the base node
        if (i == end_pos) {
          if (k == 0) break;  // the last record does not need a key

          // if nodes are merged, a current key is equivalent with the lowest one in the next node
          node = reinterpret_cast<const Node *>(nodes.at(k - 1).node_ptr);
          meta = node->low_meta_;
        }
        const auto &node_key = node->GetKey(meta);

        // insert new index entries
        for (; j < new_rec_num && Comp{}(records[j].first, node_key); ++j) {
          offset = CopyIndexEntryFrom(records[j].second, offset);
        }

        // set a key for the current record
        if (j < new_rec_num && !Comp{}(node_key, records[j].first)) {
          // a record is in a base node, but it may be deleted and inserted again
          offset = CopyIndexEntryFrom(records[j].second, offset);
          payload_is_embedded = true;
          ++j;
        } else {
          // copy a key in a base node
          offset = CopyKeyFrom(node, meta, offset);
          meta_array_[record_count_++] = meta.UpdateForInternal(offset);
          payload_is_embedded = false;
        }
      }
    }

    // copy remaining new records
    for (; j < new_rec_num; ++j) {
      offset = CopyIndexEntryFrom(records[j].second, offset);
    }

    // the last record has only a child page
    meta_array_[record_count_++] = Metadata{offset, 0, kWordSize};
  }

  void
  Split()
  {
    // get the number of records and metadata of a separator key
    const auto l_num = record_count_ >> 1UL;
    record_count_ -= l_num;
    low_meta_ = meta_array_[l_num - 1];

    // shift metadata to use a consolidated node as a split-right node
    memmove(&meta_array_[0], &meta_array_[l_num], sizeof(Metadata) * record_count_);
  }

 private:
  /*####################################################################################
   * Internal getters setters
   *##################################################################################*/

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
      const size_t key_len)  //
      -> size_t
  {
    offset -= key_len;
    if constexpr (IsVariableLengthData<T>()) {
      memcpy(ShiftAddr(this, offset), key, key_len);
    } else {
      memcpy(ShiftAddr(this, offset), &key, key_len);
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
  template <class T>
  auto
  CopyPayloadFrom(  //
      const Node *node,
      const Metadata meta,
      size_t offset)  //
      -> size_t
  {
    // copy a record from the given node
    offset -= sizeof(T);
    memcpy(ShiftAddr(this, offset), node->GetPayloadAddr(meta), sizeof(T));

    return offset;
  }

  /**
   * @brief Copy a record from a base node in the leaf level.
   *
   * @param node an original base node or delta record.
   * @param meta metadata of a target record.
   * @param offset the current offset of this node.
   * @return the updated offset value.
   */
  template <class T>
  auto
  CopyRecordFrom(  //
      const Node *node,
      const Metadata meta,
      size_t offset)  //
      -> size_t
  {
    // copy a record from the given node
    const auto rec_len = meta.GetTotalLength();
    offset -= rec_len;
    memcpy(ShiftAddr(this, offset), node->GetKeyAddr(meta), rec_len);

    // update metadata
    meta_array_[record_count_++] = meta.UpdateForLeaf(offset);

    return offset;
  }

  /**
   * @brief Copy a record from a delta record in the leaf level.
   *
   * @param rec_ptr an original delta record.
   * @param offset the current offset of this node.
   * @return the updated offset value.
   */
  template <class T>
  auto
  CopyRecordFrom(  //
      const void *rec_ptr,
      size_t offset)  //
      -> size_t
  {
    const auto *rec = reinterpret_cast<const Node *>(rec_ptr);
    if (rec->delta_type_ != kDelete) {
      // copy a record from the given node or delta record
      const auto meta = rec->low_meta_;
      const auto rec_len = meta.GetTotalLength();
      offset -= rec_len;
      memcpy(ShiftAddr(this, offset), rec->GetKeyAddr(meta), rec_len);

      // update metadata
      meta_array_[record_count_++] = meta.UpdateForLeaf(offset);
    }

    return offset;
  }

  auto
  CopyIndexEntryFrom(  //
      const void *delta_ptr,
      size_t offset)  //
      -> size_t
  {
    const auto *delta = reinterpret_cast<const Node *>(delta_ptr);
    if (delta->delta_type_ == kInsert) {
      // copy a key to exchange a child page
      const auto meta = delta->low_meta_;
      offset = CopyKeyFrom(delta, meta, offset);

      // set metadata for an inserted record
      meta_array_[record_count_++] = meta.UpdateForInternal(offset);

      // copy the next (split-right) child page
      offset = CopyPayloadFrom<LogicalID_t *>(delta, meta, offset);
    }

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

#endif  // BW_TREE_COMPONENT_NODE_HPP
