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

#ifndef BW_TREE_VARLEN_NODE_HPP
#define BW_TREE_VARLEN_NODE_HPP

#include <optional>
#include <utility>
#include <vector>

#include "bw_tree/common/consolidate_info.hpp"
#include "bw_tree/common/logical_id.hpp"
#include "common.hpp"
#include "metadata.hpp"

namespace dbgroup::index::bw_tree::component::varlen
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
  constexpr Node() : node_type_{kLeaf}, delta_type_{kNotDelta} {}

  /**
   * @brief Construct a new root node.
   *
   * @param split_ptr
   * @param left_lid
   */
  Node(  //
      const Node *split_d,
      const LogicalID *left_lid)
      : node_type_{kInternal}, delta_type_{kNotDelta}, record_count_{2}
  {
    // set a split-left page
    const auto meta = split_d->low_meta_;
    const auto key_len = meta.GetKeyLength();
    auto offset = SetPayload(kPageSize, left_lid);
    offset -= key_len;
    memcpy(ShiftAddr(this, offset), split_d->GetKeyAddr(meta), key_len);
    meta_array_[0] = meta.UpdateForInternal(offset);

    // set a split-right page
    const auto *right_lid = split_d->template GetPayload<LogicalID *>(meta);
    offset = SetPayload(offset, right_lid);
    meta_array_[1] = Metadata{offset, 0, kWordSize};
  }

  /**
   * @brief Construct a consolidated base node object.
   *
   * @param node_type a flag to indicate whether a leaf or internal node is constructed.
   */
  Node(  //
      const bool node_type,
      const size_t node_size)
      : node_type_{static_cast<NodeType>(node_type)},
        delta_type_{kNotDelta},
        node_size_{static_cast<uint32_t>(node_size)}
  {
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
   * @return the number of records in this node.
   */
  [[nodiscard]] constexpr auto
  GetRecordCount() const  //
      -> size_t
  {
    return record_count_;
  }

  template <class T = const Node *>
  [[nodiscard]] constexpr auto
  GetNext() const  //
      -> T
  {
    return reinterpret_cast<T>(next_);
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
      -> std::optional<Key>
  {
    if (low_meta_.GetKeyLength() == 0) return std::nullopt;
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

  [[nodiscard]] auto
  GetLeftmostChild() const  //
      -> LogicalID *
  {
    const auto *cur = this;
    for (; !cur->IsBaseNode(); cur = cur->template GetNext<const Node *>()) {
      // go to the next delta record or base node
    }

    // get a leftmost node
    return cur->template GetPayload<LogicalID *>(cur->meta_array_[0]);
  }

  [[nodiscard]] auto
  CopyHighKey() const  //
      -> Key
  {
    const auto key_len = high_meta_.GetKeyLength();

    Key high_key{};
    if constexpr (IsVariableLengthData<Key>()) {
      high_key = reinterpret_cast<Key>(::operator new(key_len));
      memcpy(high_key, GetKeyAddr(high_meta_), key_len);
    } else {
      memcpy(&high_key, GetKeyAddr(high_meta_), key_len);
    }

    return high_key;
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
      -> LogicalID *
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

    return GetPayload<LogicalID *>(meta_array_[begin_pos]);
  }

  [[nodiscard]] auto
  SearchEndPositionFor(const std::optional<std::pair<const Key &, bool>> &end_key) const  //
      -> std::pair<bool, size_t>
  {
    const auto is_end = IsRightmostOf(end_key);
    size_t end_pos{};
    if (is_end && end_key) {
      const auto &[e_key, e_closed] = *end_key;
      const auto [rc, pos] = SearchRecord(e_key);
      end_pos = (rc == kKeyExist && e_closed) ? pos + 1 : pos;
    } else {
      end_pos = record_count_;
    }

    return {is_end, end_pos};
  }

  template <bool kIsLeaf>
  [[nodiscard]] static auto
  PreConsolidate(std::vector<ConsolidateInfo> &consol_info)  //
      -> size_t
  {
    const auto end_pos = consol_info.size() - 1;
    size_t size = 0;

    for (size_t i = 0; i <= end_pos; ++i) {
      auto &[n_ptr, d_ptr, rec_num] = consol_info.at(i);
      const auto *node = reinterpret_cast<const Node *>(n_ptr);
      const auto *split_d = reinterpret_cast<const Node *>(d_ptr);

      // add the length of the lowest key
      if (!kIsLeaf || i == end_pos) {
        size += node->low_meta_.GetKeyLength();
      }

      // check the number of records to be consolidated
      if (split_d == nullptr) {
        rec_num = node->record_count_;

        if (i == 0) {  // add the length of the highest key
          size += node->high_meta_.GetKeyLength();
        }
      } else {
        const auto sep_meta = split_d->low_meta_;
        const auto &sep_key = split_d->GetKey(sep_meta);
        auto [rc, pos] = node->SearchRecord(sep_key);
        rec_num = (!kIsLeaf || rc == kKeyExist) ? pos + 1 : pos;

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
      const std::vector<ConsolidateInfo> &consol_info,
      const std::vector<std::pair<Key, const void *>> &records,
      bool do_split)
  {
    const auto new_rec_num = records.size();

    // copy a lowest key when consolidation
    size_t offset = kHeaderLength / 2;
    if (!do_split) {
      const auto *low_node = reinterpret_cast<const Node *>(consol_info.back().node);
      offset = CopyLowKeyFrom(low_node, low_node->low_meta_);
    }

    // perform merge-sort to consolidate a node
    size_t j = 0;
    for (int64_t k = consol_info.size() - 1; k >= 0; --k) {
      const auto [n_ptr, dummy, base_rec_num] = consol_info.at(k);
      const auto *node = reinterpret_cast<const Node *>(n_ptr);
      for (size_t i = 0; i < base_rec_num; ++i) {
        // copy new records
        const auto meta = node->meta_array_[i];
        const auto &node_key = node->GetKey(meta);
        for (; j < new_rec_num && Comp{}(records[j].first, node_key); ++j) {
          offset = CopyRecordFrom<T>(records[j].second, offset, do_split);
        }

        // check a new record is updated one
        if (j < new_rec_num && !Comp{}(node_key, records[j].first)) {
          offset = CopyRecordFrom<T>(records[j].second, offset, do_split);
          ++j;
        } else {
          offset = CopyRecordFrom<T>(node, meta, offset, do_split);
        }
      }
    }

    // copy remaining new records
    for (; j < new_rec_num; ++j) {
      offset = CopyRecordFrom<T>(records[j].second, offset, do_split);
    }

    // copy a highest key
    CopyHighKeyFrom(consol_info.front(), offset);
  }

  void
  InternalConsolidate(  //
      const std::vector<ConsolidateInfo> &consol_info,
      const std::vector<std::pair<Key, const void *>> &records,
      bool do_split)
  {
    const auto new_rec_num = records.size();

    // copy a lowest key when consolidation
    size_t offset = kHeaderLength / 2;
    if (!do_split) {
      const auto *low_node = reinterpret_cast<const Node *>(consol_info.back().node);
      offset = CopyLowKeyFrom(low_node, low_node->low_meta_);
    }

    // perform merge-sort to consolidate a node
    bool payload_is_embedded = false;
    size_t j = 0;
    for (int64_t k = consol_info.size() - 1; k >= 0; --k) {
      const auto *node = reinterpret_cast<const Node *>(consol_info.at(k).node);
      const auto end_pos = consol_info.at(k).rec_num - 1;
      for (size_t i = 0; i <= end_pos; ++i) {
        // copy a payload of a base node in advance to swap that of a new index entry
        auto meta = node->meta_array_[i];
        if (!payload_is_embedded) {  // skip a deleted page
          offset = CopyPayloadFrom<LogicalID *>(node, meta, offset, do_split);
        }

        // get a current key in the base node
        if (i == end_pos) {
          if (k == 0) break;  // the last record does not need a key

          // if nodes are merged, a current key is equivalent with the lowest one in the next node
          node = reinterpret_cast<const Node *>(consol_info.at(k - 1).node);
          meta = node->low_meta_;
        }
        const auto &node_key = node->GetKey(meta);

        // insert new index entries
        for (; j < new_rec_num && Comp{}(records[j].first, node_key); ++j) {
          offset = CopyIndexEntryFrom(records[j].second, offset, do_split);
        }

        // set a key for the current record
        if (j < new_rec_num && !Comp{}(node_key, records[j].first)) {
          // a record is in a base node, but it may be deleted and inserted again
          offset = CopyIndexEntryFrom(records[j].second, offset, do_split);
          payload_is_embedded = true;
          ++j;
        } else {
          // copy a key in a base node
          offset = CopyKeyFrom(node, meta, offset, do_split);
          payload_is_embedded = false;
        }
      }
    }

    // copy remaining new records
    for (; j < new_rec_num; ++j) {
      offset = CopyIndexEntryFrom(records[j].second, offset, do_split);
    }

    // the last record has only a child page
    meta_array_[record_count_++] = Metadata{offset, 0, kWordSize};

    // copy a highest key
    CopyHighKeyFrom(consol_info.front(), offset);
  }

 private:
  /*####################################################################################
   * Internal getters setters
   *##################################################################################*/

  [[nodiscard]] auto
  IsRightmostOf(const std::optional<std::pair<const Key &, bool>> &end_key) const  //
      -> bool
  {
    if (high_meta_.GetKeyLength() == 0) return true;  // the rightmost node
    if (!end_key) return false;                       // perform full scan
    return !Comp{}(GetKey(high_meta_), end_key->first);
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

  auto
  CopyLowKeyFrom(  //
      const Node *node,
      const Metadata meta)  //
      -> size_t
  {
    // copy the lowest key
    const auto key_len = meta.GetKeyLength();
    auto offset = node_size_;
    if (key_len > 0) {
      offset -= key_len;
      memcpy(ShiftAddr(this, offset), node->GetKeyAddr(meta), key_len);
      low_meta_ = Metadata{offset, key_len, key_len};
    } else {
      low_meta_ = Metadata{0, 0, 0};
    }

    return offset;
  }

  void
  CopyHighKeyFrom(  //
      const ConsolidateInfo &consol_info,
      size_t offset)
  {
    // prepare a node that has the highest key, and copy the next logical ID
    const Node *node{};
    Metadata meta{};
    if (consol_info.split_d == nullptr) {
      // an original or merged base node
      node = reinterpret_cast<const Node *>(consol_info.node);
      meta = node->high_meta_;
      next_ = node->next_;
    } else {
      // a split-delta record
      node = reinterpret_cast<const Node *>(consol_info.split_d);
      meta = node->low_meta_;
      next_ = node->template GetPayload<uintptr_t>(meta);
    }

    // copy the highest key
    const auto key_len = meta.GetKeyLength();
    if (key_len > 0) {
      offset -= key_len;
      memcpy(ShiftAddr(this, offset), node->GetKeyAddr(meta), key_len);
      high_meta_ = Metadata{offset, key_len, key_len};
    } else {
      high_meta_ = Metadata{0, 0, 0};
    }
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
      size_t offset,
      bool &do_split)  //
      -> size_t
  {
    const auto rec_len = meta.GetTotalLength();

    if (do_split) {
      // calculate the skipped page size
      offset += rec_len + kWordSize;
      if (offset > node_size_) {
        // this record is the end one in a split-left node
        do_split = false;
        offset = CopyLowKeyFrom(node, meta);
      }
    } else {
      // copy a record from the given node
      offset -= rec_len;
      memcpy(ShiftAddr(this, offset), node->GetKeyAddr(meta), rec_len);
      meta_array_[record_count_++] = meta.UpdateForLeaf(offset);
    }

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
      size_t offset,
      bool &do_split)  //
      -> size_t
  {
    const auto *rec = reinterpret_cast<const Node *>(rec_ptr);
    if (rec->delta_type_ != kDelete) {
      // the target record is insert/modify delta
      const auto meta = rec->low_meta_;
      const auto rec_len = meta.GetTotalLength();

      if (do_split) {
        // calculate the skipped page size
        offset += rec_len + kWordSize;
        if (offset > node_size_) {
          // this record is the end one in a split-left node
          do_split = false;
          offset = CopyLowKeyFrom(rec, meta);
        }
      } else {
        // copy a record from the given node
        offset -= rec_len;
        memcpy(ShiftAddr(this, offset), rec->GetKeyAddr(meta), rec_len);
        meta_array_[record_count_++] = meta.UpdateForLeaf(offset);
      }
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
      size_t offset,
      bool &do_split)  //
      -> size_t
  {
    const auto key_len = meta.GetKeyLength();

    if (do_split) {
      // calculate the skipped page size
      offset += key_len;
      if (offset > node_size_) {
        // this record is the end one in a split-left node
        do_split = false;
        offset = CopyLowKeyFrom(node, meta);
      }
    } else {
      // copy a record from the given node
      offset -= key_len;
      memcpy(ShiftAddr(this, offset), node->GetKeyAddr(meta), key_len);
      meta_array_[record_count_++] = meta.UpdateForInternal(offset);
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
  template <class T>
  auto
  CopyPayloadFrom(  //
      const Node *node,
      const Metadata meta,
      size_t offset,
      const bool do_split)  //
      -> size_t
  {
    if (do_split) {
      offset += 2 * kWordSize;  // the length of metadata and a logical ID
    } else {
      // copy a record from the given node
      offset -= sizeof(T);
      memcpy(ShiftAddr(this, offset), node->GetPayloadAddr(meta), sizeof(T));
    }

    return offset;
  }

  auto
  CopyIndexEntryFrom(  //
      const void *delta_ptr,
      size_t offset,
      bool &do_split)  //
      -> size_t
  {
    const auto *delta = reinterpret_cast<const Node *>(delta_ptr);
    if (delta->delta_type_ == kInsert) {
      // copy a key to exchange a child page
      const auto meta = delta->low_meta_;
      offset = CopyKeyFrom(delta, meta, offset, do_split);

      // copy the next (split-right) child page
      offset = CopyPayloadFrom<LogicalID *>(delta, meta, offset, do_split);
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
  uint32_t node_size_{kPageSize};

  /// the pointer to the next node.
  uintptr_t next_{kNullPtr};

  /// metadata of a lowest key or a first record in a delta node
  Metadata low_meta_{kPageSize, 0, 0};

  /// metadata of a highest key or a second record in a delta node
  Metadata high_meta_{kPageSize, 0, 0};

  /// an actual data block (it starts with record metadata).
  Metadata meta_array_[0];
};

}  // namespace dbgroup::index::bw_tree::component::varlen

#endif  // BW_TREE_VARLEN_NODE_HPP