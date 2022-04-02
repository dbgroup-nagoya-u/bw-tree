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

#ifndef BW_TREE_COMPONENT_VARLEN_NODE_HPP
#define BW_TREE_COMPONENT_VARLEN_NODE_HPP

#include <optional>
#include <utility>
#include <vector>

#include "bw_tree/component/consolidate_info.hpp"
#include "bw_tree/component/logical_id.hpp"
#include "metadata.hpp"

namespace dbgroup::index::bw_tree::component::varlen
{
/**
 * @brief A class for represent leaf/internal nodes in Bw-tree.
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
  constexpr Node() : is_leaf_{kLeaf}, delta_type_{kNotDelta} {}

  /**
   * @brief Construct a new root node.
   *
   * @param split_d a split-delta record.
   * @param left_lid a logical ID of a split-left child.
   */
  Node(  //
      const Node *split_d,
      const LogicalID *left_lid)
      : is_leaf_{kInternal}, delta_type_{kNotDelta}, record_count_{2}
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
   * Note that this construcor sets only header information.
   *
   * @param node_type a flag for indicating whether a leaf or internal node is constructed.
   * @param node_size the virtual size of this node.
   */
  Node(  //
      const bool node_type,
      const size_t node_size)
      : is_leaf_{static_cast<NodeType>(node_type)},
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
   * @retval false otherwise.
   */
  [[nodiscard]] constexpr auto
  IsLeaf() const  //
      -> bool
  {
    return is_leaf_;
  }

  /**
   * @retval true if this is a base node.
   * @retval false otherwise.
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

  /**
   * @brief Get the next pointer of a delta record, a base node, or a logical ID.
   *
   * Note that this funcion returns a logical ID if this is a base node.
   *
   * @tparam T a expected class to be loaded.
   * @return a pointer to the next object.
   */
  template <class T = const Node *>
  [[nodiscard]] constexpr auto
  GetNext() const  //
      -> T
  {
    return reinterpret_cast<T>(next_);
  }

  /**
   * @param position the position of record metadata to be get.
   * @return record metadata.
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
   * If this node is the leftmost node in its level, this returns std::nullopt.
   *
   * @return the lowest key if exist.
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

  /**
   * @brief Get the leftmost child node.
   *
   * If this object is actually a delta record, this function traverses a delta-chain
   * and returns the left most child from a base node.
   *
   * @return the logical ID of the leftmost child node.
   */
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

  /**
   * @brief Copy and return a highest key for scanning.
   *
   * This function allocates memory dynamically for variable-length keys, so it must be
   * released by the caller.
   *
   * @return the highest key in this node.
   */
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
   * @brief Get the position of a specified key by using binary search.
   *
   * If there is no specified key in this node, this returns the minimum position that
   * is greater than the specified key.
   *
   * NOTE: This function assumes that the given key must be in the range of this node.
   * If the given key is greater than the highest key of this node, this function will
   * returns incorrect results.
   *
   * @param key a target key.
   * @return the pair of record's existence and the searched position.
   */
  [[nodiscard]] auto
  SearchRecord(const Key &key) const  //
      -> std::pair<ReturnCode, size_t>
  {
    int64_t end_pos{};
    if (is_leaf_) {
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
   * @brief Get the corresponding child node with a specified key.
   *
   * If there is no specified key in this node, this returns the child in the minimum
   * position that is greater than the specified key.
   *
   * @param key a target key.
   * @param range_is_closed a flag for indicating a target range includes the key.
   * @return the logical ID of searched child node.
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

  /**
   * @brief Get the end position of records for scanning and check it has been finished.
   *
   * @param end_key a pair of a target key and its closed/open-interval flag.
   * @retval 1st: true if this node is end of scanning.
   * @retval 2nd: the end position for scanning.
   */
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

  /*####################################################################################
   * Public SMO functions
   *##################################################################################*/

  /**
   * @brief Compute record counts to be consolidated and the total block size.
   *
   * This function modifies a given consol_info's third variable (i.e., record count)
   * for the following consolidation procedure.
   *
   * @param consol_info the set of consolidated nodes.
   * @param is_leaf a flag for indicating a target node is leaf or internal ones.
   * @return the total block size of a consolidated node.
   */
  [[nodiscard]] static auto
  PreConsolidate(  //
      std::vector<ConsolidateInfo> &consol_info,
      const bool is_leaf)  //
      -> size_t
  {
    const auto end_pos = consol_info.size() - 1;
    size_t size = 0;

    for (size_t i = 0; i <= end_pos; ++i) {
      auto &[n_ptr, d_ptr, rec_num] = consol_info.at(i);
      const auto *node = reinterpret_cast<const Node *>(n_ptr);
      const auto *split_d = reinterpret_cast<const Node *>(d_ptr);

      // add the length of the lowest key
      if (!is_leaf || i == end_pos) {
        size += node->low_meta_.GetKeyLength();
      }

      // check the number of records to be consolidated
      if (split_d == nullptr) {
        rec_num = node->record_count_;

        if (i == 0) {
          // add the length of the highest key
          size += node->high_meta_.GetKeyLength();
        }
      } else {
        const auto sep_meta = split_d->low_meta_;
        const auto &sep_key = split_d->GetKey(sep_meta);
        auto [rc, pos] = node->SearchRecord(sep_key);
        rec_num = (!is_leaf || rc == kKeyExist) ? pos + 1 : pos;

        if (i == 0) {
          // add the length of the highest key
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

  /**
   * @brief Consolidate given leaf nodes into this.
   *
   * @tparam T a class of payloads.
   * @param consol_info the set of consolidated nodes.
   * @param records insert/modify/delete-delta records.
   * @param do_split a flag for indicating this node is split.
   */
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

  /**
   * @brief Consolidate given internal nodes into this.
   *
   * @param consol_info the set of consolidated nodes.
   * @param records insert/modify/delete-delta records.
   * @param do_split a flag for indicating this node is split.
   */
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
   * Internal constants
   *##################################################################################*/

  /// Header length in bytes.
  static constexpr size_t kHeaderLength = sizeof(Node);

  /*####################################################################################
   * Internal getters setters
   *##################################################################################*/

  /**
   * @param end_key a pair of a target key and its closed/open-interval flag.
   * @retval true if this node is a rightmost node for the given key.
   * @retval false otherwise.
   */
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
   * @brief Set a target key directly.
   *
   * @tparam T a class of keys.
   * @param offset an offset to the bottom of free space.
   * @param key a target key to be set.
   * @param key_len the length of a target key.
   * @return an offset to the set key.
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

  /**
   * @brief Set a target payload directly.
   *
   * @tparam T a class of payloads.
   * @param offset an offset to the bottom of free space.
   * @param payload a target payload to be set.
   * @return an offset to the set payload.
   */
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
   * @brief Copy a lowest key from a given base node.
   *
   * @param node a base node that includes a lowest key.
   * @param meta corresponding metadata of lowest key.
   * @return an offset to the set key.
   */
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

  /**
   * @brief Copy a highest key from a given consolidated node.
   *
   * @param consol_info a consolidated node and a corresponding split-delta record.
   * @param offset an offset to the bottom of free space.
   */
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
   * @param node an original base node.
   * @param meta the corresponding metadata of a target record.
   * @param offset an offset to the bottom of free space.
   * @param do_split a flag for ignoring split-left records if needed.
   * @return an offset to the copied record.
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
   * @param offset an offset to the bottom of free space.
   * @param do_split a flag for ignoring split-left records if needed.
   * @return an offset to the copied record.
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
   * @brief Copy a key from a base node.
   *
   * @param node an original node that has a target record.
   * @param meta the corresponding metadata of a target record.
   * @param offset an offset to the bottom of free space.
   * @param do_split a flag for ignoring split-left records if needed.
   * @return an offset to the copied key.
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
   * @brief Copy a payload from a base node.
   *
   * @param node an original node that has a target record.
   * @param meta the corresponding metadata of a target record.
   * @param offset an offset to the bottom of free space.
   * @param do_split a flag for ignoring split-left records if needed.
   * @return an offset to the copied payload.
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

  /**
   * @brief Copy an index-entry from a delta record.
   *
   * @param delta_ptr an address of a target delta record.
   * @param offset an offset to the bottom of free space.
   * @param do_split a flag for ignoring split-left records if needed.
   * @return an offset to the copied payload.
   */
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

  /// a flag for indicating whether this node is a leaf or internal node.
  uint16_t is_leaf_ : 1;

  /// a flag for indicating the types of delta records.
  uint16_t delta_type_ : 3;

  /// a blank block for alignment.
  uint16_t : 0;

  /// the number of records in this node.
  uint16_t record_count_{0};

  /// an offset to the bottom of this node.
  uint32_t node_size_{kPageSize};

  /// the pointer to a sibling node.
  uintptr_t next_{kNullPtr};

  /// metadata of a lowest key or a first record in a delta record.
  Metadata low_meta_{kPageSize, 0, 0};

  /// metadata of a highest key.
  Metadata high_meta_{kPageSize, 0, 0};

  /// an actual data block (it starts with record metadata).
  Metadata meta_array_[0];
};

}  // namespace dbgroup::index::bw_tree::component::varlen

#endif  // BW_TREE_COMPONENT_VARLEN_NODE_HPP
