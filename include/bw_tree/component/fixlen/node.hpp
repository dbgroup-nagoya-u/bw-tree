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

#ifndef BW_TREE_COMPONENT_FIXLEN_NODE_HPP
#define BW_TREE_COMPONENT_FIXLEN_NODE_HPP

#include <optional>
#include <utility>
#include <vector>

#include "bw_tree/component/common.hpp"
#include "bw_tree/component/consolidate_info.hpp"
#include "bw_tree/component/logical_id.hpp"

namespace dbgroup::index::bw_tree::component::fixlen
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
   * Type aliases
   *##################################################################################*/

  using Record = const void *;

  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct an initial root node.
   *
   */
  constexpr Node()
      : is_leaf_{kLeaf}, delta_type_{kNotDelta}, has_low_key_{0}, has_high_key_{0}, do_split_{0}
  {
  }

  /**
   * @brief Construct a new root node.
   *
   * @param split_d a split-delta record.
   * @param left_lid a logical ID of a split-left child.
   */
  Node(  //
      const Node *split_d,
      const LogicalID *left_lid)
      : is_leaf_{kInternal},
        delta_type_{kNotDelta},
        has_low_key_{0},
        has_high_key_{0},
        do_split_{0},
        record_count_{2}
  {
    keys_[0] = split_d->low_key_;
    const auto offset = SetPayload(kPageSize, left_lid) - kWordSize;
    memcpy(ShiftAddr(this, offset), split_d->keys_, kWordSize);
  }

  /**
   * @brief Construct a consolidated base node object.
   *
   * Note that this construcor sets only header information.
   *
   * @param node_type a flag for indicating whether a leaf or internal node is constructed.
   * @param node_size the virtual size of this node.
   * @param do_split a flag for skipping left-split records in consolidation.
   */
  Node(  //
      const bool node_type,
      const size_t node_size,
      const bool do_split)
      : is_leaf_{static_cast<NodeType>(node_type)},
        delta_type_{kNotDelta},
        has_low_key_{0},
        has_high_key_{0},
        do_split_{static_cast<uint16_t>(do_split)},
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
    return is_leaf_ == kLeaf;
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
    if (!has_low_key_) return std::nullopt;
    return low_key_;
  }

  /**
   * @brief Copy and return a highest key for scanning.
   *
   * NOTE: this function does not check the existence of a highest key.
   *
   * @return the highest key in this node.
   */
  [[nodiscard]] auto
  GetHighKey() const  //
      -> Key
  {
    return high_key_;
  }

  /**
   * @param pos the position of a target record.
   * @return a key in a target record.
   */
  [[nodiscard]] auto
  GetKey(const size_t pos) const  //
      -> const Key &
  {
    return keys_[pos];
  }

  /**
   * @tparam T a class of a target payload.
   * @param pos the position of a target record.
   * @return a payload in a target record.
   */
  template <class T>
  [[nodiscard]] auto
  GetPayload(const size_t pos) const  //
      -> T
  {
    T payload{};
    memcpy(&payload, GetPayloadAddr<T>(pos), sizeof(T));
    return payload;
  }

  /**
   * @tparam T a class of a target payload.
   * @param pos the position of a target record.
   * @retval 1st: a key in a target record.
   * @retval 2nd: a payload in a target record.
   */
  template <class T>
  [[nodiscard]] auto
  GetRecord(const size_t pos) const  //
      -> std::pair<Key, T>
  {
    return {keys_[pos], GetPayload<T>(pos)};
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
    for (; cur->delta_type_ != kNotDelta; cur = cur->template GetNext<const Node *>()) {
      // go to the next delta record or base node
    }

    // get a leftmost node
    return cur->template GetPayload<LogicalID *>(0);
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
    if (is_leaf_ == kLeaf) {
      end_pos = record_count_ - 1;
    } else if (!has_high_key_ || Comp{}(key, high_key_)) {
      end_pos = record_count_ - 2;
    } else {
      return {kKeyExist, record_count_ - 1};
    }

    int64_t begin_pos = 0;
    while (begin_pos <= end_pos) {
      const size_t pos = (begin_pos + end_pos) >> 1UL;  // NOLINT
      const auto &index_key = keys_[pos];

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
      const auto &index_key = keys_[pos];

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

    return GetPayload<LogicalID *>(begin_pos);
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
   * Public utilities for consolidation
   *##################################################################################*/

  /**
   * @param rec a target delta record.
   * @param key a comparison key.
   * @retval true if delta record's key is less than a given one.
   * @retval false otherwise.
   */
  [[nodiscard]] static auto
  LT(  //
      const Record rec,
      const Key &key)  //
      -> bool
  {
    const auto *delta = reinterpret_cast<const Node *>(rec);
    return Comp{}(delta->low_key_, key);
  }

  /**
   * @param rec a target delta record.
   * @param key a comparison key.
   * @retval true if delta record's key is less than or equal to a given one.
   * @retval false otherwise.
   */
  [[nodiscard]] static auto
  LE(  //
      const Record rec,
      const Key &key)  //
      -> bool
  {
    const auto *delta = reinterpret_cast<const Node *>(rec);
    return !Comp{}(key, delta->low_key_);
  }

  /**
   * @brief Compute record counts to be consolidated.
   *
   * This function modifies a given consol_info's third variable (i.e., record count)
   * for the following consolidation procedure.
   *
   * @param consol_info the set of consolidated nodes.
   * @param is_leaf a flag for indicating a target node is leaf or internal ones.
   * @return the total number of records in a consolidated node.
   */
  [[nodiscard]] static auto
  PreConsolidate(  //
      std::vector<ConsolidateInfo> &consol_info,
      const bool is_leaf)  //
      -> size_t
  {
    const auto end_pos = consol_info.size() - 1;
    size_t total_rec_num = 0;

    for (size_t i = 0; i <= end_pos; ++i) {
      auto &[n_ptr, d_ptr, rec_num] = consol_info.at(i);
      const auto *node = reinterpret_cast<const Node *>(n_ptr);
      const auto *split_d = reinterpret_cast<const Node *>(d_ptr);

      // check the number of records to be consolidated
      if (split_d == nullptr) {
        rec_num = node->record_count_;
      } else {
        const auto [rc, pos] = node->SearchRecord(split_d->low_key_);
        rec_num = (!is_leaf || rc == kKeyExist) ? pos + 1 : pos;
      }

      total_rec_num += rec_num;
    }

    return total_rec_num;
  }

  /**
   * @brief Copy a lowest key for consolidation or set an initial used page size for
   * splitting.
   *
   * @param consol_info an original node that has a lowest key.
   * @return an initial offset.
   */
  auto
  CopyLowKeyOrSetInitialOffset(const ConsolidateInfo &consol_info)  //
      -> size_t
  {
    if (do_split_) return kHeaderLength;  // the initial skipped page size
    return CopyLowKeyFrom(reinterpret_cast<const Node *>(consol_info.node));
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
      [[maybe_unused]] const size_t offset)
  {
    // prepare a node that has the highest key, and copy the next logical ID
    if (consol_info.split_d == nullptr) {
      // an original or merged base node
      const auto *node = reinterpret_cast<const Node *>(consol_info.node);
      if (node->has_high_key_) {
        has_high_key_ = 1;
        high_key_ = node->high_key_;
      }
      next_ = node->next_;
    } else {
      // a split-delta record
      const auto *split_d = reinterpret_cast<const Node *>(consol_info.split_d);
      if (split_d->has_low_key_) {
        has_high_key_ = 1;
        high_key_ = split_d->low_key_;
      }
      memcpy(&next_, split_d->keys_, kWordSize);
    }
  }

  /**
   * @brief Copy a record from a base node in the leaf level.
   *
   * @param node an original base node.
   * @param offset an offset to the bottom of free space.
   * @param pos the position of a target record.
   * @return an offset to the copied record.
   */
  template <class T>
  auto
  CopyRecordFrom(  //
      const Node *node,
      const size_t pos,
      size_t offset)  //
      -> size_t
  {
    if (!do_split_) {
      // copy a record from the given node
      keys_[record_count_++] = node->keys_[pos];
      offset -= sizeof(T);
      memcpy(ShiftAddr(this, offset), node->template GetPayloadAddr<T>(pos), sizeof(T));
    } else {
      if (offset < node_size_) {
        // calculate the skipped page size
        offset += sizeof(Key) + sizeof(T);
      } else {
        // this record is the end one in a split-left node
        do_split_ = false;
        offset = CopyLowKeyFrom(node, pos);
      }
    }

    return offset;
  }

  /**
   * @brief Copy a record from a delta record in the leaf level.
   *
   * @param rec_ptr an original delta record.
   * @param offset an offset to the bottom of free space.
   * @return an offset to the copied record.
   */
  template <class T>
  auto
  CopyRecordFrom(  //
      const Record rec_ptr,
      size_t offset)  //
      -> size_t
  {
    const auto *rec = reinterpret_cast<const Node *>(rec_ptr);
    if (rec->delta_type_ != kDelete) {  // the target record is insert/modify delta
      if (!do_split_) {
        // copy a record from the given node
        keys_[record_count_++] = rec->low_key_;
        offset -= sizeof(T);
        memcpy(ShiftAddr(this, offset), rec->keys_, sizeof(T));
      } else {
        if (offset < node_size_) {
          // calculate the skipped page size
          offset += sizeof(Key) + sizeof(T);
        } else {
          // this record is the end one in a split-left node
          do_split_ = false;
          offset = CopyLowKeyFrom(rec);
        }
      }
    }

    return offset;
  }

  /**
   * @brief Copy a key from a base node or a delta record.
   *
   * @param node an original node that has a target record.
   * @param offset an offset to the bottom of free space.
   * @param pos the position of a target record.
   * @return an offset to the copied key.
   */
  constexpr auto
  CopyKeyFrom(  //
      const Node *node,
      size_t offset,
      const int64_t pos = kCopyLowKey)  //
      -> size_t
  {
    if (!do_split_) {
      // copy a record from the given node
      keys_[record_count_++] = (pos < 0) ? node->low_key_ : node->keys_[pos];
    } else {
      if (offset < node_size_) {
        // calculate the skipped page size
        offset += sizeof(Key);
      } else {
        // this record is the end one in a split-left node
        do_split_ = false;
        offset = CopyLowKeyFrom(node, pos);
      }
    }

    return offset;
  }

  /**
   * @brief Copy a payload from a base node or a delta record.
   *
   * @param node an original node that has a target record.
   * @param offset an offset to the bottom of free space.
   * @param pos the position of a target record.
   * @return an offset to the copied payload.
   */
  template <class T>
  auto
  CopyPayloadFrom(  //
      const Node *node,
      size_t offset,
      const int64_t pos = kCopyLowKey)  //
      -> size_t
  {
    if (!do_split_) {
      // copy the next (split-right) child page
      offset -= kWordSize;
      const auto *addr = (pos < 0) ? node->keys_ : node->template GetPayloadAddr<LogicalID *>(pos);
      memcpy(ShiftAddr(this, offset), addr, kWordSize);
    } else {
      // calculate the skipped page size
      offset += kWordSize;
    }

    return offset;
  }

  /**
   * @brief Copy an index-entry from a delta record.
   *
   * @param rec_ptr an original delta record.
   * @param offset an offset to the bottom of free space.
   * @return an offset to the copied payload.
   */
  auto
  CopyIndexEntryFrom(  //
      const Record rec_ptr,
      size_t offset)  //
      -> size_t
  {
    const auto *delta = reinterpret_cast<const Node *>(rec_ptr);
    if (delta->delta_type_ == kInsert) {
      // copy a key to exchange a child page
      offset = CopyKeyFrom(delta, offset);

      // copy the next (split-right) child page
      offset = CopyPayloadFrom<LogicalID *>(delta, offset);
    }

    return offset;
  }

  /**
   * @brief Add the record count in this internal node.
   *
   * @param offset an offset to the bottom of free space.
   */
  void
  SetLastRecordForInternal([[maybe_unused]] const size_t offset)
  {
    ++record_count_;
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
    if (!has_high_key_) return true;  // the rightmost node
    if (!end_key) return false;       // perform full scan
    return !Comp{}(high_key_, end_key->first);
  }

  /**
   * @param pos the position of a target record.
   * @return an address of a target payload.
   */
  template <class T>
  [[nodiscard]] constexpr auto
  GetPayloadAddr(const size_t pos) const  //
      -> void *
  {
    return ShiftAddr(this, node_size_ - sizeof(T) * (pos + 1));
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
   * @param pos the position of a target record.
   * @return an offset to the set key.
   */
  auto
  CopyLowKeyFrom(  //
      const Node *node,
      const int64_t pos = kCopyLowKey)  //
      -> size_t
  {
    if (pos >= 0) {
      has_low_key_ = 1;
      low_key_ = node->keys_[pos];
    } else if (node->has_low_key_) {
      has_low_key_ = 1;
      low_key_ = node->low_key_;
    }

    return node_size_;
  }

  /*####################################################################################
   * Internal variables
   *##################################################################################*/

  /// a flag for indicating whether this node is a leaf or internal node.
  uint16_t is_leaf_ : 1;

  /// a flag for indicating the types of delta records.
  uint16_t delta_type_ : 3;

  /// a flag for indicating whether this delta record has a lowest-key.
  uint16_t has_low_key_ : 1;

  /// a flag for indicating whether this delta record has a highest-key.
  uint16_t has_high_key_ : 1;

  /// a flag for performing a split operation in consolidation.
  uint16_t do_split_ : 1;

  /// a blank block for alignment.
  uint16_t : 0;

  /// the number of records in this node.
  uint16_t record_count_{0};

  /// an offset to the bottom of this node.
  uint32_t node_size_{kPageSize};

  /// the pointer to a sibling node.
  uintptr_t next_{kNullPtr};

  /// the lowest key of this node.
  Key low_key_{};

  /// the highest key of this node.
  Key high_key_{};

  /// an actual data block for records.
  Key keys_[0]{};
};

}  // namespace dbgroup::index::bw_tree::component::fixlen

#endif  // BW_TREE_COMPONENT_FIXLEN_NODE_HPP
