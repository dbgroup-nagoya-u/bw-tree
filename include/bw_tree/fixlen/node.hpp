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

#ifndef BW_TREE_FIXLEN_NODE_HPP
#define BW_TREE_FIXLEN_NODE_HPP

#include <optional>
#include <utility>
#include <vector>

#include "bw_tree/common/common.hpp"
#include "bw_tree/common/consolidate_info.hpp"
#include "bw_tree/common/logical_id.hpp"

namespace dbgroup::index::bw_tree::component::fixlen
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
  constexpr Node() : node_type_{kLeaf}, delta_type_{kNotDelta}, has_low_key_{0}, has_high_key_{0} {}

  /**
   * @brief Construct a new root node.
   *
   * @param split_ptr
   * @param left_lid
   */
  Node(  //
      const Node *split_d,
      const LogicalID *left_lid)
      : node_type_{kInternal},
        delta_type_{kNotDelta},
        has_low_key_{0},
        has_high_key_{0},
        record_count_{2}
  {
    keys_[0] = split_d->low_key_;
    const auto offset = SetPayload(kPageSize, left_lid) - kWordSize;
    memcpy(ShiftAddr(this, offset), split_d->keys_, kWordSize);
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
        has_low_key_{0},
        has_high_key_{0},
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
   * @param meta metadata of a corresponding record.
   * @return a target key.
   */
  [[nodiscard]] auto
  GetKey(const size_t pos) const  //
      -> const Key &
  {
    return keys_[pos];
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
    if (!has_low_key_) return std::nullopt;
    return low_key_;
  }

  [[nodiscard]] auto
  GetHighKey() const  //
      -> std::optional<Key>
  {
    if (!has_high_key_) return std::nullopt;
    return high_key_;
  }

  /**
   * @tparam T a class of a target payload.
   * @param meta metadata of a corresponding record.
   * @return a target payload.
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

  [[nodiscard]] auto
  GetLeftmostChild() const  //
      -> LogicalID *
  {
    const auto *cur = this;
    for (; !cur->IsBaseNode(); cur = cur->template GetNext<const Node *>()) {
      // go to the next delta record or base node
    }

    // get a leftmost node
    return cur->template GetPayload<LogicalID *>(0);
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
        rec_num = (!kIsLeaf || rc == kKeyExist) ? pos + 1 : pos;
      }

      total_rec_num += rec_num;
    }

    return total_rec_num;
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
    size_t offset{};
    if (do_split) {
      offset = kHeaderLength / 2;
    } else {
      const auto *low_node = reinterpret_cast<const Node *>(consol_info.back().node);
      has_low_key_ = low_node->has_low_key_;
      low_key_ = low_node->low_key_;
      offset = node_size_;
    }

    // perform merge-sort to consolidate a node
    size_t j = 0;
    for (int64_t k = consol_info.size() - 1; k >= 0; --k) {
      const auto [n_ptr, dummy, base_rec_num] = consol_info.at(k);
      const auto *node = reinterpret_cast<const Node *>(n_ptr);
      for (size_t i = 0; i < base_rec_num; ++i) {
        // copy new records
        const auto &node_key = node->keys_[i];
        for (; j < new_rec_num && Comp{}(records[j].first, node_key); ++j) {
          offset = CopyRecordFrom<T>(records[j].second, offset, do_split);
        }

        // check a new record is updated one
        if (j < new_rec_num && !Comp{}(node_key, records[j].first)) {
          offset = CopyRecordFrom<T>(records[j].second, offset, do_split);
          ++j;
        } else {
          offset = CopyRecordFrom<T>(node, i, offset, do_split);
        }
      }
    }

    // copy remaining new records
    for (; j < new_rec_num; ++j) {
      offset = CopyRecordFrom<T>(records[j].second, offset, do_split);
    }

    // copy a highest key
    CopyHighKeyFrom(consol_info.front());
  }

  void
  InternalConsolidate(  //
      const std::vector<ConsolidateInfo> &consol_info,
      const std::vector<std::pair<Key, const void *>> &records,
      bool do_split)
  {
    const auto new_rec_num = records.size();

    // copy a lowest key when consolidation
    size_t offset{};
    if (do_split) {
      offset = kHeaderLength / 2;
    } else {
      const auto *low_node = reinterpret_cast<const Node *>(consol_info.back().node);
      has_low_key_ = low_node->has_low_key_;
      low_key_ = low_node->low_key_;
      offset = node_size_;
    }

    // perform merge-sort to consolidate a node
    bool payload_is_embedded = false;
    size_t j = 0;
    for (int64_t k = consol_info.size() - 1; k >= 0; --k) {
      const auto *node = reinterpret_cast<const Node *>(consol_info.at(k).node);
      const auto end_pos = consol_info.at(k).rec_num - 1;
      for (size_t i = 0; i <= end_pos; ++i) {
        // copy a payload of a base node in advance to swap that of a new index entry
        if (!payload_is_embedded) {  // skip a deleted page
          if (do_split) {
            offset += kWordSize;
          } else {
            // copy a record from the given node
            offset -= kWordSize;
            const auto *pay_addr = node->template GetPayloadAddr<LogicalID *>(i);
            memcpy(ShiftAddr(this, offset), pay_addr, kWordSize);
          }
        }

        // get a current key in the base node
        if (i == end_pos) {
          if (k == 0) break;  // the last record does not need a key
          // if nodes are merged, a current key is equivalent with the lowest one in the next node
          node = reinterpret_cast<const Node *>(consol_info.at(k - 1).node);
        }

        // insert new index entries
        const auto &node_key = (i == end_pos) ? node->low_key_ : node->keys_[i];
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
          if (do_split) {
            // calculate the skipped page size
            offset += sizeof(Key);
            if (offset > node_size_) {
              // this record is the end one in a split-left node
              do_split = false;
              has_low_key_ = 1;
              low_key_ = node_key;
              offset = node_size_;
            }
          } else {
            // copy a record from the given node
            keys_[record_count_++] = node_key;
          }
          payload_is_embedded = false;
        }
      }
    }

    // copy remaining new records
    for (; j < new_rec_num; ++j) {
      offset = CopyIndexEntryFrom(records[j].second, offset, do_split);
    }
    ++record_count_;

    // copy a highest key
    CopyHighKeyFrom(consol_info.front());
  }

 private:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  static constexpr size_t kHeaderLength = sizeof(Node);

  /*####################################################################################
   * Internal getters setters
   *##################################################################################*/

  [[nodiscard]] auto
  IsRightmostOf(const std::optional<std::pair<const Key &, bool>> &end_key) const  //
      -> bool
  {
    if (!has_high_key_) return true;  // the rightmost node
    if (!end_key) return false;       // perform full scan
    return !Comp{}(high_key_, end_key->first);
  }

  /**
   * @param meta metadata of a corresponding record.
   * @return an address of a target payload.
   */
  template <class T>
  [[nodiscard]] constexpr auto
  GetPayloadAddr(const size_t pos) const  //
      -> void *
  {
    return ShiftAddr(this, node_size_ - sizeof(T) * (pos + 1));
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

  void
  CopyHighKeyFrom(const ConsolidateInfo &consol_info)
  {
    // prepare a node that has the highest key, and copy the next logical ID
    if (consol_info.split_d == nullptr) {
      // an original or merged base node
      const auto *node = reinterpret_cast<const Node *>(consol_info.node);
      has_high_key_ = node->has_high_key_;
      if (has_high_key_) {
        high_key_ = node->high_key_;
      }
      next_ = node->next_;
    } else {
      // a split-delta record
      const auto *split_d = reinterpret_cast<const Node *>(consol_info.split_d);
      has_high_key_ = split_d->has_low_key_;
      if (has_high_key_) {
        high_key_ = split_d->low_key_;
      }
      memcpy(&next_, split_d->keys_, kWordSize);
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
      const size_t pos,
      size_t offset,
      bool &do_split)  //
      -> size_t
  {
    if (do_split) {
      // calculate the skipped page size
      offset += sizeof(Key) + sizeof(T);
      if (offset > node_size_) {
        // this record is the end one in a split-left node
        do_split = false;
        has_low_key_ = 1;
        low_key_ = node->keys_[pos];
        offset = node_size_;
      }
    } else {
      // copy a record from the given node
      keys_[record_count_++] = node->keys_[pos];
      offset -= sizeof(T);
      memcpy(ShiftAddr(this, offset), node->template GetPayloadAddr<T>(pos), sizeof(T));
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
      if (do_split) {
        // calculate the skipped page size
        offset += sizeof(Key) + sizeof(T);
        if (offset > node_size_) {
          // this record is the end one in a split-left node
          do_split = false;
          has_low_key_ = 1;
          low_key_ = rec->low_key_;
          offset = node_size_;
        }
      } else {
        // copy a record from the given node
        keys_[record_count_++] = rec->low_key_;
        offset -= sizeof(T);
        memcpy(ShiftAddr(this, offset), rec->keys_, sizeof(T));
      }
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
      if (do_split) {
        // calculate the skipped page size
        offset += sizeof(Key);
        if (offset > node_size_) {
          // this record is the end one in a split-left node
          do_split = false;
          has_low_key_ = 1;
          low_key_ = delta->low_key_;
          offset = node_size_;
        }
      } else {
        // copy a record from the given node
        keys_[record_count_++] = delta->low_key_;
      }

      // copy the next (split-right) child page
      if (do_split) {
        offset += kWordSize;
      } else {
        // copy a record from the given node
        offset -= kWordSize;
        memcpy(ShiftAddr(this, offset), delta->keys_, kWordSize);
      }
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

  /// a flag for indicating whether this delta record has a lowest-key.
  uint16_t has_low_key_ : 1;

  /// a flag for indicating whether this delta record has a highest-key.
  uint16_t has_high_key_ : 1;

  /// a blank block for alignment.
  uint16_t : 0;

  /// the number of records in this node.
  uint16_t record_count_{0};

  /// a blank block for alignment.
  uint32_t node_size_{kPageSize};

  /// the pointer to the next node.
  uintptr_t next_{kNullPtr};

  /// metadata of an embedded record
  Key low_key_{};

  /// metadata of a highest key
  Key high_key_{};

  /// an actual data block for records
  Key keys_[0]{};
};

}  // namespace dbgroup::index::bw_tree::component::fixlen

#endif  // BW_TREE_FIXLEN_NODE_HPP
