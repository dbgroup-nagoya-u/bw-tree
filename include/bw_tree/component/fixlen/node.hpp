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

// C++ standard libraries
#include <optional>
#include <utility>
#include <vector>

// local sources
#include "bw_tree/component/common.hpp"
#include "bw_tree/component/logical_ptr.hpp"

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

  using ScanKey = std::optional<std::tuple<const Key &, size_t, bool>>;
  using ConsolidateInfo = std::pair<const void *, const void *>;
  template <class Entry>
  using BulkIter = typename std::vector<Entry>::const_iterator;
  using NodeEntry = std::tuple<Key, PageID, size_t>;

  /*####################################################################################
   * Public classes
   *##################################################################################*/

  /**
   * @brief A class to sort delta records.
   *
   */
  struct Record {
    Key key;
    const void *ptr;
  };

  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct an initial root node.
   *
   */
  constexpr explicit Node(const bool is_inner = false)
      : is_inner_{static_cast<NodeType>(is_inner)},
        delta_type_{kNotDelta},
        has_low_key_{0},
        has_high_key_{0}
  {
  }

  /**
   * @brief Construct a new root node.
   *
   * @param split_d a split-delta record.
   * @param left_pid the page ID of a split-left child.
   */
  Node(  //
      const Node *split_d,
      const PageID left_pid)
      : is_inner_{kInner},
        delta_type_{kNotDelta},
        has_low_key_{0},
        has_high_key_{0},
        rec_count_{2},
        node_size_{kHeaderLen + 2 * (kKeyLen + kPtrLen)}
  {
    keys_[1] = split_d->low_key_;
    const auto offset = SetPayload(kPageSize, left_pid) - kPtrLen;
    memcpy(ShiftAddr(this, offset), split_d->keys_, kPtrLen);
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
    return is_inner_ == kLeaf;
  }

  /**
   * @retval true if this node is leftmost in its tree level.
   * @retval false otherwise.
   */
  [[nodiscard]] constexpr auto
  IsLeftmost() const  //
      -> bool
  {
    return has_low_key_ == 0;
  }

  /**
   * @return the byte length of this node.
   */
  [[nodiscard]] constexpr auto
  GetNodeSize() const  //
      -> size_t
  {
    return node_size_;
  }

  /**
   * @return the byte length to be modified by SMOs.
   */
  [[nodiscard]] constexpr auto
  GetNodeDiff() const  //
      -> size_t
  {
    return node_size_ - kHeaderLen;
  }

  /**
   * @return the number of records in this node.
   */
  [[nodiscard]] constexpr auto
  GetRecordCount() const  //
      -> size_t
  {
    return rec_count_;
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
   * @return The length of a lowest key.
   */
  [[nodiscard]] constexpr auto
  GetLowKeyLen() const  //
      -> size_t
  {
    return sizeof(Key) * has_low_key_;
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
      -> Key
  {
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
   * @brief Set the size of this node for scanning.
   *
   */
  void
  SetNodeSizeForScan()
  {
    node_size_ = 2 * kPageSize;
  }

  /**
   * @brief Set a sibling node.
   *
   * @param pid the page ID of the sibling node.
   */
  void
  SetNext(const PageID pid)
  {
    next_ = pid;
  }

  /*####################################################################################
   * Public getters/setters for records
   *##################################################################################*/

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
   * @return the page ID of the leftmost child node.
   */
  [[nodiscard]] auto
  GetLeftmostChild() const  //
      -> PageID
  {
    const auto *cur = this;
    for (; cur->delta_type_ != kNotDelta; cur = cur->template GetNext<const Node *>()) {
      // go to the next delta record or base node
    }

    // get a leftmost node
    return cur->template GetPayload<PageID>(0);
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
      -> std::pair<DeltaRC, size_t>
  {
    int64_t begin_pos = is_inner_ & ~static_cast<size_t>(has_low_key_);
    int64_t end_pos = rec_count_ - 1;
    while (begin_pos <= end_pos) {
      const size_t pos = (begin_pos + end_pos) >> 1UL;  // NOLINT
      const auto &index_key = keys_[pos];

      if (Comp{}(key, index_key)) {  // a target key is in a left side
        end_pos = pos - 1;
      } else if (Comp{}(index_key, key)) {  // a target key is in a right side
        begin_pos = pos + 1;
      } else {  // find an equivalent key
        return {kRecordFound, pos};
      }
    }

    return {kRecordNotFound, begin_pos};
  }

  /**
   * @brief Get the corresponding child node with a specified key.
   *
   * If there is no specified key in this node, this returns the child in the minimum
   * position that is greater than the specified key.
   *
   * @param key a target key.
   * @param closed a flag for including the same key.
   * @return the page ID of searched child node.
   */
  [[nodiscard]] auto
  SearchChild(  //
      const Key &key,
      const bool closed) const  //
      -> PageID
  {
    int64_t begin_pos = 1;
    int64_t end_pos = rec_count_ - 1;
    while (begin_pos <= end_pos) {
      const size_t pos = (begin_pos + end_pos) >> 1UL;  // NOLINT
      const auto &index_key = keys_[pos];

      if (Comp{}(key, index_key)) {  // a target key is in a left side
        end_pos = pos - 1;
      } else if (Comp{}(index_key, key)) {  // a target key is in a right side
        begin_pos = pos + 1;
      } else {  // find an equivalent key
        begin_pos = pos + static_cast<size_t>(closed);
        break;
      }
    }

    return GetPayload<PageID>(begin_pos - 1);
  }

  /**
   * @brief Get the end position of records for scanning and check it has been finished.
   *
   * @param end_key a pair of a target key and its closed/open-interval flag.
   * @retval 1st: true if this node is end of scanning.
   * @retval 2nd: the end position for scanning.
   */
  [[nodiscard]] auto
  SearchEndPositionFor(const ScanKey &end_key) const  //
      -> std::pair<bool, size_t>
  {
    const auto is_end = IsRightmostOf(end_key);
    size_t end_pos{};
    if (is_end && end_key) {
      const auto &[e_key, e_key_len, e_closed] = *end_key;
      const auto [rc, pos] = SearchRecord(e_key);
      end_pos = (rc == kRecordFound && e_closed) ? pos + 1 : pos;
    } else {
      end_pos = rec_count_;
    }

    return {is_end, end_pos};
  }

  /*####################################################################################
   * Public utilities for consolidation
   *##################################################################################*/

  /**
   * @brief Copy a lowest key for consolidation or set an initial used page size for
   * splitting.
   *
   * @param node_addr an original node that has a lowest key.
   * @return an initial offset.
   */
  auto
  CopyLowKeyFrom(const void *node_addr)  //
      -> size_t
  {
    // prepare a node that has the lowest key
    if (node_addr == this) {
      // this node is a split-right node, and so the leftmost record has the lowest key
      has_low_key_ = 1;
      low_key_ = keys_[0];
    } else {
      // this node is a consolidated node, and so the given node has the lowest key
      const auto *node = reinterpret_cast<const Node *>(node_addr);
      has_low_key_ = node->has_low_key_;
      low_key_ = node->low_key_;
    }

    return kPageSize;  // dummy value
  }

  /**
   * @brief Copy a highest key from a given consolidated node.
   *
   * @param node_addr an original node that has a highest key.
   * @param offset an offset to the bottom of free space.
   * @param is_split_left a flag for indicating this node is a split-left one.
   */
  void
  CopyHighKeyFrom(  //
      const void *node_addr,
      [[maybe_unused]] size_t offset,
      const bool is_split_left = false)
  {
    // prepare a node that has the highest key, and copy the next logical ID
    const auto *node = reinterpret_cast<const Node *>(node_addr);
    if (is_split_left) {
      has_high_key_ = 1;
      high_key_ = node->keys_[0];
    } else {
      has_high_key_ = node->has_high_key_;
      high_key_ = node->high_key_;
      next_ = node->next_;
    }
  }

  /**
   * @brief Copy a record from a base node in the leaf level.
   *
   * @param node a target base node.
   * @param orig_node an original base node.
   * @param pos the position of a target record.
   * @param offset an offset to the bottom of free space.
   * @param r_node a split-right node for switching.
   * @return an offset to the copied record.
   */
  template <class T>
  static auto
  CopyRecordFrom(  //
      Node *&node,
      const Node *orig_node,
      const size_t pos,
      size_t offset,
      Node *&r_node)  //
      -> size_t
  {
    constexpr auto kRecLen = sizeof(Key) + sizeof(T);

    // copy a record from the given node
    node->keys_[node->rec_count_++] = orig_node->keys_[pos];
    offset -= sizeof(T);
    memcpy(ShiftAddr(node, offset), orig_node->template GetPayloadAddr<T>(pos), sizeof(T));
    node->node_size_ += kRecLen;

    if (r_node != nullptr && node->node_size_ > (kPageSize - kHeaderLen) / 2) {
      // switch to the split-right node
      node = r_node;
      r_node = nullptr;
      offset = kPageSize;
    }

    return offset;
  }

  /**
   * @brief Copy a record from a delta record in the leaf level.
   *
   * @param node a target base node.
   * @param rec_ptr an original delta record.
   * @param offset an offset to the bottom of free space.
   * @param r_node a split-right node for switching.
   * @return an offset to the copied record.
   */
  template <class T>
  static auto
  CopyRecordFrom(  //
      Node *&node,
      const void *rec_ptr,
      size_t offset,
      Node *&r_node)  //
      -> size_t
  {
    constexpr auto kRecLen = sizeof(Key) + sizeof(T);

    const auto *rec = reinterpret_cast<const Node *>(rec_ptr);
    if (rec->delta_type_ != kDelete) {
      // copy a record from the given node
      node->keys_[node->rec_count_++] = rec->low_key_;
      offset -= sizeof(T);
      memcpy(ShiftAddr(node, offset), rec->keys_, sizeof(T));
      node->node_size_ += kRecLen;

      if (r_node != nullptr && node->node_size_ > (kPageSize - kHeaderLen) / 2) {
        // switch to the split-right node
        node = r_node;
        r_node = nullptr;
        offset = kPageSize;
      }
    }

    return offset;
  }

  /*####################################################################################
   * Public bulkload API
   *##################################################################################*/

  /**
   * @brief Create a node with the maximum number of records for bulkloading.
   *
   * @tparam Entry a container of a key/payload pair.
   * @param iter the begin position of target records.
   * @param iter_end the end position of target records.
   * @param prev_node a left sibling node.
   * @param this_pid the logical ID of a this node.
   * @param nodes the container of construcred nodes.
   * @param is_inner a flag for indicating inner nodes.
   */
  template <class Entry>
  void
  Bulkload(  //
      BulkIter<Entry> &iter,
      const BulkIter<Entry> &iter_end,
      Node *prev_node,
      PageID this_pid,
      std::vector<NodeEntry> &nodes,
      [[maybe_unused]] const bool is_inner)
  {
    using Payload = std::tuple_element_t<1, Entry>;

    constexpr auto kRecLen = kKeyLen + sizeof(Payload);

    // extract and insert entries into this node
    auto offset = kPageSize;
    for (; iter < iter_end; ++iter) {
      // check whether the node has sufficient space
      if (node_size_ + kRecLen > kNodeCapacityForBulkLoading) break;
      node_size_ += kRecLen;

      // insert an entry into this node
      const auto &[key, payload, key_len, pay_len] = ParseEntry(*iter);
      offset = SetPayload(offset, payload);
      keys_[rec_count_++] = key;
    }

    // set a lowest key
    has_low_key_ = 1;
    low_key_ = keys_[0];

    // link the sibling nodes if exist
    if (prev_node != nullptr) {
      prev_node->LinkNext(this_pid, this);
    }

    nodes.emplace_back(low_key_, this_pid, kKeyLen);
  }

  /**
   * @brief Link border nodes between partial trees.
   *
   * @tparam MappingTable a class for representing mapping tables.
   * @param left_pid the page ID of a highest border node in a left tree.
   * @param right_pid the page ID of a highest border node in a right tree.
   * @param m_table a mapping table to resolve page IDs.
   */
  template <class MappingTable>
  static void
  LinkVerticalBorderNodes(  //
      PageID left_pid,
      PageID right_pid,
      const MappingTable &m_table)
  {
    if (left_pid == kNullPtr) return;

    while (true) {
      const auto *left_lptr = m_table.GetLogicalPtr(left_pid);
      auto *left_node = left_lptr->template Load<Node *>();
      const auto *right_lptr = m_table.GetLogicalPtr(right_pid);
      auto *right_node = right_lptr->template Load<Node *>();

      left_node->LinkNext(right_pid, right_node);
      if (left_node->is_inner_ == kLeaf) return;  // all the border nodes are linked

      // go down to the lower level
      right_pid = right_node->template GetPayload<PageID>(0);
      left_pid = left_node->template GetPayload<PageID>(left_node->rec_count_ - 1);
    }
  }

  /**
   * @brief Remove the leftmost keys from the leftmost nodes.
   *
   * @tparam MappingTable a class for representing mapping tables.
   * @param pid the logical ID of a root node.
   * @param m_table a mapping table to resolve page IDs.
   */
  template <class MappingTable>
  static void
  RemoveLeftmostKeys(  //
      PageID pid,
      const MappingTable &m_table)
  {
    while (true) {
      // remove the lowest key
      const auto *lptr = m_table.GetLogicalPtr(pid);
      auto *node = lptr->template Load<Node *>();
      node->has_low_key_ = 0;
      if (node->is_inner_ == kLeaf) return;

      // go down to the lower level
      pid = node->template GetPayload<PageID>(0);
    }
  }

 private:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  /// Header length in bytes.
  static constexpr size_t kHeaderLen = sizeof(Node);

  /// the length of keys.
  static constexpr size_t kKeyLen = sizeof(Key);

  /// the length of child pointers.
  static constexpr size_t kPtrLen = sizeof(PageID);

  /*####################################################################################
   * Internal getters/setters
   *##################################################################################*/

  /**
   * @param end_key a pair of a target key and its closed/open-interval flag.
   * @retval true if this node is a rightmost node for the given key.
   * @retval false otherwise.
   */
  [[nodiscard]] auto
  IsRightmostOf(const ScanKey &end_key) const  //
      -> bool
  {
    if (!has_high_key_) return true;  // the rightmost node
    if (!end_key) return false;       // perform full scan

    const auto &[end_k, dummy, closed] = *end_key;
    return Comp{}(end_k, high_key_) || (!closed && !Comp{}(high_key_, end_k));
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
    constexpr size_t kPageAlign = kPageSize - 1;
    const auto page_size = (node_size_ + kPageAlign) & ~kPageAlign;
    return ShiftAddr(this, page_size - sizeof(T) * (pos + 1));
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
   * @brief Link this node to a right sibling node.
   *
   * @param right_pid the page ID of a right sibling node.
   * @param right_node the right sibling node.
   */
  void
  LinkNext(  //
      const PageID right_pid,
      Node *right_node)
  {
    // set a sibling link
    next_ = right_pid;

    // copy the lowest key in the right node as a highest key in this node
    has_high_key_ = 1;
    high_key_ = right_node->low_key_;
  }

  /*####################################################################################
   * Internal variables
   *##################################################################################*/

  /// a flag for indicating whether this node is a leaf or internal node.
  uint16_t is_inner_ : 1;

  /// a flag for indicating the types of delta records.
  uint16_t delta_type_ : 3;

  /// a flag for indicating whether this delta record has a lowest-key.
  uint16_t has_low_key_ : 1;

  /// a flag for indicating whether this delta record has a highest-key.
  uint16_t has_high_key_ : 1;

  /// a blank block for alignment.
  uint16_t : 0;

  /// the number of records in this node.
  uint16_t rec_count_{0};

  /// the size of this node in bytes.
  uint32_t node_size_{kHeaderLen};

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
