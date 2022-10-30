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
   * Type aliases
   *##################################################################################*/

  using Record = std::pair<Key, const void *>;
  using ScanKey = std::optional<std::tuple<const Key &, size_t, bool>>;
  template <class Entry>
  using BulkIter = typename std::vector<Entry>::const_iterator;
  using NodeEntry = std::tuple<Key, LogicalID *, size_t>;

  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct an initial root node.
   *
   */
  constexpr explicit Node(const bool is_inner = false)
      : is_inner_{static_cast<NodeType>(is_inner)}, delta_type_{kNotDelta}, do_split_{0}
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
      : is_inner_{kInternal}, delta_type_{kNotDelta}, do_split_{0}, record_count_{2}
  {
    // set a split-left page
    auto offset = SetPayload(kPageSize, left_lid);
    meta_array_[0] = Metadata{offset, 0, kWordSize};

    // set a split-right page
    const auto meta = split_d->low_meta_;
    const auto key_len = meta.GetKeyLength();
    const auto *right_lid = split_d->template GetPayload<LogicalID *>(meta);
    offset = SetPayload(offset, right_lid);
    offset -= key_len;
    memcpy(ShiftAddr(this, offset), split_d->GetKeyAddr(meta), key_len);
    meta_array_[1] = Metadata{offset, key_len, key_len + kWordSize};
  }

  /**
   * @brief Construct a consolidated base node object.
   *
   * Note that this construcor sets only header information.
   *
   * @param is_inner a flag for indicating whether a leaf or internal node is constructed.
   * @param node_size the virtual size of this node.
   * @param do_split a flag for skipping left-split records in consolidation.
   */
  Node(  //
      const bool is_inner,
      const size_t node_size,
      const bool do_split)
      : is_inner_{static_cast<NodeType>(is_inner)},
        delta_type_{kNotDelta},
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
    return low_meta_.GetKeyLength() == 0;
  }

  /**
   * @return the byte length of this node.
   */
  [[nodiscard]] constexpr auto
  GetNodeSize() const  //
      -> size_t
  {
    return (node_size_ < kPageSize) ? kPageSize : node_size_;
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
    if (low_meta_.GetKeyLength() == 0) return std::nullopt;
    return GetKey(low_meta_);
  }

  /**
   * @brief Copy and return a highest key for scanning.
   *
   * NOTE: this function does not check the existence of a highest key.
   * NOTE: this function allocates memory dynamically for variable-length keys, so it
   * must be released by the caller.
   *
   * @return the highest key in this node.
   */
  [[nodiscard]] auto
  GetHighKey() const  //
      -> Key
  {
    const auto key_len = high_meta_.GetKeyLength();

    Key high_key{};
    if constexpr (IsVarLenData<Key>()) {
      high_key = reinterpret_cast<Key>(::operator new(key_len));
      memcpy(high_key, GetKeyAddr(high_meta_), key_len);
    } else {
      memcpy(&high_key, GetKeyAddr(high_meta_), key_len);
    }

    return high_key;
  }

  /**
   * @param pos the position of a target record.
   * @return a key in a target record.
   */
  [[nodiscard]] auto
  GetKey(const size_t pos) const  //
      -> Key
  {
    return GetKey(meta_array_[pos]);
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
    return GetPayload<T>(meta_array_[pos]);
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
    const auto meta = meta_array_[pos];
    return {GetKey(meta), GetPayload<T>(meta)};
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
      -> std::pair<DeltaRC, size_t>
  {
    int64_t begin_pos = is_inner_;
    int64_t end_pos = record_count_ - 1;
    while (begin_pos <= end_pos) {
      const size_t pos = (begin_pos + end_pos) >> 1UL;  // NOLINT
      const auto &index_key = GetKey(meta_array_[pos]);

      if (Comp{}(key, index_key)) {  // a target key is in a left side
        end_pos = pos - 1;
      } else if (Comp{}(index_key, key)) {  // a target key is in a right side
        begin_pos = pos + 1;
      } else {  // find an equivalent key
        return {kRecordFound, pos};
      }
    }

    return {kRecordDeleted, begin_pos};
  }

  /**
   * @brief Get the corresponding child node with a specified key.
   *
   * If there is no specified key in this node, this returns the child in the minimum
   * position that is greater than the specified key.
   *
   * @param key a target key.
   * @return the logical ID of searched child node.
   */
  [[nodiscard]] auto
  SearchChild(const Key &key) const  //
      -> LogicalID *
  {
    const auto [rc, pos] = SearchRecord(key);
    return GetPayload<LogicalID *>((rc == kRecordFound) ? pos : pos - 1);
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
      const Record &rec,
      const Key &key)  //
      -> bool
  {
    return Comp{}(rec.first, key);
  }

  /**
   * @param rec a target delta record.
   * @param key a comparison key.
   * @retval true if delta record's key is less than or equal to a given one.
   * @retval false otherwise.
   */
  [[nodiscard]] static auto
  LE(  //
      const Record &rec,
      const Key &key)  //
      -> bool
  {
    return !Comp{}(key, rec.first);
  }

  /**
   * @brief Compute record counts to be consolidated and the total block size.
   *
   * This function modifies a given consol_info's third variable (i.e., record count)
   * for the following consolidation procedure.
   *
   * @param consol_info the set of consolidated nodes.
   * @param is_inner a flag for indicating a target node is leaf or internal ones.
   * @return the total block size of a consolidated node.
   */
  [[nodiscard]] static auto
  PreConsolidate(  //
      std::vector<ConsolidateInfo> &consol_info,
      const bool is_inner)  //
      -> size_t
  {
    const auto end_pos = consol_info.size() - 1;
    size_t size = 0;

    for (size_t i = 0; i <= end_pos; ++i) {
      auto &[n_ptr, d_ptr, rec_num] = consol_info.at(i);
      const auto *node = reinterpret_cast<const Node *>(n_ptr);
      const auto *split_d = reinterpret_cast<const Node *>(d_ptr);

      // add the length of the lowest key
      if (is_inner || i == end_pos) {
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
        rec_num = node->SearchRecord(sep_key).second;

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
   * @brief Copy a lowest key for consolidation or set an initial used page size for
   * splitting.
   *
   * @param consol_info an original node that has a lowest key.
   * @param offset an offset to the bottom of free space.
   * @return an initial offset.
   */
  auto
  CopyLowKeyFrom(  //
      const ConsolidateInfo &consol_info,
      size_t offset)  //
      -> size_t
  {
    if (is_inner_) {
      // inner nodes have the lowest key in a record region
      const auto key_len = meta_array_[0].GetKeyLength();
      low_meta_ = Metadata{meta_array_[0].GetOffset(), key_len, key_len};
      return offset;
    }

    // prepare a node that has the lowest key
    const Node *node{};
    Metadata meta{};
    if (node_size_ < kPageSize) {
      // this node is a split-right node, and so the leftmost record has the lowest key
      node = const_cast<const Node *>(this);
      meta = meta_array_[0];
    } else {
      // this node is a consolidated node, and so the given node has the lowest key
      node = reinterpret_cast<const Node *>(consol_info.node);
      meta = node->low_meta_;
    }

    // copy the lowest key
    const auto key_len = meta.GetKeyLength();
    offset -= key_len;
    memcpy(ShiftAddr(this, offset), node->GetKeyAddr(meta), key_len);
    low_meta_ = Metadata{offset, key_len, key_len};

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
   * @param pos the position of a target record.
   * @param offset an offset to the bottom of free space.
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
    const auto meta = node->meta_array_[pos];
    const auto rec_len = meta.GetTotalLength();

    if (!do_split_) {
      // copy a record from the given node
      offset -= rec_len;
      memcpy(ShiftAddr(this, offset), node->GetKeyAddr(meta), rec_len);
      meta_array_[record_count_++] = meta.UpdateForLeaf(offset);
    } else if (kPageSize - offset < node_size_) {
      // calculate the skipped page size
      offset -= rec_len + kWordSize;
    } else {
      // this record is the end one in a split-left node
      do_split_ = false;
      offset = kPageSize;
    }

    return offset;
  }

  /**
   * @brief Copy a record from a delta record in the leaf level.
   *
   * @param rec_pair a pair of original delta record and its key.
   * @param offset an offset to the bottom of free space.
   * @return an offset to the copied record.
   */
  template <class T>
  auto
  CopyRecordFrom(  //
      const Record &rec_pair,
      size_t offset)  //
      -> size_t
  {
    const auto *rec = reinterpret_cast<const Node *>(rec_pair.second);
    if (rec->delta_type_ != kDelete) {
      // the target record is insert/modify delta
      const auto meta = rec->low_meta_;
      const auto rec_len = meta.GetTotalLength();

      if (!do_split_) {
        // copy a record from the given node
        offset -= rec_len;
        memcpy(ShiftAddr(this, offset), rec->GetKeyAddr(meta), rec_len);
        meta_array_[record_count_++] = meta.UpdateForLeaf(offset);
      } else if (kPageSize - offset < node_size_) {
        // calculate the skipped page size
        offset -= rec_len + kWordSize;
      } else {
        // this record is the end one in a split-left node
        do_split_ = false;
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
   * @param this_lid the logical ID of a this node.
   * @param nodes the container of construcred nodes.
   */
  template <class Entry>
  void
  Bulkload(  //
      BulkIter<Entry> &iter,
      const BulkIter<Entry> &iter_end,
      Node *prev_node,
      LogicalID *this_lid,
      std::vector<NodeEntry> &nodes)
  {
    using Payload = std::tuple_element_t<1, Entry>;

    constexpr auto kMaxKeyLen = (IsVarLenData<Key>()) ? kMaxVarDataSize : sizeof(Key);

    // extract and insert entries into this node
    auto offset = kPageSize - kMaxKeyLen;  // reserve the space for a highest key
    auto node_size = kHeaderLength + kMaxKeyLen;
    for (; iter < iter_end; ++iter) {
      const auto &[key, payload, key_len] = ParseEntry(*iter);
      const auto rec_len = key_len + sizeof(Payload);

      // check whether the node has sufficent space
      node_size += rec_len + sizeof(Metadata);
      if (node_size > kPageSize) break;

      // insert an entry into this node
      offset = SetPayload(offset, payload);
      offset = SetKey(offset, key, key_len);
      meta_array_[record_count_++] = Metadata{offset, key_len, rec_len};
    }

    // set a lowest key
    const auto low_meta = meta_array_[0];
    const auto low_key_len = low_meta.GetKeyLength();
    low_meta_ = Metadata{low_meta.GetOffset(), low_key_len, low_key_len};

    // link the sibling nodes if exist
    if (prev_node != nullptr) {
      prev_node->LinkNext(this_lid);
    }

    nodes.emplace_back(*GetLowKey(), this_lid, low_key_len);
  }

  /**
   * @brief Link border nodes between partial trees.
   *
   * @param left_lid the logical ID of a highest border node in a left tree.
   * @param right_lid the logical ID of a highest border node in a right tree.
   */
  static void
  LinkVerticalBorderNodes(  //
      const LogicalID *left_lid,
      const LogicalID *right_lid)
  {
    if (left_lid == nullptr) return;

    while (true) {
      auto *left_node = left_lid->Load<Node *>();
      left_node->LinkNext(right_lid);
      if (left_node->is_inner_ == kLeaf) return;  // all the border nodes are linked

      // go down to the lower level
      auto *right_node = right_lid->Load<Node *>();
      right_lid = right_node->template GetPayload<LogicalID *>(0);
      left_lid = left_node->template GetPayload<LogicalID *>(left_node->record_count_ - 1);
    }
  }

  /**
   * @brief Remove the leftmost keys from the leftmost nodes.
   *
   * @param lid the logical ID of a root node.
   */
  static void
  RemoveLeftmostKeys(const LogicalID *lid)
  {
    while (true) {
      // remove the lowest key
      auto *node = lid->Load<Node *>();
      node->low_meta_ = Metadata{kPageSize, 0, 0};
      if (node->is_inner_ == kLeaf) return;

      // remove the leftmost key in a record region of an inner node
      const auto meta = node->meta_array_[0];
      const auto offset = meta.GetOffset();
      const auto key_len = meta.GetKeyLength();
      const auto rec_len = meta.GetTotalLength();
      node->meta_array_[0] = Metadata{offset + key_len, 0, rec_len - key_len};

      // go down to the lower level
      lid = node->template GetPayload<LogicalID *>(0);
    }
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
  IsRightmostOf(const ScanKey &end_key) const  //
      -> bool
  {
    if (high_meta_.GetKeyLength() == 0) return true;  // the rightmost node
    if (!end_key) return false;                       // perform full scan

    const auto &high_k = GetKey(high_meta_);
    const auto &[end_k, dummy, closed] = *end_key;
    return Comp{}(end_k, high_k) || (!closed && !Comp{}(high_k, end_k));
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
   * @return a target key.
   */
  [[nodiscard]] auto
  GetKey(const Metadata meta) const  //
      -> Key
  {
    if constexpr (IsVarLenData<Key>()) {
      return reinterpret_cast<Key>(GetKeyAddr(meta));
    } else {
      Key key{};
      memcpy(&key, GetKeyAddr(meta), sizeof(Key));
      return key;
    }
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
    if constexpr (IsVarLenData<T>()) {
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
   * @brief Parse an entry of bulkload according to key's type.
   *
   * @tparam Payload a payload type.
   * @tparam Entry std::pair or std::tuple for containing entries.
   * @param entry a bulkload entry.
   * @retval 1st: a target key.
   * @retval 2nd: a target payload.
   * @retval 3rd: the length of a target key.
   */
  template <class Entry>
  constexpr auto
  ParseEntry(const Entry &entry)  //
      -> std::tuple<Key, std::tuple_element_t<1, Entry>, size_t>
  {
    constexpr auto kTupleSize = std::tuple_size_v<Entry>;
    static_assert(2 <= kTupleSize && kTupleSize <= 3);

    if constexpr (kTupleSize == 3) {
      return entry;
    } else {
      const auto &[key, payload] = entry;
      return {key, payload, sizeof(Key)};
    }
  }

  /**
   * @brief Link this node and a right sibling node.
   *
   * @param right_lid the logical ID of a right sibling node.
   */
  void
  LinkNext(const LogicalID *right_lid)
  {
    // set a sibling link
    next_ = reinterpret_cast<uintptr_t>(right_lid);

    // copy the lowest key in the right node as a highest key in this node
    auto *right_node = right_lid->Load<Node *>();
    const auto low_meta = right_node->low_meta_;
    const auto key_len = low_meta.GetKeyLength();
    const auto offset = kPageSize - key_len;
    memcpy(ShiftAddr(this, offset), right_node->GetKeyAddr(low_meta), key_len);
    high_meta_ = Metadata{offset, key_len, key_len};
  }

  /*####################################################################################
   * Internal variables
   *##################################################################################*/

  /// a flag for indicating whether this node is a leaf or internal node.
  uint16_t is_inner_ : 1;

  /// a flag for indicating the types of delta records.
  uint16_t delta_type_ : 3;

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

  /// metadata of a lowest key or a first record in a delta record.
  Metadata low_meta_{kPageSize, 0, 0};

  /// metadata of a highest key.
  Metadata high_meta_{kPageSize, 0, 0};

  /// an actual data block (it starts with record metadata).
  Metadata meta_array_[0];
};

}  // namespace dbgroup::index::bw_tree::component::varlen

#endif  // BW_TREE_COMPONENT_VARLEN_NODE_HPP
