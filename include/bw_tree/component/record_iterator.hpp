/*
 * Copyright 2023 Database Group, Nagoya University
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

#ifndef BW_TREE_COMPONENT_RECORD_ITERATOR_HPP
#define BW_TREE_COMPONENT_RECORD_ITERATOR_HPP

// local sources
#include "bw_tree/component/common.hpp"

namespace dbgroup::index::bw_tree
{
// forward declaratoin
template <class K, class V, class C, bool kIsVarLen>
class BwTree;

namespace component
{
/**
 * @brief A class for representing an iterator of scan results.
 *
 */
template <class Key, class Payload, class Comp, bool kIsVarLen>
class RecordIterator
{
 public:
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using ScanKey = std::optional<std::tuple<const Key &, size_t, bool>>;
  using BwTree_t = BwTree<Key, Payload, Comp, kIsVarLen>;
  using Node_t = typename BwTree_t::Node_t;

  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new object as an initial iterator.
   *
   * @param bw_tree a pointer to an index.
   * @param node a copied node for scanning results.
   * @param begin_pos the begin position of a current node.
   * @param end_pos the end position of a current node.
   * @param end_key an optional end-point key.
   * @param is_end a flag for indicating a current node is rightmost in scan-range.
   */
  RecordIterator(  //
      BwTree_t *bw_tree,
      Node_t *node,
      size_t begin_pos,
      size_t end_pos,
      const ScanKey end_key,
      const bool is_end)
      : bw_tree_{bw_tree},
        node_{node},
        rec_count_{end_pos},
        current_pos_{begin_pos},
        end_key_{std::move(end_key)},
        is_end_{is_end}
  {
  }

  /**
   * @brief Construct a new object for sibling scanning.
   *
   * @param node a copied node for scanning results.
   * @param begin_pos the begin position of a current node.
   * @param end_pos the end position of a current node.
   * @param is_end a flag for indicating a current node is rightmost in scan-range.
   */
  RecordIterator(  //
      Node_t *node,
      size_t begin_pos,
      size_t end_pos,
      const bool is_end)
      : node_{node}, rec_count_{end_pos}, current_pos_{begin_pos}, is_end_{is_end}
  {
  }

  RecordIterator(const RecordIterator &) = delete;
  RecordIterator(RecordIterator &&) = delete;

  auto operator=(const RecordIterator &) -> RecordIterator & = delete;

  constexpr auto
  operator=(RecordIterator &&obj) noexcept  //
      -> RecordIterator &
  {
    node_ = obj.node_;
    rec_count_ = obj.rec_count_;
    current_pos_ = obj.current_pos_;
    is_end_ = obj.is_end_;

    return *this;
  }

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  /**
   * @brief Destroy the iterator and a retained node if exist.
   *
   */
  ~RecordIterator() = default;

  /*####################################################################################
   * Public operators for iterators
   *##################################################################################*/

  /**
   * @retval true if this iterator indicates a live record.
   * @retval false otherwise.
   */
  explicit
  operator bool()
  {
    return HasRecord();
  }

  /**
   * @return a current key and payload pair.
   */
  auto
  operator*() const  //
      -> std::pair<Key, Payload>
  {
    return node_->template GetRecord<Payload>(current_pos_);
  }

  /**
   * @brief Forward this iterator.
   *
   */
  constexpr void
  operator++()
  {
    ++current_pos_;
  }

  /*####################################################################################
   * Public getters/setters
   *##################################################################################*/

  /**
   * @brief Check if there are any records left.
   *
   * NOTE: this may call a scanning function internally to get a sibling node.
   *
   * @retval true if there are any records or next node left.
   * @retval false otherwise.
   */
  [[nodiscard]] auto
  HasRecord()  //
      -> bool
  {
    while (true) {
      if (current_pos_ < rec_count_) return true;  // records remain in this node
      if (is_end_) return false;                   // this node is the end of range-scan

      // go to the next sibling node and continue scanning
      const auto &next_key = node_->GetHighKey();
      const auto sib_pid = node_->template GetNext<PageID>();
      *this = bw_tree_->SiblingScan(sib_pid, node_, next_key, end_key_);
    }
  }

  /**
   * @return a key of a current record
   */
  [[nodiscard]] auto
  GetKey() const  //
      -> Key
  {
    return node_->GetKey(current_pos_);
  }

  /**
   * @return a payload of a current record
   */
  [[nodiscard]] auto
  GetPayload() const  //
      -> Payload
  {
    return node_->template GetPayload<Payload>(current_pos_);
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// a pointer to a BwTree_t for sibling scanning.
  BwTree_t *bw_tree_{nullptr};

  /// the pointer to a node that includes partial scan results.
  Node_t *node_{nullptr};

  /// the number of records in this node.
  size_t rec_count_{0};

  /// the position of a current record.
  size_t current_pos_{0};

  /// the end key given from a user.
  ScanKey end_key_{};

  /// a flag for indicating a current node is rightmost in scan-range.
  bool is_end_{true};
};

}  // namespace component
}  // namespace dbgroup::index::bw_tree

#endif  // BW_TREE_COMPONENT_RECORD_ITERATOR_HPP
