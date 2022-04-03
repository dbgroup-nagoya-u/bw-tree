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

#ifndef BW_TREE_COMPONENT_VARLEN_RECORD_ITERATOR_HPP
#define BW_TREE_COMPONENT_VARLEN_RECORD_ITERATOR_HPP

#include "bw_tree/component/bw_tree_internal.hpp"
#include "delta_record.hpp"
#include "node.hpp"

namespace dbgroup::index::bw_tree::component::varlen
{

/**
 * @brief A class to represent a iterator for scan results.
 *
 * @tparam Key a target key class
 * @tparam Payload a target payload class
 * @tparam Comp a key-comparator class
 */
template <class Key_t, class Payload_t, class Comp>
class RecordIterator
{
 public:
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using Key = Key_t;
  using Payload = Payload_t;
  using Node_t = Node<Key, Comp>;
  using Delta_t = DeltaRecord<Key, Comp>;
  using BwTree_t = BwTree<RecordIterator, kIsVarLen>;
  using NodeGC_t = ::dbgroup::memory::EpochBasedGC<Node_t, Delta_t>;

  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  RecordIterator(  //
      BwTree_t *bw_tree,
      NodeGC_t *gc,
      Node_t *node,
      size_t begin_pos,
      size_t end_pos,
      const std::optional<std::pair<const Key &, bool>> end_key,
      const bool is_end)
      : bw_tree_{bw_tree},
        gc_{gc},
        node_{node},
        record_count_{end_pos},
        current_pos_{begin_pos},
        current_meta_{node_->GetMetadata(current_pos_)},
        end_key_{std::move(end_key)},
        is_end_{is_end}
  {
  }

  RecordIterator(  //
      Node_t *node,
      size_t begin_pos,
      size_t end_pos,
      const bool is_end)
      : node_{node}, record_count_{end_pos}, current_pos_{begin_pos}, is_end_{is_end}
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
    obj.node_ = nullptr;
    record_count_ = obj.record_count_;
    current_pos_ = obj.current_pos_;
    current_meta_ = node_->GetMetadata(current_pos_);
    is_end_ = obj.is_end_;

    return *this;
  }

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  ~RecordIterator()
  {
    if (node_ != nullptr) {
      gc_->AddGarbage(node_);
    }
  }

  /*####################################################################################
   * Public operators for iterators
   *##################################################################################*/

  /**
   * @return a current key and payload pair.
   */
  auto
  operator*() const  //
      -> std::pair<Key, Payload>
  {
    return {GetKey(), GetPayload()};
  }

  /**
   * @brief Forward this iterator.
   *
   */
  constexpr void
  operator++()
  {
    ++current_pos_;
    current_meta_ = node_->GetMetadata(current_pos_);
  }

  /*####################################################################################
   * Public getters/setters
   *##################################################################################*/

  /**
   * @brief Check if there are any records left.
   *
   * Note that this may call a scan function internally to get a sibling node.
   *
   * @retval true if there are any records or next node left.
   * @retval false otherwise.
   */
  [[nodiscard]] auto
  HasNext()  //
      -> bool
  {
    while (true) {
      if (current_pos_ < record_count_) return true;  // records remain in this node
      if (is_end_) return false;                      // this node is the end of range-scan

      // go to the next sibling node and continue scanning
      const auto &next_key = node_->CopyHighKey();
      auto *sib_page = node_->template GetNext<LogicalID *>();
      *this = bw_tree_->SiblingScan(sib_page, node_, next_key, end_key_);
      if constexpr (IsVariableLengthData<Key>()) {
        delete next_key;
      }
    }
  }

  /**
   * @return Key: a key of a current record
   */
  [[nodiscard]] auto
  GetKey() const  //
      -> Key
  {
    return node_->GetKey(current_meta_);
  }

  /**
   * @return Payload: a payload of a current record
   */
  [[nodiscard]] auto
  GetPayload() const  //
      -> Payload
  {
    return node_->template GetPayload<Payload>(current_meta_);
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// a pointer to BwTree to perform continuous scan
  BwTree_t *bw_tree_{nullptr};

  /// garbage collector
  NodeGC_t *gc_{nullptr};

  /// the pointer to a node that includes partial scan results
  Node_t *node_{nullptr};

  /// the number of records in this node.
  size_t record_count_{0};

  /// an index of a current record
  size_t current_pos_{0};

  /// the metadata of a current record
  Metadata current_meta_{};

  /// the end key given from a user
  std::optional<std::pair<const Key &, bool>> end_key_{};

  bool is_end_{true};
};

}  // namespace dbgroup::index::bw_tree::component::varlen

#endif  // BW_TREE_COMPONENT_VARLEN_RECORD_ITERATOR_HPP
