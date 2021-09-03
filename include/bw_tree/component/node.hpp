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

#pragma once

#include <functional>

#include "common.hpp"

namespace dbgroup::index::bw_tree::component
{
/**
 * @brief A class to represent nodes in Bw-tree.
 *
 * Note that this class represents both base nodes and delta nodes.
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Compare a comparetor class for keys.
 */
template <class Key, class Payload, class Compare>
class Node
{
 private:
  /*################################################################################################
   * Internal variables
   *##############################################################################################*/

  /// a flag to indicate whether this node is a leaf or internal node.
  const uint16_t node_type_ : 1;

  /// a flag to indicate the types of a delta node.
  const uint16_t delta_type_ : 3;

  /// a blank block for alignment.
  uint16_t : 0;

  /// the number of records in this node.
  const uint16_t record_count_;

  /// a blank block for alignment.
  uint64_t : 0;

  /// the pointer to the next node.
  Node *next_node_;

  /// an actual data block.
  std::byte *data_block_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  /**
   * @brief Construct a new base node object.
   *
   * @param node_type a flag to indicate whether a leaf or internal node is constructed.
   * @param record_count the number of records in this node.
   * @param next_node the pointer to a sibling node.
   */
  Node(  //
      const NodeType node_type,
      const size_t record_count,
      const Node *next_node)
      : node_type_{node_type},
        delta_type_{DeltaNodeType::kNotDelta},
        record_count_{record_count},
        next_node_{next_node}
  {
  }

  /**
   * @brief Construct a new delta node object.
   *
   * @param node_type a flag to indicate whether a leaf or internal node is constructed.
   * @param delta_type a flag to indicate the type of a constructed delta node.
   * @param next_node the pointer to a next delta/base node.
   */
  Node(  //
      const NodeType node_type,
      const DeltaNodeType delta_type,
      const Node *next_node)
      : node_type_{node_type}, delta_type_{delta_type}, next_node_{next_node}
  {
  }

  /**
   * @brief Destroy the node object.
   *
   */
  ~Node() = default;

  Node(const Node &) = delete;
  Node &operator=(const Node &) = delete;
  Node(Node &&) = delete;
  Node &operator=(Node &&) = delete;

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  /**
   * @retval true if this is a leaf node.
   * @retval false if this is an internal node.
   */
  constexpr bool
  IsLeaf() const
  {
    return node_type_;
  }

  /**
   * @return DeltaNodeType: the type of a delta node.
   */
  constexpr DeltaNodeType
  GetDeltaNodeType() const
  {
    return delta_type_;
  }

  /**
   * @return size_t: the number of records in this node.
   */
  constexpr size_t
  GetRecordCount() const
  {
    return record_count_;
  }
}

}  // namespace dbgroup::index::bw_tree::component
