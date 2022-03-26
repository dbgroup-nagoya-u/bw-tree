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

#ifndef BW_TREE_COMPONENT_CONSOLIDATE_INFO_HPP
#define BW_TREE_COMPONENT_CONSOLIDATE_INFO_HPP

#include "common.hpp"
#include "metadata.hpp"

namespace dbgroup::index::bw_tree::component
{
// forward declarations for cast
template <class Key, class Comp>
class Node;

/**
 * @brief A class for rataining a node and its separator key to be consolidated.
 *
 */
template <class Key, class Comp>
struct ConsolidateInfo {
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using Node_t = Node<Key, Comp>;

 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  ConsolidateInfo(  //
      const void *node_ptr,
      const void *sep_ptr)
      : node{reinterpret_cast<const Node_t *>(node_ptr)},
        split_d{reinterpret_cast<const Node_t *>(sep_ptr)}
  {
  }

  /*####################################################################################
   * Public member variables
   *##################################################################################*/

  // an address of a base node to be consolidated.
  const Node_t *node{};

  // an address of a corresponding split-delta record if exist.
  const Node_t *split_d{};

  // the number of records to be consolidated.
  size_t rec_num{0};
};

}  // namespace dbgroup::index::bw_tree::component

#endif  // BW_TREE_COMPONENT_CONSOLIDATE_INFO_HPP
