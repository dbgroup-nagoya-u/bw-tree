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

#ifndef BW_TREE_TEST_FIX_VAR_SWITCH_HPP
#define BW_TREE_TEST_FIX_VAR_SWITCH_HPP

#include "bw_tree/component/fixlen/delta_record.hpp"
#include "bw_tree/component/fixlen/node.hpp"
#include "bw_tree/component/varlen/delta_record.hpp"
#include "bw_tree/component/varlen/node.hpp"

struct VarLen {
  template <class Key, class Comp>
  using Node = ::dbgroup::index::bw_tree::component::varlen::Node<Key, Comp>;
  template <class Key, class Comp>
  using Delta = ::dbgroup::index::bw_tree::component::varlen::DeltaRecord<Key, Comp>;

  static constexpr bool kUseVarLen = true;
};

struct FixLen {
  template <class Key, class Comp>
  using Node = ::dbgroup::index::bw_tree::component::fixlen::Node<Key, Comp>;
  template <class Key, class Comp>
  using Delta = ::dbgroup::index::bw_tree::component::fixlen::DeltaRecord<Key, Comp>;

  static constexpr bool kUseVarLen = false;
};

#endif  // BW_TREE_TEST_FIX_VAR_SWITCH_HPP