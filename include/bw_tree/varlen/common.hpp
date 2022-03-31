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

#ifndef BW_TREE_VARLEN_COMMON_HPP
#define BW_TREE_VARLEN_COMMON_HPP

#include "bw_tree/common/common.hpp"

namespace dbgroup::index::bw_tree::component::varlen
{
/*##################################################################################################
 * Internal constants
 *################################################################################################*/

/// Header length in bytes.
constexpr size_t kHeaderLength = 4 * kWordSize;

/*##################################################################################################
 * Internal utility functions
 *################################################################################################*/

template <class Key, class Payload>
constexpr auto
GetMaxDeltaSize()  //
    -> size_t
{
  const auto key_length = (IsVariableLengthData<Key>()) ? kMaxVarDataSize : sizeof(Key);
  const auto pay_length = (sizeof(Payload) > kWordSize) ? sizeof(Payload) : kWordSize;
  return kHeaderLength + 2 * key_length + pay_length;
}

}  // namespace dbgroup::index::bw_tree::component::varlen

#endif  // BW_TREE_VARLEN_COMMON_HPP
