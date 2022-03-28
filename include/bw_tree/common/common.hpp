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

#ifndef BW_TREE_COMMON_COMMON_HPP
#define BW_TREE_COMMON_COMMON_HPP

#include <cstring>
#include <memory>

#include "bw_tree/utility.hpp"

namespace dbgroup::index::bw_tree::component
{
/*##################################################################################################
 * Internal enum and classes
 *################################################################################################*/

/**
 * @brief Internal return codes for representing results of delta chain traversal.
 *
 */
enum DeltaRC
{
  kRecordFound = -100,
  kRecordDeleted,
  kNodeRemoved,
  kKeyIsInSibling,
  kReachBaseNode = 0
};

enum SMOStatus
{
  kSplitMayIncomplete = -100,
  kMergeMayIncomplete,
  kNoPartialSMOs = 0
};

/**
 * @brief A flag to distinguish leaf/internal nodes.
 *
 */
enum NodeType : uint16_t
{
  kInternal = 0,
  kLeaf
};

/**
 * @brief A flag to represent the types of delta nodes
 *
 */
enum DeltaType : uint16_t
{
  kNotDelta = 0,
  kInsert,
  kModify,
  kDelete,
  kSplit,
  kRemoveNode,
  kMerge
};

/*##################################################################################################
 * Internal constants
 *################################################################################################*/

constexpr bool kClosed = true;

constexpr uintptr_t kNullPtr = 0;

/// the capacity of each mapping table.
constexpr size_t kMappingTableCapacity = (kPageSize - kWordSize) / kWordSize;

/*##################################################################################################
 * Internal utility functions
 *################################################################################################*/

/**
 * @tparam Compare a comparator class.
 * @tparam T a target class.
 * @param obj_1 an object to be compared.
 * @param obj_2 another object to be compared.
 * @retval true if given objects are equivalent.
 * @retval false if given objects are different.
 */
template <class Compare, class T>
constexpr bool
IsEqual(  //
    const T &obj_1,
    const T &obj_2)
{
  return !Compare{}(obj_1, obj_2) && !Compare{}(obj_2, obj_1);
}

/**
 * @brief Shift a memory address by byte offsets.
 *
 * @param addr an original address.
 * @param offset an offset to shift.
 * @return void* a shifted address.
 */
constexpr void *
ShiftAddr(  //
    const void *addr,
    const size_t offset)
{
  return static_cast<std::byte *>(const_cast<void *>(addr)) + offset;
}

}  // namespace dbgroup::index::bw_tree::component

#endif  // BW_TREE_COMMON_COMMON_HPP
