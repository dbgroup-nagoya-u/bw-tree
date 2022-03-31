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

#include "bw_tree/utility.hpp"

namespace dbgroup::index::bw_tree::component
{
/*######################################################################################
 * Internal enum and classes
 *####################################################################################*/

/**
 * @brief Internal return codes for representing results of delta-chain traversal.
 *
 */
enum DeltaRC
{
  kReachBaseNode = 0,
  kRecordFound,
  kRecordDeleted,
  kNodeRemoved,
  kKeyIsInSibling
};

/**
 * @brief Internal recurn codes for representing a status of SMOs.
 *
 */
enum SMOStatus
{
  kNoPartialSMOs = 0,
  kSplitMayIncomplete,
  kMergeMayIncomplete
};

/**
 * @brief A flag for distinguishing leaf/internal nodes.
 *
 */
enum NodeType : uint16_t
{
  kInternal = 0,
  kLeaf
};

/**
 * @brief A flag for representing the types of delta records.
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

/*######################################################################################
 * Internal constants
 *####################################################################################*/

/// a flag for indicating closed intervals
constexpr bool kClosed = true;

/// the NULL value for uintptr_t
constexpr uintptr_t kNullPtr = 0;

/// the capacity of each mapping table.
constexpr size_t kMappingTableCapacity = (kPageSize - kWordSize) / kWordSize;

/*######################################################################################
 * Internal utility functions
 *####################################################################################*/

/**
 * @tparam Compare a comparator class.
 * @tparam T a target class.
 * @param obj_1 an object to be compared.
 * @param obj_2 another object to be compared.
 * @retval true if given objects are equivalent.
 * @retval false otherwise.
 */
template <class Compare, class T>
constexpr auto
IsEqual(  //
    const T &obj_1,
    const T &obj_2)  //
    -> bool
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
constexpr auto
ShiftAddr(  //
    const void *addr,
    const size_t offset)  //
    -> void *
{
  return static_cast<std::byte *>(const_cast<void *>(addr)) + offset;
}

}  // namespace dbgroup::index::bw_tree::component

#endif  // BW_TREE_COMMON_COMMON_HPP
