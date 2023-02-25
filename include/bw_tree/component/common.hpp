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

#ifndef BW_TREE_COMPONENT_COMMON_HPP
#define BW_TREE_COMPONENT_COMMON_HPP

// C++ standard libraries
#include <functional>

// external system libraries
#ifdef BW_TREE_HAS_SPINLOCK_HINT
#include <xmmintrin.h>
#endif

// local sources
#include "bw_tree/utility.hpp"

// macro definitions
#ifdef BW_TREE_HAS_SPINLOCK_HINT
#define BW_TREE_SPINLOCK_HINT _mm_pause();  // NOLINT
#else
#define BW_TREE_SPINLOCK_HINT /* do nothing */
#endif

namespace dbgroup::index::bw_tree::component
{
/*######################################################################################
 * Internal enum and classes
 *####################################################################################*/

/**
 * @brief Internal return codes for representing results of delta-chain traversal.
 *
 */
enum DeltaRC {
  kReachBaseNode = 0,
  kRecordFound,
  kRecordNotFound,
  kNodeRemoved,
  kKeyIsInSibling,
  kAbortMerge,
};

/**
 * @brief A flag for distinguishing leaf/internal nodes.
 *
 */
enum NodeType : uint16_t {
  kLeaf = 0,
  kInner,
};

/**
 * @brief A flag for representing the types of delta records.
 *
 */
enum DeltaType : uint16_t {
  kNotDelta = 0,
  kInsert,
  kModify,
  kDelete,
  kRemoveNode,
  kMerge,
};

/*######################################################################################
 * Internal constants
 *####################################################################################*/

/// bits for word alignments.
constexpr size_t kWordAlign = kWordSize - 1;

/// bits for cache line alignments.
constexpr size_t kCacheAlign = kCacheLineSize - 1;

/// the NULL value for uintptr_t
constexpr uintptr_t kNullPtr = 0;

/// the capacity of each mapping table.
constexpr size_t kMappingTableCapacity = (kPageSize - kWordSize) / kWordSize;

/*######################################################################################
 * Internal utility classes
 *####################################################################################*/

/**
 * @brief A struct for representing GG node pages.
 *
 */
struct NodePage {
  // do not call destructor
  using T = void;

  // reuse pages
  static constexpr bool kReusePages = true;

  // use the standard delete function to release garbage
  static const inline std::function<void(void *)> deleter = [](void *ptr) {
    ::operator delete(ptr);
  };
};

/**
 * @brief A struct for representing GC delta pages.
 *
 */
struct DeltaPage {
  // do not call destructor
  using T = void;

  // reuse pages
  static constexpr bool kReusePages = true;

  // use the standard delete function to release garbage
  static const inline std::function<void(void *)> deleter = [](void *ptr) {
    ::operator delete(ptr);
  };
};

/*######################################################################################
 * Internal utility functions
 *####################################################################################*/

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

#endif  // BW_TREE_COMPONENT_COMMON_HPP
