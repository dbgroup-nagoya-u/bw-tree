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

/// Alias for representing logical page IDs.
using PageID = uint64_t;

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

/// the chacheline size for memory alignment.
constexpr std::align_val_t kCacheAlignVal = static_cast<std::align_val_t>(kCacheLineSize);

/// the NULL value for uintptr_t
constexpr uintptr_t kNullPtr = 0;

/// leave free space for later modifications.
constexpr size_t kNodeCapacityForBulkLoading = kPageSize * 0.9;

/*######################################################################################
 * Internal utility classes
 *####################################################################################*/

/**
 * @brief A deleter function to release aligned pages.
 *
 * @param ptr the address of pages to be released.
 */
inline void
DeleteAlignedPtr(void *ptr)
{
  ::operator delete(ptr, kCacheAlignVal);
}

/**
 * @brief Allocate a memory region with virtual memory page alignments.
 *
 * @param size the expected page size.
 * @return the address of an allocated page.
 */
template <class T>
auto
AllocVMAlignedPage(const size_t size)  //
    -> T
{
  constexpr auto kVMPageAlign = static_cast<std::align_val_t>(kVMPageSize);
  return reinterpret_cast<T>(::operator new(size, kVMPageAlign));
}

/**
 * @brief A deleter function to release pages aligned for virtual memory addresses.
 *
 * @param page the address of pages to be released.
 */
inline void
ReleaseVMAlignedPage(void *page)
{
  constexpr auto kVMPageAlign = static_cast<std::align_val_t>(kVMPageSize);
  ::operator delete(page, kVMPageAlign);
}

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
  static const inline std::function<void(void *)> deleter{DeleteAlignedPtr};
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
  static const inline std::function<void(void *)> deleter{DeleteAlignedPtr};
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

/**
 * @brief Parse an entry of bulkload according to key's type.
 *
 * @tparam Entry std::pair or std::tuple for containing entries.
 * @param entry a bulkload entry.
 * @retval 1st: a target key.
 * @retval 2nd: a target payload.
 * @retval 3rd: the length of a target key.
 * @retval 4th: the length of a target payload.
 */
template <class Entry>
constexpr auto
ParseEntry(const Entry &entry)  //
    -> std::tuple<std::tuple_element_t<0, Entry>, std::tuple_element_t<1, Entry>, size_t, size_t>
{
  using Key = std::tuple_element_t<0, Entry>;
  using Payload = std::tuple_element_t<1, Entry>;

  constexpr auto kTupleSize = std::tuple_size_v<Entry>;
  static_assert(2 <= kTupleSize && kTupleSize <= 4);

  if constexpr (kTupleSize == 4) {
    return entry;
  } else if constexpr (kTupleSize == 3) {
    const auto &[key, payload, key_len] = entry;
    return {key, payload, key_len, sizeof(Payload)};
  } else {
    const auto &[key, payload] = entry;
    return {key, payload, sizeof(Key), sizeof(Payload)};
  }
}

/**
 * @brief Parse an entry of bulkload according to key's type.
 *
 * @tparam Entry std::pair or std::tuple for containing entries.
 * @param entry a bulkload entry.
 * @retval 1st: a target key.
 * @retval 2nd: the length of a target key.
 */
template <class Entry>
constexpr auto
ParseKey(const Entry &entry)  //
    -> std::pair<std::tuple_element_t<0, Entry>, size_t>
{
  using Key = std::tuple_element_t<0, Entry>;

  constexpr auto kTupleSize = std::tuple_size_v<Entry>;
  static_assert(2 <= kTupleSize && kTupleSize <= 4);

  if constexpr (kTupleSize == 4) {
    const auto &[key, payload, key_len, pay_len] = entry;
    return {key, key_len};
  } else if constexpr (kTupleSize == 3) {
    const auto &[key, payload, key_len] = entry;
    return {key, key_len};
  } else {
    const auto &[key, payload] = entry;
    return {key, sizeof(Key)};
  }
}

}  // namespace dbgroup::index::bw_tree::component

#endif  // BW_TREE_COMPONENT_COMMON_HPP
