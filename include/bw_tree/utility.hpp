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

#ifndef BW_TREE_UTILITY_HPP
#define BW_TREE_UTILITY_HPP

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

namespace dbgroup::index::bw_tree
{
/*######################################################################################
 * Global constants
 *####################################################################################*/

/// Assumes that one word is represented by 8 bytes.
constexpr size_t kWordSize = sizeof(uintptr_t);

/// the default time interval for garbage collection [us].
constexpr size_t kDefaultGCTime = 100000;

/// the default number of worker threads for garbage collection.
constexpr size_t kDefaultGCThreadNum = 1;

/// a flag for indicating closed intervals
constexpr bool kClosed = true;

/// a flag for indicating closed intervals
constexpr bool kOpen = false;

/*######################################################################################
 * Utility enum and classes
 *####################################################################################*/

/**
 * @brief Return codes for APIs of a Bw-tree.
 *
 */
enum ReturnCode {
  kKeyNotExist = -2,
  kKeyExist,
  kSuccess = 0,
};

/**
 * @brief Compare binary keys as CString.
 *
 * NOTE: the end of every key must be '\\0'.
 *
 */
struct CompareAsCString {
  constexpr auto
  operator()(  //
      const void *a,
      const void *b) const noexcept  //
      -> bool
  {
    if (a == nullptr) return false;
    if (b == nullptr) return true;
    return strcmp(static_cast<const char *>(a), static_cast<const char *>(b)) < 0;
  }
};

/**
 * @tparam T a target class.
 * @retval true if a target class is variable-length data.
 * @retval false otherwise.
 */
template <class T>
constexpr auto
IsVarLenData()  //
    -> bool
{
  if constexpr (std::is_same_v<T, char *> || std::is_same_v<T, std::byte *>) {
    return true;
  } else {
    return false;
  }
}

/**
 * @param val a target value.
 * @return the binary logarithm of a given value.
 */
constexpr auto
Log2(const size_t val)  //
    -> size_t
{
  if (val == 0) return 0;
  return (val == 1) ? 0 : Log2(val >> 1UL) + 1;
}

/*######################################################################################
 * Tuning parameters for Bw-tree
 *####################################################################################*/

/// The default page size of each node
constexpr size_t kPageSize = BW_TREE_PAGE_SIZE;

/// The number of delta records for invoking consolidation
constexpr size_t kMaxDeltaRecordNum = BW_TREE_MAX_DELTA_RECORD_NUM;

/// The minimum size of nodes for invoking merging
constexpr size_t kMinNodeSize = BW_TREE_MIN_NODE_SIZE;

/// The maximun size of variable-length data
constexpr size_t kMaxVarDataSize = BW_TREE_MAX_VARIABLE_DATA_SIZE;

/// a flag for indicating optimized page layouts for fixed-length data.
constexpr bool kOptimizeForFixLenData = false;

// Check whether the specified page size is valid
static_assert(kPageSize % kWordSize == 0);
static_assert(kMaxVarDataSize * 2 < kPageSize);

}  // namespace dbgroup::index::bw_tree

#endif  // BW_TREE_UTILITY_HPP
