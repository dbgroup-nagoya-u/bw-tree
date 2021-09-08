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

#include <string.h>

#include <cassert>
#include <cstddef>
#include <cstdint>

namespace dbgroup::index::bw_tree
{
/*##################################################################################################
 * Utility enum and classes
 *################################################################################################*/

/**
 * @brief Return codes for APIs of a Bw-tree.
 *
 */
enum ReturnCode
{
  kSuccess = 0,
  kKeyNotExist,
  kKeyExist
};

/**
 * @brief Compare binary keys as CString. The end of every key must be '\\0'.
 *
 */
struct CompareAsCString {
  constexpr bool
  operator()(const void *a, const void *b) const noexcept
  {
    if (a == nullptr) {
      return false;
    } else if (b == nullptr) {
      return true;
    } else {
      return strcmp(static_cast<const char *>(a), static_cast<const char *>(b)) < 0;
    }
  }
};

/**
 * @tparam T a target class.
 * @retval true if a target class is variable-length data.
 * @retval false if a target class is static-length data.
 */
template <class T>
constexpr bool
IsVariableLengthData()
{
  static_assert(std::is_trivially_copyable_v<T>);
  return false;
}

/*##################################################################################################
 * Tuning parameters for Bw-tree
 *################################################################################################*/

/// Assumes that one word is represented by 8 bytes
constexpr size_t kWordLength = 8;

/// Assumes that one word is represented by 8 bytes
constexpr size_t kCacheLineSize = 64;

#ifdef BW_TREE_PAGE_SIZE
/// The page size of each node
constexpr size_t kPageSize = BW_TREE_PAGE_SIZE;
#else
constexpr size_t kPageSize = 8192;
#endif

#ifdef BW_TREE_MAX_DELTA_NODE_NUM
/// The page size of each node
constexpr size_t kMaxDeltaNodeNum = BW_TREE_MAX_DELTA_NODE_NUM;
#else
constexpr size_t kMaxDeltaNodeNum = 16;
#endif

/// check whether the specified page size is valid
static_assert(kPageSize % kWordLength == 0);

}  // namespace dbgroup::index::bw_tree
