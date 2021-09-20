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

#include "bw_tree/utility.hpp"

#ifdef BW_TREE_TEST_THREAD_NUM
static constexpr size_t kThreadNum = BW_TREE_TEST_THREAD_NUM;
#else
static constexpr size_t kThreadNum = 8;
#endif

// aliases for typed tests
using UInt32Comp = std::less<uint32_t>;
using UInt64Comp = std::less<uint64_t>;
using CStrComp = dbgroup::index::bw_tree::CompareAsCString;
using PtrComp = std::less<uint64_t *>;

namespace dbgroup::index::bw_tree
{
/**
 * @brief Use CString as variable-length data in tests.
 *
 */
template <>
constexpr bool
IsVariableLengthData<char *>()
{
  return true;
}

}  // namespace dbgroup::index::bw_tree

template <class T>
void
PrepareTestData(  //
    T *data_array,
    const size_t data_num,
    [[maybe_unused]] const size_t data_length)
{
  if constexpr (::dbgroup::index::bw_tree::IsVariableLengthData<T>()) {
    // variable-length data
    for (size_t i = 0; i < data_num; ++i) {
      auto data = reinterpret_cast<char *>(malloc(data_length));
      snprintf(data, data_length, "%08lu", i);
      data_array[i] = reinterpret_cast<T>(data);
    }
  } else if constexpr (std::is_same_v<T, uint64_t *>) {
    // pointer data
    for (size_t i = 0; i < data_num; ++i) {
      auto data = reinterpret_cast<uint64_t *>(malloc(data_length));
      *data = i;
      data_array[i] = data;
    }
  } else {
    // static-length data
    for (size_t i = 0; i < data_num; ++i) {
      data_array[i] = i;
    }
  }
}

template <class T>
void
ReleaseTestData(  //
    [[maybe_unused]] T *data_array,
    const size_t data_num)
{
  if constexpr (std::is_pointer_v<T>) {
    for (size_t i = 0; i < data_num; ++i) {
      free(data_array[i]);
    }
  }
}
