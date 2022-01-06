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

#ifndef BW_TREE_TEST_COMMON_HPP
#define BW_TREE_TEST_COMMON_HPP

#include <functional>

#include "bw_tree/utility.hpp"

/*######################################################################################
 * Classes for testing
 *####################################################################################*/

/**
 * @brief An example class.
 *
 */
class MyClass
{
 public:
  constexpr MyClass() = default;

  constexpr MyClass(const MyClass &) = default;
  constexpr MyClass(MyClass &&) = default;

  constexpr auto operator=(const MyClass &) -> MyClass & = default;
  constexpr auto operator=(MyClass &&) -> MyClass & = default;

  ~MyClass() = default;

  constexpr auto
  operator=(const uint64_t value)  //
      -> MyClass &
  {
    data_ = value;
    return *this;
  }

  // enable std::less to compare this class
  constexpr auto
  operator<(const MyClass &comp) const  //
      -> bool
  {
    return data_ < comp.data_;
  }

 private:
  uint64_t data_{};
};

/*######################################################################################
 * Constants for testing
 *####################################################################################*/

#ifdef BW_TREE_TEST_THREAD_NUM
static constexpr size_t kThreadNum = BW_TREE_TEST_THREAD_NUM;
#else
static constexpr size_t kThreadNum = 8;
#endif

constexpr size_t kVariableDataLength = 12;

constexpr size_t kRandomSeed = 10;

/*######################################################################################
 * Global utilities
 *####################################################################################*/

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
constexpr auto
GetDataLength()  //
    -> size_t
{
  if constexpr (::dbgroup::index::bw_tree::IsVariableLengthData<T>()) {
    return kVariableDataLength;
  } else {
    return sizeof(T);
  }
}

template <class T>
void
PrepareTestData(  //
    T *data_array,
    const size_t data_num)
{
  if constexpr (::dbgroup::index::bw_tree::IsVariableLengthData<T>()) {
    // variable-length data
    for (size_t i = 0; i < data_num; ++i) {
      auto *data = reinterpret_cast<char *>(malloc(kVariableDataLength));
      snprintf(data, kVariableDataLength, "%011lu", i);  // NOLINT
      data_array[i] = reinterpret_cast<T>(data);
    }
  } else if constexpr (std::is_same_v<T, uint64_t *>) {
    // pointer data
    for (size_t i = 0; i < data_num; ++i) {
      auto *data = reinterpret_cast<uint64_t *>(malloc(sizeof(uint64_t)));
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

/*######################################################################################
 * Type definitions for templated tests
 *####################################################################################*/

struct UInt8 {
  using Data = uint64_t;
  using Comp = std::less<uint64_t>;
};

struct Int8 {
  using Data = int64_t;
  using Comp = std::less<int64_t>;
};

struct UInt4 {
  using Data = uint32_t;
  using Comp = std::less<uint32_t>;
};

struct Ptr {
  struct PtrComp {
    constexpr bool
    operator()(const uint64_t *a, const uint64_t *b) const noexcept
    {
      return *a < *b;
    }
  };

  using Data = uint64_t *;
  using Comp = PtrComp;
};

struct Var {
  using Data = char *;
  using Comp = dbgroup::index::bw_tree::CompareAsCString;
};

struct Original {
  using Data = MyClass;
  using Comp = std::less<MyClass>;
};

#endif  // BW_TREE_TEST_COMMON_HPP
