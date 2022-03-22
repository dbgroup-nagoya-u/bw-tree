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

#ifndef BW_TREE_COMPONENT_LOGICAL_ID_HPP
#define BW_TREE_COMPONENT_LOGICAL_ID_HPP

#include <atomic>

#include "common.hpp"

namespace dbgroup::index::bw_tree::component
{
/**
 * @brief A class for wrapping physical pointers by logical ones.
 *
 */
class LogicalID
{
 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  constexpr LogicalID() = default;

  LogicalID(const LogicalID &) = delete;
  LogicalID(LogicalID &&) = delete;

  LogicalID &operator=(const LogicalID &) = delete;
  LogicalID &operator=(LogicalID &&) = delete;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  ~LogicalID() = default;

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/

  template <class T>
  [[nodiscard]] auto
  Load() const  //
      -> T
  {
    static_assert(std::is_same_v<T, uintptr_t> || std::is_pointer_v<T>);

    return reinterpret_cast<T>(physical_ptr_.load(std::memory_order_acquire));
  }

  template <class T>
  void
  Store(const T desired)
  {
    static_assert(std::is_same_v<T, uintptr_t> || std::is_pointer_v<T>);

    physical_ptr_.store(reinterpret_cast<uintptr_t>(desired), std::memory_order_release);
  }

  template <class T1, class T2>
  auto
  CASWeak(  //
      const T1 expected,
      const T2 desired)  //
      -> bool
  {
    static_assert(std::is_same_v<T1, uintptr_t> || std::is_pointer_v<T1>);
    static_assert(std::is_same_v<T2, uintptr_t> || std::is_pointer_v<T2>);

    auto old_ptr = reinterpret_cast<uintptr_t>(expected);
    return physical_ptr_.compare_exchange_weak(  //
        old_ptr,                                 //
        reinterpret_cast<uintptr_t>(desired),    //
        std::memory_order_release);
  }

  template <class T1, class T2>
  auto
  CASStrong(  //
      const T1 expected,
      const T2 desired)  //
      -> bool
  {
    static_assert(std::is_same_v<T1, uintptr_t> || std::is_pointer_v<T1>);
    static_assert(std::is_same_v<T2, uintptr_t> || std::is_pointer_v<T2>);

    auto old_ptr = reinterpret_cast<uintptr_t>(expected);
    return physical_ptr_.compare_exchange_strong(  //
        old_ptr,                                   //
        reinterpret_cast<uintptr_t>(desired),      //
        std::memory_order_release);
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  // an address of a base node to be consolidated.
  std::atomic_uintptr_t physical_ptr_{kNullPtr};
};

}  // namespace dbgroup::index::bw_tree::component

#endif  // BW_TREE_COMPONENT_LOGICAL_ID_HPP
