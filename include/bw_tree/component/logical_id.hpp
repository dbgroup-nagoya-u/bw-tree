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
 * @brief A class for wrapping physical pointers in logical ones.
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

  auto operator=(const LogicalID &) -> LogicalID & = delete;
  auto operator=(LogicalID &&) -> LogicalID & = delete;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  ~LogicalID() = default;

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/

  /**
   * @brief Load a current physical pointer from this logical ID.
   *
   * Note that this function sets an acquire fence.
   *
   * @tparam T a target class to be loaded.
   * @return an address of a base node or a delta record.
   */
  template <class T>
  [[nodiscard]] auto
  Load() const  //
      -> T
  {
    static_assert(IsValidTarget<T>());

    return reinterpret_cast<T>(physical_ptr_.load(std::memory_order_acquire));
  }

  /**
   * @brief Store a physical pointer into this logical ID.
   *
   * Note that this function sets a release fence.
   *
   * @tparam T a target class to be stored.
   * @param desired a physical pointer to be stored.
   */
  template <class T>
  void
  Store(const T desired)
  {
    static_assert(IsValidTarget<T>());

    physical_ptr_.store(reinterpret_cast<uintptr_t>(desired), std::memory_order_release);
  }

  /**
   * @brief Clear a stored address from this logical ID.
   *
   */
  void
  Clear()
  {
    physical_ptr_.store(kNullPtr, std::memory_order_relaxed);
  }

  /**
   * @brief Perform a weak-CAS operation with given expected/desired values.
   *
   * Note that this function sets a release fence.
   *
   * @tparam T1 a class of an expected value.
   * @tparam T2 a class of a desired value.
   * @param expected an expected value to be compared.
   * @param desired a desired value to be stored.
   * @return true if this CAS operation succeeds.
   * @return false otherwise.
   */
  template <class T1, class T2>
  auto
  CASWeak(  //
      const T1 expected,
      const T2 desired)  //
      -> bool
  {
    static_assert(IsValidTarget<T1>());
    static_assert(IsValidTarget<T2>());

    auto old_ptr = reinterpret_cast<uintptr_t>(expected);
    return physical_ptr_.compare_exchange_weak(  //
        old_ptr,                                 //
        reinterpret_cast<uintptr_t>(desired),    //
        std::memory_order_release);
  }

  /**
   * @brief Perform a strong-CAS operation with given expected/desired values.
   *
   * Note that this function sets a release fence.
   *
   * @tparam T1 a class of an expected value.
   * @tparam T2 a class of a desired value.
   * @param expected an expected value to be compared.
   * @param desired a desired value to be stored.
   * @return true if this CAS operation succeeds.
   * @return false otherwise.
   */
  template <class T1, class T2>
  auto
  CASStrong(  //
      const T1 expected,
      const T2 desired)  //
      -> bool
  {
    static_assert(IsValidTarget<T1>());
    static_assert(IsValidTarget<T2>());

    auto old_ptr = reinterpret_cast<uintptr_t>(expected);
    return physical_ptr_.compare_exchange_strong(  //
        old_ptr,                                   //
        reinterpret_cast<uintptr_t>(desired),      //
        std::memory_order_release);
  }

 private:
  /*####################################################################################
   * Internal utility functions
   *##################################################################################*/

  /**
   * @tparam T a target class to be wrapped.
   * @retval true if a given template class is valid as physical pointers.
   * @retval false otherwise.
   */
  template <class T>
  static constexpr auto
  IsValidTarget()  //
      -> bool
  {
    if constexpr (std::is_same_v<T, uintptr_t>) {
      return true;
    } else if constexpr (std::is_pointer_v<T>) {
      return true;
    } else {
      return false;
    }
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  // an address of a node or a delta record.
  std::atomic_uintptr_t physical_ptr_{kNullPtr};
};

}  // namespace dbgroup::index::bw_tree::component

#endif  // BW_TREE_COMPONENT_LOGICAL_ID_HPP
