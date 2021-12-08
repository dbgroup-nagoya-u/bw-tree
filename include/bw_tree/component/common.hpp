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

#include <cstring>
#include <memory>

#include "../utility.hpp"

namespace dbgroup::index::bw_tree::component
{
/*##################################################################################################
 * Internal enum and classes
 *################################################################################################*/

/**
 * @brief Internal return codes to represent results of node modification.
 *
 */
enum NodeReturnCode
{
  kSuccess = 0,
  kKeyNotExist,
  kKeyExist
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
enum DeltaNodeType : uint16_t
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

/// alias of memory order for simplicity.
constexpr auto mo_relax = std::memory_order_relaxed;

/// Header length in bytes.
constexpr size_t kHeaderLength = 28;

/*##################################################################################################
 * Internal utility functions
 *################################################################################################*/

template <class T>
constexpr const void *
GetAddr(const T &obj)
{
  if constexpr (IsVariableLengthData<T>()) {
    return reinterpret_cast<const void *>(obj);
  } else {
    return reinterpret_cast<const void *>(&obj);
  }
}

template <class Key, class Comp, class T1, class T2>
constexpr bool
LT(const T1 &a, const T2 &b)
{
  if constexpr (std::is_same_v<T1, Key> && std::is_same_v<T2, Key>) {
    return Comp{}(a, b);
  } else if constexpr (std::is_same_v<T1, Key> && std::is_same_v<T2, void *>) {
    if constexpr (IsVariableLengthData<Key>()) {
      return Comp{}(a, reinterpret_cast<const Key>(b));
    } else {
      return Comp{}(a, *reinterpret_cast<const Key *>(b));
    }
  } else if constexpr (std::is_same_v<T1, void *> && std::is_same_v<T2, Key>) {
    if constexpr (IsVariableLengthData<Key>()) {
      return Comp{}(reinterpret_cast<const Key>(a), b);
    } else {
      return Comp{}(*reinterpret_cast<const Key *>(a), b);
    }
  } else {
    if constexpr (IsVariableLengthData<Key>()) {
      return Comp{}(reinterpret_cast<Key>(const_cast<void *>(a)),
                    reinterpret_cast<Key>(const_cast<void *>(b)));
    } else {
      return Comp{}(*reinterpret_cast<const Key *>(a), *reinterpret_cast<const Key *>(b));
    }
  }
}

/**
 * @brief Cast a given pointer to a specified pointer type.
 *
 * @tparam T a target pointer type.
 * @param addr a target pointer.
 * @return T: a casted pointer.
 */
template <class T>
constexpr T
Cast(const void *addr)
{
  static_assert(std::is_pointer_v<T>);

  return static_cast<T>(const_cast<void *>(addr));
}

/**
 * @brief Compute the maximum number of records in a node.
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @return size_t the expected maximum number of records.
 */
template <class Key, class Payload>
constexpr size_t
GetMaxRecordNum()
{
  auto record_min_length = kWordLength;
  if constexpr (std::is_same_v<Key, std::byte *>) {
    record_min_length += 1;
  } else {
    record_min_length += sizeof(Key);
  }
  if constexpr (std::is_same_v<Payload, std::byte *>) {
    record_min_length += 1;
  } else {
    record_min_length += sizeof(Payload);
  }
  return (kPageSize - kHeaderLength) / record_min_length;
}

/**
 * @tparam Comp a comparator class.
 * @tparam T a target class.
 * @param obj_1 an object to be compared.
 * @param obj_2 another object to be compared.
 * @retval true if given objects are equivalent.
 * @retval false if given objects are different.
 */
template <class Key, class Comp>
constexpr bool
IsEqual(  //
    const void *obj_1,
    const void *obj_2)
{
  return !LT<Key, Comp>(obj_1, obj_2) && !LT<Key, Comp>(obj_2, obj_1);
}

/**
 * @tparam Key a target key class.
 * @tparam Comp a comparator class for target keys.
 * @param key a target key.
 * @param begin_key a begin key of a range condition.
 * @param begin_closed a flag to indicate whether the begin side of range is closed.
 * @param end_key an end key of a range condition.
 * @param end_closed a flag to indicate whether the end side of range is closed.
 * @retval true if a target key is in a range.
 * @retval false if a target key is outside of a range.
 */
template <class Key, class Comp>
constexpr bool
IsInRange(  //
    const void *key,
    const void *begin_key,
    const bool begin_closed,
    const void *end_key,
    const bool end_closed)
{
  if (begin_key == nullptr && end_key == nullptr) {
    // no range condition
    return true;
  } else if (begin_key == nullptr) {
    // less than or equal to
    return LT<Key, Comp>(key, end_key) || (end_closed && !LT<Key, Comp>(end_key, key));
  } else if (end_key == nullptr) {
    // greater than or equal to
    return LT<Key, Comp>(begin_key, key) || (begin_closed && !LT<Key, Comp>(key, begin_key));
  } else {
    // between
    return !((LT<Key, Comp>(key, begin_key) || LT<Key, Comp>(end_key, key))
             || (!begin_closed && IsEqual<Key, Comp>(key, begin_key))
             || (!end_closed && IsEqual<Key, Comp>(key, end_key)));
  }
}

/**
 * @brief Get minimum value of T
 *
 * @return a value which less others in T.
 */

template <class T>
const T
GetMinimum()
{
  if constexpr (IsVariableLengthData<T>()) {
    const char *minimum_key = "";
    return *reinterpret_cast<T *>(reinterpret_cast<void *>(&minimum_key));
  } else {
    return std::numeric_limits<T>::min();
  }
}

/**
 * @brief Shift a memory address by byte offsets.
 *
 * @param addr an original address.
 * @param offset an offset to shift.
 * @return void* a shifted address.
 */
constexpr void *
ShiftAddress(  //
    const void *addr,
    const size_t offset)
{
  return static_cast<std::byte *>(const_cast<void *>(addr)) + offset;
}

/**
 * @brief A wrapper of a deleter class for unique_ptr/shared_ptr.
 *
 * @tparam Payload a class to be deleted by this deleter.
 */
template <class Payload>
struct PayloadDeleter {
  constexpr PayloadDeleter() noexcept = default;

  template <class Up, typename = typename std::enable_if_t<std::is_convertible_v<Up *, Payload *>>>
  PayloadDeleter(const PayloadDeleter<Up> &) noexcept
  {
  }

  void
  operator()(Payload *ptr) const
  {
    static_assert(!std::is_void_v<Payload>, "can't delete pointer to incomplete type");
    static_assert(sizeof(Payload) > 0, "can't delete pointer to incomplete type");

    ::operator delete(ptr);
  }
};

}  // namespace dbgroup::index::bw_tree::component
