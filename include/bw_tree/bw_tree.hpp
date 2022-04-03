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

#ifndef BW_TREE_BW_TREE_HPP
#define BW_TREE_BW_TREE_HPP

#include "component/bw_tree_internal.hpp"
#include "component/fixlen/record_iterator.hpp"
#include "component/varlen/record_iterator.hpp"

namespace dbgroup::index::bw_tree
{

template <class Key, class Payload, class Comp = ::std::less<Key>>
class BwTreeVarLen
{
 public:
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using Iterator_t = component::varlen::RecordIterator<Key, Payload, Comp>;
  using BwTree_t = component::BwTree<Iterator_t, component::kIsVarLen>;

  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new BwTree object.
   *
   * @param gc_interval_microsec GC internal [us]
   */
  explicit BwTreeVarLen(  //
      const size_t gc_interval_microsec = kDefaultGCTime,
      const size_t gc_thread_num = kDefaultGCThreadNum)
      : bw_tree_{gc_interval_microsec, gc_thread_num}
  {
  }

  BwTreeVarLen(const BwTreeVarLen &) = delete;
  BwTreeVarLen(BwTreeVarLen &&) = delete;

  BwTreeVarLen &operator=(const BwTreeVarLen &) = delete;
  BwTreeVarLen &operator=(BwTreeVarLen &&) = delete;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  /**
   * @brief Destroy the BwTree object.
   *
   */
  ~BwTreeVarLen() = default;

  /*####################################################################################
   * Public read APIs
   *##################################################################################*/

  /**
   * @brief Read a payload of a specified key if it exists.
   *
   * This function returns two return codes: kSuccess and kKeyNotExist. If a return code
   * is kSuccess, a returned pair contains a target payload. If a return code is
   * kKeyNotExist, the value of a returned payload is undefined.
   *
   * @param key a target key.
   * @return a read payload or std::nullopt.
   */
  auto
  Read(const Key &key)  //
      -> std::optional<Payload>
  {
    return bw_tree_.Read(key);
  }

  /**
   * @brief Perform a range scan with specified keys.
   *
   * @param begin_key a pair of a begin key and its openness (true=closed).
   * @param end_key a pair of an end key and its openness (true=closed).
   * @return an iterator to access target records.
   */
  auto
  Scan(  //
      const std::optional<std::pair<const Key &, bool>> &begin_key = std::nullopt,
      const std::optional<std::pair<const Key &, bool>> &end_key = std::nullopt)  //
      -> Iterator_t
  {
    return bw_tree_.Scan(begin_key, end_key);
  }

  /*####################################################################################
   * Public write APIs
   *##################################################################################*/

  /**
   * @brief Write (i.e., put) a specified kay/payload pair.
   *
   * If a specified key does not exist in the index, this function performs an insert
   * operation. If a specified key has been already inserted, this function perfroms an
   * update operation. Thus, this function always returns kSuccess as a return code.
   *
   * Note that if a target key/payload is binary data, it is required to specify its
   * length in bytes.
   *
   * @param key a target key to be written.
   * @param payload a target payload to be written.
   * @return kSuccess.
   */
  auto
  Write(  //
      const Key &key,
      const Payload &payload,
      const size_t key_len = sizeof(Key))  //
      -> ReturnCode
  {
    return bw_tree_.Write(key, payload, key_len);
  }

  /**
   * @brief Insert a specified key/payload pair.
   *
   * This function performs a uniqueness check in its processing. If a specified key
   * does not exist, this function insert a target payload into the index. If a
   * specified key exists in the index, this function does nothing and returns kKeyExist
   * as a return code.
   *
   * Note that if a target key/payload is binary data, it is required to specify its
   * length in bytes.
   *
   * @param key a target key to be written.
   * @param payload a target payload to be written.
   * @retval.kSuccess if inserted.
   * @retval kKeyExist if a specified key exists.
   */
  ReturnCode
  Insert(  //
      const Key &key,
      const Payload &payload,
      const size_t key_len = sizeof(Key))
  {
    return bw_tree_.Insert(key, payload, key_len);
  }

  /**
   * @brief Update a target kay with a specified payload.
   *
   * This function performs a uniqueness check in its processing. If a specified key
   * exist, this function update a target payload. If a specified key does not exist in
   * the index, this function does nothing and returns kKeyNotExist as a return code.
   *
   * Note that if a target key/payload is binary data, it is required to specify its
   * length in bytes.
   *
   * @param key a target key to be written.
   * @param payload a target payload to be written.
   * @retval kSuccess if updated.
   * @retval kKeyNotExist if a specified key does not exist.
   */
  ReturnCode
  Update(  //
      const Key &key,
      const Payload &payload,
      const size_t key_len = sizeof(Key))
  {
    return bw_tree_.Update(key, payload, key_len);
  }

  /**
   * @brief Delete a target kay from the index.
   *
   * This function performs a uniqueness check in its processing. If a specified key
   * exist, this function deletes it. If a specified key does not exist in the index,
   * this function does nothing and returns kKeyNotExist as a return code.
   *
   * Note that if a target key is binary data, it is required to specify its length in
   * bytes.
   *
   * @param key a target key to be written.
   * @retval kSuccess if deleted.
   * @retval kKeyNotExist if a specified key does not exist.
   */
  auto
  Delete(const Key &key,
         const size_t key_len = sizeof(Key))  //
      -> ReturnCode
  {
    return bw_tree_.Delete(key, key_len);
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// an actual instance of a Bw-tree
  BwTree_t bw_tree_{kDefaultGCTime, kDefaultGCThreadNum};
};

/**
 * @brief A class to represent Bw-tree.
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Comp a comparetor class for keys.
 */
template <class Key, class Payload, class Comp = ::std::less<Key>>
class BwTreeFixLen
{
 public:
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using Iterator_t = component::fixlen::RecordIterator<Key, Payload, Comp>;
  using BwTree_t = component::BwTree<Iterator_t, !component::kIsVarLen>;

  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new BwTree object.
   *
   * @param gc_interval_microsec GC internal [us]
   */
  explicit BwTreeFixLen(  //
      const size_t gc_interval_microsec = kDefaultGCTime,
      const size_t gc_thread_num = kDefaultGCThreadNum)
      : bw_tree_{gc_interval_microsec, gc_thread_num}
  {
  }

  BwTreeFixLen(const BwTreeFixLen &) = delete;
  BwTreeFixLen(BwTreeFixLen &&) = delete;

  BwTreeFixLen &operator=(const BwTreeFixLen &) = delete;
  BwTreeFixLen &operator=(BwTreeFixLen &&) = delete;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  /**
   * @brief Destroy the BwTree object.
   *
   */
  ~BwTreeFixLen() = default;

  /*####################################################################################
   * Public read APIs
   *##################################################################################*/

  /**
   * @brief Read a payload of a specified key if it exists.
   *
   * This function returns two return codes: kSuccess and kKeyNotExist. If a return code
   * is kSuccess, a returned pair contains a target payload. If a return code is
   * kKeyNotExist, the value of a returned payload is undefined.
   *
   * @param key a target key.
   * @return a read payload or std::nullopt.
   */
  auto
  Read(const Key &key)  //
      -> std::optional<Payload>
  {
    return bw_tree_.Read(key);
  }

  /**
   * @brief Perform a range scan with specified keys.
   *
   * @param begin_key a pair of a begin key and its openness (true=closed).
   * @param end_key a pair of an end key and its openness (true=closed).
   * @return an iterator to access target records.
   */
  auto
  Scan(  //
      const std::optional<std::pair<const Key &, bool>> &begin_key = std::nullopt,
      const std::optional<std::pair<const Key &, bool>> &end_key = std::nullopt)  //
      -> Iterator_t
  {
    return bw_tree_.Scan(begin_key, end_key);
  }

  /*####################################################################################
   * Public write APIs
   *##################################################################################*/

  /**
   * @brief Write (i.e., put) a specified kay/payload pair.
   *
   * If a specified key does not exist in the index, this function performs an insert
   * operation. If a specified key has been already inserted, this function perfroms an
   * update operation. Thus, this function always returns kSuccess as a return code.
   *
   * Note that if a target key/payload is binary data, it is required to specify its
   * length in bytes.
   *
   * @param key a target key to be written.
   * @param payload a target payload to be written.
   * @return kSuccess.
   */
  auto
  Write(  //
      const Key &key,
      const Payload &payload)  //
      -> ReturnCode
  {
    return bw_tree_.Write(key, payload);
  }

  /**
   * @brief Insert a specified key/payload pair.
   *
   * This function performs a uniqueness check in its processing. If a specified key
   * does not exist, this function insert a target payload into the index. If a
   * specified key exists in the index, this function does nothing and returns kKeyExist
   * as a return code.
   *
   * Note that if a target key/payload is binary data, it is required to specify its
   * length in bytes.
   *
   * @param key a target key to be written.
   * @param payload a target payload to be written.
   * @retval.kSuccess if inserted.
   * @retval kKeyExist if a specified key exists.
   */
  ReturnCode
  Insert(  //
      const Key &key,
      const Payload &payload)
  {
    return bw_tree_.Insert(key, payload);
  }

  /**
   * @brief Update a target kay with a specified payload.
   *
   * This function performs a uniqueness check in its processing. If a specified key
   * exist, this function update a target payload. If a specified key does not exist in
   * the index, this function does nothing and returns kKeyNotExist as a return code.
   *
   * Note that if a target key/payload is binary data, it is required to specify its
   * length in bytes.
   *
   * @param key a target key to be written.
   * @param payload a target payload to be written.
   * @retval kSuccess if updated.
   * @retval kKeyNotExist if a specified key does not exist.
   */
  ReturnCode
  Update(  //
      const Key &key,
      const Payload &payload)
  {
    return bw_tree_.Update(key, payload);
  }

  /**
   * @brief Delete a target kay from the index.
   *
   * This function performs a uniqueness check in its processing. If a specified key
   * exist, this function deletes it. If a specified key does not exist in the index,
   * this function does nothing and returns kKeyNotExist as a return code.
   *
   * Note that if a target key is binary data, it is required to specify its length in
   * bytes.
   *
   * @param key a target key to be written.
   * @retval kSuccess if deleted.
   * @retval kKeyNotExist if a specified key does not exist.
   */
  auto
  Delete(const Key &key)  //
      -> ReturnCode
  {
    return bw_tree_.Delete(key);
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// an actual instance of a Bw-tree
  BwTree_t bw_tree_{kDefaultGCTime, kDefaultGCThreadNum};
};

}  // namespace dbgroup::index::bw_tree

#endif  // BW_TREE_BW_TREE_HPP
