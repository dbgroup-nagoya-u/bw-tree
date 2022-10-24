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
#include "component/fixlen/delta_record.hpp"
#include "component/fixlen/node.hpp"
#include "component/varlen/delta_record.hpp"
#include "component/varlen/node.hpp"

namespace dbgroup::index::bw_tree
{
/**
 * @brief A class for representing Bw-trees with variable-length keys.
 *
 * This implementation can store variable-length keys (i.e., 'text' type in PostgreSQL).
 * If you use only fixed-length keys, 'BwTreeFixLen' will achieve better performance.
 *
 * @tparam Key a class of stored keys.
 * @tparam Payload a class of stored payloads (only fixed-length data for simplicity).
 * @tparam Comp a class for ordering keys.
 */
template <class Key, class Payload, class Comp = ::std::less<Key>>
class BwTreeVarLen
{
 public:
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using Node_t = component::varlen::Node<Key, Comp>;
  using Delta_t = component::varlen::DeltaRecord<Key, Comp>;
  using BwTree_t = component::BwTree<Payload, Node_t, Delta_t, component::kIsVarLen>;
  using Iterator_t = typename BwTree_t::RecordIterator;
  using ScanKey = std::optional<std::tuple<const Key &, size_t, bool>>;

  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new BwTree object.
   *
   * @param gc_interval_microsec time interval for performing GC [us].
   * @param gc_thread_num the number of worker threads for GC.
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
   * @brief Read the payload corresponding to a given key if it exists.
   *
   * @param key a target key.
   * @retval the payload of a given key wrapped with std::optional if it is in this tree.
   * @retval std::nullopt otherwise.
   */
  auto
  Read(  //
      const Key &key,
      [[maybe_unused]] const size_t key_len = sizeof(Key))  //
      -> std::optional<Payload>
  {
    return bw_tree_.Read(key);
  }

  /**
   * @brief Perform a range scan with given keys.
   *
   * @param begin_key a pair of a begin key and its openness (true=closed).
   * @param end_key a pair of an end key and its openness (true=closed).
   * @return an iterator to access scanned records.
   */
  auto
  Scan(  //
      const ScanKey &begin_key = std::nullopt,
      const ScanKey &end_key = std::nullopt)  //
      -> Iterator_t
  {
    return bw_tree_.Scan(begin_key, end_key);
  }

  /*####################################################################################
   * Public write APIs
   *##################################################################################*/

  /**
   * @brief Write (i.e., put) a given key/payload pair.
   *
   * If a given key does not exist in this tree, this function performs an insert
   * operation. If a given key has been already inserted, this function perfroms an
   * update operation. Thus, this function always returns kSuccess as a return code.
   *
   * @param key a target key to be written.
   * @param payload a target payload to be written.
   * @param key_len the length of a target key.
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
   * @brief Insert a given key/payload pair.
   *
   * This function performs a uniqueness check in its processing. If a given key does
   * not exist in this tree, this function inserts a target payload to this tree. If
   * there is a given key in this tree, this function does nothing and returns kKeyExist
   * as a return code.
   *
   * @param key a target key to be inserted.
   * @param payload a target payload to be inserted.
   * @param key_len the length of a target key.
   * @retval.kSuccess if inserted.
   * @retval kKeyExist otherwise.
   */
  auto
  Insert(  //
      const Key &key,
      const Payload &payload,
      const size_t key_len = sizeof(Key))  //
      -> ReturnCode
  {
    return bw_tree_.Insert(key, payload, key_len);
  }

  /**
   * @brief Update the record corresponding to a given key with a given payload.
   *
   * This function performs a uniqueness check in its processing. If there is a given
   * key in this tree, this function updates the corresponding record. If a given key
   * does not exist in this tree, this function does nothing and returns kKeyNotExist as
   * a return code.
   *
   * @param key a target key to be updated.
   * @param payload a payload for updating.
   * @param key_len the length of a target key.
   * @retval kSuccess if updated.
   * @retval kKeyNotExist otherwise.
   */
  auto
  Update(  //
      const Key &key,
      const Payload &payload,
      const size_t key_len = sizeof(Key))  //
      -> ReturnCode
  {
    return bw_tree_.Update(key, payload, key_len);
  }

  /**
   * @brief Delete the record corresponding to a given key from this tree.
   *
   * This function performs a uniqueness check in its processing. If there is a given
   * key in this tree, this function deletes it. If a given key does not exist in this
   * tree, this function does nothing and returns kKeyNotExist as a return code.
   *
   * @param key a target key to be deleted.
   * @param key_len the length of a target key.
   * @retval kSuccess if deleted.
   * @retval kKeyNotExist otherwise.
   */
  auto
  Delete(const Key &key,
         const size_t key_len = sizeof(Key))  //
      -> ReturnCode
  {
    return bw_tree_.Delete(key, key_len);
  }

  /*####################################################################################
   * Public bulkload API
   *##################################################################################*/

  /**
   * @brief Bulkload specified kay/payload pairs.
   *
   * This function bulkloads given entries into this index. The entries are assumed to
   * be given as a vector of pairs of Key and Payload (or key/payload/key-length for
   * variable-length keys). Note that keys in records are assumed to be unique and
   * sorted.
   *
   * @param entries vector of entries to be bulkloaded.
   * @param thread_num the number of threads to perform bulkloading.
   * @return kSuccess.
   */
  template <class Entry>
  auto
  Bulkload(  //
      const std::vector<Entry> &entries,
      const size_t thread_num = 1)  //
      -> ReturnCode
  {
    return bw_tree_.Bulkload(entries, thread_num);
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// an actual instance of a Bw-tree
  BwTree_t bw_tree_{kDefaultGCTime, kDefaultGCThreadNum};
};

/**
 * @brief A class for representing Bw-trees with fixed-length keys.
 *
 * This implementation is optimized for fixed-length keys. If you want to store
 * variable-length keys, use 'BwTreeVarLen' instead.
 *
 * @tparam Key a class of stored keys.
 * @tparam Payload a class of stored payloads (only fixed-length data for simplicity).
 * @tparam Comp a class for ordering keys.
 */
template <class Key, class Payload, class Comp = ::std::less<Key>>
class BwTreeFixLen
{
 public:
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using Node_t = component::fixlen::Node<Key, Comp>;
  using Delta_t = component::fixlen::DeltaRecord<Key, Comp>;
  using BwTree_t = component::BwTree<Payload, Node_t, Delta_t, !component::kIsVarLen>;
  using Iterator_t = typename BwTree_t::RecordIterator;
  using ScanKey = std::optional<std::tuple<const Key &, size_t, bool>>;

  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new BwTree object.
   *
   * @param gc_interval_microsec time interval for performing GC [us].
   * @param gc_thread_num the number of worker threads for GC.
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
   * @brief Read the payload corresponding to a given key if it exists.
   *
   * @param key a target key.
   * @retval the payload of a given key wrapped with std::optional if it is in this tree.
   * @retval std::nullopt otherwise.
   */
  auto
  Read(  //
      const Key &key,
      [[maybe_unused]] const size_t key_len)  //
      -> std::optional<Payload>
  {
    return bw_tree_.Read(key);
  }

  /**
   * @brief Perform a range scan with given keys.
   *
   * @param begin_key a pair of a begin key and its openness (true=closed).
   * @param end_key a pair of an end key and its openness (true=closed).
   * @return an iterator to access scanned records.
   */
  auto
  Scan(  //
      const ScanKey &begin_key = std::nullopt,
      const ScanKey &end_key = std::nullopt)  //
      -> Iterator_t
  {
    return bw_tree_.Scan(begin_key, end_key);
  }

  /*####################################################################################
   * Public write APIs
   *##################################################################################*/

  /**
   * @brief Write (i.e., put) a given key/payload pair.
   *
   * If a given key does not exist in this tree, this function performs an insert
   * operation. If a given key has been already inserted, this function perfroms an
   * update operation. Thus, this function always returns kSuccess as a return code.
   *
   * @param key a target key to be written.
   * @param payload a target payload to be written.
   * @return kSuccess.
   */
  auto
  Write(  //
      const Key &key,
      const Payload &payload,
      [[maybe_unused]] const size_t key_len)  //
      -> ReturnCode
  {
    return bw_tree_.Write(key, payload);
  }

  /**
   * @brief Insert a given key/payload pair.
   *
   * This function performs a uniqueness check in its processing. If a given key does
   * not exist in this tree, this function inserts a target payload to this tree. If
   * there is a given key in this tree, this function does nothing and returns kKeyExist
   * as a return code.
   *
   * @param key a target key to be inserted.
   * @param payload a target payload to be inserted.
   * @retval.kSuccess if inserted.
   * @retval kKeyExist otherwise.
   */
  auto
  Insert(  //
      const Key &key,
      const Payload &payload,
      [[maybe_unused]] const size_t key_len)  //
      -> ReturnCode
  {
    return bw_tree_.Insert(key, payload);
  }

  /**
   * @brief Update the record corresponding to a given key with a given payload.
   *
   * This function performs a uniqueness check in its processing. If there is a given
   * key in this tree, this function updates the corresponding record. If a given key
   * does not exist in this tree, this function does nothing and returns kKeyNotExist as
   * a return code.
   *
   * @param key a target key to be updated.
   * @param payload a payload for updating.
   * @retval kSuccess if updated.
   * @retval kKeyNotExist otherwise.
   */
  auto
  Update(  //
      const Key &key,
      const Payload &payload,
      [[maybe_unused]] const size_t key_len)  //
      -> ReturnCode
  {
    return bw_tree_.Update(key, payload);
  }

  /**
   * @brief Delete the record corresponding to a given key from this tree.
   *
   * This function performs a uniqueness check in its processing. If there is a given
   * key in this tree, this function deletes it. If a given key does not exist in this
   * tree, this function does nothing and returns kKeyNotExist as a return code.
   *
   * @param key a target key to be deleted.
   * @retval kSuccess if deleted.
   * @retval kKeyNotExist otherwise.
   */
  auto
  Delete(  //
      const Key &key,
      [[maybe_unused]] const size_t key_len)  //
      -> ReturnCode
  {
    return bw_tree_.Delete(key);
  }

  /*####################################################################################
   * Public bulkload API
   *##################################################################################*/

  /**
   * @brief Bulkload specified kay/payload pairs.
   *
   * This function bulkloads given entries into this index. The entries are assumed to
   * be given as a vector of pairs of Key and Payload. Note that keys in records are
   * assumed to be unique and sorted.
   *
   * @param entries vector of entries to be bulkloaded.
   * @param thread_num the number of threads to perform bulkloading.
   * @return kSuccess.
   */
  auto
  Bulkload(  //
      const std::vector<std::pair<Key, Payload>> &entries,
      const size_t thread_num = 1)  //
      -> ReturnCode
  {
    return bw_tree_.Bulkload(entries, thread_num);
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
