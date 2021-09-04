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

#include <array>
#include <atomic>
#include <mutex>
#include <vector>

#include "node.hpp"

namespace dbgroup::index::bw_tree::component
{
/**
 * @brief
 *
 * @tparam Key a target key class.
 * @tparam Payload a target payload class.
 * @tparam Compare a comparetor class for keys.
 */
template <class Key, class Payload, class Compare>
class MappingTable
{
  using Node_t = Node<Key, Payload, Compare>;
  using Mapping_t = std::atomic<Node_t *>;

 public:
  /*################################################################################################
   * Public constants
   *##############################################################################################*/

  static constexpr size_t kDefaultTableCapacity =
      (kPageSize - sizeof(std::atomic_size_t)) / sizeof(Mapping_t);

 private:
  /*################################################################################################
   * Internal classes
   *##############################################################################################*/

  class BufferedMap
  {
   private:
    /*##############################################################################################
     * Internal member variables
     *############################################################################################*/

    std::atomic_size_t head_id_;

    std::array<Mapping_t, kDefaultTableCapacity> logical_ids_;

   public:
    /*##############################################################################################
     * Public constructors/destructors
     *############################################################################################*/

    BufferedMap() { static_assert(sizeof(BufferedMap) == kPageSize); }

    ~BufferedMap()
    {
      const size_t size = head_id_.load(mo_relax);
      for (size_t i = 0; i < size; ++i) {
        auto node = logical_ids_[i].load(mo_relax);
        if (node != nullptr) ::dbgroup::memory::Delete(node);
      }
    }

    /*##############################################################################################
     * Public getters/setters
     *############################################################################################*/

    Mapping_t *
    ReserveNewID()
    {
      auto current_id = head_id_.load(mo_relax);
      do {
        // if the buffer is full, return NULL
        if (current_id >= kDefaultTableCapacity) return nullptr;
      } while (!head_id_.compare_exchange_weak(current_id, current_id + 1, mo_relax));

      return &logical_ids_[current_id];
    }
  };

  /*################################################################################################
   * Internal variables
   *##############################################################################################*/

  std::atomic<BufferedMap *> table_;

  std::vector<BufferedMap *> old_tables_;

  std::mutex mtx_;

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  static BufferedMap *
  CreateNewTable()
  {
    return ::dbgroup::memory::CallocNew<BufferedMap>(kPageSize);
  }

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  MappingTable() : table_{CreateNewTable()} {}

  ~MappingTable()
  {
    for (auto &&table : old_tables_) {
      ::dbgroup::memory::Delete(table);
    }
  }

  MappingTable(const MappingTable &) = delete;
  MappingTable &operator=(const MappingTable &) = delete;
  MappingTable(MappingTable &&) = delete;
  MappingTable &operator=(MappingTable &&) = delete;

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  Mapping_t *
  GetNewLogicalID()
  {
    auto current_table = table_.load(mo_relax);
    auto new_id = current_table->ReserveNewID();
    while (new_id == nullptr) {
      auto new_table = CreateNewTable();
      if (table_.compare_exchange_weak(current_table, new_table, mo_relax)) {
        // since install succeeds, get new ID from the new table
        new_id = new_table->ReserveNewID();

        // retain the old table to release nodes in it
        const auto guard = std::unique_lock<std::mutex>(mtx_);  // use lock for simplicity
        old_tables_.emplace_back(current_table);
      } else {
        // since another thread may install a new mapping table, recheck a current table
        ::dbgroup::memory::Delete(new_table);
        new_id = current_table->ReserveNewID();
      }
    }
    return new_id;
  }
};

}  // namespace dbgroup::index::bw_tree::component
