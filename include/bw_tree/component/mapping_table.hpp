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
 * @tparam Compare a comparetor class for keys.
 */
template <class Key, class Compare>
class MappingTable
{
  using Node_t = Node<Key, Compare>;
  using Mapping_t = std::atomic<Node_t *>;

 public:
  /*################################################################################################
   * Public constants
   *##############################################################################################*/

  /// capacity of mapping information that can be maintained in each table.
  static constexpr size_t kDefaultTableCapacity =
      (kPageSize - sizeof(std::atomic_size_t)) / sizeof(Mapping_t);

 private:
  /*################################################################################################
   * Internal classes
   *##############################################################################################*/

  /**
   * @brief An internal class to represent a certain mapping table.
   *
   */
  class BufferedMap
  {
   private:
    /*##############################################################################################
     * Internal member variables
     *############################################################################################*/

    /// the current head ID of this table (the total number of reserved IDs).
    std::atomic_size_t head_id_;

    /// an actual mapping table.
    std::array<Mapping_t, kDefaultTableCapacity> logical_ids_;

   public:
    /*##############################################################################################
     * Public constructors/destructors
     *############################################################################################*/

    /**
     * @brief Construct a new mapping buffer instance.
     *
     */
    BufferedMap() { static_assert(sizeof(BufferedMap) == kPageSize); }

    /**
     * @brief Destroy the object.
     *
     * This dectructor will delete all the nodes in this table.
     */
    ~BufferedMap()
    {
      const size_t size = head_id_.load(mo_relax);
      for (size_t i = 0; i < size; ++i) {
        Node_t *node = logical_ids_[i].load(mo_relax);
        if (node == nullptr) continue;

        ::dbgroup::memory::Delete(node);
      }
    }

    /*##############################################################################################
     * new/delete definitions
     *############################################################################################*/

    static void *operator new(std::size_t) { return calloc(1UL, kPageSize); }

    static void
    operator delete(void *p) noexcept
    {
      free(p);
    }

    /*##############################################################################################
     * Public getters/setters
     *############################################################################################*/

    /**
     * @brief Reserve a new logical ID (i.e., the pointer to a mapping record).
     *
     * If this function returns nullptr, it means that this table is full.
     *
     * @return Mapping_t*: a reserved logical ID.
     */
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

  /// a current mapping table.
  std::atomic<BufferedMap *> table_;

  /// full mapping tables.
  std::vector<BufferedMap *> full_tables_;

  /// a mutex object to modify full_tables_.
  std::mutex full_tables_mtx_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  /**
   * @brief Construct a new MappingTable object.
   *
   */
  MappingTable()
  {
    const auto table = new BufferedMap{};
    table_.store(table, mo_relax);
    full_tables_.emplace_back(table);
  }

  /**
   * @brief Destroy the MappingTable object.
   *
   * This destructor will deletes all the actual pages of a Bw-tree.
   */
  ~MappingTable()
  {
    for (auto &&table : full_tables_) {
      delete table;
    }
  }

  MappingTable(const MappingTable &) = delete;
  MappingTable &operator=(const MappingTable &) = delete;
  MappingTable(MappingTable &&) = delete;
  MappingTable &operator=(MappingTable &&) = delete;

  /*################################################################################################
   * Public getters/setters
   *##############################################################################################*/

  /**
   * @brief Get a new logical ID (i.e., an address to mapping information).
   *
   * @return Mapping_t*: a reserved logical ID.
   */
  Mapping_t *
  GetNewLogicalID()
  {
    auto current_table = table_.load(mo_relax);
    auto new_id = current_table->ReserveNewID();
    while (new_id == nullptr) {
      auto new_table = new BufferedMap{};
      if (table_.compare_exchange_weak(current_table, new_table, mo_relax)) {
        // since install succeeds, get new ID from the new table
        new_id = new_table->ReserveNewID();

        // retain the old table to release nodes in it
        const auto guard = std::unique_lock<std::mutex>(full_tables_mtx_);
        full_tables_.emplace_back(new_table);
      } else {
        // since another thread may install a new mapping table, recheck a current table
        delete new_table;
        new_id = current_table->ReserveNewID();
      }
    }
    return new_id;
  }
};

}  // namespace dbgroup::index::bw_tree::component
