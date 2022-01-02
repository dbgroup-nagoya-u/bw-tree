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

#ifndef BW_TREE_COMPONENT_MAPPING_TABLE_HPP
#define BW_TREE_COMPONENT_MAPPING_TABLE_HPP

#include <array>
#include <atomic>
#include <mutex>
#include <vector>

#include "delta_record.hpp"
#include "node.hpp"

namespace dbgroup::index::bw_tree::component
{
/**
 * @brief
 *
 * @tparam Key a target key class.
 * @tparam Comp a comparetor class for keys.
 */
template <class Key, class Comp>
class MappingTable
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using Node_t = Node<Key, Comp>;
  using DeltaRecord_t = DeltaRecord<Key, Comp>;

 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new MappingTable object.
   *
   */
  MappingTable()
  {
    auto *table = new BufferedMap{};
    table_.store(table, std::memory_order_relaxed);

    std::unique_lock guard{full_tables_mtx_};
    full_tables_.reserve(kExpectedTableNum);
    full_tables_.emplace_back(table);
  }

  MappingTable(const MappingTable &) = delete;
  MappingTable &operator=(const MappingTable &) = delete;
  MappingTable(MappingTable &&) = delete;
  MappingTable &operator=(MappingTable &&) = delete;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

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

  /*####################################################################################
   * Public getters/setters
   *##################################################################################*/

  /**
   * @brief Get a new logical ID (i.e., an address to mapping information).
   *
   * @return std::atomic_uintptr_t*: a reserved logical ID.
   */
  auto
  GetNewLogicalID()  //
      -> std::atomic_uintptr_t *
  {
    auto *current_table = table_.load(std::memory_order_relaxed);
    auto new_id = current_table->ReserveNewID();
    while (new_id == nullptr) {
      std::unique_lock guard{full_tables_mtx_, std::defer_lock};
      if (guard.try_lock()) {
        auto *new_table = new BufferedMap{};
        table_.store(new_table, std::memory_order_relaxed);
        new_id = new_table->ReserveNewID();

        // retain the old table to release nodes in it
        full_tables_.emplace_back(new_table);
      } else {
        // since another thread may install a new mapping table, recheck a current table
        current_table = table_.load(std::memory_order_relaxed);
        new_id = current_table->ReserveNewID();
      }
    }
    return new_id;
  }

 private:
  /*####################################################################################
   * Internal classes
   *##################################################################################*/

  /**
   * @brief An internal class to represent a certain mapping table.
   *
   */
  class BufferedMap
  {
   public:
    /*##################################################################################
     * Public constructors and assignment operators
     *################################################################################*/

    /**
     * @brief Construct a new mapping buffer instance.
     *
     */
    BufferedMap()
    {
      for (size_t i = 0; i < kMappingTableCapacity; ++i) {
        auto *elem_ptr = reinterpret_cast<uintptr_t *>(&logical_ids_[i]);
        *elem_ptr = kNullPtr;
      }
    }

    BufferedMap(const BufferedMap &) = delete;
    BufferedMap &operator=(const BufferedMap &) = delete;
    BufferedMap(BufferedMap &&) = delete;
    BufferedMap &operator=(BufferedMap &&) = delete;

    /*##################################################################################
     * Public destructors
     *################################################################################*/

    /**
     * @brief Destroy the object.
     *
     * This dectructor will delete all the nodes in this table.
     */
    ~BufferedMap()
    {
      auto size = head_id_.load(std::memory_order_relaxed);
      if (size > kMappingTableCapacity) {
        size = kMappingTableCapacity;
      }

      for (size_t i = 0; i < size; ++i) {
        auto ptr = logical_ids_[i].load(std::memory_order_acquire);
        if (ptr == kNullPtr) continue;

        // delete delta records
        auto *rec = reinterpret_cast<DeltaRecord_t *>(ptr);
        while (!rec->IsBaseNode()) {
          ptr = rec->GetNext();
          delete rec;
          rec = reinterpret_cast<DeltaRecord_t *>(ptr);
        }

        // delete a base node
        auto *node = reinterpret_cast<Node_t *>(ptr);
        delete node;
      }
    }

    /*##################################################################################
     * Public getters/setters
     *################################################################################*/

    /**
     * @brief Reserve a new logical ID (i.e., the pointer to a mapping record).
     *
     * If this function returns nullptr, it means that this table is full.
     *
     * @return std::atomic_uintptr_t*: a reserved logical ID.
     */
    auto
    ReserveNewID()  //
        -> std::atomic_uintptr_t *
    {
      auto current_id = head_id_.fetch_add(1, std::memory_order_relaxed);

      if (current_id >= kMappingTableCapacity) return nullptr;
      return &logical_ids_[current_id];
    }

   private:
    /*##################################################################################
     * Internal member variables
     *################################################################################*/

    /// the current head ID of this table (the total number of reserved IDs).
    std::atomic_size_t head_id_{0};

    /// an actual mapping table.
    std::array<std::atomic_uintptr_t, kMappingTableCapacity> logical_ids_{};
  };

  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  static constexpr size_t kExpectedTableNum = 128;

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// a current mapping table.
  std::atomic<BufferedMap *> table_{};

  /// full mapping tables.
  std::vector<BufferedMap *> full_tables_{};

  /// a mutex object to modify full_tables_.
  std::mutex full_tables_mtx_{};
};

}  // namespace dbgroup::index::bw_tree::component

#endif  // BW_TREE_COMPONENT_MAPPING_TABLE_HPP
