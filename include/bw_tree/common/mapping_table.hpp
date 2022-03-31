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

#ifndef BW_TREE_COMMON_MAPPING_TABLE_HPP
#define BW_TREE_COMMON_MAPPING_TABLE_HPP

#include <array>
#include <atomic>
#include <mutex>
#include <vector>

#include "logical_id.hpp"

namespace dbgroup::index::bw_tree::component
{
/**
 * @brief A class for managing locigal IDs.
 *
 * @tparam Node a class for representing base nodes.
 * @tparam Delta a class for representing delta records.
 */
template <class Node, class Delta>
class MappingTable
{
 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new MappingTable object.
   *
   */
  MappingTable() : table_{new BufferedMap{}}
  {
    tables_.reserve(kDefaultTableNum);
    tables_.emplace_back(table_.load(std::memory_order_relaxed));
  }

  MappingTable(const MappingTable &) = delete;
  MappingTable(MappingTable &&) = delete;

  auto operator=(const MappingTable &) -> MappingTable & = delete;
  auto operator=(MappingTable &&) -> MappingTable & = delete;

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
    for (auto &&table : tables_) {
      delete table;
    }
  }

  /*####################################################################################
   * Public getters/setters
   *##################################################################################*/

  /**
   * @brief Get a new logical ID.
   *
   * @return a reserved logical ID.
   */
  auto
  GetNewLogicalID()  //
      -> LogicalID *
  {
    auto *current_table = table_.load(std::memory_order_relaxed);
    auto *new_id = current_table->ReserveNewID();
    while (new_id == nullptr) {
      std::unique_lock guard{full_tables_mtx_, std::defer_lock};
      if (guard.try_lock()) {
        auto *new_table = new BufferedMap{};
        table_.store(new_table, std::memory_order_relaxed);
        new_id = new_table->ReserveNewID();

        // retain the new table to release nodes in it
        tables_.emplace_back(new_table);
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
   * @brief An internal class for representing an actual mapping table.
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
    constexpr BufferedMap() = default;

    BufferedMap(const BufferedMap &) = delete;
    BufferedMap(BufferedMap &&) = delete;

    auto operator=(const BufferedMap &) -> BufferedMap & = delete;
    auto operator=(BufferedMap &&) -> BufferedMap & = delete;

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
      auto size = head_pos_.load(std::memory_order_relaxed);
      if (size > kMappingTableCapacity) {
        size = kMappingTableCapacity;
      }

      for (size_t i = 0; i < size; ++i) {
        auto *rec = logical_ids_[i].template Load<Delta *>();
        if (rec == nullptr) continue;

        // delete delta records
        while (!rec->IsBaseNode()) {
          auto *next = rec->GetNext();
          delete rec;
          rec = next;
        }

        // delete a base node
        auto *node = reinterpret_cast<Node *>(rec);
        delete node;
      }
    }

    /*##################################################################################
     * Public getters/setters
     *################################################################################*/

    /**
     * @brief Reserve a new logical ID.
     *
     * If this function returns nullptr, it means that this table is full.
     *
     * @return a reserved logical ID.
     */
    auto
    ReserveNewID()  //
        -> LogicalID *
    {
      const auto current_id = head_pos_.fetch_add(1, std::memory_order_relaxed);

      if (current_id >= kMappingTableCapacity) return nullptr;
      return &logical_ids_[current_id];
    }

   private:
    /*##################################################################################
     * Internal member variables
     *################################################################################*/

    /// the current head position of this table (i.e., the total number of reserved IDs).
    std::atomic_size_t head_pos_{0};

    /// an actual mapping table.
    std::array<LogicalID, kMappingTableCapacity> logical_ids_{};
  };

  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  /// the default number of mapping tables.
  static constexpr size_t kDefaultTableNum = 128;

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// a current mapping table.
  std::atomic<BufferedMap *> table_{};

  /// full mapping tables.
  std::vector<BufferedMap *> tables_{};

  /// a mutex object to modify tables_.
  std::mutex full_tables_mtx_{};
};

}  // namespace dbgroup::index::bw_tree::component

#endif  // BW_TREE_COMMON_MAPPING_TABLE_HPP
