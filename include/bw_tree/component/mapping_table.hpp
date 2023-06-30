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

// C++ standard libraries
#include <array>
#include <atomic>
#include <mutex>
#include <vector>

// local sources
#include "bw_tree/component/logical_ptr.hpp"

namespace dbgroup::index::bw_tree::component
{
/**
 * @brief A class for managing locigal IDs.
 *
 * @tparam Node a class for representing base nodes.
 * @tparam Delta a class for representing delta records.
 */
template <class Node, class Delta>
class alignas(kVMPageSize) MappingTable
{
 public:
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using Row = LogicalPtr *;
  using Table = std::atomic<Row> *;

  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new MappingTable object.
   *
   */
  MappingTable()
  {
    auto *row = AllocVMAlignedPage<Row>(kVMPageSize);
    auto *table = AllocVMAlignedPage<Table>(kVMPageSize);
    table[0].store(row, std::memory_order_relaxed);
    tables_[0].store(table, std::memory_order_relaxed);
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
    const auto cur_id = cnt_.load(std::memory_order_relaxed);
    const auto table_id = (cur_id >> kTabShift) & kIDMask;
    for (size_t i = 0; i < table_id; ++i) {
      const auto is_last = (i + 1) == table_id;
      const auto row_num = is_last ? ((cur_id >> kRowShift) & kIDMask) : kArrayCapacity;
      const auto col_num = is_last ? (cur_id & kIDMask) : kArrayCapacity;

      auto *table = tables_[i].load(std::memory_order_relaxed);
      for (size_t j = 0; j < row_num; ++j) {
        auto *row = table[j].load(std::memory_order_relaxed);
        for (size_t k = 0; k < col_num; ++k) {
          ReleaseLogicalPtr(row[k]);
        }
        ReleaseVMAlignedPage(row);
      }
      ReleaseVMAlignedPage(table);
    }
  }

  /*####################################################################################
   * Public getters/setters
   *##################################################################################*/

  /**
   * @brief Get a new page ID.
   *
   * @return a reserved page ID.
   */
  auto
  GetNewPageID()  //
      -> uint64_t
  {
    while (true) {
      auto cur_id = cnt_.load(std::memory_order_relaxed);
      assert(((cur_id >> kTabShift) & kIDMask) < kArrayCapacity);

      if ((cur_id & kIDMask) < kArrayCapacity) {
        // try reserving a new ID
        auto new_id = cur_id + kColIDUnit;
        if (cnt_.compare_exchange_weak(cur_id, new_id, std::memory_order_relaxed)) {
          // check the current row is full
          if ((new_id & kIDMask) < kArrayCapacity) return cur_id;

          // check the current table is full
          new_id += kRowIDUnit;
          const auto row_id = (new_id >> kRowShift) & kIDMask;
          if (row_id < kArrayCapacity) {
            // prepare a new row
            auto *row = AllocVMAlignedPage<Row>(kVMPageSize);
            auto *table = tables_[(new_id >> kTabShift) & kIDMask].load(std::memory_order_relaxed);
            table[row_id].store(row, std::memory_order_relaxed);
            cnt_.store(new_id & ~kIDMask, std::memory_order_relaxed);
            return cur_id;
          }

          // prepare a new table
          auto *row = AllocVMAlignedPage<Row>(kVMPageSize);
          auto *table = AllocVMAlignedPage<Table>(kVMPageSize);
          table[0].store(row, std::memory_order_relaxed);
          new_id += kTabIDUnit;
          tables_[(new_id >> kTabShift) & kIDMask].store(table, std::memory_order_relaxed);
          cnt_.store(new_id & ~(kTabIDUnit - 1UL), std::memory_order_relaxed);
          return cur_id;
        }
      }

      // wait for the other thread to prepare a new pointer array
      BW_TREE_SPINLOCK_HINT;
    }
  }

  /**
   * @param id a source page ID.
   * @return an address of a logical pointer.
   */
  [[nodiscard]] auto
  GetLogicalPtr(const uint64_t id) const  //
      -> LogicalPtr *
  {
    const auto *table = tables_[(id >> kTabShift) & kIDMask].load(std::memory_order_relaxed);
    const auto *row = table[(id >> kRowShift) & kIDMask].load(std::memory_order_relaxed);
    return const_cast<LogicalPtr *>(&(row[id & kIDMask]));
  }

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  /**
   * @brief Collect statistical data of this tree.
   *
   * @retval 1st: the number of nodes (always zero).
   * @retval 2nd: the actual usage in bytes.
   * @retval 3rd: the virtual usage in bytes.
   */
  auto
  CollectStatisticalData()  //
      -> std::tuple<size_t, size_t, size_t>
  {
    const auto id = cnt_.load(std::memory_order_relaxed);
    const auto tab_id = (id >> kTabShift) & kIDMask;
    const auto row_id = (id >> kRowShift) & kIDMask;
    const auto col_id = id & kIDMask;

    const auto reserved = (tab_id * (kArrayCapacity + 1) + row_id + 3) * kVMPageSize;
    const auto empty_num = 2 * kArrayCapacity + kTableCapacity - col_id - row_id - tab_id - 3;
    const auto used = reserved - empty_num * kWordSize;

    return {0, used, reserved};
  }

 private:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  /// the begin bit position of row IDs.
  static constexpr size_t kRowShift = 16;

  /// the begin bit position of table IDs.
  static constexpr size_t kTabShift = 32;

  /// the begin bit position for indicating null pointers.
  static constexpr size_t kMSBShift = 63;

  /// the unit value for incrementing column IDs.
  static constexpr uint64_t kColIDUnit = 1UL;

  /// the unit value for incrementing row IDs.
  static constexpr uint64_t kRowIDUnit = 1UL << kRowShift;

  /// the unit value for incrementing table IDs.
  static constexpr uint64_t kTabIDUnit = 1UL << kTabShift;

  /// a bit mask for extracting IDs.
  static constexpr uint64_t kIDMask = 0xFFFFUL;

  /// the capacity of each array (rows and columns).
  static constexpr size_t kArrayCapacity = kVMPageSize / kWordSize;

  /// the capacity of a table.
  static constexpr size_t kTableCapacity = (kVMPageSize - kCacheLineSize) / kWordSize;

  /*####################################################################################
   * Internal utilities
   *##################################################################################*/

  /**
   * @brief Release delta records and nodes in a given logical pointer.
   *
   * @param lid a target logical pointer.
   */
  void
  ReleaseLogicalPtr(LogicalPtr &lid)
  {
    auto *rec = lid.template Load<Delta *>();

    // delete delta records
    while (rec != nullptr && rec->GetDeltaType() != kNotDelta) {
      auto *next = rec->GetNext();
      if (rec->GetDeltaType() == kMerge) {
        DeleteAlignedPtr(rec->template GetPayload<void *>());
      }

      DeleteAlignedPtr(rec);
      rec = next;
    }
    DeleteAlignedPtr(rec);
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// an atomic counter for incrementing page IDs.
  std::atomic_uint64_t cnt_{1UL << kMSBShift};

  /// padding space for the cache line alignment.
  size_t padding_[(kCacheLineSize - kWordSize) / kWordSize]{};

  /// mapping tables.
  std::atomic<Table> tables_[kTableCapacity]{};
};

}  // namespace dbgroup::index::bw_tree::component

#endif  // BW_TREE_COMPONENT_MAPPING_TABLE_HPP
