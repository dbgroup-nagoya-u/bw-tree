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

#include "bw_tree/component/mapping_table.hpp"

#include <algorithm>
#include <future>
#include <memory>
#include <thread>

#include "common.hpp"
#include "gtest/gtest.h"

namespace dbgroup::index::bw_tree::component::test
{
class MappingTableFixture : public testing::Test
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using Key = uint64_t;
  using Payload = uint64_t;
  using MappingTable_t = MappingTable<Key, std::less<Key>>;
  using IDContainer = std::vector<std::atomic_uintptr_t *>;

 protected:
  /*####################################################################################
   * Setup/Teardown
   *##################################################################################*/

  void
  SetUp() override
  {
    table_ = std::make_unique<MappingTable_t>();
  }

  void
  TearDown() override
  {
  }

  /*####################################################################################
   * Utility functions
   *##################################################################################*/

  auto
  GetLogicalIDs(const size_t id_num)  //
      -> IDContainer
  {
    IDContainer ids{};
    ids.reserve(id_num);

    for (size_t i = 0; i < id_num; ++i) {
      ids.emplace_back(table_->GetNewLogicalID());
    }

    return ids;
  }

  auto
  GetLogicalIDsWithMultiThreads(const size_t id_num)  //
      -> IDContainer
  {
    // lambda function to run tests with multi-threads
    auto f = [&](std::promise<IDContainer> p, const size_t id_num) {
      auto &&ids = GetLogicalIDs(id_num);
      p.set_value(std::move(ids));
    };

    // run GetNewLogicalGetID with multi-threads
    std::vector<std::future<IDContainer>> futures{};
    futures.reserve(kThreadNum);
    for (size_t i = 0; i < kThreadNum; ++i) {
      std::promise<IDContainer> p;
      futures.emplace_back(p.get_future());
      std::thread{f, std::move(p), id_num}.detach();
    }

    // gather results
    IDContainer ids{};
    ids.reserve(id_num * kThreadNum);
    for (auto &&future : futures) {
      auto &&ids_per_thread = future.get();
      ids.insert(ids.end(), ids_per_thread.begin(), ids_per_thread.end());
    }

    return ids;
  }

  /*####################################################################################
   * Functions for verification
   *##################################################################################*/

  void
  VerifyLogicalIDs(IDContainer &ids)
  {
    std::sort(ids.begin(), ids.end());
    auto &&actual_end = std::unique(ids.begin(), ids.end());

    EXPECT_EQ(ids.end(), actual_end);
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  std::unique_ptr<MappingTable_t> table_{nullptr};
};

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

/*--------------------------------------------------------------------------------------
 * Utility tests
 *------------------------------------------------------------------------------------*/

TEST_F(MappingTableFixture, GetNewLogicalIDWithAFewIDsGetUniqueIDs)
{
  auto &&ids = GetLogicalIDs(kMappingTableCapacity - 1);
  VerifyLogicalIDs(ids);
}

TEST_F(MappingTableFixture, GetNewLogicalIDWithManyIDsGetUniqueIDs)
{
  auto &&ids = GetLogicalIDs(kMappingTableCapacity * 10);
  VerifyLogicalIDs(ids);
}

TEST_F(MappingTableFixture, GetNewLogicalIDWithAFewIDsByMultiThreadsGetUniqueIDs)
{
  auto &&ids = GetLogicalIDsWithMultiThreads((kMappingTableCapacity / kThreadNum) - 1);
  VerifyLogicalIDs(ids);
}

TEST_F(MappingTableFixture, GetNewLogicalIDWighManyIDsByMultiThreadsGetUniqueIDs)
{
  auto &&ids = GetLogicalIDsWithMultiThreads(kMappingTableCapacity * 10);
  VerifyLogicalIDs(ids);
}

}  // namespace dbgroup::index::bw_tree::component::test
