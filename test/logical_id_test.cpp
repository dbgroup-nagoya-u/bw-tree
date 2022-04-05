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

#include "bw_tree/component/logical_id.hpp"

#include "common.hpp"
#include "gtest/gtest.h"

namespace dbgroup::index::bw_tree::component::test
{

/*######################################################################################
 * Global constants
 *####################################################################################*/

constexpr uintptr_t kDummyPtr = 128;

/*######################################################################################
 * Fixture class definition
 *####################################################################################*/

class LogicalIDFixture : public testing::Test
{
 protected:
  /*####################################################################################
   * Setup/Teardown
   *##################################################################################*/

  void
  SetUp() override
  {
  }

  void
  TearDown() override
  {
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  LogicalID lid_{};
};

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

/*--------------------------------------------------------------------------------------
 * Constructor tests
 *------------------------------------------------------------------------------------*/

TEST_F(LogicalIDFixture, InitialLogicalIDHasNULL)
{  //
  EXPECT_EQ(kNullPtr, lid_.Load<uintptr_t>());
}

/*--------------------------------------------------------------------------------------
 * Utility tests
 *------------------------------------------------------------------------------------*/

TEST_F(LogicalIDFixture, LoadStoredValue)
{
  lid_.Store(kDummyPtr);
  EXPECT_EQ(kDummyPtr, lid_.Load<uintptr_t>());
}

TEST_F(LogicalIDFixture, ClearRemoveStoredValue)
{
  lid_.Store(kDummyPtr);
  lid_.Clear();
  EXPECT_EQ(kNullPtr, lid_.Load<uintptr_t>());
}

TEST_F(LogicalIDFixture, CASWeakStoreDesiredValueIfExpectedOneStored)
{
  EXPECT_TRUE(lid_.CASWeak(kNullPtr, kDummyPtr));
  EXPECT_EQ(kDummyPtr, lid_.Load<uintptr_t>());
}

TEST_F(LogicalIDFixture, CASWeakFailIfUnexpectedValueStored)
{
  EXPECT_FALSE(lid_.CASWeak(kDummyPtr, kNullPtr));
}

TEST_F(LogicalIDFixture, CASStrongStoreDesiredValueIfExpectedOneStored)
{
  EXPECT_TRUE(lid_.CASStrong(kNullPtr, kDummyPtr));
  EXPECT_EQ(kDummyPtr, lid_.Load<uintptr_t>());
}

TEST_F(LogicalIDFixture, CASStrongFailIfUnexpectedValueStored)
{
  EXPECT_FALSE(lid_.CASStrong(kDummyPtr, kNullPtr));
}

}  // namespace dbgroup::index::bw_tree::component::test
