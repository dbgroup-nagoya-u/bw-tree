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

#include "bw_tree/component/metadata.hpp"

#include "common.hpp"
#include "gtest/gtest.h"

namespace dbgroup::index::bw_tree::component::test
{
/*######################################################################################
 * Global constants
 *####################################################################################*/

constexpr size_t kExpectedOffset = 256;
constexpr size_t kExpectedKeyLength = 8;
constexpr size_t kExpectedTotalLength = 16;
constexpr size_t kExpectedPayloadLength = kExpectedTotalLength - kExpectedKeyLength;

class MetadataFixture : public testing::Test
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

  Metadata meta_{kExpectedOffset, kExpectedKeyLength, kExpectedTotalLength};
};

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

/*--------------------------------------------------------------------------------------
 * Constructor tests
 *------------------------------------------------------------------------------------*/

TEST_F(MetadataFixture, ConstructWithArgumentsCreateExpectedMetadata)
{
  EXPECT_EQ(kExpectedOffset, meta_.GetOffset());
  EXPECT_EQ(kExpectedKeyLength, meta_.GetKeyLength());
  EXPECT_EQ(kExpectedPayloadLength, meta_.GetPayloadLength());
  EXPECT_EQ(kExpectedTotalLength, meta_.GetTotalLength());
}

/*--------------------------------------------------------------------------------------
 * Getter/setter tests
 *------------------------------------------------------------------------------------*/

TEST_F(MetadataFixture, SetOffsetUpdateOffsetWOSideEffects)
{
  const size_t updated_offset = kExpectedOffset / 2;

  meta_.SetOffset(updated_offset);

  EXPECT_EQ(updated_offset, meta_.GetOffset());
  EXPECT_EQ(kExpectedKeyLength, meta_.GetKeyLength());
  EXPECT_EQ(kExpectedPayloadLength, meta_.GetPayloadLength());
  EXPECT_EQ(kExpectedTotalLength, meta_.GetTotalLength());
}

}  // namespace dbgroup::index::bw_tree::component::test
