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

#include "gtest/gtest.h"

namespace dbgroup::index::bw_tree::component::test
{
class MetadataFixture : public testing::Test
{
 protected:
  /*################################################################################################
   * Internal constants
   *##############################################################################################*/

  static constexpr size_t kExpectedOffset = 256;
  static constexpr size_t kExpectedKeyLength = 8;
  static constexpr size_t kExpectedTotalLength = 16;

  /*################################################################################################
   * Setup/Teardown
   *##############################################################################################*/

  Metadata meta;

  void
  SetUp() override
  {
    meta = Metadata{kExpectedOffset, kExpectedKeyLength, kExpectedTotalLength};
  }

  void
  TearDown() override
  {
  }
};

/*--------------------------------------------------------------------------------------------------
 * Constructor tests
 *------------------------------------------------------------------------------------------------*/

TEST_F(MetadataFixture, Construct_DefaultMetadata_CorrectlyInitialized)
{
  EXPECT_EQ(kExpectedOffset, meta.GetOffset());
  EXPECT_EQ(kExpectedKeyLength, meta.GetKeyLength());
  EXPECT_EQ(kExpectedTotalLength, meta.GetTotalLength());
}

/*--------------------------------------------------------------------------------------------------
 * Getter tests
 *------------------------------------------------------------------------------------------------*/

TEST_F(MetadataFixture, GetPayloadLength_DefaultMetadata_ReturnCorrectPayloadLength)
{
  EXPECT_EQ(kExpectedTotalLength - kExpectedKeyLength, meta.GetPayloadLength());
}

/*--------------------------------------------------------------------------------------------------
 * Utility tests
 *------------------------------------------------------------------------------------------------*/

TEST_F(MetadataFixture, UpdateOffset_DefaultMetadata_GetUpdatedOffset)
{
  const size_t updated_offset = kExpectedOffset / 2;

  meta = meta.UpdateOffset(updated_offset);

  EXPECT_EQ(updated_offset, meta.GetOffset());
}

}  // namespace dbgroup::index::bw_tree::component::test
