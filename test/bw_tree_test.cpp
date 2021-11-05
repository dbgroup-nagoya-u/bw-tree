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

#include "bw_tree/bw_tree.hpp"

#include <memory>
#include <vector>

#include "common.hpp"
#include "gtest/gtest.h"

namespace dbgroup::index::bw_tree::test
{
// use a supper template to define key-payload pair templates
template <class KeyType, class PayloadType, class KeyComparator, class PayloadComparator>
struct KeyPayload {
  using Key = KeyType;
  using Payload = PayloadType;
  using KeyComp = KeyComparator;
  using PayloadComp = PayloadComparator;
};

template <class KeyPayload>
class BwTreeFixture : public testing::Test
{
 protected:
  // extract key-payload types
  using Key = typename KeyPayload::Key;
  using Payload = typename KeyPayload::Payload;
  using KeyComp = typename KeyPayload::KeyComp;
  using PayloadComp = typename KeyPayload::PayloadComp;

  // define type aliases for simplicity
  using Node_t = component::Node<Key, std::less<Key>>;
  using Metadata = component::Metadata;
  using BwTree_t = BwTree<Key, Payload, KeyComp>;

  /*################################################################################################
   * Internal constants
   *##############################################################################################*/

  static constexpr size_t kKeyLength = GetDataLength<Key>();
  static constexpr size_t kPayloadLength = GetDataLength<Payload>();
  static constexpr size_t kRecordLength = kKeyLength + kPayloadLength;
  static constexpr size_t kMaxRecordNum =
      (kPageSize - component::kHeaderLength) / (kRecordLength + sizeof(Metadata));
  static constexpr size_t kKeyNumForTest = kMaxRecordNum * kMaxRecordNum + 1;
  static constexpr size_t kSmallKeyNum = kMaxDeltaNodeNum - 1;

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  // actual keys and payloads
  Key keys[kKeyNumForTest];
  Payload payloads[kKeyNumForTest];

  // a target node and its expected metadata
  std::unique_ptr<BwTree_t> bw_tree;

  /*################################################################################################
   * Setup/Teardown
   *##############################################################################################*/

  void
  SetUp() override
  {
    bw_tree = std::make_unique<BwTree_t>(1000);
    PrepareTestData(keys, kKeyNumForTest);
    PrepareTestData(payloads, kKeyNumForTest);
  }

  void
  TearDown() override
  {
    ReleaseTestData(keys, kKeyNumForTest);
    ReleaseTestData(payloads, kKeyNumForTest);
  }

  /*################################################################################################
   * Functions for verification
   *##############################################################################################*/

  void
  VerifyRead(  //
      const size_t key_id,
      const size_t expected_id,
      const bool expect_fail = false)
  {
    const auto [rc, actual] = bw_tree->Read(keys[key_id]);

    if (expect_fail) {
      EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
    } else {
      bool result;
      EXPECT_EQ(ReturnCode::kSuccess, rc);
      if constexpr (IsVariableLengthData<Payload>()) {
        result = component::IsEqual<Payload, PayloadComp>(payloads[expected_id], actual.get());
      } else {
        result = component::IsEqual<Payload, PayloadComp>(component::GetAddr(payloads[expected_id]),
                                                          component::GetAddr(actual));
      }
      EXPECT_TRUE(result);
    }
  }

  void
  VerifyWrite(  //
      const size_t key_id,
      const size_t payload_id)
  {
    auto rc = bw_tree->Write(keys[key_id], payloads[payload_id], kKeyLength, kPayloadLength);

    EXPECT_EQ(ReturnCode::kSuccess, rc);
  }

  void
  VerifyInsert(  //
      const size_t key_id,
      const size_t payload_id,
      const bool expect_fail = false)
  {
    auto rc = bw_tree->Insert(keys[key_id], payloads[payload_id], kKeyLength, kPayloadLength);
    if (expect_fail) {
      EXPECT_EQ(ReturnCode::kKeyExist, rc);
    } else {
      EXPECT_EQ(ReturnCode::kSuccess, rc);
    }
  }

  void
  VerifyUpdate(  //
      const size_t key_id,
      const size_t payload_id,
      const bool expect_fail = false)
  {
    auto rc = bw_tree->Update(keys[key_id], payloads[payload_id], kKeyLength, kPayloadLength);
    if (expect_fail) {
      EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
    } else {
      EXPECT_EQ(ReturnCode::kSuccess, rc);
    }
  }
};

/*##################################################################################################
 * Preparation for typed testing
 *################################################################################################*/

using KeyPayloadPairs = ::testing::Types<KeyPayload<uint64_t, uint64_t, UInt64Comp, UInt64Comp>,
                                         KeyPayload<char *, uint64_t, CStrComp, UInt64Comp>,
                                         KeyPayload<uint64_t, char *, UInt64Comp, CStrComp>,
                                         KeyPayload<char *, char *, CStrComp, CStrComp>,
                                         KeyPayload<uint32_t, uint64_t, UInt32Comp, UInt64Comp>,
                                         KeyPayload<uint32_t, uint32_t, UInt32Comp, UInt32Comp>,
                                         KeyPayload<uint64_t, uint64_t *, UInt64Comp, PtrComp>>;
TYPED_TEST_CASE(BwTreeFixture, KeyPayloadPairs);

/*##################################################################################################
 * Unit test definitions
 *################################################################################################*/

/*--------------------------------------------------------------------------------------------------
 * Read operation tests
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(BwTreeFixture, Read_EmptyIndex_ReadFail)
{  //
  TestFixture::VerifyRead(0, 0, true);
}

/*--------------------------------------------------------------------------------------------------
 * Insert operation tests
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(BwTreeFixture, Insert_UniqueKeys_ReadInsertedValues)
{
  const size_t repeat_num = TestFixture::kSmallKeyNum;

  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyInsert(i, i, false);
  }
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyRead(i, i);
  }
}

TYPED_TEST(BwTreeFixture, Insert_DuplicateKeys_ReadPreviousValue)
{
  const size_t repeat_num = TestFixture::kSmallKeyNum / 2;

  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyInsert(i, i, false);
  }
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyInsert(i, i + 1, true);
  }
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyRead(i, i);
  }
}

/*--------------------------------------------------------------------------------------------------
 * Update operation tests
 *------------------------------------------------------------------------------------------------*/
TYPED_TEST(BwTreeFixture, Update_ExistKeys_ReadUpdatedValues)
{
  const size_t repeat_num = TestFixture::kSmallKeyNum;

  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }

  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyUpdate(i, i + 1);
  }

  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyRead(i, i + 1);
  }
}

TYPED_TEST(BwTreeFixture, Update_ExistKeys_WithLeafConsolidate_ReadUpdatedValues)
{
  const size_t repeat_num = TestFixture::kMaxRecordNum / 2;

  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }

  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyUpdate(i, i + 1);
  }

  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyRead(i, i + 1);
  }
}

TYPED_TEST(BwTreeFixture, Update_ExistKeysRepeatedlyWithLeafConsolidate_ReadUpdatedValues)
{
  const size_t repeat_num = TestFixture::kMaxRecordNum;

  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i);
    TestFixture::VerifyUpdate(i, i + 1);
    TestFixture::VerifyUpdate(i, i + 2);
  }

  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyRead(i, i + 2);
  }
}

TYPED_TEST(BwTreeFixture, Update_ExistKeysRepeadtedlyWithLeafSplit_ReadUpdatedValues)
{
  const size_t repeat_num = TestFixture::kMaxRecordNum * 2;

  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i);
    TestFixture::VerifyUpdate(i, i + 1);
    TestFixture::VerifyUpdate(i, i + 2);
  }

  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyRead(i, i + 2);
  }
}

TYPED_TEST(BwTreeFixture, Update_NotExistKeys_UpdateFails)
{
  const size_t repeat_num = TestFixture::kSmallKeyNum / 2;

  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyUpdate(i, i, true);
  }
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyRead(i, i, true);
  }
}

/*--------------------------------------------------------------------------------------------------
 * Write operation tests
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(BwTreeFixture, Write_UniqueKeys_ReadWrittenValues)
{
  const size_t repeat_num = TestFixture::kSmallKeyNum;

  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyRead(i, i);
  }
}

TYPED_TEST(BwTreeFixture, Write_DuplicateKeys_ReadLatestValue)
{
  const size_t repeat_num = TestFixture::kSmallKeyNum / 2;

  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i + 1);
  }
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyRead(i, i + 1);
  }
}

TYPED_TEST(BwTreeFixture, Write_UniqueKeysWithLeafConsolidate_ReadWrittenValues)
{
  const size_t repeat_num = TestFixture::kMaxRecordNum;

  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyRead(i, i);
  }
}

TYPED_TEST(BwTreeFixture, Write_DuplicateKeysWithLeafConsolidate_ReadWrittenValues)
{
  const size_t repeat_num = TestFixture::kMaxRecordNum / 2;

  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i + 1);
  }
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyRead(i, i + 1);
  }
}

TYPED_TEST(BwTreeFixture, Write_UniqueKeysWithLeafSplit_ReadWrittenValues)
{
  const size_t repeat_num = TestFixture::kMaxRecordNum * 2;

  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyRead(i, i);
  }
}

TYPED_TEST(BwTreeFixture, Write_DuplicateKeysWithLeafSplit_ReadWrittenValues)
{
  const size_t repeat_num = TestFixture::kMaxRecordNum * 2;

  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i + 1);
  }
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyRead(i, i + 1);
  }
}

TYPED_TEST(BwTreeFixture, Write_UniqueKeysWithInternalConsolidation_ReadWrittenValues)
{
  const size_t repeat_num = TestFixture::kMaxRecordNum * TestFixture::kSmallKeyNum;

  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyRead(i, i);
  }
}

TYPED_TEST(BwTreeFixture, Write_DuplicateKeysWithInternalConsolidation_ReadWrittenValues)
{
  const size_t repeat_num = TestFixture::kMaxRecordNum * TestFixture::kSmallKeyNum;

  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i + 1);
  }
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyRead(i, i + 1);
  }
}

TYPED_TEST(BwTreeFixture, Write_UniqueKeysWithInternalSplit_ReadWrittenValues)
{
  const size_t repeat_num = TestFixture::kMaxRecordNum * TestFixture::kMaxRecordNum;

  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyRead(i, i);
  }
}

TYPED_TEST(BwTreeFixture, Write_DuplicateKeysWithInternalSplit_ReadWrittenValues)
{
  const size_t repeat_num = TestFixture::kMaxRecordNum * TestFixture::kMaxRecordNum;

  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i + 1);
  }
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyRead(i, i + 1);
  }
}

}  // namespace dbgroup::index::bw_tree::test
