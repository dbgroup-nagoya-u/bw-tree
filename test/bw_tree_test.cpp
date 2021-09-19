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

  static constexpr size_t kKeyNumForTest = 40960;
  static constexpr size_t kSmallKeyNum = kMaxDeltaNodeNum - 1;
  static constexpr size_t kLargeKeyNum = kKeyNumForTest - 1;
  static constexpr size_t kKeyLength = kWordLength;
  static constexpr size_t kPayloadLength = kWordLength;

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  // actual keys and payloads
  size_t key_length;
  size_t payload_length;
  Key keys[kKeyNumForTest];
  Payload payloads[kKeyNumForTest];

  // the length of a record and its maximum number
  size_t record_length;
  size_t max_record_num;

  // a target node and its expected metadata
  std::unique_ptr<BwTree_t> bw_tree;

  /*################################################################################################
   * Setup/Teardown
   *##############################################################################################*/

  void
  SetUp() override
  {
    // prepare keys
    key_length = (IsVariableLengthData<Key>()) ? 7 : sizeof(Key);
    PrepareTestData(keys, kKeyNumForTest, key_length);

    // prepare payloads
    payload_length = (IsVariableLengthData<Payload>()) ? 7 : sizeof(Payload);
    PrepareTestData(payloads, kKeyNumForTest, payload_length);

    // set a record length and its maximum number
    record_length = key_length + payload_length;
    max_record_num = (kPageSize - component::kHeaderLength) / (record_length + sizeof(Metadata));
    if (max_record_num >= kKeyNumForTest) max_record_num = kKeyNumForTest - 1;

    bw_tree = std::make_unique<BwTree_t>(1000);
  }

  void
  TearDown() override
  {
    ReleaseTestData(keys, kKeyNumForTest);
    ReleaseTestData(payloads, kKeyNumForTest);
  }

  /*################################################################################################
   * Utility functions
   *##############################################################################################*/

  size_t
  GetMaxRecordNumInPage() const
  {
    return max_record_num;
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
      EXPECT_EQ(ReturnCode::kSuccess, rc);
      if constexpr (IsVariableLengthData<Payload>()) {
        EXPECT_TRUE(component::IsEqual<PayloadComp>(payloads[expected_id], actual.get()));
      } else {
        EXPECT_TRUE(component::IsEqual<PayloadComp>(payloads[expected_id], actual));
      }
    }
  }

  void
  VerifyWrite(  //
      const size_t key_id,
      const size_t payload_id)
  {
    auto rc = bw_tree->Write(keys[key_id], payloads[payload_id], key_length, payload_length);

    EXPECT_EQ(ReturnCode::kSuccess, rc);
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

TYPED_TEST(BwTreeFixture, Write_UniqueKeysWithConsolidate_ReadWrittenValues)
{
  const size_t repeat_num = TestFixture::GetMaxRecordNumInPage();

  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyRead(i, i);
  }
}

TYPED_TEST(BwTreeFixture, Write_DuplicateKeysWithConsolidate_ReadWrittenValues)
{
  const size_t repeat_num = TestFixture::GetMaxRecordNumInPage() / 2;

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

TYPED_TEST(BwTreeFixture, Write_UniqueKeysWithSplit_ReadWrittenValues)
{
  const size_t repeat_num = TestFixture::GetMaxRecordNumInPage() * 2;

  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyRead(i, i);
  }
}

}  // namespace dbgroup::index::bw_tree::test
