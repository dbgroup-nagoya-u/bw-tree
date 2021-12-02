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
  using RecordIterator_t = component::RecordIterator<Key, Payload, KeyComp>;
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
  VerifyScan(  //
      const size_t begin_key_id,
      const bool begin_null,
      const bool begin_closed,
      const size_t end_key_id,
      const bool end_null,
      const bool end_closed,
      const std::vector<size_t> &expected_keys,
      const std::vector<size_t> &expected_payloads)
  {
    const Key *begin_key = nullptr, *end_key = nullptr;
    if (!begin_null) begin_key = &keys[begin_key_id];
    if (!end_null) end_key = &keys[end_key_id];

    RecordIterator_t iter = bw_tree->Scan(begin_key, begin_closed);
    size_t count = 0;

    for (; iter.HasNext(); ++iter, ++count) {
      const auto [key, payload] = *iter;
      bool key_comp_result = true, payload_comp_result = true;
      if (end_key != nullptr) {
        if (component::LT<Key, KeyComp>(*end_key, key)
            || ((!component::LT<Key, KeyComp>(key, *end_key))
                && (!end_closed)))
          break;
      }

      if constexpr (IsVariableLengthData<Key>()) {
        key_comp_result = component::IsEqual<Key, KeyComp>(keys[expected_keys[count]], key);
      } else {
        key_comp_result = component::IsEqual<Key, KeyComp>(
            component::GetAddr(keys[expected_keys[count]]), component::GetAddr(key));
      }

      if constexpr (IsVariableLengthData<Payload>()) {
        payload_comp_result =
            component::IsEqual<Payload, PayloadComp>(payloads[expected_payloads[count]], payload);
      } else {
        payload_comp_result = component::IsEqual<Payload, PayloadComp>(
            component::GetAddr(payloads[expected_payloads[count]]), component::GetAddr(payload));
      }

      EXPECT_TRUE(key_comp_result);
      EXPECT_TRUE(payload_comp_result);
    }
    EXPECT_EQ(expected_keys.size(), count);

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
TYPED_TEST(BwTreeFixture, Insert_UniqueKeysWithLeafConsolidate_ReadWrittenValues)
{
  const size_t repeat_num = TestFixture::kMaxRecordNum;

  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyInsert(i, i);
  }
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyRead(i, i);
  }
}

TYPED_TEST(BwTreeFixture, Insert_UniqueKeysWithLeafSplit_ReadWrittenValues)
{
  const size_t repeat_num = TestFixture::kMaxRecordNum * 2;

  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyInsert(i, i);
  }
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyRead(i, i);
  }
}

TYPED_TEST(BwTreeFixture, Insert_DuplicateKeys_InsertFails)
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

TYPED_TEST(BwTreeFixture, Update_ExistKeysWithLeafConsolidate_ReadUpdatedValues)
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

/*--------------------------------------------------------------------------------------------------
 * Scan operation tests
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(BwTreeFixture, Scan_EmptyNode_ScanEmptyPage)
{  //
  std::vector<size_t> expected_ids;
  TestFixture::VerifyScan(0, true, true, 0, true, true, expected_ids, expected_ids);
}

TYPED_TEST(BwTreeFixture, Scan_UniqueKeys_ScanInsertedRecords)
{  //
  std::vector<size_t> expected_ids;
  for (size_t i = 0; i < TestFixture::kMaxRecordNum; ++i) {
    TestFixture::VerifyWrite(i, i);
    expected_ids.emplace_back(i);
  }

  TestFixture::VerifyScan(0, true, true, 0, true, true, expected_ids, expected_ids);
}

TYPED_TEST(BwTreeFixture, Scan_DuplicateKeys_ScanUpdatedRecords)
{  //
  const size_t repeat_num = TestFixture::kSmallKeyNum / 2;

  std::vector<size_t> expected_keys;
  std::vector<size_t> expected_payloads;
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyUpdate(i, i + 1);
    expected_keys.emplace_back(i);
    expected_payloads.emplace_back(i + 1);
  }

  TestFixture::VerifyScan(0, true, true, 0, true, true, expected_keys, expected_payloads);
}

TYPED_TEST(BwTreeFixture, Scan_LeftOpened_ScanInRangeRecords)
{  //
  const size_t repeat_num = TestFixture::kMaxRecordNum;
  const size_t half_num = repeat_num / 2;
  std::vector<size_t> expected_ids;
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i);
    if (i > half_num) expected_ids.emplace_back(i);
  }

  TestFixture::VerifyScan(half_num, false, false, 0, true, true, expected_ids, expected_ids);
}

TYPED_TEST(BwTreeFixture, Scan_LeftClosed_ScanInRangeRecords)
{  //
  const size_t repeat_num = TestFixture::kMaxRecordNum;
  const size_t half_num = repeat_num / 2;
  std::vector<size_t> expected_ids;
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i);
    if (i >= half_num) expected_ids.emplace_back(i);
  }

  TestFixture::VerifyScan(half_num, false, true, 0, true, true, expected_ids, expected_ids);
}

TYPED_TEST(BwTreeFixture, Scan_RightOpened_ScanInRangeRecords)
{  //
  const size_t repeat_num = TestFixture::kMaxRecordNum;
  const size_t half_num = TestFixture::kMaxRecordNum / 2;

  std::vector<size_t> expected_ids;
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i);
    if (i < half_num) expected_ids.emplace_back(i);
  }

  TestFixture::VerifyScan(0, true, true, half_num, false, false, expected_ids, expected_ids);
}

TYPED_TEST(BwTreeFixture, Scan_RightClosed_ScanInRangeRecords)
{  //
  const size_t repeat_num = TestFixture::kMaxRecordNum;
  const size_t half_num = TestFixture::kMaxRecordNum / 2;

  std::vector<size_t> expected_ids;
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i);
    if (i <= half_num) expected_ids.emplace_back(i);
  }

  TestFixture::VerifyScan(0, true, true, half_num, false, true, expected_ids, expected_ids);
}

TYPED_TEST(BwTreeFixture, Scan_UniqueKeysWithLeafSplit_ScanInsertedRecords)
{  //
  const size_t repeat_num = TestFixture::kMaxRecordNum * 2;
  std::vector<size_t> expected_ids;
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i);
    expected_ids.emplace_back(i);
  }

  TestFixture::VerifyScan(0, true, true, 0, true, true, expected_ids, expected_ids);
}

TYPED_TEST(BwTreeFixture, Scan_DuplicateKeysWithLeafSplit_ScanUpdatedRecords)
{  //
  const size_t repeat_num = TestFixture::kMaxRecordNum * 2;

  std::vector<size_t> expected_keys;
  std::vector<size_t> expected_payloads;
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyUpdate(i, i + 1);
    expected_keys.emplace_back(i);
    expected_payloads.emplace_back(i + 1);
  }

  TestFixture::VerifyScan(0, true, true, 0, true, true, expected_keys, expected_payloads);
}

TYPED_TEST(BwTreeFixture, Scan_LeftOpenedWithLeafSplit_ScanInRangeRecords)
{  //
  const size_t repeat_num = TestFixture::kMaxRecordNum * 2;
  const size_t half_key_num = repeat_num / 2;

  std::vector<size_t> expected_ids;
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i);
    if (i > half_key_num) expected_ids.emplace_back(i);
  }

  TestFixture::VerifyScan(half_key_num, false, false, 0, true, true, expected_ids, expected_ids);
}

TYPED_TEST(BwTreeFixture, Scan_LeftClosedWithLeafSplit_ScanInRangeRecords)
{  //
  const size_t repeat_num = TestFixture::kMaxRecordNum * 2;
  const size_t half_key_num = repeat_num / 2;

  std::vector<size_t> expected_ids;
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i);
    if (i >= half_key_num) expected_ids.emplace_back(i);
  }

  TestFixture::VerifyScan(half_key_num, false, true, 0, true, true, expected_ids, expected_ids);
}

TYPED_TEST(BwTreeFixture, Scan_RightOpenedWithLeafSplit_ScanInRangeRecords)
{  //
  const size_t repeat_num = TestFixture::kMaxRecordNum * 2;
  const size_t half_key_num = repeat_num / 2;

  std::vector<size_t> expected_ids;
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i);
    if (i < half_key_num) expected_ids.emplace_back(i);
  }

  TestFixture::VerifyScan(0, true, true, half_key_num, false, false, expected_ids, expected_ids);
}

TYPED_TEST(BwTreeFixture, Scan_RightClosedWithLeafSplit_ScanInRangeRecords)
{  //
  const size_t repeat_num = TestFixture::kMaxRecordNum * 2;
  const size_t half_key_num = repeat_num / 2;

  std::vector<size_t> expected_ids;
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i);
    if (i <= half_key_num) expected_ids.emplace_back(i);
  }

  TestFixture::VerifyScan(0, true, true, half_key_num, false, true, expected_ids, expected_ids);
}

TYPED_TEST(BwTreeFixture, Scan_DuplicateKeysWithInternalSplit_ScanUpdatedRecords)
{  //
  const size_t repeat_num = TestFixture::kMaxRecordNum * TestFixture::kMaxRecordNum;

  std::vector<size_t> expected_keys;
  std::vector<size_t> expected_payloads;
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < repeat_num; ++i) {
    TestFixture::VerifyUpdate(i, i + 1);
    expected_keys.emplace_back(i);
    expected_payloads.emplace_back(i + 1);
  }

  TestFixture::VerifyScan(0, true, true, 0, true, true, expected_keys, expected_payloads);
}

}  // namespace dbgroup::index::bw_tree::test
