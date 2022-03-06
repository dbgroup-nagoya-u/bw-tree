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
/*######################################################################################
 * Classes for templated testing
 *####################################################################################*/

template <class KeyType, class PayloadType>
struct KeyPayload {
  using Key = KeyType;
  using Payload = PayloadType;
};

/*######################################################################################
 * Global constants
 *####################################################################################*/

constexpr auto kHeaderLen = component::kHeaderLength;
constexpr size_t kGCTime = 1000;
constexpr bool kExpectSuccess = true;
constexpr bool kExpectFailed = false;
constexpr bool kRangeClosed = true;
constexpr bool kRangeOpened = false;

/*######################################################################################
 * Fixture class definition
 *####################################################################################*/

template <class KeyPayload>
class BwTreeFixture : public testing::Test
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  // extract key-payload types
  using Key = typename KeyPayload::Key::Data;
  using Payload = typename KeyPayload::Payload::Data;
  using KeyComp = typename KeyPayload::Key::Comp;
  using PayloadComp = typename KeyPayload::Payload::Comp;

  // define type aliases for simplicity
  using Node_t = component::Node<Key, std::less<>>;
  using Metadata = component::Metadata;
  using BwTree_t = BwTree<Key, Payload, KeyComp>;

 protected:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  static constexpr size_t kKeyLen = GetDataLength<Key>();
  static constexpr size_t kPayLen = GetDataLength<Payload>();
  static constexpr size_t kRecLen = kKeyLen + kPayLen;
  static constexpr size_t kRecNumInNode = (kPageSize - kHeaderLen) / (kRecLen + sizeof(Metadata));
  static constexpr size_t kMaxRecNumForTest = 2 * kRecNumInNode * kRecNumInNode;
  static constexpr size_t kKeyNumForTest = 2 * kRecNumInNode * kRecNumInNode + 2;

  /*####################################################################################
   * Setup/Teardown
   *##################################################################################*/

  void
  SetUp() override
  {
    PrepareTestData(keys_, kKeyNumForTest);
    PrepareTestData(payloads_, kKeyNumForTest);

    bw_tree_ = std::make_unique<BwTree_t>(kGCTime);
  }

  void
  TearDown() override
  {
    ReleaseTestData(keys_, kKeyNumForTest);
    ReleaseTestData(payloads_, kKeyNumForTest);
  }

  /*####################################################################################
   * Functions for verification
   *##################################################################################*/

  void
  VerifyRead(  //
      const size_t key_id,
      const size_t expected_id,
      const bool expect_success)
  {
    const auto &read_val = bw_tree_->Read(keys_[key_id]);
    if (expect_success) {
      EXPECT_TRUE(read_val);

      const auto &expected = payloads_[expected_id];
      const auto &actual = read_val.value();
      EXPECT_TRUE(component::IsEqual<PayloadComp>(expected, actual));
      if constexpr (IsVariableLengthData<Payload>()) {
        delete actual;
      }
    } else {
      EXPECT_FALSE(read_val);
    }
  }

  void
  VerifyScan(  //
      const std::optional<std::pair<size_t, bool>> begin_ref,
      const std::optional<std::pair<size_t, bool>> end_ref)
  {
    std::optional<std::pair<const Key &, bool>> begin_key = std::nullopt;
    size_t begin_pos = 0;
    if (begin_ref) {
      auto &&[begin_id, begin_closed] = *begin_ref;
      begin_key.emplace(keys_[begin_id], begin_closed);
      begin_pos = (begin_closed) ? begin_id : begin_id + 1;
    }

    std::optional<std::pair<const Key &, bool>> end_key = std::nullopt;
    size_t end_pos = 0;
    if (end_ref) {
      auto &&[end_id, end_closed] = *end_ref;
      end_key.emplace(keys_[end_id], end_closed);
      end_pos = (end_closed) ? end_id + 1 : end_id;
    }

    auto iter = bw_tree_->Scan(begin_key, end_key);

    for (; iter.HasNext(); ++iter, ++begin_pos) {
      auto [key, payload] = *iter;
      EXPECT_TRUE(component::IsEqual<KeyComp>(keys_[begin_pos], key));
      EXPECT_TRUE(component::IsEqual<PayloadComp>(payloads_[begin_pos], payload));
    }
    EXPECT_FALSE(iter.HasNext());

    if (end_ref) {
      EXPECT_EQ(begin_pos, end_pos);
    }
  }

  void
  VerifyWrite(  //
      const size_t key_id,
      const size_t pay_id)
  {
    auto rc = bw_tree_->Write(keys_[key_id], payloads_[pay_id], kKeyLen, kPayLen);

    EXPECT_EQ(ReturnCode::kSuccess, rc);
  }

  void
  VerifyInsert(  //
      const size_t key_id,
      const size_t payload_id,
      const bool expect_success)
  {
    ReturnCode expected_rc = (expect_success) ? kSuccess : kKeyExist;

    auto rc = bw_tree_->Insert(keys_[key_id], payloads_[payload_id], kKeyLen, kPayLen);
    EXPECT_EQ(expected_rc, rc);
  }

  void
  VerifyUpdate(  //
      const size_t key_id,
      const size_t payload_id,
      const bool expect_success)
  {
    ReturnCode expected_rc = (expect_success) ? kSuccess : kKeyNotExist;

    auto rc = bw_tree_->Update(keys_[key_id], payloads_[payload_id], kKeyLen, kPayLen);
    EXPECT_EQ(expected_rc, rc);
  }

  void
  VerifyDelete(  //
      const size_t key_id,
      const bool expect_success)
  {
    ReturnCode expected_rc = (expect_success) ? kSuccess : kKeyNotExist;

    auto rc = bw_tree_->Delete(keys_[key_id], kKeyLen);
    EXPECT_EQ(expected_rc, rc);
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  // actual keys and payloads
  Key keys_[kKeyNumForTest]{};
  Payload payloads_[kKeyNumForTest]{};

  // a target node and its expected metadata
  std::unique_ptr<BwTree_t> bw_tree_{nullptr};
};

/*######################################################################################
 * Preparation for typed testing
 *####################################################################################*/

using KeyPayloadPairs = ::testing::Types<  //
    KeyPayload<UInt8, UInt8>,              // both fixed
    KeyPayload<Var, UInt8>,                // variable-fixed
    KeyPayload<UInt8, Var>,                // fixed-variable
    KeyPayload<Var, Var>,                  // both variable
    KeyPayload<Ptr, Ptr>,                  // pointer key/payload
    KeyPayload<Original, Original>         // original key/payload
    >;
TYPED_TEST_SUITE(BwTreeFixture, KeyPayloadPairs);

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

/*--------------------------------------------------------------------------------------
 * Structure modification operations
 *------------------------------------------------------------------------------------*/

TYPED_TEST(BwTreeFixture, WriteWithoutSMOsReadWrittenValues)
{
  const size_t rec_num = kMaxDeltaNodeNum;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(BwTreeFixture, WriteWithConsolidationReadWrittenValues)
{
  const size_t rec_num = TestFixture::kRecNumInNode;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(BwTreeFixture, WriteWithRootLeafSplitReadWrittenValues)
{
  const size_t rec_num = TestFixture::kRecNumInNode * 1.5;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(BwTreeFixture, WriteWithRootInternalSplitReadWrittenValues)
{
  const size_t rec_num = TestFixture::kRecNumInNode * TestFixture::kRecNumInNode;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(BwTreeFixture, WriteWithInternalSplitReadWrittenValues)
{
  const size_t rec_num = TestFixture::kMaxRecNumForTest;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

/*--------------------------------------------------------------------------------------
 * Read operation tests
 *------------------------------------------------------------------------------------*/

TYPED_TEST(BwTreeFixture, ReadWithEmptyIndexFail)
{  //
  TestFixture::VerifyRead(0, 0, kExpectFailed);
}

/*--------------------------------------------------------------------------------------
 * Scan operation
 *------------------------------------------------------------------------------------*/

TYPED_TEST(BwTreeFixture, ScanWithoutKeysPerformFullScan)
{
  const size_t rec_num = TestFixture::kMaxRecNumForTest;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }

  TestFixture::VerifyScan(std::nullopt, std::nullopt);
}

TYPED_TEST(BwTreeFixture, ScanWithClosedRangeIncludeLeftRightEnd)
{
  const size_t rec_num = TestFixture::kMaxRecNumForTest;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }

  TestFixture::VerifyScan(std::make_pair(0, kRangeClosed),
                          std::make_pair(rec_num - 1, kRangeClosed));
}

TYPED_TEST(BwTreeFixture, ScanWithOpenedRangeExcludeLeftRightEnd)
{
  const size_t rec_num = TestFixture::kMaxRecNumForTest;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }

  TestFixture::VerifyScan(std::make_pair(0, kRangeOpened),
                          std::make_pair(rec_num - 1, kRangeOpened));
}

/*--------------------------------------------------------------------------------------
 * Write operation
 *------------------------------------------------------------------------------------*/

TYPED_TEST(BwTreeFixture, WriteWithDuplicateKeysReadLatestValues)
{
  const size_t rec_num = TestFixture::kMaxRecNumForTest;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i + 1);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i + 1, kExpectSuccess);
  }
}

/*--------------------------------------------------------------------------------------
 * Insert operation
 *------------------------------------------------------------------------------------*/

TYPED_TEST(BwTreeFixture, InsertWithUniqueKeysReadInsertedValues)
{
  const size_t rec_num = TestFixture::kMaxRecNumForTest;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyInsert(i, i, kExpectSuccess);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(BwTreeFixture, InsertWithDuplicateKeysFail)
{
  const size_t rec_num = TestFixture::kMaxRecNumForTest;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyInsert(i, i, kExpectSuccess);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyInsert(i, i + 1, kExpectFailed);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectSuccess);
  }
}

TYPED_TEST(BwTreeFixture, InsertWithDeletedKeysSucceed)
{
  const size_t rec_num = TestFixture::kMaxRecNumForTest;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyInsert(i, i, kExpectSuccess);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyDelete(i, kExpectSuccess);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyInsert(i, i + 1, kExpectSuccess);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i + 1, kExpectSuccess);
  }
}

/*--------------------------------------------------------------------------------------
 * Update operation
 *------------------------------------------------------------------------------------*/

TYPED_TEST(BwTreeFixture, UpdateWithDuplicateKeysReadUpdatedValues)
{
  const size_t rec_num = TestFixture::kMaxRecNumForTest;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyUpdate(i, i + 1, kExpectSuccess);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i + 1, kExpectSuccess);
  }
}

TYPED_TEST(BwTreeFixture, UpdateNotInsertedKeysFail)
{
  const size_t rec_num = TestFixture::kMaxRecNumForTest;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyUpdate(i, i, kExpectFailed);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectFailed);
  }
}

TYPED_TEST(BwTreeFixture, UpdateWithDeletedKeysFail)
{
  const size_t rec_num = TestFixture::kMaxRecNumForTest;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyDelete(i, kExpectSuccess);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyUpdate(i, i, kExpectFailed);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectFailed);
  }
}

/*--------------------------------------------------------------------------------------
 * Delete operation
 *------------------------------------------------------------------------------------*/

TYPED_TEST(BwTreeFixture, DeleteWithDuplicateKeysReadFail)
{
  const size_t rec_num = TestFixture::kMaxRecNumForTest;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyWrite(i, i);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyDelete(i, kExpectSuccess);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectFailed);
  }
}

TYPED_TEST(BwTreeFixture, DeleteNotInsertedKeysFail)
{
  const size_t rec_num = TestFixture::kMaxRecNumForTest;

  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyDelete(i, kExpectFailed);
  }
  for (size_t i = 0; i < rec_num; ++i) {
    TestFixture::VerifyRead(i, i, kExpectFailed);
  }
}

}  // namespace dbgroup::index::bw_tree::test
