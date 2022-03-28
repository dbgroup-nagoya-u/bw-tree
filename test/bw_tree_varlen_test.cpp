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

#include "bw_tree/bw_tree_varlen.hpp"

#include <algorithm>
#include <memory>
#include <random>
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

constexpr auto kHeaderLen = component::varlen::kHeaderLength;
constexpr size_t kGCTime = 1000;
constexpr bool kExpectSuccess = true;
constexpr bool kExpectFailed = false;
constexpr bool kRangeClosed = true;
constexpr bool kRangeOpened = false;
constexpr bool kWriteTwice = true;
constexpr bool kWithWrite = true;
constexpr bool kWithDelete = true;
constexpr bool kShuffled = true;

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
  using Metadata = component::varlen::Metadata;
  using BwTree_t = BwTreeVarLen<Key, Payload, KeyComp>;

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
   * Utility functions
   *##################################################################################*/

  [[nodiscard]] auto
  CreateTargetIDs(  //
      const size_t rec_num,
      const bool is_shuffled) const  //
      -> std::vector<size_t>
  {
    std::mt19937_64 rand_engine{kRandomSeed};

    std::vector<size_t> target_ids{};
    target_ids.reserve(rec_num);
    for (size_t i = 0; i < rec_num; ++i) {
      target_ids.emplace_back(i);
    }

    if (is_shuffled) {
      std::shuffle(target_ids.begin(), target_ids.end(), rand_engine);
    }

    return target_ids;
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
    auto rc = bw_tree_->Write(keys_[key_id], payloads_[pay_id], kKeyLen);

    EXPECT_EQ(ReturnCode::kSuccess, rc);
  }

  void
  VerifyInsert(  //
      const size_t key_id,
      const size_t payload_id,
      const bool expect_success)
  {
    ReturnCode expected_rc = (expect_success) ? kSuccess : kKeyExist;

    auto rc = bw_tree_->Insert(keys_[key_id], payloads_[payload_id], kKeyLen);
    EXPECT_EQ(expected_rc, rc);
  }

  void
  VerifyUpdate(  //
      const size_t key_id,
      const size_t payload_id,
      const bool expect_success)
  {
    ReturnCode expected_rc = (expect_success) ? kSuccess : kKeyNotExist;

    auto rc = bw_tree_->Update(keys_[key_id], payloads_[payload_id], kKeyLen);
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

  void
  VerifyWritesWith(  //
      const bool write_twice,
      const bool is_shuffled,
      const size_t ops_num = kMaxRecNumForTest)
  {
    const auto &target_ids = CreateTargetIDs(ops_num, is_shuffled);

    for (size_t i = 0; i < ops_num; ++i) {
      const auto id = target_ids.at(i);
      VerifyWrite(id, id);
    }
    if (write_twice) {
      for (size_t i = 0; i < ops_num; ++i) {
        const auto id = target_ids.at(i);
        VerifyWrite(id, id + 1);
      }
    }
    for (size_t i = 0; i < ops_num; ++i) {
      const auto key_id = target_ids.at(i);
      const auto val_id = (write_twice) ? key_id + 1 : key_id;
      VerifyRead(key_id, val_id, kExpectSuccess);
    }
  }

  void
  VerifyInsertsWith(  //
      const bool write_twice,
      const bool with_delete,
      const bool is_shuffled)
  {
    const auto &target_ids = CreateTargetIDs(kMaxRecNumForTest, is_shuffled);

    for (size_t i = 0; i < kMaxRecNumForTest; ++i) {
      const auto id = target_ids.at(i);
      VerifyInsert(id, id, kExpectSuccess);
    }
    if (with_delete) {
      for (size_t i = 0; i < kMaxRecNumForTest; ++i) {
        const auto id = target_ids.at(i);
        VerifyDelete(id, kExpectSuccess);
      }
    }
    if (write_twice) {
      for (size_t i = 0; i < kMaxRecNumForTest; ++i) {
        const auto key_id = target_ids.at(i);
        VerifyInsert(key_id, key_id + 1, with_delete);
      }
    }
    for (size_t i = 0; i < kMaxRecNumForTest; ++i) {
      const auto key_id = target_ids.at(i);
      const auto val_id = (write_twice && with_delete) ? key_id + 1 : key_id;
      VerifyRead(key_id, val_id, kExpectSuccess);
    }
  }

  void
  VerifyUpdatesWith(  //
      const bool with_write,
      const bool with_delete,
      const bool is_shuffled)
  {
    const auto &target_ids = CreateTargetIDs(kMaxRecNumForTest, is_shuffled);
    const auto expect_update = with_write && !with_delete;

    if (with_write) {
      for (size_t i = 0; i < kMaxRecNumForTest; ++i) {
        const auto id = target_ids.at(i);
        VerifyWrite(id, id);
      }
    }
    if (with_delete) {
      for (size_t i = 0; i < kMaxRecNumForTest; ++i) {
        const auto id = target_ids.at(i);
        VerifyDelete(id, kExpectSuccess);
      }
    }
    for (size_t i = 0; i < kMaxRecNumForTest; ++i) {
      const auto key_id = target_ids.at(i);
      VerifyUpdate(key_id, key_id + 1, expect_update);
    }
    for (size_t i = 0; i < kMaxRecNumForTest; ++i) {
      const auto key_id = target_ids.at(i);
      const auto val_id = (expect_update) ? key_id + 1 : key_id;
      VerifyRead(key_id, val_id, expect_update);
    }
  }

  void
  VerifyDeletesWith(  //
      const bool with_write,
      const bool with_delete,
      const bool is_shuffled)
  {
    const auto &target_ids = CreateTargetIDs(kMaxRecNumForTest, is_shuffled);
    const auto expect_delete = with_write && !with_delete;

    if (with_write) {
      for (size_t i = 0; i < kMaxRecNumForTest; ++i) {
        const auto id = target_ids.at(i);
        VerifyWrite(id, id);
      }
    }
    if (with_delete) {
      for (size_t i = 0; i < kMaxRecNumForTest; ++i) {
        const auto id = target_ids.at(i);
        VerifyDelete(id, kExpectSuccess);
      }
    }
    for (size_t i = 0; i < kMaxRecNumForTest; ++i) {
      const auto key_id = target_ids.at(i);
      VerifyDelete(key_id, expect_delete);
    }
    for (size_t i = 0; i < kMaxRecNumForTest; ++i) {
      const auto key_id = target_ids.at(i);
      VerifyRead(key_id, key_id, kExpectFailed);
    }
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
    KeyPayload<UInt8, UInt8>,              // fixed-length keys
    KeyPayload<UInt4, UInt8>,              // small keys
    KeyPayload<UInt8, UInt4>,              // small payloads
    KeyPayload<UInt4, UInt4>,              // small keys/payloads
    KeyPayload<Var, UInt8>,                // variable-length keys
    KeyPayload<Ptr, Ptr>,                  // pointer key/payload
    KeyPayload<Original, Original>         // original type key/payload
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
  TestFixture::VerifyWritesWith(!kWriteTwice, !kShuffled, rec_num);
}

TYPED_TEST(BwTreeFixture, WriteWithConsolidationReadWrittenValues)
{
  const size_t rec_num = TestFixture::kRecNumInNode;
  TestFixture::VerifyWritesWith(!kWriteTwice, !kShuffled, rec_num);
}

TYPED_TEST(BwTreeFixture, WriteWithRootLeafSplitReadWrittenValues)
{
  const size_t rec_num = TestFixture::kRecNumInNode * 1.5;
  TestFixture::VerifyWritesWith(!kWriteTwice, !kShuffled, rec_num);
}

TYPED_TEST(BwTreeFixture, WriteWithRootInternalSplitReadWrittenValues)
{
  const size_t rec_num = TestFixture::kRecNumInNode * TestFixture::kRecNumInNode;
  TestFixture::VerifyWritesWith(!kWriteTwice, !kShuffled, rec_num);
}

TYPED_TEST(BwTreeFixture, WriteWithInternalSplitReadWrittenValues)
{
  TestFixture::VerifyWritesWith(!kWriteTwice, !kShuffled);
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

TYPED_TEST(BwTreeFixture, WriteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyWritesWith(kWriteTwice, !kShuffled);
}

TYPED_TEST(BwTreeFixture, RandomWriteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyWritesWith(kWriteTwice, kShuffled);
}

/*--------------------------------------------------------------------------------------
 * Insert operation
 *------------------------------------------------------------------------------------*/

TYPED_TEST(BwTreeFixture, InsertWithUniqueKeysSucceed)
{
  TestFixture::VerifyInsertsWith(!kWriteTwice, !kWithDelete, !kShuffled);
}

TYPED_TEST(BwTreeFixture, InsertWithDuplicateKeysFail)
{
  TestFixture::VerifyInsertsWith(kWriteTwice, !kWithDelete, !kShuffled);
}

TYPED_TEST(BwTreeFixture, InsertWithDeletedKeysSucceed)
{
  TestFixture::VerifyInsertsWith(kWriteTwice, kWithDelete, !kShuffled);
}

TYPED_TEST(BwTreeFixture, RandomInsertWithUniqueKeysSucceed)
{
  TestFixture::VerifyInsertsWith(!kWriteTwice, !kWithDelete, kShuffled);
}

TYPED_TEST(BwTreeFixture, RandomInsertWithDuplicateKeysFail)
{
  TestFixture::VerifyInsertsWith(kWriteTwice, !kWithDelete, kShuffled);
}

TYPED_TEST(BwTreeFixture, RandomInsertWithDeletedKeysSucceed)
{
  TestFixture::VerifyInsertsWith(kWriteTwice, kWithDelete, kShuffled);
}

/*--------------------------------------------------------------------------------------
 * Update operation
 *------------------------------------------------------------------------------------*/

TYPED_TEST(BwTreeFixture, UpdateWithDuplicateKeysSucceed)
{
  TestFixture::VerifyUpdatesWith(kWithWrite, !kWithDelete, !kShuffled);
}

TYPED_TEST(BwTreeFixture, UpdateWithNotInsertedKeysFail)
{
  TestFixture::VerifyUpdatesWith(!kWithWrite, !kWithDelete, !kShuffled);
}

TYPED_TEST(BwTreeFixture, UpdateWithDeletedKeysFail)
{
  TestFixture::VerifyUpdatesWith(kWithWrite, kWithDelete, !kShuffled);
}

TYPED_TEST(BwTreeFixture, RandomUpdateWithDuplicateKeysSucceed)
{
  TestFixture::VerifyUpdatesWith(kWithWrite, !kWithDelete, kShuffled);
}

TYPED_TEST(BwTreeFixture, RandomUpdateWithNotInsertedKeysFail)
{
  TestFixture::VerifyUpdatesWith(!kWithWrite, !kWithDelete, kShuffled);
}

TYPED_TEST(BwTreeFixture, RandomUpdateWithDeletedKeysFail)
{
  TestFixture::VerifyUpdatesWith(kWithWrite, kWithDelete, kShuffled);
}

/*--------------------------------------------------------------------------------------
 * Delete operation
 *------------------------------------------------------------------------------------*/

TYPED_TEST(BwTreeFixture, DeleteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyDeletesWith(kWithWrite, !kWithDelete, !kShuffled);
}

TYPED_TEST(BwTreeFixture, DeleteWithNotInsertedKeysFail)
{
  TestFixture::VerifyDeletesWith(!kWithWrite, !kWithDelete, !kShuffled);
}

TYPED_TEST(BwTreeFixture, DeleteWithDeletedKeysFail)
{
  TestFixture::VerifyDeletesWith(kWithWrite, kWithDelete, !kShuffled);
}

TYPED_TEST(BwTreeFixture, RandomDeleteWithDuplicateKeysSucceed)
{
  TestFixture::VerifyDeletesWith(kWithWrite, !kWithDelete, kShuffled);
}

TYPED_TEST(BwTreeFixture, RandomDeleteWithNotInsertedKeysFail)
{
  TestFixture::VerifyDeletesWith(!kWithWrite, !kWithDelete, kShuffled);
}

TYPED_TEST(BwTreeFixture, RandomDeleteWithDeletedKeysFail)
{
  TestFixture::VerifyDeletesWith(kWithWrite, kWithDelete, kShuffled);
}

}  // namespace dbgroup::index::bw_tree::test
