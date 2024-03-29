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

// C++ standard libraries
#include <algorithm>
#include <memory>
#include <random>
#include <vector>

// external sources
#include "external/index-fixtures/common.hpp"
#include "gtest/gtest.h"

// local sources
#include "bw_tree/component/fixlen/delta_record.hpp"
#include "bw_tree/component/varlen/delta_record.hpp"

namespace dbgroup::index::bw_tree
{
/**
 * @brief Use CString as variable-length data in tests.
 *
 */
template <>
constexpr auto
IsVarLenData<char *>()  //
    -> bool
{
  return true;
}

}  // namespace dbgroup::index::bw_tree

namespace dbgroup::index::bw_tree::component::test
{
/*######################################################################################
 * Classes for templated testing
 *####################################################################################*/

template <template <class K, class C> class DeltaType, class KeyType, class PayloadType>
struct Target {
  using Key = KeyType;
  using Payload = PayloadType;
  using Delta = DeltaType<typename Key::Data, typename Key::Comp>;

  static constexpr bool kUseVarLen =
      std::is_same_v<Delta, varlen::DeltaRecord<typename Key::Data, typename Key::Comp>>;
};

/*######################################################################################
 * Fixture class definition
 *####################################################################################*/

template <class Target>
class DeltaRecordFixture : public testing::Test
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  // extract key-payload types
  using Key = typename Target::Key::Data;
  using Payload = typename Target::Payload::Data;
  using KeyComp = typename Target::Key::Comp;
  using PayComp = typename Target::Payload::Comp;

  // define type aliases for simplicity
  using Delta_t = typename Target::Delta;
  using Record = typename Delta_t::Record;
  using KeyWOPtr = std::remove_pointer_t<Key>;

 protected:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  static constexpr bool kUseVarLen = Target::kUseVarLen;
  static constexpr size_t kKeyNumForTest = 64;
  static constexpr size_t kRandomSeed = DBGROUP_TEST_RANDOM_SEED;

  /*####################################################################################
   * Setup/Teardown
   *##################################################################################*/

  void
  SetUp() override
  {
    keys_ = ::dbgroup::index::test::PrepareTestData<Key>(kKeyNumForTest);
    payloads_ = ::dbgroup::index::test::PrepareTestData<Payload>(kKeyNumForTest);
  }

  void
  TearDown() override
  {
    ::dbgroup::index::test::ReleaseTestData(keys_);
    ::dbgroup::index::test::ReleaseTestData(payloads_);
  }

  /*####################################################################################
   * Utilities for testing
   *##################################################################################*/

  static auto
  GetPage()  //
      -> void *
  {
    return ::operator new(Delta_t::template GetMaxDeltaSize<Payload>());
  }

  static auto
  CreateLeafInsertModifyDelta(  //
      const DeltaType type,
      const Key &key,
      const Payload &payload)  //
      -> std::unique_ptr<Delta_t>
  {
    const auto key_len = ::dbgroup::index::test::GetLength(key);
    auto *raw_p = new (GetPage()) Delta_t{type, key, key_len, payload};
    return std::unique_ptr<Delta_t>{raw_p};
  }

  static auto
  CreateLeafDeleteDelta(const Key &key)  //
      -> std::unique_ptr<Delta_t>
  {
    const auto key_len = ::dbgroup::index::test::GetLength(key);
    auto *raw_p = new (GetPage()) Delta_t{key, key_len};

    return std::unique_ptr<Delta_t>{raw_p};
  }

  static auto
  CreateSplitMergeDelta(  //
      const DeltaType type,
      const std::unique_ptr<Delta_t> &dummy_d,
      const PageID dummy_pid)  //
      -> std::unique_ptr<Delta_t>
  {
    auto *raw_p = new (GetPage()) Delta_t{type, dummy_d.get(), dummy_pid};
    return std::unique_ptr<Delta_t>{raw_p};
  }

  static void
  CheckLowKey(  //
      const std::unique_ptr<Delta_t> &delta,
      const Key &key)
  {
    EXPECT_TRUE(delta->HasSameKey(key));
    EXPECT_TRUE(::dbgroup::index::test::IsEqual<KeyComp>(key, delta->GetKey()));
  }

  static auto
  CreateTempRecords()
  {
    thread_local std::array<Record, kMaxDeltaRecordNum> arr{};

    if constexpr (IsVarLenData<Key>()) {
      thread_local std::unique_ptr<KeyWOPtr, std::function<void(void *)>>  //
          page{::dbgroup::memory::Allocate<KeyWOPtr>(kMaxVarDataSize * kMaxDeltaRecordNum),
               ::dbgroup::memory::Release<KeyWOPtr>};

      const auto *top_addr = page.get();
      for (size_t i = 0; i < kMaxDeltaRecordNum; ++i) {
        arr[i].key = reinterpret_cast<Key>(ShiftAddr(top_addr, i * kMaxVarDataSize));
      }
    }

    return arr;
  }

  /*####################################################################################
   * Functions for verification
   *##################################################################################*/

  void
  VerifyLeafInsertModifyConstructor(const DeltaType type)
  {
    const auto &key = keys_[0];
    const auto &payload = payloads_[0];
    const auto &delta = CreateLeafInsertModifyDelta(type, key, payload);
    const auto &act_pay = delta->template GetPayload<Payload>();

    EXPECT_TRUE(delta->IsLeaf());
    EXPECT_EQ(type, delta->GetDeltaType());
    EXPECT_EQ(nullptr, delta->GetNext());
    EXPECT_TRUE(::dbgroup::index::test::IsEqual<PayComp>(payload, act_pay));

    CheckLowKey(delta, key);
  }

  void
  VerifyLeafDeleteConstructor()
  {
    const auto &key = keys_[0];
    const auto &delta = CreateLeafDeleteDelta(key);

    EXPECT_TRUE(delta->IsLeaf());
    EXPECT_EQ(kDelete, delta->GetDeltaType());
    EXPECT_EQ(nullptr, delta->GetNext());

    CheckLowKey(delta, key);
  }

  void
  VerifySplitMergeConstructor(const DeltaType type)
  {
    const auto &key = keys_[0];
    const auto &payload = payloads_[0];
    const auto &dummy_d = CreateLeafInsertModifyDelta(kInsert, key, payload);

    const PageID dummy_pid = kNullPtr;
    const auto &delta = CreateSplitMergeDelta(type, dummy_d, dummy_pid);

    EXPECT_TRUE(delta->IsLeaf());
    EXPECT_EQ(type, delta->GetDeltaType());
    EXPECT_EQ(nullptr, delta->GetNext());
    EXPECT_EQ(dummy_pid, delta->template GetPayload<PageID>());

    CheckLowKey(delta, key);
  }

  void
  VerifyRemoveNodeConstructor()
  {
    std::unique_ptr<Delta_t> delta{new (GetPage()) Delta_t{true}};

    EXPECT_TRUE(delta->IsLeaf());
    EXPECT_EQ(kRemoveNode, delta->GetDeltaType());
    EXPECT_EQ(nullptr, delta->GetNext());
  }

  void
  VerifyAddByInsertionSortTo()
  {
    constexpr auto kHalfDeltaNum = kMaxDeltaRecordNum / 2;

    std::vector<size_t> ids{};
    std::vector<std::unique_ptr<Delta_t>> entities{};
    std::mt19937_64 rand{kRandomSeed};
    auto &&records = CreateTempRecords();

    for (size_t i = 0; i < kHalfDeltaNum; ++i) {
      ids.emplace_back(i);
    }
    std::shuffle(ids.begin(), ids.end(), rand);

    size_t count = 0;
    for (const auto &id : ids) {
      auto &&delta = CreateLeafInsertModifyDelta(kDelete, keys_[id], payloads_[id]);
      delta->AddByInsertionSortTo(records, count);
      entities.emplace_back(std::move(delta));
    }
    for (const auto &id : ids) {
      auto &&delta = CreateLeafInsertModifyDelta(kInsert, keys_[id], payloads_[id]);
      delta->AddByInsertionSortTo(records, count);
      entities.emplace_back(std::move(delta));
    }

    ASSERT_EQ(kHalfDeltaNum, count);
    for (size_t i = 0; i < kHalfDeltaNum - 1; ++i) {
      EXPECT_TRUE(KeyComp{}(records.at(i).key, records.at(i + 1).key));
    }
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// actual keys
  std::vector<Key> keys_{};

  /// actual payloads
  std::vector<Payload> payloads_{};
};

/*######################################################################################
 * Preparation for typed testing
 *####################################################################################*/

using UInt8 = ::dbgroup::index::test::UInt8;
using UInt4 = ::dbgroup::index::test::UInt4;
using Int8 = ::dbgroup::index::test::Int8;
using Var = ::dbgroup::index::test::Var;
using Ptr = ::dbgroup::index::test::Ptr;
using Original = ::dbgroup::index::test::Original;

template <class K, class C>
using VarLenRecord = varlen::DeltaRecord<K, C>;

template <class K, class C>
using FixLenRecord = fixlen::DeltaRecord<K, C>;

using TestTargets = ::testing::Types<          //
    Target<VarLenRecord, UInt8, UInt8>,        // fixed-length keys
    Target<VarLenRecord, UInt4, UInt8>,        // small keys
    Target<VarLenRecord, UInt8, UInt4>,        // small payloads
    Target<VarLenRecord, UInt4, UInt4>,        // small keys/payloads
    Target<VarLenRecord, Var, UInt8>,          // variable-length keys
    Target<VarLenRecord, Ptr, Ptr>,            // pointer key/payload
    Target<VarLenRecord, Original, Original>,  // original type key/payload
    Target<FixLenRecord, UInt8, UInt8>,        // fixed-length keys
    Target<FixLenRecord, UInt4, UInt8>,        // small keys
    Target<FixLenRecord, UInt8, UInt4>,        // small payloads
    Target<FixLenRecord, UInt4, UInt4>,        // small keys/payloads
    Target<FixLenRecord, Ptr, Ptr>,            // pointer key/payload
    Target<FixLenRecord, Original, Original>   // original type key/payload
    >;
TYPED_TEST_SUITE(DeltaRecordFixture, TestTargets);

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

/*--------------------------------------------------------------------------------------
 * Constructor tests
 *------------------------------------------------------------------------------------*/

TYPED_TEST(DeltaRecordFixture, ConstructedLeafInsertModifyDeltasHaveExpectedValues)
{
  TestFixture::VerifyLeafInsertModifyConstructor(kInsert);
  TestFixture::VerifyLeafInsertModifyConstructor(kModify);
}

TYPED_TEST(DeltaRecordFixture, ConstructedLeafDeleteDeltasHaveExpectedValues)
{
  TestFixture::VerifyLeafDeleteConstructor();
}

TYPED_TEST(DeltaRecordFixture, ConstructedSplitMergeDeltasHaveExpectedValues)
{
  TestFixture::VerifySplitMergeConstructor(kMerge);
}

TYPED_TEST(DeltaRecordFixture, ConstructedRemoveNodeDeltasHaveExpectedValues)
{
  TestFixture::VerifyRemoveNodeConstructor();
}

/*--------------------------------------------------------------------------------------
 * Utility tests
 *------------------------------------------------------------------------------------*/

TYPED_TEST(DeltaRecordFixture, InsertionSortOrderGivenRecordsAndRemoveDuplication)
{
  TestFixture::VerifyAddByInsertionSortTo();
}

}  // namespace dbgroup::index::bw_tree::component::test
