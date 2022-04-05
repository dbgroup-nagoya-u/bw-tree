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

#include "bw_tree/component/varlen/delta_record.hpp"

#include <algorithm>
#include <memory>
#include <random>
#include <vector>

#include "common.hpp"
#include "fix_var_switch.hpp"
#include "gtest/gtest.h"

namespace dbgroup::index::bw_tree::component::test
{
/*######################################################################################
 * Classes for templated testing
 *####################################################################################*/

template <class BwTreeType, class KeyType, class PayloadType>
struct Target {
  using Tree = BwTreeType;
  using Key = KeyType;
  using Payload = PayloadType;
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
  using Delta_t = typename Target::Tree::template Delta<Key, KeyComp>;
  using Record = typename Delta_t::Record;

 protected:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  static constexpr bool kUseVarLen = Target::Tree::kUseVarLen;
  static constexpr size_t kKeyLen = GetDataLength<Key>();
  static constexpr size_t kKeyNumForTest = 64;

  /*####################################################################################
   * Setup/Teardown
   *##################################################################################*/

  void
  SetUp() override
  {
    PrepareTestData(keys_, kKeyNumForTest);
    PrepareTestData(payloads_, kKeyNumForTest);
  }

  void
  TearDown() override
  {
    ReleaseTestData(keys_, kKeyNumForTest);
    ReleaseTestData(payloads_, kKeyNumForTest);
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
    Delta_t *raw_p{};
    if constexpr (kUseVarLen) {
      raw_p = new (GetPage()) Delta_t{type, key, kKeyLen, payload};
    } else {
      raw_p = new (GetPage()) Delta_t{type, key, payload};
    }
    return std::unique_ptr<Delta_t>{raw_p};
  }

  static auto
  CreateLeafDeleteDelta(const Key &key)  //
      -> std::unique_ptr<Delta_t>
  {
    Delta_t *raw_p{};
    if constexpr (kUseVarLen) {
      raw_p = new (GetPage()) Delta_t{key, kKeyLen};
    } else {
      raw_p = new (GetPage()) Delta_t{key};
    }

    return std::unique_ptr<Delta_t>{raw_p};
  }

  static auto
  CreateSplitMergeDelta(  //
      const DeltaType type,
      const std::unique_ptr<Delta_t> &dummy_d,
      const LogicalID *dummy_lid)  //
      -> std::unique_ptr<Delta_t>
  {
    auto *raw_p = new (GetPage()) Delta_t{type, dummy_d.get(), dummy_lid};
    return std::unique_ptr<Delta_t>{raw_p};
  }

  static void
  CheckLowKey(  //
      const std::unique_ptr<Delta_t> &delta,
      const Key &key)
  {
    EXPECT_TRUE(delta->HasSameKey(key));
    EXPECT_TRUE(IsEqual<KeyComp>(key, delta->GetKey()));

    const auto &low_key = delta->GetLowKey();
    EXPECT_TRUE(low_key);
    EXPECT_TRUE(IsEqual<KeyComp>(key, *low_key));
  }

  static auto
  LessThan(  //
      const Record &rec_a,
      const Record &rec_b)  //
      -> bool
  {
    if constexpr (kUseVarLen) {
      return KeyComp{}(rec_a.first, rec_b.first);
    } else {
      const auto *delta_a = reinterpret_cast<const Delta_t *>(rec_a);
      const auto *delta_b = reinterpret_cast<const Delta_t *>(rec_b);
      return KeyComp{}(delta_a->GetKey(), delta_b->GetKey());
    }
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

    EXPECT_TRUE(delta->IsLeaf());
    EXPECT_EQ(type, delta->GetDeltaType());
    EXPECT_EQ(nullptr, delta->GetNext());
    EXPECT_TRUE(IsEqual<PayComp>(payload, delta->template GetPayload<Payload>()));

    CheckLowKey(delta, key);
    EXPECT_FALSE(delta->GetHighKey());
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
    EXPECT_FALSE(delta->GetHighKey());
  }

  void
  VerifySplitMergeConstructor(const DeltaType type)
  {
    const auto &key = keys_[0];
    const auto &payload = payloads_[0];
    const auto &dummy_d = CreateLeafInsertModifyDelta(kInsert, key, payload);

    LogicalID *dummy_lid = nullptr;
    const auto &delta = CreateSplitMergeDelta(type, dummy_d, dummy_lid);

    EXPECT_TRUE(delta->IsLeaf());
    EXPECT_EQ(type, delta->GetDeltaType());
    EXPECT_EQ(nullptr, delta->GetNext());
    EXPECT_EQ(dummy_lid, delta->template GetPayload<LogicalID *>());

    CheckLowKey(delta, key);
    EXPECT_FALSE(delta->GetHighKey());
  }

  void
  VerifyRemoveNodeConstructor()
  {
    const auto &key = keys_[0];
    const auto &payload = payloads_[0];
    const auto &dummy_d = CreateLeafInsertModifyDelta(kInsert, key, payload);

    auto *raw_p = new (GetPage()) Delta_t{kRemoveNode, dummy_d.get()};
    std::unique_ptr<Delta_t> delta{raw_p};

    EXPECT_TRUE(delta->IsLeaf());
    EXPECT_EQ(kRemoveNode, delta->GetDeltaType());
    EXPECT_EQ(dummy_d.get(), delta->GetNext());
  }

  void
  VerifyInsertIndexEntryConstructor()
  {
    const auto &key = keys_[0];
    const auto &payload = payloads_[0];
    LogicalID *dummy_lid = nullptr;
    const auto &dummy_d = CreateLeafInsertModifyDelta(kInsert, key, payload);
    const auto &dummy_d2 = CreateSplitMergeDelta(kSplit, dummy_d, dummy_lid);

    auto *raw_p = new (GetPage()) Delta_t{dummy_d2.get()};
    std::unique_ptr<Delta_t> delta{raw_p};

    EXPECT_FALSE(delta->IsLeaf());
    EXPECT_EQ(kInsert, delta->GetDeltaType());
    EXPECT_EQ(nullptr, delta->GetNext());
    EXPECT_EQ(dummy_lid, delta->template GetPayload<LogicalID *>());

    CheckLowKey(delta, key);
    EXPECT_FALSE(delta->GetHighKey());
  }

  void
  VerifyDeleteIndexEntryConstructor()
  {
    const auto &key = keys_[0];
    const auto &payload = payloads_[0];
    LogicalID *dummy_lid{};
    const auto &dummy_d = CreateLeafInsertModifyDelta(kInsert, key, payload);
    const auto &dummy_d2 = CreateSplitMergeDelta(kSplit, dummy_d, dummy_lid);

    dummy_lid = nullptr;
    auto *raw_p = new (GetPage()) Delta_t{dummy_d2.get(), dummy_lid};
    std::unique_ptr<Delta_t> delta{raw_p};

    EXPECT_FALSE(delta->IsLeaf());
    EXPECT_EQ(kDelete, delta->GetDeltaType());
    EXPECT_EQ(nullptr, delta->GetNext());
    EXPECT_EQ(dummy_lid, delta->template GetPayload<LogicalID *>());

    CheckLowKey(delta, key);
    EXPECT_FALSE(delta->GetHighKey());
  }

  void
  VerifyAddByInsertionSortTo()
  {
    std::vector<size_t> ids{};
    std::vector<std::unique_ptr<Delta_t>> entities{};
    std::vector<Record> records{};
    std::mt19937_64 rand{kRandomSeed};

    for (size_t i = 0; i < kKeyNumForTest; ++i) {
      ids.emplace_back(i);
    }
    std::shuffle(ids.begin(), ids.end(), rand);

    size_t diff = 0;
    for (const auto &id : ids) {
      auto &&delta = CreateLeafInsertModifyDelta(kDelete, keys_[id], payloads_[id]);
      diff += delta->template AddByInsertionSortTo<Payload>(std::nullopt, records);
      entities.emplace_back(std::move(delta));
    }
    for (const auto &id : ids) {
      auto &&delta = CreateLeafInsertModifyDelta(kInsert, keys_[id], payloads_[id]);
      diff += delta->template AddByInsertionSortTo<Payload>(std::nullopt, records);
      entities.emplace_back(std::move(delta));
    }

    EXPECT_EQ(diff, 0);
    ASSERT_EQ(kKeyNumForTest, records.size());
    for (size_t i = 0; i < kKeyNumForTest - 1; ++i) {
      EXPECT_TRUE(LessThan(records.at(i), records.at(i + 1)));
    }
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  // actual keys and payloads
  Key keys_[kKeyNumForTest]{};
  Payload payloads_[kKeyNumForTest]{};
};

/*######################################################################################
 * Preparation for typed testing
 *####################################################################################*/

using TestTargets = ::testing::Types<    //
    Target<VarLen, UInt8, UInt8>,        // fixed-length keys
    Target<VarLen, UInt4, UInt8>,        // small keys
    Target<VarLen, UInt8, UInt4>,        // small payloads
    Target<VarLen, UInt4, UInt4>,        // small keys/payloads
    Target<VarLen, Var, UInt8>,          // variable-length keys
    Target<VarLen, Ptr, Ptr>,            // pointer key/payload
    Target<VarLen, Original, Original>,  // original type key/payload
    Target<FixLen, UInt8, UInt8>,        // fixed-length keys
    Target<FixLen, UInt4, UInt8>,        // small keys
    Target<FixLen, UInt8, UInt4>,        // small payloads
    Target<FixLen, UInt4, UInt4>,        // small keys/payloads
    Target<FixLen, Ptr, Ptr>,            // pointer key/payload
    Target<FixLen, Original, Original>   // original type key/payload
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
  TestFixture::VerifySplitMergeConstructor(kSplit);
  TestFixture::VerifySplitMergeConstructor(kMerge);
}

TYPED_TEST(DeltaRecordFixture, ConstructedRemoveNodeDeltasHaveExpectedValues)
{
  TestFixture::VerifyRemoveNodeConstructor();
}

TYPED_TEST(DeltaRecordFixture, ConstructedInsertIndexEntryDeltasHaveExpectedValues)
{
  TestFixture::VerifyInsertIndexEntryConstructor();
}

TYPED_TEST(DeltaRecordFixture, ConstructedDeleteIndexEntryDeltasHaveExpectedValues)
{
  TestFixture::VerifyDeleteIndexEntryConstructor();
}

/*--------------------------------------------------------------------------------------
 * Utility tests
 *------------------------------------------------------------------------------------*/

TYPED_TEST(DeltaRecordFixture, InsertionSortOrderGivenRecordsAndRemoveDuplication)
{
  TestFixture::VerifyAddByInsertionSortTo();
}

}  // namespace dbgroup::index::bw_tree::component::test
