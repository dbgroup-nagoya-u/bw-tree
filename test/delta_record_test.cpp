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

#include "bw_tree/component/delta_record.hpp"

#include <memory>
#include <vector>

#include "common.hpp"
#include "gtest/gtest.h"

namespace dbgroup::index::bw_tree::component::test
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
 * Fixture class definition
 *####################################################################################*/

template <class KeyPayload>
class DeltaRecordFixture : public testing::Test
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  // extract key-payload types
  using Key = typename KeyPayload::Key::Data;
  using Payload = typename KeyPayload::Payload::Data;
  using KeyComp = typename KeyPayload::Key::Comp;
  using PayComp = typename KeyPayload::Payload::Comp;

  // define type aliases for simplicity
  using DeltaRecord_t = DeltaRecord<Key, std::less<>>;

 protected:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  static constexpr size_t kKeyLen = GetDataLength<Key>();
  static constexpr size_t kPayLen = GetDataLength<Payload>();
  static constexpr size_t kRecLen = kKeyLen + kPayLen;
  static constexpr size_t kMaxRecNum = (kPageSize - kHeaderLength) / (kRecLen + sizeof(Metadata));
  static constexpr size_t kKeyNumForTest = kMaxRecNum;

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
    return ::operator new(GetMaxDeltaSize<Key, Payload>());
  }

  auto
  CreateLeafRecord(  //
      const DeltaType type,
      const Key &key,
      const Payload &payload)  //
      -> std::unique_ptr<DeltaRecord_t>
  {
    auto *raw_p = new (GetPage()) DeltaRecord_t{type, key, kKeyLen, payload, kPayLen};
    return std::unique_ptr<DeltaRecord_t>{raw_p};
  }

  auto
  CreateSplitRecord(  //
      const DeltaType type,
      const Key &key,
      const Payload &payload)  //
      -> std::unique_ptr<DeltaRecord_t>
  {
    auto *raw_p = new (GetPage()) DeltaRecord_t{type, key, kKeyLen, payload, kPayLen};
    return std::unique_ptr<DeltaRecord_t>{raw_p};
  }

  /*####################################################################################
   * Functions for verification
   *##################################################################################*/

  void
  VerifyLeafConstructor(const DeltaType type)
  {
    const auto &key = keys_[0];
    const auto &payload = payloads_[0];
    const auto &delta = CreateLeafRecord(type, key, payload);

    EXPECT_TRUE(delta->IsLeaf());
    EXPECT_FALSE(delta->IsBaseNode());
    EXPECT_EQ(kNullPtr, delta->GetNext());
    EXPECT_TRUE(IsEqual<KeyComp>(key, delta->GetKey()));
    EXPECT_TRUE(IsEqual<PayComp>(payload, delta->template GetPayload<Payload>()));
  }

  void
  VerifySplitDeltaConstructor()
  {
    const auto &key = keys_[0];
    const auto &right_node = CreateLeafRecord(DeltaType::kNotDelta, key, payloads_[0]);
    const auto right_ptr = reinterpret_cast<uintptr_t>(right_node.get());
    std::atomic_uintptr_t right_page{right_ptr};
    auto sib_page = reinterpret_cast<uintptr_t>(&right_page);

    // verify split delta
    auto *raw_p = new (GetPage()) DeltaRecord_t{kSplit, right_node.get(), &right_page, sib_page};
    std::unique_ptr<DeltaRecord_t> delta{raw_p};

    EXPECT_TRUE(delta->IsLeaf());
    EXPECT_FALSE(delta->IsBaseNode());
    EXPECT_EQ(sib_page, delta->GetNext());
    EXPECT_TRUE(IsEqual<KeyComp>(key, delta->GetKey()));
    EXPECT_EQ(sib_page, delta->template GetPayload<uintptr_t>());

    // verify index-entry delta
    raw_p = new (GetPage()) DeltaRecord_t{delta.get()};
    delta.reset(raw_p);

    EXPECT_FALSE(delta->IsLeaf());
    EXPECT_FALSE(delta->IsBaseNode());
    EXPECT_TRUE(IsEqual<KeyComp>(key, delta->GetKey()));
    EXPECT_EQ(sib_page, delta->template GetPayload<uintptr_t>());
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

using KeyPayloadPairs = ::testing::Types<  //
    KeyPayload<UInt8, UInt8>,              // both fixed
    KeyPayload<Var, UInt8>,                // variable-fixed
    KeyPayload<UInt8, Var>,                // fixed-variable
    KeyPayload<Var, Var>,                  // both variable
    KeyPayload<Ptr, Ptr>,                  // pointer key/payload
    KeyPayload<Original, Original>         // original key/payload
    >;
TYPED_TEST_SUITE(DeltaRecordFixture, KeyPayloadPairs);

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

/*--------------------------------------------------------------------------------------
 * Constructor tests
 *------------------------------------------------------------------------------------*/

TYPED_TEST(DeltaRecordFixture, ConstructLeafDeltas)
{
  TestFixture::VerifyLeafConstructor(DeltaType::kInsert);
  TestFixture::VerifyLeafConstructor(DeltaType::kModify);
  TestFixture::VerifyLeafConstructor(DeltaType::kDelete);
}

TYPED_TEST(DeltaRecordFixture, ConstructSplitDeltas)
{  //
  TestFixture::VerifySplitDeltaConstructor();
}

}  // namespace dbgroup::index::bw_tree::component::test
