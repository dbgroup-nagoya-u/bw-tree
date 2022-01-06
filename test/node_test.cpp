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

#include "bw_tree/component/node.hpp"

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
class NodeFixture : public testing::Test
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
  using Node_t = Node<Key, std::less<>>;

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
    return ::operator new(kPageSize);
  }

  /*####################################################################################
   * Functions for verification
   *##################################################################################*/

  void
  VerifyInitialRootConstructor()
  {
    auto *raw_p = new (GetPage()) Node_t{};
    std::unique_ptr<Node_t> node{raw_p};

    EXPECT_TRUE(node->IsLeaf());
    EXPECT_TRUE(node->IsBaseNode());
    EXPECT_EQ(nullptr, node->GetSiblingNode());
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
TYPED_TEST_SUITE(NodeFixture, KeyPayloadPairs);

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

/*--------------------------------------------------------------------------------------
 * Constructor tests
 *------------------------------------------------------------------------------------*/

TYPED_TEST(NodeFixture, ConstructInitialRoot)
{  //
  TestFixture::VerifyInitialRootConstructor();
}

}  // namespace dbgroup::index::bw_tree::component::test
