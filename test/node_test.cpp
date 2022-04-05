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

#include "bw_tree/component/varlen/node.hpp"

#include <memory>
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
class NodeFixture : public testing::Test
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
  using Node_t = typename Target::Tree::template Node<Key, KeyComp>;

 protected:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

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
    EXPECT_EQ(0, node->GetRecordCount());
    EXPECT_EQ(nullptr, node->GetNext());
    EXPECT_FALSE(node->GetLowKey());
  }

  void
  VerifyNewNodeConstructor()
  {
    constexpr bool kDummyFlag = false;
    auto *raw_p = new (GetPage()) Node_t{kLeaf, kPageSize, kDummyFlag};
    std::unique_ptr<Node_t> node{raw_p};

    EXPECT_TRUE(node->IsLeaf());
    EXPECT_EQ(0, node->GetRecordCount());
    EXPECT_EQ(nullptr, node->GetNext());
    EXPECT_FALSE(node->GetLowKey());
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
TYPED_TEST_SUITE(NodeFixture, TestTargets);

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

/*--------------------------------------------------------------------------------------
 * Constructor tests
 *------------------------------------------------------------------------------------*/

TYPED_TEST(NodeFixture, ConstructedInitialRootHasExpectedValues)
{  //
  TestFixture::VerifyInitialRootConstructor();
}

TYPED_TEST(NodeFixture, ConstructedNewNodeHasExpectedValues)
{  //
  TestFixture::VerifyNewNodeConstructor();
}

}  // namespace dbgroup::index::bw_tree::component::test
