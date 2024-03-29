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
#include <memory>
#include <vector>

// external sources
#include "external/index-fixtures/common.hpp"
#include "gtest/gtest.h"

// local sources
#include "bw_tree/component/fixlen/node.hpp"
#include "bw_tree/component/varlen/node.hpp"

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

template <template <class K, class C> class NodeType, class KeyType, class PayloadType>
struct Target {
  using Key = KeyType;
  using Payload = PayloadType;
  using Node = NodeType<typename Key::Data, typename Key::Comp>;
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

  // define type aliases for simplicity
  using Node_t = typename Target::Node;

 protected:
  /*####################################################################################
   * Setup/Teardown
   *##################################################################################*/

  void
  SetUp() override
  {
  }

  void
  TearDown() override
  {
  }

  /*####################################################################################
   * Utilities for testing
   *##################################################################################*/

  static auto
  GetPage()  //
      -> void *
  {
    return ::dbgroup::memory::Allocate<NodePage>();
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
    EXPECT_EQ(0, node->GetLowKeyLen());
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/
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
using VarLenNode = varlen::Node<K, C>;

template <class K, class C>
using FixLenNode = fixlen::Node<K, C>;

using TestTargets = ::testing::Types<        //
    Target<VarLenNode, UInt8, UInt8>,        // fixed-length keys
    Target<VarLenNode, UInt4, UInt8>,        // small keys
    Target<VarLenNode, UInt8, UInt4>,        // small payloads
    Target<VarLenNode, UInt4, UInt4>,        // small keys/payloads
    Target<VarLenNode, Var, UInt8>,          // variable-length keys
    Target<VarLenNode, Ptr, Ptr>,            // pointer key/payload
    Target<VarLenNode, Original, Original>,  // original type key/payload
    Target<FixLenNode, UInt8, UInt8>,        // fixed-length keys
    Target<FixLenNode, UInt4, UInt8>,        // small keys
    Target<FixLenNode, UInt8, UInt4>,        // small payloads
    Target<FixLenNode, UInt4, UInt4>,        // small keys/payloads
    Target<FixLenNode, Ptr, Ptr>,            // pointer key/payload
    Target<FixLenNode, Original, Original>   // original type key/payload
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

}  // namespace dbgroup::index::bw_tree::component::test
