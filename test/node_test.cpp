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
// use a supper template to define key-payload pair templates
template <class KeyType, class PayloadType, class KeyComparator, class PayloadComparator>
struct KeyPayload {
  using Key = KeyType;
  using Payload = PayloadType;
  using KeyComp = KeyComparator;
  using PayloadComp = PayloadComparator;
};

template <class KeyPayload>
class NodeFixture : public testing::Test
{
 protected:
  // extract key-payload types
  using Key = typename KeyPayload::Key;
  using Payload = typename KeyPayload::Payload;
  using KeyComp = typename KeyPayload::KeyComp;
  using PayloadComp = typename KeyPayload::PayloadComp;

  // define type aliases for simplicity
  using Node_t = Node<Key, Payload, std::less<Key>>;

  /*################################################################################################
   * Internal constants
   *##############################################################################################*/

  static constexpr size_t kRecordLength = sizeof(Key) + sizeof(Payload);
  static constexpr size_t kMaxRecordNum =
      (kPageSize - kHeaderLength) / (kRecordLength + sizeof(Metadata));

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  std::vector<Metadata> meta_vec;

  std::unique_ptr<Node_t> node;

  /*################################################################################################
   * Setup/Teardown
   *##############################################################################################*/

  void
  SetUp() override
  {
  }

  void
  TearDown() override
  {
  }

  /*################################################################################################
   * Functions for verification
   *##############################################################################################*/

  void
  VerifyConstructor(  //
      const bool is_leaf,
      const bool is_delta)
  {
    const auto node_type = (is_leaf) ? NodeType::kLeaf : NodeType::kInternal;

    if (is_delta) {
      node = std::make_unique<Node_t>(node_type, DeltaNodeType::kInsert, nullptr);

      EXPECT_EQ(DeltaNodeType::kInsert, node->GetDeltaNodeType());
    } else {
      node = std::make_unique<Node_t>(node_type, 0, nullptr);

      EXPECT_EQ(DeltaNodeType::kNotDelta, node->GetDeltaNodeType());
      EXPECT_EQ(0, node->GetRecordCount());
    }
    EXPECT_FALSE(is_leaf ^ node->IsLeaf());
    EXPECT_EQ(nullptr, node->GetNextNode());
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
TYPED_TEST_CASE(NodeFixture, KeyPayloadPairs);

/*##################################################################################################
 * Unit test definitions
 *################################################################################################*/

/*--------------------------------------------------------------------------------------------------
 * Constructor tests
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(NodeFixture, Construct_LeafBaseNode_CorrectlyInitialized)
{
  TestFixture::VerifyConstructor(true, false);
}

TYPED_TEST(NodeFixture, Construct_InternalBaseNode_CorrectlyInitialized)
{
  TestFixture::VerifyConstructor(false, false);
}

TYPED_TEST(NodeFixture, Construct_LeafDeltaNode_CorrectlyInitialized)
{
  TestFixture::VerifyConstructor(true, true);
}

TYPED_TEST(NodeFixture, Construct_InternalDeltaNode_CorrectlyInitialized)
{
  TestFixture::VerifyConstructor(false, true);
}

/*--------------------------------------------------------------------------------------------------
 * Getter/setter tests
 *------------------------------------------------------------------------------------------------*/

}  // namespace dbgroup::index::bw_tree::component::test
