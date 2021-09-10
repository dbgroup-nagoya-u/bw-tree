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
  using Node_t = Node<Key, std::less<Key>>;

  /*################################################################################################
   * Internal constants
   *##############################################################################################*/

  static constexpr size_t kKeyNumForTest = 1024;
  static constexpr size_t kKeyLength = kWordLength;
  static constexpr size_t kPayloadLength = kWordLength;

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  // actual keys and payloads
  size_t key_length;
  size_t payload_length;
  Key keys[kKeyNumForTest];
  Payload payloads[kKeyNumForTest];

  // the length of a record and its maximum number
  size_t record_length;
  size_t max_record_num;

  // a target node and its expected metadata
  std::unique_ptr<Node_t> node;
  std::vector<Metadata> meta_vec;

  /*################################################################################################
   * Setup/Teardown
   *##############################################################################################*/

  void
  SetUp() override
  {
    // prepare keys
    key_length = (IsVariableLengthData<Key>()) ? 7 : sizeof(Key);
    PrepareTestData(keys, kKeyNumForTest, key_length);

    // prepare payloads
    payload_length = (IsVariableLengthData<Payload>()) ? 7 : sizeof(Payload);
    PrepareTestData(payloads, kKeyNumForTest, payload_length);

    // set a record length and its maximum number
    record_length = key_length + payload_length;
    max_record_num = (kPageSize - kHeaderLength) / (record_length + sizeof(Metadata));
    if (max_record_num > kKeyNumForTest) max_record_num = kKeyNumForTest;
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
  VerifyConstructor(  //
      const bool is_leaf,
      const bool is_delta)
  {
    const auto node_type = (is_leaf) ? NodeType::kLeaf : NodeType::kInternal;

    if (is_delta) {
      node = std::make_unique<Node_t>(node_type, DeltaNodeType::kInsert);

      EXPECT_EQ(DeltaNodeType::kInsert, node->GetDeltaNodeType());
    } else {
      node = std::make_unique<Node_t>(node_type, 0, nullptr);

      EXPECT_EQ(DeltaNodeType::kNotDelta, node->GetDeltaNodeType());
      EXPECT_EQ(0, node->GetRecordCount());
      EXPECT_EQ(nullptr, node->GetNextNode());
    }

    EXPECT_FALSE(is_leaf ^ node->IsLeaf());
  }

  void
  VerifySetterGetter()
  {
    meta_vec.reserve(max_record_num);

    // creat an empty node
    node.reset(new (malloc(kPageSize)) Node_t{NodeType::kLeaf, max_record_num, nullptr});

    // set records and keep their metadata
    size_t offset = kPageSize;
    for (size_t i = 0; i < max_record_num; ++i) {
      // set a record
      node->SetPayload(offset, payloads[i], payload_length);
      node->SetKey(offset, keys[i], key_length);
      node->SetMetadata(i, offset, key_length, record_length);

      // keep metadata for verification
      meta_vec.emplace_back(offset, key_length, record_length);
    }

    // verify records and their metadata
    for (size_t i = 0; i < max_record_num; ++i) {
      const auto meta = node->GetMetadata(i);
      EXPECT_EQ(meta_vec.at(i), meta);
      VerifyKey(i, meta);
      VerifyPayload(i, meta);
    }
  }

  void
  VerifyKey(  //
      const size_t idx,
      const Metadata meta)
  {
    auto key = node->GetKey(meta);
    EXPECT_TRUE(IsEqual<KeyComp>(key, keys[idx]));
  }

  void
  VerifyPayload(  //
      const size_t idx,
      const Metadata meta)
  {
    Payload payload{};
    node->CopyPayload(meta, payload);
    EXPECT_TRUE(IsEqual<PayloadComp>(payload, payloads[idx]));

    if constexpr (IsVariableLengthData<Payload>()) {
      ::dbgroup::memory::Delete(payload);
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

TYPED_TEST(NodeFixture, SetterGetter_EmptyNode_CorrectlySetAndGet)
{
  TestFixture::VerifySetterGetter();
}

}  // namespace dbgroup::index::bw_tree::component::test
