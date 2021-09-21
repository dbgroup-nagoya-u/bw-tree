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

  static constexpr size_t kKeyLength = GetDataLength<Key>();
  static constexpr size_t kPayloadLength = GetDataLength<Payload>();
  static constexpr size_t kRecordLength = kKeyLength + kPayloadLength;
  static constexpr size_t kMaxRecordNum =
      (kPageSize - component::kHeaderLength) / (kRecordLength + sizeof(Metadata));
  static constexpr size_t kKeyNumForTest = 1024;

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  // actual keys and payloads
  Key keys[kKeyNumForTest];
  Payload payloads[kKeyNumForTest];

  // a target node and its expected metadata
  Node_t *node;
  std::vector<Metadata> meta_vec;

  /*################################################################################################
   * Setup/Teardown
   *##############################################################################################*/

  void
  SetUp() override
  {
    node = Node_t::CreateNode(kPageSize, NodeType::kLeaf, kMaxRecordNum, nullptr);
    PrepareTestData(keys, kKeyNumForTest);
    PrepareTestData(payloads, kKeyNumForTest);
  }

  void
  TearDown() override
  {
    Node_t::DeleteNode(node);

    ReleaseTestData(keys, kKeyNumForTest);
    ReleaseTestData(payloads, kKeyNumForTest);
  }

  /*################################################################################################
   * Functions for verification
   *##############################################################################################*/

  void
  VerifySetterGetter()
  {
    meta_vec.reserve(kMaxRecordNum);

    // set records and keep their metadata
    size_t offset = kPageSize;
    for (size_t i = 0; i < kMaxRecordNum; ++i) {
      // set a record
      node->SetPayload(offset, payloads[i], kPayloadLength);
      node->SetKey(offset, GetAddr(keys[i]), kKeyLength);
      node->SetMetadata(i, Metadata{offset, kKeyLength, kRecordLength});

      // keep metadata for verification
      meta_vec.emplace_back(offset, kKeyLength, kRecordLength);
    }

    // verify records and their metadata
    for (size_t i = 0; i < kMaxRecordNum; ++i) {
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
    const auto key = node->GetKeyAddr(meta);
    const auto result = IsEqual<Key, KeyComp>(key, GetAddr(keys[idx]));
    EXPECT_TRUE(result);
  }

  void
  VerifyPayload(  //
      const size_t idx,
      const Metadata meta)
  {
    Payload payload{};
    node->CopyPayload(meta, payload);

    const auto result = IsEqual<Payload, PayloadComp>(GetAddr(payload), GetAddr(payloads[idx]));
    EXPECT_TRUE(result);

    if constexpr (IsVariableLengthData<Payload>()) {
      ::operator delete(payload);
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
 * Getter/setter tests
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(NodeFixture, SetterGetter_EmptyNode_CorrectlySetAndGet)
{
  TestFixture::VerifySetterGetter();
}

}  // namespace dbgroup::index::bw_tree::component::test
