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

#include <future>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

#include "bw_tree/bw_tree.hpp"
#include "common.hpp"
#include "gtest/gtest.h"

namespace dbgroup::index::bw_tree::test
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
class BwTreeFixture : public testing::Test
{
 protected:
  // extract key-payload types
  using Key = typename KeyPayload::Key;
  using Payload = typename KeyPayload::Payload;
  using KeyComp = typename KeyPayload::KeyComp;
  using PayloadComp = typename KeyPayload::PayloadComp;

  // define type aliases for simplicity
  using Node_t = component::Node<Key, std::less<Key>>;
  using Metadata = component::Metadata;
  using BwTree_t = BwTree<Key, Payload, KeyComp>;

  /*################################################################################################
   * Internal constants
   *##############################################################################################*/

  static constexpr size_t kKeyLength = GetDataLength<Key>();
  static constexpr size_t kPayloadLength = GetDataLength<Payload>();
  static constexpr size_t kRecordLength = kKeyLength + kPayloadLength;
  static constexpr size_t kMaxRecordNum =
      (kPageSize - component::kHeaderLength) / (kRecordLength + sizeof(Metadata));
  static constexpr size_t kKeyNumForTest = kMaxRecordNum * kMaxRecordNum + 1;
  static constexpr size_t kSmallKeyNum = kMaxDeltaNodeNum - 1;

  enum WriteType
  {
    kWrite,
    kInsert,
    kUpdate,
    kDelete
  };

  struct Operation {
    WriteType w_type;
    size_t key_id;
    size_t payload_id;
  };

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  // actual keys and payloads
  Key keys[kKeyNumForTest];
  Payload payloads[kKeyNumForTest];

  // a target node and its expected metadata
  std::unique_ptr<BwTree_t> bw_tree;

  std::uniform_int_distribution<size_t> id_dist{0, kKeyNumForTest - 2};

  std::shared_mutex main_lock;

  std::shared_mutex worker_lock;

  /*################################################################################################
   * Setup/Teardown
   *##############################################################################################*/

  void
  SetUp() override
  {
    bw_tree = std::make_unique<BwTree_t>(1000);
    PrepareTestData(keys, kKeyNumForTest);
    PrepareTestData(payloads, kKeyNumForTest);
  }

  void
  TearDown() override
  {
    ReleaseTestData(keys, kKeyNumForTest);
    ReleaseTestData(payloads, kKeyNumForTest);
  }

  /*################################################################################################
   * Utility functions
   *##############################################################################################*/

  ReturnCode
  PerformWriteOperation(const Operation &ops)
  {
    const auto key = keys[ops.key_id];
    const auto payload = payloads[ops.payload_id];

    switch (ops.w_type) {
      case kInsert:
        // return bw_tree->Insert(key, payload, kKeyLength, kPayloadLength);
      case kUpdate:
        // return bw_tree->Update(key, payload, kKeyLength, kPayloadLength);
      case kDelete:
        // return bw_tree->Delete(key, kKeyLength);
      case kWrite:
        break;
    }
    return bw_tree->Write(key, payload, kKeyLength, kPayloadLength);
  }

  Operation
  PrepareOperation(  //
      const WriteType w_type,
      std::mt19937_64 &rand_engine)
  {
    const auto id = id_dist(rand_engine);

    switch (w_type) {
      case kWrite:
      case kInsert:
      case kDelete:
        break;
      case kUpdate:
        return Operation{w_type, id, id + 1};
    }
    return Operation{w_type, id, id};
  }

  void
  WriteRandomKeys(  //
      const size_t write_num,
      const WriteType w_type,
      const size_t rand_seed,
      std::promise<std::vector<size_t>> p)
  {
    std::vector<Operation> operations;
    std::vector<size_t> written_ids;
    operations.reserve(write_num);
    written_ids.reserve(write_num);

    {  // create a lock to prevent a main thread
      const std::shared_lock<std::shared_mutex> guard{main_lock};

      // prepare operations to be executed
      std::mt19937_64 rand_engine{rand_seed};
      for (size_t i = 0; i < write_num; ++i) {
        operations.emplace_back(PrepareOperation(w_type, rand_engine));
      }
    }

    {  // wait for a main thread to release a lock
      const std::shared_lock<std::shared_mutex> lock{worker_lock};

      // perform and gather results
      for (auto &&ops : operations) {
        if (PerformWriteOperation(ops) == ReturnCode::kSuccess) {
          written_ids.emplace_back(ops.key_id);
        }
      }
    }

    // return results via promise
    p.set_value(std::move(written_ids));
  }

  std::vector<size_t>
  RunOverMultiThread(  //
      const size_t write_num,
      const size_t thread_num,
      const WriteType w_type)
  {
    std::vector<std::future<std::vector<size_t>>> futures;

    {  // create a lock to prevent workers from executing
      const std::unique_lock<std::shared_mutex> guard{worker_lock};

      // run a function over multi-threads with promise
      std::mt19937_64 rand_engine(kRandomSeed);
      for (size_t i = 0; i < thread_num; ++i) {
        std::promise<std::vector<size_t>> p;
        futures.emplace_back(p.get_future());
        const auto rand_seed = rand_engine();
        std::thread{
            &BwTreeFixture::WriteRandomKeys, this, write_num, w_type, rand_seed, std::move(p)}
            .detach();
      }

      // wait for all workers to finish initialization
      const std::unique_lock<std::shared_mutex> lock{main_lock};
    }

    // gather results via promise-future
    std::vector<size_t> written_ids;
    written_ids.reserve(write_num * thread_num);
    for (auto &&future : futures) {
      auto tmp_written = future.get();
      written_ids.insert(written_ids.end(), tmp_written.begin(), tmp_written.end());
    }

    return written_ids;
  }

  /*################################################################################################
   * Functions for verification
   *##############################################################################################*/

  void
  VerifyRead(  //
      const size_t key_id,
      const size_t expected_id,
      const bool expect_fail = false)
  {
    const auto [rc, actual] = bw_tree->Read(keys[key_id]);

    if (expect_fail) {
      EXPECT_EQ(ReturnCode::kKeyNotExist, rc);
    } else {
      bool result;
      EXPECT_EQ(ReturnCode::kSuccess, rc);
      if constexpr (IsVariableLengthData<Payload>()) {
        result = component::IsEqual<Payload, PayloadComp>(payloads[expected_id], actual.get());
      } else {
        result = component::IsEqual<Payload, PayloadComp>(component::GetAddr(payloads[expected_id]),
                                                          component::GetAddr(actual));
      }
      EXPECT_TRUE(result);
    }
  }

  void
  VerifyWrite()
  {
    const size_t write_num = (kKeyNumForTest - 1) / kThreadNum;

    auto written_ids = RunOverMultiThread(write_num, kThreadNum, WriteType::kWrite);
    for (auto &&id : written_ids) {
      VerifyRead(id, id);
    }
  }
};

/*##################################################################################################
 * Preparation for typed testing
 *################################################################################################*/

using KeyPayloadPairs = ::testing::Types<KeyPayload<uint64_t,
                                                    uint64_t,
                                                    UInt64Comp,
                                                    UInt64Comp>>;  //,
//                                          KeyPayload<char *, uint64_t, CStrComp, UInt64Comp>,
//                                          KeyPayload<uint64_t, char *, UInt64Comp, CStrComp>,
//                                          KeyPayload<char *, char *, CStrComp, CStrComp>,
//                                          KeyPayload<uint32_t, uint64_t, UInt32Comp, UInt64Comp>,
//                                          KeyPayload<uint32_t, uint32_t, UInt32Comp, UInt32Comp>,
//                                          KeyPayload<uint64_t, uint64_t *, UInt64Comp, PtrComp>>;
TYPED_TEST_CASE(BwTreeFixture, KeyPayloadPairs);

/*##################################################################################################
 * Unit test definitions
 *################################################################################################*/

TYPED_TEST(BwTreeFixture, Write_MultiThreads_ReadWrittenPayloads)
{  //
  TestFixture::VerifyWrite();
}

}  // namespace dbgroup::index::bw_tree::test
