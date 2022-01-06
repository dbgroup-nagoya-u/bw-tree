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
/*######################################################################################
 * Classes for templated testing
 *####################################################################################*/

template <class KeyType, class PayloadType>
struct KeyPayload {
  using Key = KeyType;
  using Payload = PayloadType;
};

/*######################################################################################
 * Global constants
 *####################################################################################*/

constexpr auto kHeaderLen = component::kHeaderLength;
constexpr size_t kGCTime = 1000;
constexpr bool kExpectSuccess = true;
constexpr bool kExpectFailed = false;
constexpr bool kRangeClosed = true;
constexpr bool kRangeOpened = false;

/*######################################################################################
 * Fixture class definition
 *####################################################################################*/

template <class KeyPayload>
class BwTreeFixture : public testing::Test
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  // extract key-payload types
  using Key = typename KeyPayload::Key::Data;
  using Payload = typename KeyPayload::Payload::Data;
  using KeyComp = typename KeyPayload::Key::Comp;
  using PayloadComp = typename KeyPayload::Payload::Comp;

  // define type aliases for simplicity
  using Node_t = component::Node<Key, std::less<Key>>;
  using Metadata = component::Metadata;
  using BwTree_t = BwTree<Key, Payload, KeyComp>;

 protected:
  /*####################################################################################
   * Internal classes
   *##################################################################################*/

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

  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  static constexpr size_t kKeyLen = GetDataLength<Key>();
  static constexpr size_t kPayLen = GetDataLength<Payload>();
  static constexpr size_t kKeyNumForTest = 8 * 8192 * kThreadNum;

  /*####################################################################################
   * Setup/Teardown
   *##################################################################################*/

  void
  SetUp() override
  {
    PrepareTestData(keys_, kKeyNumForTest);
    PrepareTestData(payloads_, kKeyNumForTest);

    index_ = std::make_unique<BwTree_t>(kGCTime);
  }

  void
  TearDown() override
  {
    ReleaseTestData(keys_, kKeyNumForTest);
    ReleaseTestData(payloads_, kKeyNumForTest);

    index_.reset(nullptr);
  }

  /*####################################################################################
   * Utility functions
   *##################################################################################*/

  auto
  PerformWriteOperation(const Operation &ops)  //
      -> ReturnCode
  {
    const auto key = keys_[ops.key_id];
    const auto payload = payloads_[ops.payload_id];

    switch (ops.w_type) {
      case WriteType::kInsert:
        return index_->Insert(key, payload, kKeyLen, kPayLen);
      case WriteType::kUpdate:
        return index_->Update(key, payload, kKeyLen, kPayLen);
      case WriteType::kDelete:
        // return index_->Delete(key, kKeyLen);
      case WriteType::kWrite:
      default:
        return index_->Write(key, payload, kKeyLen, kPayLen);
    }
  }

  auto
  PrepareOperation(  //
      const WriteType w_type,
      std::mt19937_64 &rand_engine)  //
      -> Operation
  {
    const auto id = id_dist_(rand_engine);

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
      const std::shared_lock<std::shared_mutex> guard{main_lock_};

      // prepare operations to be executed
      std::mt19937_64 rand_engine{rand_seed};
      for (size_t i = 0; i < write_num; ++i) {
        operations.emplace_back(PrepareOperation(w_type, rand_engine));
      }
    }

    {  // wait for a main thread to release a lock
      const std::shared_lock<std::shared_mutex> lock{worker_lock_};

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

  auto
  RunOverMultiThread(  //
      const size_t write_num,
      const size_t thread_num,
      const WriteType w_type)  //
      -> std::vector<size_t>
  {
    std::vector<std::future<std::vector<size_t>>> futures;

    {  // create a lock to prevent workers from executing
      const std::unique_lock<std::shared_mutex> guard{worker_lock_};

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
      const std::unique_lock<std::shared_mutex> lock{main_lock_};
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

  /*####################################################################################
   * Functions for verification
   *##################################################################################*/

  void
  VerifyRead(  //
      const size_t key_id,
      const size_t expected_id,
      const bool expect_success = true)
  {
    const auto read_val = index_->Read(keys_[key_id]);
    if (expect_success) {
      EXPECT_TRUE(read_val);

      const auto expected_val = payloads_[expected_id];
      const auto actual_val = read_val.value();
      EXPECT_TRUE(component::IsEqual<PayloadComp>(expected_val, actual_val));
      if constexpr (IsVariableLengthData<Payload>()) {
        delete actual_val;
      }
    } else {
      EXPECT_FALSE(read_val);
    }
  }

  void
  VerifyWrite()
  {
    const size_t write_num = (kKeyNumForTest - 1) / kThreadNum;

    auto &&written_ids = RunOverMultiThread(write_num, kThreadNum, WriteType::kWrite);
    for (auto &&id : written_ids) {
      VerifyRead(id, id);
    }
  }

  void
  VerifyInsert()
  {
    const size_t write_num = (kKeyNumForTest - 1) / kThreadNum;

    auto &&written_ids = RunOverMultiThread(write_num, kThreadNum, WriteType::kInsert);
    for (auto &&id : written_ids) {
      VerifyRead(id, id);
    }

    written_ids = RunOverMultiThread(write_num, kThreadNum, WriteType::kInsert);
    EXPECT_EQ(0, written_ids.size());
  }

  void
  VerifyUpdate()
  {
    const size_t write_num = (kKeyNumForTest - 1) / kThreadNum;

    auto &&written_ids = RunOverMultiThread(write_num, kThreadNum, WriteType::kUpdate);
    EXPECT_EQ(0, written_ids.size());

    written_ids = RunOverMultiThread(write_num, kThreadNum, WriteType::kInsert);
    auto &&updated_ids = RunOverMultiThread(write_num, kThreadNum, WriteType::kUpdate);

    std::sort(updated_ids.begin(), updated_ids.end());
    updated_ids.erase(std::unique(updated_ids.begin(), updated_ids.end()), updated_ids.end());

    EXPECT_EQ(written_ids.size(), updated_ids.size());
    for (auto &&id : updated_ids) {
      VerifyRead(id, id + 1);
    }
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  // actual keys and payloads
  Key keys_[kKeyNumForTest];
  Payload payloads_[kKeyNumForTest];

  // a test target BzTree
  std::unique_ptr<BwTree_t> index_{nullptr};

  std::uniform_int_distribution<size_t> id_dist_{0, kKeyNumForTest - 2};

  std::shared_mutex main_lock_{};

  std::shared_mutex worker_lock_{};
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
TYPED_TEST_SUITE(BwTreeFixture, KeyPayloadPairs);

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

TYPED_TEST(BwTreeFixture, Write_MultiThreads_ReadWrittenPayloads)
{  //
  TestFixture::VerifyWrite();
}

TYPED_TEST(BwTreeFixture, InsertWithMultiThreadsReadInsertedPayloads)
{  //
  TestFixture::VerifyInsert();
}

TYPED_TEST(BwTreeFixture, UpdateWithMultiThreadsReadUpdatedPayloads)
{  //
  TestFixture::VerifyUpdate();
}

}  // namespace dbgroup::index::bw_tree::test
