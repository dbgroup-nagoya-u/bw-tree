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

constexpr size_t kN = 1e5;
constexpr size_t kKeyNumForTest = kN * kThreadNum / 2;
constexpr size_t kGCTime = 100000;
constexpr bool kExpectSuccess = true;
constexpr bool kExpectFailed = false;

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
  using Node_t = component::Node<Key, std::less<>>;
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
    constexpr Operation(  //
        const WriteType type,
        const size_t k_id,
        const size_t p_id)
        : w_type{type}, key_id{k_id}, payload_id{p_id}
    {
    }

    WriteType w_type{};
    size_t key_id{};
    size_t payload_id{};
  };

  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  static constexpr size_t kKeyLen = GetDataLength<Key>();
  static constexpr size_t kPayLen = GetDataLength<Payload>();

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
    const auto &key = keys_[ops.key_id];
    const auto &payload = payloads_[ops.payload_id];

    switch (ops.w_type) {
      case kInsert:
        return index_->Insert(key, payload, kKeyLen);
      case kUpdate:
        return index_->Update(key, payload, kKeyLen);
      case kDelete:
        return index_->Delete(key, kKeyLen);
      case kWrite:
      default:
        return index_->Write(key, payload, kKeyLen);
    }
  }

  void
  RunWorker(  //
      const WriteType w_type,
      const size_t rand_seed,
      std::promise<std::vector<size_t>> p)
  {
    std::vector<Operation> operations{};
    std::vector<size_t> written_ids{};
    operations.reserve(kN);
    written_ids.reserve(kN);

    {  // create a lock to prevent a main thread
      const std::shared_lock guard{main_lock_};

      // prepare operations to be executed
      std::mt19937_64 rand_engine{rand_seed};
      for (size_t i = 0; i < kN; ++i) {
        const auto id = id_dist_(rand_engine);
        switch (w_type) {
          case kUpdate:
            operations.emplace_back(w_type, id, id + 1);
            break;
          case kWrite:
          case kInsert:
          case kDelete:
          default:
            operations.emplace_back(w_type, id, id);
        }
      }
    }

    {  // wait for a main thread to release a lock
      const std::shared_lock lock{worker_lock_};

      // perform and gather results
      for (const auto &ops : operations) {
        if (PerformWriteOperation(ops) == kSuccess) {
          written_ids.emplace_back(ops.key_id);
        }
      }
    }

    // return results via promise
    p.set_value(std::move(written_ids));
  }

  auto
  RunOverMultiThread(const WriteType w_type)  //
      -> std::vector<size_t>
  {
    std::vector<std::future<std::vector<size_t>>> futures{};

    {  // create a lock to prevent workers from executing
      const std::unique_lock guard{worker_lock_};

      // run a function over multi-threads with promise
      std::mt19937_64 rand_engine{kRandomSeed};
      for (size_t i = 0; i < kThreadNum; ++i) {
        std::promise<std::vector<size_t>> p{};
        futures.emplace_back(p.get_future());
        const auto seed = rand_engine();
        std::thread{&BwTreeFixture::RunWorker, this, w_type, seed, std::move(p)}.detach();
      }

      // wait for all workers to finish initialization
      const std::unique_lock lock{main_lock_};
    }

    // gather results via promise-future
    std::vector<size_t> written_ids{};
    written_ids.reserve(kN * kThreadNum);
    for (auto &&future : futures) {
      const auto &tmp_written = future.get();
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
      const bool expect_success)
  {
    const auto &read_val = index_->Read(keys_[key_id]);
    if (expect_success) {
      EXPECT_TRUE(read_val);

      const auto &expected_val = payloads_[expected_id];
      const auto &actual_val = read_val.value();
      EXPECT_TRUE(component::IsEqual<PayloadComp>(expected_val, actual_val));
    } else {
      EXPECT_FALSE(read_val);
    }
  }

  void
  VerifyWrittenValuesWithMultiThreads(  //
      const std::vector<size_t> &written_ids,
      const WriteType w_type)
  {
    auto f = [&](const size_t begin_pos, const size_t n) {
      const auto end_pos = begin_pos + n;
      for (size_t i = begin_pos; i < end_pos; ++i) {
        const auto id = written_ids.at(i);
        switch (w_type) {
          case kDelete:
            VerifyRead(id, id, kExpectFailed);
            break;
          case kUpdate:
            VerifyRead(id, id + 1, kExpectSuccess);
            break;
          case kWrite:
          case kInsert:
          default:
            VerifyRead(id, id, kExpectSuccess);
            break;
        }
      }
    };

    const auto vec_size = written_ids.size();
    std::vector<std::thread> threads{};
    threads.reserve(kThreadNum);

    size_t begin_pos = 0;
    for (size_t i = 0; i < kThreadNum; ++i) {
      const size_t n = (vec_size + i) / kThreadNum;
      threads.emplace_back(f, begin_pos, n);
    }
    for (auto &&t : threads) {
      t.join();
    }
  }

  void
  VerifyWrite()
  {
    // reading written records succeeds
    auto &&written_ids = RunOverMultiThread(kWrite);
    VerifyWrittenValuesWithMultiThreads(written_ids, kWrite);
  }

  void
  VerifyInsert()
  {
    // insert operations do not insert duplicated records
    auto &&written_ids = RunOverMultiThread(kInsert);
    std::sort(written_ids.begin(), written_ids.end());
    const auto &end_it = std::unique(written_ids.begin(), written_ids.end());
    EXPECT_EQ(std::distance(end_it, written_ids.end()), 0);

    // reading inserted records succeeds
    VerifyWrittenValuesWithMultiThreads(written_ids, kInsert);

    // inserting duplicate records fails
    written_ids = RunOverMultiThread(kInsert);
    EXPECT_EQ(0, written_ids.size());
  }

  void
  VerifyUpdate()
  {
    // updating not inserted records fails
    auto &&written_ids = RunOverMultiThread(kUpdate);
    EXPECT_EQ(0, written_ids.size());

    // updating inserted records succeeds
    written_ids = RunOverMultiThread(kInsert);
    auto &&updated_ids = RunOverMultiThread(kUpdate);
    // update operations may succeed multiple times, so remove duplications
    std::sort(updated_ids.begin(), updated_ids.end());
    updated_ids.erase(std::unique(updated_ids.begin(), updated_ids.end()), updated_ids.end());
    EXPECT_EQ(written_ids.size(), updated_ids.size());

    // updated values must be read
    VerifyWrittenValuesWithMultiThreads(written_ids, kUpdate);
  }

  void
  VerifyDelete()
  {
    // deleting not inserted records fails
    auto &&written_ids = RunOverMultiThread(kDelete);
    EXPECT_EQ(0, written_ids.size());

    // deleting inserted records succeeds
    written_ids = RunOverMultiThread(kInsert);
    auto &&deleted_ids = RunOverMultiThread(kDelete);
    EXPECT_EQ(written_ids.size(), deleted_ids.size());

    // reading deleted records fails
    VerifyWrittenValuesWithMultiThreads(written_ids, kDelete);
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  // actual keys and payloads
  Key keys_[kKeyNumForTest]{};
  Payload payloads_[kKeyNumForTest]{};

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
    KeyPayload<UInt8, UInt8>,              // fixed-length keys
    KeyPayload<UInt4, UInt8>,              // small keys
    KeyPayload<UInt8, UInt4>,              // small payloads
    KeyPayload<UInt4, UInt4>,              // small keys/payloads
    KeyPayload<Var, UInt8>,                // variable-length keys
    KeyPayload<Ptr, Ptr>,                  // pointer key/payload
    KeyPayload<Original, Original>         // original type key/payload
    >;
TYPED_TEST_SUITE(BwTreeFixture, KeyPayloadPairs);

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

TYPED_TEST(BwTreeFixture, WriteWithMultiThreadsReadWrittenPayloads)
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

TYPED_TEST(BwTreeFixture, DeleteWithMultiThreadsReadDeletedKeysFail)
{  //
  TestFixture::VerifyDelete();
}

}  // namespace dbgroup::index::bw_tree::test
