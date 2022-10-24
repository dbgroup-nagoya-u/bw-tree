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

#include "bw_tree/bw_tree.hpp"

// our libraries
#include "external/index-fixtures/index_fixture.hpp"

namespace dbgroup::index::test
{
/*######################################################################################
 * Preparation for typed testing
 *####################################################################################*/

template <class K, class V, class C>
using BwTreeFixLen = ::dbgroup::index::bw_tree::BwTreeFixLen<K, V, C>;

using TestTargets = ::testing::Types<            //
    IndexInfo<BwTreeFixLen, UInt8, UInt8>,       // fixed-length keys
    IndexInfo<BwTreeFixLen, UInt4, UInt8>,       // small keys
    IndexInfo<BwTreeFixLen, UInt8, UInt4>,       // small payloads
    IndexInfo<BwTreeFixLen, UInt4, UInt4>,       // small keys/payloads
    IndexInfo<BwTreeFixLen, Ptr, Ptr>,           // pointer keys/payloads
    IndexInfo<BwTreeFixLen, Original, Original>  // original class keys/payloads
    >;
TYPED_TEST_SUITE(IndexFixture, TestTargets);

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

#include "external/index-fixtures/index_fixture_test_definitions.hpp"

}  // namespace dbgroup::index::test
