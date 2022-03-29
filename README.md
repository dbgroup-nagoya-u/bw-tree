# Bw-tree

![Ubuntu-20.04](https://github.com/dbgroup-nagoya-u/bw-tree/workflows/Ubuntu-20.04/badge.svg?branch=main)

This repository is an open source implementation of a [Bw-tree](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/bw-tree-icde2013-final.pdf)[1] for research use. The purpose of this implementation is to reproduce a Bw-tree and measure its performance.

> [1] J. Levandoski, D. Lomet, S. Sengupta, "The Bw-Tree: A B-tree for New Hardware Platforms,” In Proc. ICDE, pp. 302-313, 2013.

## Build

**Note**: this is a header only library. You can use this without pre-build.

### Prerequisites

```bash
sudo apt update && sudo apt install -y build-essential cmake
```

### Build Options

#### Tuning Parameters

- `BW_TREE_PAGE_SIZE`: invoking a split-operation if the size of a base page exceeds this threshold  (default `8192`).
- `BW_TREE_MAX_DELTA_RECORD_NUM`: invoking consolidation if the number of delta records exceeds this threshold (default `8`).
- `BW_TREE_MIN_NODE_SIZE`: invoking a merge-operation if the size of a node becomes lower than this threshold (default `${BW_TREE_PAGE_SIZE} / 8`).
- `BW_TREE_MAX_VARIABLE_DATA_SIZE`: the expected maximum size of a variable-length data in this library (default `128`).

#### Build Options for Unit Testing

- `BW_TREE_BUILD_TESTS`: building unit tests for this library if `ON` (default `OFF`).
- `BW_TREE_TEST_THREAD_NUM`: the maximum number of threads to perform unit tests (default `8`).

### Build and Run Unit Tests

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DBW_TREE_BUILD_TESTS=ON ..
make -j
ctest -C Release
```

## Usage

### Linking by CMake

1. Download the files in any way you prefer (e.g., `git submodule`).

    ```bash
    cd <your_project_workspace>
    mkdir external
    git submodule add https://github.com/dbgroup-nagoya-u/bw-tree.git external/bw-tree
    ```

1. Add this library to your build in `CMakeLists.txt`.

    ```cmake
    add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/bw-tree")

    add_executable(
      <target_bin_name>
      [<source> ...]
    )
    target_link_libraries(
      <target_bin_name> PRIVATE
      bw_tree
    )
    ```

### Read/Write APIs

If you use fixed length types as keys/values, you can use our Bw-tree by simply declaring it.

```cpp
#include <iostream>

#include "bw_tree/bw_tree.hpp"

using Key = uint64_t;
using Value = uint64_t;

using BwTree_t = ::dbgroup::index::bw_tree::BwTree<Key, Value>;
using ::dbgroup::index::bw_tree::ReturnCode;

int
main([[maybe_unused]] int argc, [[maybe_unused]] char** argv)
{
  // create a Bw-tree instance
  BwTree_t bw_tree{};

  // write key/value pairs
  bw_tree.Write(0UL, 0UL);

  // insert a duplicate key
  if (bw_tree.Insert(0UL, 1UL) != ReturnCode::kSuccess) {
    // inserting duplicate keys must fail, so we insert a new key
    bw_tree.Insert(1UL, 1UL);
  }

  // update a not inserted key
  if (bw_tree.Update(2UL, 2UL) != ReturnCode::kSuccess) {
    // updating non-existent keys must fail, so we update an inserted key
    bw_tree.Update(0UL, 2UL);
  }

  // delete a not inserted key
  if (bw_tree.Delete(2UL) != ReturnCode::kSuccess) {
    // deleting non-existent keys must fail, so we delete an inserted key
    bw_tree.Delete(1UL);
  }

  // read a deleted key
  auto [rc, value] = bw_tree.Read(1UL);
  if (rc != ReturnCode::kSuccess) {
    // reading deleted keys must fail, so we read an existent key
    std::tie(rc, value) = bw_tree.Read(0UL);

    std::cout << "Return code: " << rc << std::endl;
    std::cout << "Read value : " << value << std::endl;
  }

  return 0;
}
```

This code will output the following results.

```txt
Return code: 0
Read value : 2
```

### Range Scanning

A `Scan` function returns an iterator for scan results. We prepare a `HasNext` function and `*`/`++` operators to access scan results. If you give `nullptr` as a begin/end key, the index treats it as a negative/positive infinity value.

```cpp
#include <iostream>

#include "bw_tree/bw_tree.hpp"

using Key = uint64_t;
using Value = uint64_t;

using BwTree_t = ::dbgroup::index::bw_tree::BwTree<Key, Value>;
using ::dbgroup::index::bw_tree::ReturnCode;

int
main([[maybe_unused]] int argc, [[maybe_unused]] char** argv)
{
  // create a Bw-tree instance
  BwTree_t bw_tree{};

  // write key/value pairs
  for (uint64_t i = 0; i < 10; ++i) {
    bw_tree.Write(i, i);
  }

  // full scan
  uint64_t sum = 0;
  for (auto iter = bw_tree.Scan(); iter.HasNext(); ++iter) {
    auto [key, value] = *iter;
    // auto value = iter.GetPayload();  // you can get a value by itself
    sum += value;
  }
  std::cout << "Sum: " << sum << std::endl;

  // scan greater than: (3, infinity)
  sum = 0;
  uint64_t begin_key = 3;
  for (auto iter = bw_tree.Scan(&begin_key, false); iter.HasNext(); ++iter) {
    auto [key, value] = *iter;
    sum += value;
  }
  std::cout << "Sum: " << sum << std::endl;

  // scan less than or equal to: (-infinity, 7]
  sum = 0;
  uint64_t end_key = 7;
  for (auto iter = bw_tree.Scan(nullptr, false, &end_key, true); iter.HasNext(); ++iter) {
    auto [key, value] = *iter;
    sum += value;
  }
  std::cout << "Sum: " << sum << std::endl;

  // scan between: [3, 7)
  sum = 0;
  for (auto iter = bw_tree.Scan(&begin_key, true, &end_key, false); iter.HasNext(); ++iter) {
    auto [key, value] = *iter;
    sum += value;
  }
  std::cout << "Sum: " << sum << std::endl;

  return 0;
}
```

This code will output the following results.

```txt
Sum: 45
Sum: 39
Sum: 28
Sum: 18
```

### Multi-Threading

This library is a thread-safe implementation. You can call all the APIs (i.e., `Read`, `Scan`, `Write`, `Insert`, `Update`, and `Delete`) from multi-threads concurrently. Note that concurrent writes follow the last write win protocol, and so you need to some concurrency control methods (e.g., snapshot isolation) externally to guarantee the order of read/write operations.

```cpp
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "bw_tree/bw_tree.hpp"

using Key = uint64_t;
using Value = uint64_t;

using BwTree_t = ::dbgroup::index::bw_tree::BwTree<Key, Value>;
using ::dbgroup::index::bw_tree::ReturnCode;

uint64_t
Sum(const std::unique_ptr<BwTree_t>& bw_tree)
{
  uint64_t sum = 0;
  for (auto iter = bw_tree->Scan(); iter.HasNext(); ++iter) {
    sum += iter.GetPayload();
  }
  return sum;
}

int
main([[maybe_unused]] int argc, [[maybe_unused]] char** argv)
{
  // create a Bw-tree instance
  auto bw_tree = std::make_unique<BwTree_t>();

  // a lambda function for a multi-threading example
  auto f = [&](const uint64_t begin_id, const uint64_t end_id) {
    for (uint64_t i = begin_id; i < end_id; ++i) {
      bw_tree->Write(i, i);
    }
  };

  // write values by single threads
  std::vector<std::thread> threads;
  threads.emplace_back(f, 0, 4e6);
  for (auto&& t : threads) t.join();

  // compute the sum of all the values for validation
  std::cout << "Sum: " << Sum(bw_tree) << std::endl;

  // reset a Bw-tree instance
  bw_tree = std::make_unique<BwTree_t>();

  // write values by four threads
  threads.clear();
  threads.emplace_back(f, 0, 1e6);
  threads.emplace_back(f, 1e6, 2e6);
  threads.emplace_back(f, 2e6, 3e6);
  threads.emplace_back(f, 3e6, 4e6);
  for (auto&& t : threads) t.join();

  // check all the values are written
  std::cout << "Sum: " << Sum(bw_tree) << std::endl;

  return 0;
}
```

This code will output the following results.

```txt
Sum: 7999998000000
Sum: 7999998000000
```

### Variable Length Keys/Values

If you use variable length keys/values (i.e., binary data), you need to specify theier lengths for each API except for the read API. Note that we use `std::byte*` to represent binary data, and so you may need to cast your keys/values to write a Bw-tree instance, such as `reinterpret_cast<std::byte*>(&<key_instance>)`.

```cpp
#include <stdio.h>

#include <iostream>

#include "bw_tree/bw_tree.hpp"

// we use std::byte* to represent binary data
using Key = std::byte*;
using Value = std::byte*;

// we prepare a comparator for CString as an example
using ::dbgroup::index::bw_tree::CompareAsCString;

using BwTree_t = ::dbgroup::index::bw_tree::BwTree<Key, Value, CompareAsCString>;
using ::dbgroup::index::bw_tree::ReturnCode;

int
main([[maybe_unused]] int argc, [[maybe_unused]] char** argv)
{
  constexpr size_t kWordLength = 8;

  // create a Bw-tree instance
  BwTree_t bw_tree{};

  // prepare a variable-length key/value
  char key[kWordLength], value[kWordLength];
  snprintf(key, kWordLength, "key");
  snprintf(value, kWordLength, "value");

  // the length of CString includes '\0'
  bw_tree.Write(reinterpret_cast<std::byte*>(key), reinterpret_cast<std::byte*>(value), 4, 6);

  // in the case of variable values, the type of the return value is std::unique_ptr<std::byte>
  auto [rc, read_value] = bw_tree.Read(reinterpret_cast<std::byte*>(key));
  std::cout << "Return code: " << rc << std::endl;
  std::cout << "Read value : " << reinterpret_cast<char*>(read_value.get()) << std::endl;

  return 0;
}
```

This code will output the following results.

```txt
Return code: 0
Read value : value
```
