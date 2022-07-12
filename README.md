# Bw-tree

![Ubuntu-20.04](https://github.com/dbgroup-nagoya-u/bw-tree/workflows/Ubuntu-20.04/badge.svg?branch=main)

This repository is an open source implementation of a Bw-tree[^1] for research use. The purpose of this implementation is to reproduce a Bw-tree and measure its performance.

## Build

**Note**: this is a header only library. You can use this without pre-build.

### Prerequisites

```bash
sudo apt update && sudo apt install -y build-essential cmake
```

### Build Options

#### Tuning Parameters

- `BW_TREE_PAGE_SIZE`: Page size in bytes (default `1024`).
- `BW_TREE_MAX_DELTA_RECORD_NUM`: Invoking consolidation if the number of delta records exceeds this threshold (default `(2 * Log2(kPageSize / 256))`).
- `BW_TREE_MIN_NODE_SIZE`: Invoking a merge-operation if the size of a node becomes lower than this threshold (default `${BW_TREE_PAGE_SIZE} / 16`).
- `BW_TREE_MAX_VARIABLE_DATA_SIZE`: The expected maximum size of a variable-length data (default `128`).

#### Build Options for Unit Testing

- `BW_TREE_BUILD_TESTS`: Building unit tests for this library if `ON` (default `OFF`).
- `BW_TREE_TEST_THREAD_NUM`: The maximum number of threads to perform unit tests (default `8`).
- `BW_TREE_TEST_RANDOM_SEED`: A fixed seed value to reproduce unit tests (default `0`).
- `BW_TREE_TEST_EXEC_NUM`: The number of executions per a thread (default `1E5`).
- `BW_TREE_TEST_OVERRIDE_MIMALLOC`: Override entire memory allocation with mimalloc (default `OFF`).
    - NOTE: we use `find_package(mimalloc 1.7 REQUIRED)` to link mimalloc.

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

We provide the same read/write APIs for the reproduced indexes. See [here](https://github.com/dbgroup-nagoya-u/index-benchmark/wiki/Common-APIs-for-Index-Implementations) for common APIs and usage examples.

[^1]: [J. Levandoski, D. Lomet, and S. Sengupta, "The Bw-Tree: A B-tree for New Hardware Platforms,” In Proc. ICDE, pp. 302-313, 2013](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/bw-tree-icde2013-final.pdf).
