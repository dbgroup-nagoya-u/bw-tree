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

- `BW_TREE_PAGE_SIZE`: invoking a split-operation if the size of a base page exceeds this threshold  (default `1024`).
- `BW_TREE_MAX_DELTA_RECORD_NUM`: invoking consolidation if the number of delta records exceeds this threshold (default `2 * Log2(${BW_TREE_PAGE_SIZE} / 256)`, i.e., `4`).
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

We provide the same read/write APIs for the reproduced indexes. See [here](https://github.com/dbgroup-nagoya-u/index-benchmark/wiki/Common-APIs-for-Index-Implementations) for common APIs and usage examples.

[^1]: [J. Levandoski, D. Lomet, and S. Sengupta, "The Bw-Tree: A B-tree for New Hardware Platforms,” In Proc. ICDE, pp. 302-313, 2013](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/bw-tree-icde2013-final.pdf).
