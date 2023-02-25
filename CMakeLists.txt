cmake_minimum_required(VERSION "3.8")

# decline in-source build
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif()

# project information
project(
  bw_tree
  VERSION "0.1.0"
  DESCRIPTION "An open-source Bw-tree library."
  LANGUAGES "CXX"
)

# check SSE support for spinlock hints
include(CheckIncludeFileCXX)
CHECK_INCLUDE_FILE_CXX("xmmintrin.h" BW_TREE_HAS_SPINLOCK_HINT)

#--------------------------------------------------------------------------------------#
# Bw-tree's configurations
#--------------------------------------------------------------------------------------#

set(BW_TREE_PAGE_SIZE "1024" CACHE STRING "Page size in bytes.")
set(BW_TREE_DELTA_RECORD_NUM_THRESHOLD "(2 * Log2(kPageSize / 256))" CACHE STRING "Invoking consolidation if the number of delta records exceeds this threshold.")
set(BW_TREE_MAX_DELTA_RECORD_NUM "64" CACHE STRING "Waiting for other threads if the number of delta records exceeds this threshold.")
set(BW_TREE_MIN_NODE_SIZE "(kPageSize / 16)" CACHE STRING "Invoking a merge-operation if the size of a node becomes lower than this threshold.")
set(BW_TREE_MAX_VARIABLE_DATA_SIZE "128" CACHE STRING "The expected maximum size of a variable-length data.")
set(BW_TREE_RETRY_THRESHOLD "10" CACHE STRING "The maximum number of retries for preventing busy loops.")
set(BW_TREE_SLEEP_TIME "10" CACHE STRING "A sleep time for preventing busy loops [us].")

#--------------------------------------------------------------------------------------#
# Configure external libraries
#--------------------------------------------------------------------------------------#

# configure GC
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/memory-manager")

#--------------------------------------------------------------------------------------#
# Build targets
#--------------------------------------------------------------------------------------#

if(NOT TARGET dbgroup::${PROJECT_NAME})
  add_library(${PROJECT_NAME} INTERFACE)
  add_library(dbgroup::${PROJECT_NAME} ALIAS ${PROJECT_NAME})
  target_compile_features(${PROJECT_NAME} INTERFACE
    "cxx_std_17"
  )
  target_compile_definitions(${PROJECT_NAME} INTERFACE
    BW_TREE_PAGE_SIZE=${BW_TREE_PAGE_SIZE}
    BW_TREE_DELTA_RECORD_NUM_THRESHOLD=${BW_TREE_DELTA_RECORD_NUM_THRESHOLD}
    BW_TREE_MAX_DELTA_RECORD_NUM=${BW_TREE_MAX_DELTA_RECORD_NUM}
    BW_TREE_MIN_NODE_SIZE=${BW_TREE_MIN_NODE_SIZE}
    BW_TREE_MAX_VARIABLE_DATA_SIZE=${BW_TREE_MAX_VARIABLE_DATA_SIZE}
    BW_TREE_RETRY_THRESHOLD=${BW_TREE_RETRY_THRESHOLD}
    BW_TREE_SLEEP_TIME=${BW_TREE_SLEEP_TIME}
    $<$<BOOL:${BW_TREE_HAS_SPINLOCK_HINT}>:BW_TREE_HAS_SPINLOCK_HINT>
  )
  target_include_directories(${PROJECT_NAME} INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
  )
  target_link_libraries(${PROJECT_NAME} INTERFACE
    dbgroup::memory_manager
  )
endif()

#--------------------------------------------------------------------------------------#
# Build unit tests
#--------------------------------------------------------------------------------------#

option(BW_TREE_BUILD_TESTS "Build unit tests for Bw-tree" OFF)
if(${BW_TREE_BUILD_TESTS})
  enable_testing()
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/test")
endif()
