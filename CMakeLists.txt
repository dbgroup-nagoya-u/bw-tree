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

# define source directories
set(BW_TREE_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
set(MEMORY_MANAGER_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/external/memory-manager")

#--------------------------------------------------------------------------------------#
# Bw-tree's configurations
#--------------------------------------------------------------------------------------#

set(BW_TREE_PAGE_SIZE "1024" CACHE STRING " Page size in bytes.")
set(BW_TREE_MAX_DELTA_RECORD_NUM "(2 * Log2(kPageSize / 256))" CACHE STRING " Invoking consolidation if the number of delta records exceeds this threshold.")
set(BW_TREE_MIN_NODE_SIZE "(kPageSize / 16)" CACHE STRING " Invoking a merge-operation if the size of a node becomes lower than this threshold.")
set(BW_TREE_MAX_VARIABLE_DATA_SIZE "128" CACHE STRING " The expected maximum size of a variable-length data.")

#--------------------------------------------------------------------------------------#
# Configure external libraries
#--------------------------------------------------------------------------------------#

# configure GC
add_subdirectory("${MEMORY_MANAGER_SOURCE_DIR}")

#--------------------------------------------------------------------------------------#
# Build targets
#--------------------------------------------------------------------------------------#

if(NOT TARGET bw_tree::bw_tree)
  add_library(bw_tree INTERFACE)
  add_library(bw_tree::bw_tree ALIAS bw_tree)
  target_compile_features(bw_tree INTERFACE
    "cxx_std_17"
  )
  target_compile_definitions(bw_tree INTERFACE
    BW_TREE_PAGE_SIZE=${BW_TREE_PAGE_SIZE}
    BW_TREE_MAX_DELTA_RECORD_NUM=${BW_TREE_MAX_DELTA_RECORD_NUM}
    BW_TREE_MIN_NODE_SIZE=${BW_TREE_MIN_NODE_SIZE}
    BW_TREE_MAX_VARIABLE_DATA_SIZE=${BW_TREE_MAX_VARIABLE_DATA_SIZE}
  )
  target_include_directories(bw_tree INTERFACE
    ${BW_TREE_SOURCE_DIR}/include
  )
  target_link_libraries(bw_tree INTERFACE
    memory_manager::memory_manager
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
