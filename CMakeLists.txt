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
  target_include_directories(bw_tree INTERFACE
    "${BW_TREE_SOURCE_DIR}/include"
  )
  target_compile_definitions(bw_tree INTERFACE
    $<$<BOOL:${BW_TREE_PAGE_SIZE}>:BW_TREE_PAGE_SIZE>
    $<$<BOOL:${BW_TREE_MAX_DELTA_NODE_NUM}>:BW_TREE_MAX_DELTA_NODE_NUM>
    $<$<BOOL:${BW_TREE_MAX_VARIABLE_DATA_SIZE}>:BW_TREE_MAX_VARIABLE_DATA_SIZE>
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
