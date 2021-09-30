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
add_subdirectory("${MEMORY_MANAGER_SOURCE_DIR}")

#--------------------------------------------------------------------------------------#
# Build targets
#--------------------------------------------------------------------------------------#

add_library(bw_tree INTERFACE)
target_compile_features(
  bw_tree INTERFACE
  "cxx_std_17"
)
target_compile_options(
  bw_tree INTERFACE
  -Wall
  -Wextra
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:"-O2 -march=native">
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"RelWithDebInfo">:"-g3 -Og -pg">
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Debug">:"-g3 -O0 -pg">
)
target_include_directories(
  bw_tree INTERFACE
  "${BW_TREE_SOURCE_DIR}/include"
)
target_link_libraries(
  bw_tree INTERFACE
  memory_manager
)

#--------------------------------------------------------------------------------------#
# Add compile options
#--------------------------------------------------------------------------------------#

#--------------------------------------------------------------------------------------#
# Build unit tests
#--------------------------------------------------------------------------------------#

option(BW_TREE_BUILD_TESTS "Build unit tests for Bw-tree" OFF)
if(${BW_TREE_BUILD_TESTS})
  enable_testing()
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/test")
endif()
