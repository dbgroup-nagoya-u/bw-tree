#--------------------------------------------------------------------------------------#
# Configure GoogleTest
#--------------------------------------------------------------------------------------#

configure_file(CMakeLists-googletest.txt.in googletest-download/CMakeLists.txt)
execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download )
if(result)
  message(FATAL_ERROR "CMake step for googletest failed: ${result}")
endif()
execute_process(COMMAND ${CMAKE_COMMAND} --build .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download )
if(result)
  message(FATAL_ERROR "Build step for googletest failed: ${result}")
endif()

# Prevent overriding the parent project's compiler/linker
# settings on Windows
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

# Add googletest directly to our build. This defines
# the gtest and gtest_main targets.
add_subdirectory(${CMAKE_CURRENT_BINARY_DIR}/googletest-src
                 ${CMAKE_CURRENT_BINARY_DIR}/googletest-build
                 EXCLUDE_FROM_ALL)

#--------------------------------------------------------------------------------------#
# Configurations for testing
#--------------------------------------------------------------------------------------#

set(BW_TREE_TEST_THREAD_NUM "8" CACHE STRING "The number of worker threads for testing.")
set(BW_TREE_TEST_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")

#--------------------------------------------------------------------------------------#
# Build Unit Tests
#--------------------------------------------------------------------------------------#

# define function to add unit tests in the same format
function(ADD_BW_TREE_TEST BW_TREE_TEST_TARGET)
  add_executable(${BW_TREE_TEST_TARGET}
    "${CMAKE_CURRENT_SOURCE_DIR}/${BW_TREE_TEST_TARGET}.cpp"
  )
  target_compile_features(${BW_TREE_TEST_TARGET} PRIVATE
    "cxx_std_17"
  )
  target_compile_options(${BW_TREE_TEST_TARGET} PRIVATE
    -Wall
    -Wextra
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:"-O2 -march=native">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"RelWithDebInfo">:"-g3 -Og -pg">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Debug">:"-g3 -O0 -pg">
  )
  target_compile_definitions(${BW_TREE_TEST_TARGET} PRIVATE
    BW_TREE_TEST_THREAD_NUM=${BW_TREE_TEST_THREAD_NUM}
  )
  target_include_directories(${BW_TREE_TEST_TARGET} PRIVATE
    ${BW_TREE_TEST_SOURCE_DIR}
  )
  target_link_libraries(${BW_TREE_TEST_TARGET} PRIVATE
    bw_tree
    gtest_main
  )

  add_test(
    NAME ${BW_TREE_TEST_TARGET}
    COMMAND $<TARGET_FILE:${BW_TREE_TEST_TARGET}>
  )
endfunction()

# add unit tests to build targets
ADD_BW_TREE_TEST("logical_id_test")
ADD_BW_TREE_TEST("mapping_table_test")
ADD_BW_TREE_TEST("metadata_test")
ADD_BW_TREE_TEST("delta_record_test")
ADD_BW_TREE_TEST("node_test")
ADD_BW_TREE_TEST("bw_tree_test")
ADD_BW_TREE_TEST("bw_tree_multi_thread_test")
