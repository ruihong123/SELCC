cmake_minimum_required(VERSION 3.30)
set (CMAKE_CXX_FLAGS "-Wall -Wextra -Wpedantic")
include(FetchContent)
FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
        DOWNLOAD_EXTRACT_TIMESTAMP false
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()
add_executable(
  basics
  basics.cpp
)
target_link_libraries(
  basics
  GTest::gtest_main
  GTest::gmock_main
        RTree
)

add_executable(
  memory
  memory.cpp
)
target_link_libraries(
  memory
  GTest::gtest_main
  GTest::gmock_main
        RTree
)

# copy bad data input file to build folder
configure_file(baddata.txt baddata.txt COPYONLY)
add_executable(
  badData
  badData.cpp
)
target_link_libraries(
  badData
  GTest::gtest_main
  GTest::gmock_main
        RTree
)

add_executable(
  largeData
  largeData.cpp
)
target_link_libraries(
  largeData
  GTest::gtest_main
  GTest::gmock_main
        RTree
)

add_executable(
  treeList
  treeList.cpp
)
target_link_libraries(
  treeList
  GTest::gtest_main
  GTest::gmock_main
        RTree
)

add_executable(
  nearestNeighbour
  nearestNeighbour.cpp
)
target_link_libraries(
  nearestNeighbour
  GTest::gtest_main
  GTest::gmock_main
        RTree
)

include(GoogleTest)
gtest_discover_tests(basics)
gtest_discover_tests(memory)
gtest_discover_tests(badData)
gtest_discover_tests(largeData)
gtest_discover_tests(treeList)
gtest_discover_tests(nearestNeighbour)

