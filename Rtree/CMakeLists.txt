cmake_minimum_required(VERSION 3.14)

project(RTree)

add_library(RTree RTree.h
        RTree.cpp)
target_include_directories(RTree PUBLIC RTree.h)
target_include_directories(RTree PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(RTree DSMEngine)
option(ENABLE_TESTING ON)

#if (ENABLE_TESTING)
# GoogleTest requires at least C++14
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)



add_subdirectory(tests)
#endif()