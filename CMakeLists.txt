cmake_minimum_required(VERSION 3.10)
project(MemoryEngine)

set(CMAKE_CXX_STANDARD 20)

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DROCKSDB_SUPPORT_THREAD_LOCAL -Wall -Wextra -pthread -fsanitize=address")
#set(CMAKE_CXX_FLAGS_RELEASE  "${CMAKE_CXX_FLAGS_RELEASE} -O3 -Wall -Wextra -pthread")
#set(CMAKE_CXX_FLAGS_DEBUG   "${CMAKE_CXX_FLAGS_DEBUG} -O0 -ggdb -Wall -Wextra -pthread")
include_directories(
        "${PROJECT_SOURCE_DIR}"
#        "${PROJECT_SOURCE_DIR}/third_party/googletest/googlemock"
#        "${PROJECT_SOURCE_DIR}/third_party/googletest/googletest"
        )

add_library(DSMEngine util/locks.cpp util/locks.h util/page.cpp util/page.h btr/btr_itr.cpp)
target_sources(DSMEngine
        PRIVATE
        "btr/Btr.cpp"
        "util/ThreadPool.cpp"
        "util/ThreadPool.h"
        "util/allocator.h"
        "port/port_posix.h"
        "port/port_posix.cc"
        "port/port.h"

        "util/arena.cc"
        "util/arena.h"
        "util/autovector.h"
        "util/concurrent_arena.cc"
        "util/concurrent_arena.h"
        "util/core_local.h"
#        "util/bloom.cc"
#        "util/bloom_impl.h"
        "util/cache.cc"
        #    "util/clock.cc"
        "util/coding.cc"
        "util/coding.h"
        "util/Common.cpp"
        "util/comparator.cc"
        "util/crc32c.cc"
        "util/crc32c.h"

        "util/fastrange.h"
        "util/filter_policy.cc"
        "util/hash.cc"
        "util/hash.h"
        "util/logging.cc"
        "util/logging.h"
        "util/mutexlock.h"
        "util/page.cpp"
        "util/page.h"
        "util/no_destructor.h"
        "util/options.cc"
        "util/random.cc"
        "util/random.h"
        "util/rdma.cc"
        "util/rdma.h"
        "util/thread_local.cc"
        "util/thread_local.h"
        "util/status.cc"
        "test/zipf.h"

        # Only CMake 3.3+ supports PUBLIC sources in targets exported by "install".
        $<$<VERSION_GREATER:CMAKE_VERSION,3.2>:PUBLIC>
        "include/Btr.h"
        "include/Common.h"
        "include/Config.h"
        "include/HugePageAlloc.h"
        "include/Timer.h"
        )

target_include_directories(DSMEngine
        PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>

        )
enable_testing()
message(STATUS "cmake flag is release ${CMAKE_CXX_FLAGS_RELEASE}")
# This project is tested using GoogleTest.
add_subdirectory("third_party/googletest")
find_package(ibverbs REQUIRED)
target_link_libraries(DSMEngine ibverbs::ibverbs -lcityhash -lboost_system -lboost_coroutine)
if(WITH_NUMA)
    find_package(NUMA REQUIRED)
    add_definitions(-DNUMA)
    include_directories(${NUMA_INCLUDE_DIR})
    target_link_libraries(DSMEngine NUMA::NUMA)
endif()
add_executable(TestTrial trial_test.cpp)
add_executable(memory_server "server.cc"
        "memory_node/memory_node_keeper.h"
        "memory_node/memory_node_keeper.cpp")
add_executable(btree_bench test/benchmark.cpp)
target_link_libraries(TestTrial DSMEngine)
target_link_libraries(memory_server DSMEngine)
target_link_libraries(btree_bench DSMEngine)


#add_subdirectory("third_party/googlemock")
function(TimberSaw_test test_file)
    get_filename_component(test_target_name "${test_file}" NAME_WE)

    add_executable("${test_target_name}" "")
#    target_include_directories("${test_target_name}"
#            PUBLIC
#            ${PROJECT_SOURCE_DIR}/third_party/googletest/googletest/include
#            ${PROJECT_SOURCE_DIR}/third_party/googletest/googlemock/include
#            )
    target_sources("${test_target_name}"
            PRIVATE
#            "${PROJECT_BINARY_DIR}/${TimberSaw_PORT_CONFIG_DIR}/port_config.h"
#            "util/testutil.cc"
#            "util/testutil.h"
            "${test_file}"
            )

    target_link_libraries("${test_target_name}" DSMEngine gmock gtest)

#    target_compile_definitions("${test_target_name}"
#            PRIVATE
#            ${TimberSaw_PLATFORM_NAME}=1
#            )
#    if (NOT HAVE_CXX17_HAS_INCLUDE)
#        target_compile_definitions("${test_target_name}"
#                PRIVATE
#                TimberSaw_HAS_PORT_CONFIG_H=1
#                )
#    endif(NOT HAVE_CXX17_HAS_INCLUDE)

    add_test(NAME "${test_target_name}" COMMAND "${test_target_name}")
endfunction(TimberSaw_test)

    TimberSaw_test("test/RDMA_Manager_Test.cpp")
    TimberSaw_test("test/Btree_Test.cpp")
#  TimberSaw_test("db/fault_injection_test.cc")
#
#  TimberSaw_test("issues/issue178_test.cc")
#  TimberSaw_test("issues/issue200_test.cc")
#  TimberSaw_test("issues/issue320_test.cc")
#
#  TimberSaw_test("util/env_test.cc")
#  TimberSaw_test("util/status_test.cc")
#  TimberSaw_test("util/no_destructor_test.cc")