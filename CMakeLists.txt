cmake_minimum_required(VERSION 3.10)
project(MemoryEngine)

set(CMAKE_CXX_STANDARD 20)

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DROCKSDB_SUPPORT_THREAD_LOCAL -Wall -Wextra -pthread -ggdb") #-fsanitize=address -fsanitize=leak
#add_compile_options(-fsanitize=address)
#add_link_options(-fsanitize=address)
#set(CMAKE_CXX_FLAGS_RELEASE  "${CMAKE_CXX_FLAGS_RELEASE} -O3 -Wall -Wextra -pthread")
#set(CMAKE_CXX_FLAGS_DEBUG   "${CMAKE_CXX_FLAGS_DEBUG} -O0 -ggdb -Wall -Wextra -pthread")
include_directories(
        "${PROJECT_SOURCE_DIR}"
#        "${PROJECT_SOURCE_DIR}/third_party/googletest/googlemock"
#        "${PROJECT_SOURCE_DIR}/third_party/googletest/googletest"
        )

add_library(DSMEngine util/Local_opt_locks.cpp util/Local_opt_locks.h btr/btr_itr.cpp)
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
        "storage/cache.cc"
        #    "util/clock.cc"
        "util/coding.cc"
        "util/coding.h"
        "storage/Common.cpp"
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
        "util/mutexlock.cpp"
#        "util/epic_log.h"
#        "util/epic_log.cc"

        "storage/page.cpp"
        "storage/page.h"
        "storage/Record.h"
        "storage/DDSM.cpp"
        "util/no_destructor.h"
        "util/options.cc"
        "util/random.cc"
        "util/random.h"
        "storage/rdma.cc"
        "storage/rdma.h"
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
        "include/DDSM.h"
        )

target_include_directories(DSMEngine
        PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>

        )
enable_testing()
#message(STATUS "cmake flag is release ${CMAKE_CXX_FLAGS_RELEASE}")
# This project is tested using GoogleTest.
#add_subdirectory("third_party/googletest")
find_package(ibverbs REQUIRED)
target_link_libraries(DSMEngine ibverbs::ibverbs -lcityhash -lboost_system -lboost_coroutine -lmemcached)
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
add_executable(memory_server_term "server_termination.cc"
        "memory_node/memory_node_keeper.h"
        "memory_node/memory_node_keeper.cpp")
add_executable(btree_bench test/benchmark_btree.cpp)
add_executable(micro_bench test/benchmark_micro.cpp)
target_link_libraries(TestTrial DSMEngine)
target_link_libraries(memory_server DSMEngine)
target_link_libraries(memory_server_term DSMEngine)
target_link_libraries(btree_bench DSMEngine)
target_link_libraries(micro_bench DSMEngine)


#add_subdirectory("third_party/googlemock")
function(Engine_test test_file)
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

    target_link_libraries("${test_target_name}" DSMEngine)



    add_test(NAME "${test_target_name}" COMMAND "${test_target_name}")
endfunction(Engine_test)

Engine_test("test/RDMA_Manager_Test.cpp")
Engine_test("test/Btree_Test.cpp")
