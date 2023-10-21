#include <iostream>
#include <numa.h>
#include <cstring>
#include <chrono>
#include <thread>
#include <vector>
#include <atomic>
#include <random>
constexpr size_t ACCESSED_DATA_SIZE = 1 << 26; // 64 MB of memory, smaller than L3 cache on bigdata servers.
constexpr uint64_t NUM_STEPS = 1024*1024ull*1024; // 1 Billion of operation

constexpr size_t CACHE_LINE_SIZE = 64; // Assuming a common cache line size of 64 bytes
constexpr int NUM_THREADS = 1; // Number of threads
int num_numa_nodes;
std::atomic<uint16_t> start_sync = 0;
std::atomic<uint16_t> end_sync = 0;
void memset_on_node(int node, std::vector<char*>& buffers, std::vector<size_t>& cache_lines) {
    numa_run_on_node(node); // Run on the specified NUMA node
    srand(node);
    uint64_t total_block_number = ACCESSED_DATA_SIZE / CACHE_LINE_SIZE;
    uint64_t block_number_per_shard = (ACCESSED_DATA_SIZE / num_numa_nodes) / CACHE_LINE_SIZE;
    //TODO: decrease the generated set size to 4 times of the block number.
    for (size_t i = 0; i < 16*total_block_number; ++i) {
        cache_lines.push_back(rand()%total_block_number);
    }
    // warm up
    uint64_t operation_count = 0;
    while (operation_count < NUM_STEPS) {
        for (size_t cache_line_idx : cache_lines) {

            uint64_t get_block_idx = cache_line_idx % block_number_per_shard;
            uint64_t  buffer_index = cache_line_idx / block_number_per_shard;
            size_t target_offset = get_block_idx * CACHE_LINE_SIZE;
            memset(buffers[buffer_index]+ target_offset, node, CACHE_LINE_SIZE);
            operation_count++;
            if (operation_count >= NUM_STEPS) {
                break;
            }
        }
    }

    printf("Node %d warm up finished\n", node);
    // real run.
    start_sync.fetch_add(1);
    while (start_sync.load() != NUM_THREADS) {}

    operation_count = 0;
    while (operation_count < NUM_STEPS) {
        for (size_t cache_line_idx: cache_lines) {

            uint64_t get_block_idx = cache_line_idx % block_number_per_shard;
            uint64_t buffer_index = cache_line_idx / block_number_per_shard;
            size_t target_offset = get_block_idx * CACHE_LINE_SIZE;
            memset(buffers[buffer_index] + target_offset, node, CACHE_LINE_SIZE);
            operation_count++;
            if (operation_count >= NUM_STEPS) {
                break;
            }
        }
    }

    end_sync.fetch_add(1);
}

int main() {




    num_numa_nodes = numa_num_configured_nodes(); // Get the number of configured NUMA nodes
    std::cout << "Number of NUMA nodes: " << num_numa_nodes << std::endl;
    // Allocate memory on each NUMA node
    printf("Prepare the accessed data over all the numa nodes\n");
    std::vector<char*> buffers(num_numa_nodes);
    for (int i = 0; i < num_numa_nodes; ++i) {
        numa_run_on_node(i); // Run on the specified NUMA node
        buffers[i] = static_cast<char*>(numa_alloc_local(ACCESSED_DATA_SIZE / num_numa_nodes));
    }
    printf("Finish memory allocation\n");


    // Distribute cache lines round-robinly
    std::vector<std::vector<size_t>> thread_cache_lines(NUM_THREADS);


    std::vector<std::thread> threads;

    for (int i = 0; i < NUM_THREADS; ++i) {
        threads.emplace_back(memset_on_node, i % num_numa_nodes, std::ref(buffers), std::ref(thread_cache_lines[i]));
    }
    printf("Start the test\n");


    while (start_sync.load() != NUM_THREADS) {}
    auto start = std::chrono::high_resolution_clock::now();

    while (end_sync.load() != NUM_THREADS) {}
    auto end = std::chrono::high_resolution_clock::now();

    for (auto& thread : threads) {
        thread.join(); // Wait for all threads to finish
    }

    std::chrono::duration<double> duration =std::chrono::duration_cast<std::chrono::microseconds>( end - start);
    uint64_t throughput = NUM_STEPS*1000*1000ull*NUM_THREADS / (duration.count());
    std::cout << "Time taken: " << duration.count() << " micro seconds" << std::endl;
    std::cout << "Aggregated throughput is : " << throughput << " ops/sec" << std::endl;
    // Free the allocated memory
    for (int i = 0; i < num_numa_nodes; ++i) {
        numa_free(buffers[i], ACCESSED_DATA_SIZE / num_numa_nodes);
    }

    return 0;
}
