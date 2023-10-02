//
// Created by ruihong on 9/27/23.
//
// Copyright (c) 2018 The GAM Authors


#include <thread>
#include <atomic>
#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <cstring>
#include <mutex>
#include <set>

#include "Common.h"
#include "storage/rdma.h"
#include "storage/page.h"
//#include "util/epic_log.h"
#include "storage/rdma.h"
#include "DDSM.h"

//#define PERF_GET
//#define PERF_MALLOC

//#define BENCHMARK_DEBUG
//#define STATS_COLLECTION
//#define LOCAL_MEMORY

#define STEPS 204800 //100M much larger than 10M L3 cache
#define DEBUG_LEVEL LOG_WARNING

#define SYNC_KEY STEPS
using namespace DSMEngine;

uint16_t node_id;

bool is_master = false;
uint16_t tcp_port=19843;
//string ip_master = get_local_ip("eth0");
//string ip_worker = get_local_ip("eth0");
//int port_master = 12345;
//int port_worker = 12346;

const char* result_file = "result.csv";

//exp parameters
long ITERATION = 2000000;
//long FENCE_PERIOD = 1000;
int no_thread = 2;
int no_node = 0;
//int remote_ratio = 0;  //0..100
int shared_ratio = 10;  //0..100
int space_locality = 10;  //0..100
int time_locality = 10;  //0..100 (how probable it is to re-visit the current position)
int read_ratio = 10;  //0..100
int op_type = 0;  //0: read/write; 1: rlock/wlock; 2: rlock+read/wlock+write
int compute_num = 0;
int memory_num = 100;

float cache_th = 0.15;  //0.15
uint64_t cache_size = 0;
uint64_t allocated_mem_size = 0;

//runtime statistics
std::atomic<long> remote_access(0);
std::atomic<long> shared_access(0);
std::atomic<long> space_local_access(0);
std::atomic<long> time_local_access(0);
std::atomic<long> read_access(0);

std::atomic<long> total_throughput(0);
std::atomic<long> avg_latency(0);

bool reset = false;

std::set<GlobalAddress> gen_accesses;
std::set<GlobalAddress> real_accesses;
std::mutex stat_lock;

int addr_size = sizeof(GlobalAddress);
int item_size = addr_size;
int items_per_block =  kLeafPageSize / item_size;

inline int GetRandom(int min, int max, unsigned int* seedp) {
    int ret = (rand_r(seedp) % (max - min)) + min;
    return ret;
}
struct timespec init_time;
void init() __attribute__ ((constructor));
void fini() __attribute__ ((destructor));
void init() {
    clock_gettime(CLOCK_REALTIME, &init_time);
}
long get_time() {
//	struct timeval start;
//	gettimeofday(&start, NULL);
//	return start.tv_sec*1000l*1000+start.tv_usec;
    struct timespec start;
    clock_gettime(CLOCK_REALTIME, &start);
    return (start.tv_sec - init_time.tv_sec) * 1000l * 1000 * 1000
           + (start.tv_nsec - init_time.tv_nsec);;
}
bool TrueOrFalse(double probability, unsigned int* seedp) {
    return (rand_r(seedp) % 100) < probability;
}
GlobalAddress TOPAGE(GlobalAddress addr){
    GlobalAddress ret = addr;
    ret.offset = (ret.offset/kLeafPageSize)*kLeafPageSize;
    return ret;
}

//int GetRandom(int min, int max, unsigned int* seedp) {
//	int ret = (rand_r(seedp) % (max-min)) + min;
//	return ret;
//}

int CyclingIncr(int a, int cycle_size) {
    return ++a == cycle_size ? 0 : a;
}

double Revise(double orig, int remaining, bool positive) {
    if (positive) {  //false positive
        return (remaining * orig - 1) / remaining;
    } else {  //false negative
        return (remaining * orig + 1) / remaining;
    }
}



void Init(DDSM* ddsm, GlobalAddress data[], GlobalAddress access[], bool shared[], int id,
          unsigned int* seedp) {
    printf( "start init\n");

//  int l_remote_ratio = remote_ratio;
    int l_space_locality = space_locality;
    int l_shared_ratio = shared_ratio;

    //the main thread (id == 0) in the master node (is_master == true)
    // is responsible for reference data access pattern
    if (is_master && id == 0) {
        for (int i = 0; i < STEPS; i++) {
            //There is no meanings to do the if clause below.
            if (TrueOrFalse(l_shared_ratio, seedp)) {
                shared[i] = true;
            } else {
                shared[i] = false;
            }
#ifdef LOCAL_MEMORY
            int ret = posix_memalign((void **)&data[i], BLOCK_SIZE, BLOCK_SIZE);
      epicAssert(!ret);
      epicAssert(data[i] % BLOCK_SIZE == 0);
#else
//      if (TrueOrFalse(remote_ratio, seedp)) {
            data[i] = ddsm->Allocate_Remote(Internal_and_Leaf);
//      } else {
//        data[i] = alloc->AlignedMalloc(BLOCK_SIZE);
//      }
#endif
            if (shared_ratio != 0)
                //Register the allocation for master into a key value store.
                ddsm->memSet((const char*)&i, sizeof(i), (const char*)(&data[i]), sizeof(GlobalAddress));
#ifdef BENCHMARK_DEBUG
            if (shared_ratio != 0) {
        GAddr readback;
        int back_ret = alloc->Get(i, &readback);
        epicAssert(back_ret == addr_size);
        epicAssert(readback == data[i]);
      }
#endif
        }
    } else {
        for (int i = 0; i < STEPS; i++) {
            //we prioritize the shared ratio over other parameters
            if (TrueOrFalse(l_shared_ratio, seedp)) {
                GlobalAddress addr;
                size_t v_size;
                //If this block is shared, then acqurie the shared block the same as master.
                char* value = ddsm->memGet((const char*)&i, sizeof(i), &v_size);
                assert(v_size == addr_size && v_size == sizeof(GlobalAddress));
                data[i] = *(GlobalAddress*)value;
                //revise the l_remote_ratio accordingly if we get the shared addr violate the remote probability
//        if (TrueOrFalse(l_remote_ratio, seedp)) {  //should be remote
//          if (alloc->GetID() == WID(addr)) {  //false negative
//            l_remote_ratio = Revise(l_remote_ratio, STEPS - i - 1, false);
//          }
//        } else {  //shouldn't be remote
//          if (alloc->GetID() != WID(addr)) {  //false positive
//            l_remote_ratio = Revise(l_remote_ratio, STEPS - i - 1, true);
//          }
//        }
                shared[i] = true;
            } else {
#ifdef LOCAL_MEMORY
                int ret = posix_memalign((void **)&data[i], BLOCK_SIZE, BLOCK_SIZE);
        epicAssert(data[i] % BLOCK_SIZE == 0);
#else
//        if (TrueOrFalse(remote_ratio, seedp)) {
                data[i] = ddsm->Allocate_Remote(Internal_and_Leaf);
//        } else {
//          data[i] = alloc->AlignedMalloc(BLOCK_SIZE);
//        }
#endif
                shared[i] = false;
            }
        }
    }
    //access[0] = data[0];
    access[0] = data[GetRandom(0, STEPS, seedp)];
#ifdef STATS_COLLECTION
    stat_lock.lock();
  gen_accesses.insert(TOBLOCK(access[0]));
  stat_lock.unlock();
#endif
    // Access is the address of future acesses.
    for (int i = 1; i < ITERATION; i++) {
        //PopulateOneBlock(alloc, data, ldata, i, l_remote_ratio, l_space_locality, seedp);
        GlobalAddress next;
        if (TrueOrFalse(space_locality, seedp)) {
            next = access[i - 1];
            if (TOPAGE(next) != TOPAGE(access[i - 1])) {
                next = TOPAGE(access[i - 1]);
            }
        } else {
            GlobalAddress n = data[GetRandom(0, STEPS, seedp)];
            while (TOPAGE(n) == TOPAGE(access[i - 1])) {
                n = data[GetRandom(0, STEPS, seedp)];
            }
            next = GADD(n, GetRandom(0, items_per_block, seedp) * item_size);

//            GlobalAddress n = data[GetRandom(0, STEPS, seedp)];
//            while (n == access[i - 1]) {
//                n = data[GetRandom(0, STEPS, seedp)];
//            }
//            next = n;
        }
        access[i] = next;
#ifdef STATS_COLLECTION
        stat_lock.lock();
    gen_accesses.insert(TOBLOCK(next));
    stat_lock.unlock();
#endif
    }
    printf("end init\n");
}

bool Equal(char buf1[], char buf2[], int size) {
    int i;
    for (i = 0; i < size; i++) {
        if (buf1[i] != buf2[i]) {
            break;
        }
    }
    return i == size ? true : false;
}

void Run(DDSM* alloc, GlobalAddress data[], GlobalAddress access[],
         std::unordered_map<uint64_t , int>& addr_to_pos, bool shared[], int id,
         unsigned int* seedp, bool warmup) {

    GlobalAddress to_access = access[0];  //access starting point
    char buf[item_size];
    int ret;
    int j = 0;
//	int writes = 0;
//	GAddr fence_addr = alloc->Malloc(1);
//	epicAssert(fence_addr);
    long start = get_time();
    for (int i = 0; i < ITERATION; i++) {
//		if(writes == FENCE_PERIOD) {
//			alloc->MFence();
//			char c;
//			ret = alloc->Read(fence_addr, &c, 1);
//			epicAssert(ret == 1);
//			writes = 0;
//		}

#ifdef STATS_COLLECTION
        int pos;
    try {
      pos = addr_to_pos.at(TOBLOCK(to_access));
    } catch (const exception& e) {
      epicLog(LOG_WARNING, "cannot find pos for addr %lx\n", TOBLOCK(to_access), e.what());
      epicAssert(false);
    }
    if(WID(to_access) != alloc->GetID()) {
      remote_access++;
    }
    if(shared[pos]) {
      shared_access++;
    }
    stat_lock.lock();
    real_accesses.insert(TOBLOCK(to_access));
    //printf("%lx,%lx\n", TOBLOCK(to_access), to_access);
    stat_lock.unlock();
#endif
        switch (op_type) {
            case 0:  //read/write
                if (TrueOrFalse(read_ratio, seedp)) {
                    void* page_buffer;
                    Cache::Handle handle;
                    alloc->PrePage_Read(page_buffer, TOPAGE(to_access), &handle);
                    memcpy(buf, (char*)page_buffer + (to_access.offset % kLeafPageSize), item_size);
                    alloc->PostPage_Read(TOPAGE(to_access), &handle);

                } else {
                    void* page_buffer;
                    Cache::Handle handle;
                    memset(buf, i, item_size);
                    alloc->PrePage_Write(page_buffer, TOPAGE(to_access), &handle);
                    // Can not write to random place because we can not hurt the metadata in the page.
                    memcpy((char*)page_buffer + (64), buf, item_size);
                    alloc->PostPage_Write(TOPAGE(to_access), &handle);
                }
                break;
            case 1:  //rlock/wlock
            {
                if (TrueOrFalse(read_ratio, seedp)) {
                    void* page_buffer;
                    Cache::Handle handle;
                    alloc->PrePage_Read(page_buffer, TOPAGE(to_access), &handle);
                    memcpy(buf, (char*)page_buffer + (to_access.offset % kLeafPageSize), item_size);
                    alloc->PostPage_Read(TOPAGE(to_access), &handle);

                } else {
                    void* page_buffer;
                    Cache::Handle handle;
                    memset(buf, i, item_size);
                    alloc->PrePage_Write(page_buffer, TOPAGE(to_access), &handle);
                    // Can not write to random place because we can not hurt the metadata in the page.
                    memcpy((char*)page_buffer + (64), buf, item_size);
                    alloc->PostPage_Write(TOPAGE(to_access), &handle);
                }
                break;
            }
            case 2:  //rlock+read/wlock+write Is this GAM PSO
            {
                if (TrueOrFalse(read_ratio, seedp)) {
                    void* page_buffer;
                    Cache::Handle handle;
                    alloc->PrePage_Read(page_buffer, TOPAGE(to_access), &handle);
                    memcpy(buf, (char*)page_buffer + (to_access.offset % kLeafPageSize), item_size);
                    alloc->PostPage_Read(TOPAGE(to_access), &handle);

                } else {
                    void* page_buffer;
                    Cache::Handle handle;
                    memset(buf, i, item_size);
                    alloc->PrePage_Write(page_buffer, TOPAGE(to_access), &handle);
                    // Can not write to random place because we can not hurt the metadata in the page.
                    memcpy((char*)page_buffer + (64), buf, item_size);
                    alloc->PostPage_Write(TOPAGE(to_access), &handle);
                }
                break;
            }
            case 3:  //try_rlock/try_wlock
            {
                if (TrueOrFalse(read_ratio, seedp)) {
                    void* page_buffer;
                    Cache::Handle handle;
                    alloc->PrePage_Read(page_buffer, TOPAGE(to_access), &handle);
                    memcpy(buf, (char*)page_buffer + (to_access.offset % kLeafPageSize), item_size);
                    alloc->PostPage_Read(TOPAGE(to_access), &handle);

                } else {
                    void* page_buffer;
                    Cache::Handle handle;
                    memset(buf, i, item_size);
                    alloc->PrePage_Write(page_buffer, TOPAGE(to_access), &handle);
                    // Can not write to random place because we can not hurt the metadata in the page.
                    memcpy((char*)page_buffer + (64), buf, item_size);
                    alloc->PostPage_Write(TOPAGE(to_access), &handle);
                }
                break;
            }
            default:
                printf( "unknown op type\n");
                break;
        }

        //time locality
        if (TrueOrFalse(time_locality, seedp)) {
            //we keep to access the same addr
            //epicLog(LOG_DEBUG, "keep to access the current location");
#ifdef STATS_COLLECTION
            time_local_access++;
#endif
        } else {
            j++;
            if (j == ITERATION) {
                j = 0;
                assert(i == ITERATION - 1);
            }
#ifdef STATS_COLLECTION
            if (TOBLOCK(to_access) == TOBLOCK(access[j])) {
        space_local_access++;
      }
#endif
            to_access = access[j];
            //epicAssert(buf == to_access || addr_to_pos.count(buf) == 0);
        }
    }

//    if (op_type == 0) {
//        // issue a fence and a read request to the last address to ensure all previous
//        // op have been done
//        alloc->MFence();
//        ret = alloc->Read(to_access, buf, item_size);
//    }

    long end = get_time();
    long throughput = ITERATION / ((double) (end - start) / 1000 / 1000 / 1000);
    long latency = (end - start) / ITERATION;
    printf(
            "node_id %d, thread %d, average throughput = %ld per-second, latency = %ld ns %s",
            node_id, id, throughput, latency, warmup ? "(warmup)" : "");
    if (!warmup) {
        total_throughput.fetch_add(throughput);
        avg_latency.fetch_add(latency);
    }
}

void Benchmark(int id, DDSM* alloc) {

    unsigned int seedp = compute_num * alloc->GetID() + id;
    printf("seedp = %d\n", seedp);

#ifdef PERF_MALLOC
    long it = 1000000;
  long start = get_time();
  int len = sizeof(int);
  GAddr addrs[it];
  for (int i = 0; i < it; i++) {
    len = GetRandom(1, 10485, &seedp);
    addrs[i] = alloc->Malloc(len);
    alloc->Free(addrs[i]);
    epicAssert(addrs[i]);
  }
  long end = get_time();
  long duration = end - start;
  epicLog(LOG_WARNING, "Malloc (local): throughput = %lf op/s, latency = %ld ns",
      (double )it / ((double )duration / 1000 / 1000 / 1000),
      duration / it);

//	start = get_time();
//	for (int i = 0; i < it; i++) {
//		alloc->Free(addrs[i]);
//	}
//	end = get_time();
//	duration = end - start;
//	epicLog(LOG_WARNING, "Free (local): throughput = %lf op/s, latency = %ld ns",
//			(double )it / ((double )duration / 1000 / 1000 / 1000),
//			duration / it);

  start = get_time();
  for (int i = 0; i < it; i++) {
    len = GetRandom(1, 1048576, &seedp);
    addrs[i] = alloc->Malloc(len, REMOTE);
    alloc->Free(addrs[i]);
  }
  end = get_time();
  duration = end - start;
  epicLog(LOG_WARNING, "Malloc (remote): throughput = %lf op/s, latency = %ld ns",
      (double )it / ((double )duration / 1000 / 1000 / 1000),
      duration / it);

//	start = get_time();
//	for (int i = 0; i < it; i++) {
//		alloc->Free(addrs[i]);
//	}
//	end = get_time();
//	duration = end - start;
//	epicLog(LOG_WARNING, "Free (remote): throughput = %lf op/s, latency = %ld ns",
//			(double )it / ((double )duration / 1000 / 1000 / 1000),
//			duration / it);
#endif

    GlobalAddress *data = (GlobalAddress*) malloc(sizeof(GlobalAddress) * STEPS);
    std::unordered_map<uint64_t , int> addr_to_pos;
//	GAddr** ldata =(GAddr**)malloc(sizeof(GAddr*)*STEPS);
//	//init local data arrays
//	for(int i = 0; i < STEPS; i++) {
//		ldata[i] = (GAddr*)malloc(BLOCK_SIZE);
//	}
    GlobalAddress* access = (GlobalAddress*) malloc(sizeof(GlobalAddress) * ITERATION);

    //bool shared[STEPS];
    bool* shared = (bool*) malloc(sizeof(bool) * STEPS);

    Init(alloc, data, access, shared, id, &seedp);

    //init addr_to_pos map
    for (int i = 0; i < STEPS; i++) {
        addr_to_pos[data[i]] = i;
    }

//	//localize the shared data access part
//	for(int i = 0; i < STEPS; i++) {
//		for(int j = 0; j < items_per_block; j++) {
//			if(addr_to_pos.count(TOBLOCK(ldata[i][j])) == 0) {
//				//FIXME:remove
//				epicAssert(!(id == 0 && is_master)); //cannot be the master process
//				epicAssert(ldata[i][j] == TOBLOCK(ldata[i][j])); //should be the starting addr of the next block in the master access trace
//				//ldata[i][j] = data[GetRandom(0, STEPS, &seedp)]; //data[CyclingIncr(i, STEPS)];  //update to the starting addr of the next block in its private access trace
//				GAddr n = GADD(data[GetRandom(0, STEPS, &seedp)], GetRandom(0, items_per_block, &seedp)*item_size);
//				while(TOBLOCK(n) == TOBLOCK(data[i])) { //data[CyclingIncr(i, STEPS)];
//					n = GADD(data[GetRandom(0, STEPS, &seedp)], GetRandom(0, items_per_block, &seedp)*item_size);
//				}
//				ldata[i][j] = n;
//			}
//		}
//	}

    printf("start warmup the benchmark on thread %d", id);

    bool warmup = true;
    Run(alloc, data, access, addr_to_pos, shared, id, &seedp, warmup);

    uint64_t SYNC_RUN_BASE = SYNC_KEY + compute_num * 2;
    int sync_id = SYNC_RUN_BASE + compute_num * node_id + id;
    alloc->rdma_mg->sync_with_computes_Cside();
//    alloc->Put(sync_id, &sync_id, sizeof(int));
//
//    for (int i = 1; i <= compute_num; i++) {
//        for (int j = 0; j < no_thread; j++) {
//            epicLog(LOG_WARNING, "waiting for node %d, thread %d", i, j);
//            alloc->Get(SYNC_RUN_BASE + compute_num * i + j, &sync_id);
//            epicAssert(sync_id == SYNC_RUN_BASE + compute_num * i + j);
//            epicLog(LOG_WARNING, "get sync_id %d from node %d, thread %d", sync_id, i,
//                    j);
//        }
//    }

    warmup = false;
    // reset cache statistics
//    stat_lock.lock();
//    if (!reset) {
//        alloc->ResetCacheStatistics();
//        reset = true;
//    }
//    stat_lock.unlock();

    printf( "start run the benchmark on thread %d\n", id);
    Run(alloc, data, access, addr_to_pos, shared, id, &seedp, warmup);
//#ifndef LOCAL_MEMORY
//    //make sure all the requests are complete
//    alloc->MFence();
//    alloc->WLock(data[0], BLOCK_SIZE);
//    alloc->UnLock(data[0], BLOCK_SIZE);
//#endif
}

int main(int argc, char* argv[]) {
    //the first argument should be the program name
    for (int i = 1; i < argc; i++) {

        if (strcmp(argv[i], "--is_master") == 0) {
            is_master = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--this_node_id") == 0) {
            node_id = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--tcp_port") == 0) {
            tcp_port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--no_thread") == 0) {
            no_thread = atoi(argv[++i]);
//    } else if (strcmp(argv[i], "--remote_ratio") == 0) {
//      remote_ratio = atoi(argv[++i]);  //0..100
        } else if (strcmp(argv[i], "--shared_ratio") == 0) {
            shared_ratio = atoi(argv[++i]);  //0..100
        } else if (strcmp(argv[i], "--read_ratio") == 0) {
            read_ratio = atoi(argv[++i]);  //0..100
        } else if (strcmp(argv[i], "--space_locality") == 0) {
            space_locality = atoi(argv[++i]);  //0..100
        } else if (strcmp(argv[i], "--time_locality") == 0) {
            time_locality = atoi(argv[++i]);  //0..100
        } else if (strcmp(argv[i], "--op_type") == 0) {
            op_type = atoi(argv[++i]);  //0..100
//    } else if (strcmp(argv[i], "--compute_num") == 0) {
//      compute_num = atoi(argv[++i]);  //0..100
        } else if (strcmp(argv[i], "--result_file") == 0) {
            result_file = argv[++i];  //0..100
        } else if (strcmp(argv[i], "--item_size") == 0) {
            item_size = atoi(argv[++i]);
            items_per_block = kLeafPageSize / item_size;

        } else if (strcmp(argv[i], "--cache_size") == 0) {
            cache_size = atoi(argv[++i]);
            cache_size = cache_size * 1024ull * 1024 * 1024;
        } else if (strcmp(argv[i], "--compute_num") == 0) {
            compute_num = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--memory_num") == 0) {
            memory_num = atoi(argv[++i]);
        } else {
            fprintf(stderr, "Unrecognized option %s for benchmark\n", argv[i]);
        }
    }

#ifdef LOCAL_MEMORY
    int memory_type = 0;  //"local memory";
#else
    int memory_type = 1;  //"global memory";
#endif
    printf("Currently configuration is: ");
//    printf(  is_master == 1 ? "true" : "false", no_thread, compute_num);
    printf(
            "compute_num = %d, no_thread = %d, shared_ratio: %d, read_ratio: %d, "
            "space_locality: %d, time_locality: %d, op_type = %s, memory_type = %s, item_size = %d, cache_th = %f, result_file = %s\n",
            compute_num,
            no_thread,
//      remote_ratio,
            shared_ratio,
            read_ratio,
            space_locality,
            time_locality,
            op_type == 0 ?
            "read/write" :
            (op_type == 1 ?
             "rlock/wlock" :
             (op_type == 2 ? "rlock+read/wlock+write" : "try_rlock/try_wlock")),
            memory_type == 0 ? "local memory" : "global memory", item_size, cache_th,
            result_file);

    //srand(1);

    DSMEngine::Cache* cache_ptr = DSMEngine::NewLRUCache(cache_size);

    struct DSMEngine::config_t config = {
            NULL,  /* dev_name */
            NULL,  /* server_name */
            tcp_port, /* tcp_port */
            1,	 /* ib_port */ //physical
            1, /* gid_idx */
            4*10*1024*1024, /*initial local buffer size*/
            node_id,
            cache_ptr
    };
//    DSMEngine::RDMA_Manager::node_id = ThisNodeID;

    auto rdma_mg = DSMEngine::RDMA_Manager::Get_Instance(&config);
    assert(cache_ptr->GetCapacity()> 10000);
    DDSM ddsm = DDSM(cache_ptr, rdma_mg);
    compute_num = ddsm.rdma_mg->GetComputeNodeNum();
    memory_num = ddsm.rdma_mg->GetMemoryNodeNum();
    sleep(1);

    //sync with all the other workers
    //check all the workers are started
    int id;
    node_id = ddsm.GetID();
    printf("This node id is %d\n", node_id);
//    //synchronize here.
//    alloc->Put(SYNC_KEY + node_id, &node_id, sizeof(int));
//    for (int i = 1; i <= no_node; i++) {
//        alloc->Get(SYNC_KEY + i, &id);
//        epicAssert(id == i);
//    }

#ifdef PERF_GET
    int it = 1000000;
  alloc->Put(UINT_MAX, &it, sizeof(int));
  long start = get_time();
  int ib;
  for (int i = 0; i < it; i++) {
    alloc->Get(UINT_MAX, &ib);
    epicAssert(ib == it);
  }
  long end = get_time();
  long duration = end - start;
  epicLog(LOG_WARNING, "GET: throughput = %lf op/s, latency = %ld ns",
      (double )it / ((double )duration / 1000 / 1000 / 1000),
      duration / it);
#endif

    std::thread* ths[no_thread];
    for (int i = 0; i < no_thread; i++) {
        ths[i] = new std::thread(Benchmark, i, &ddsm);
    }
    for (int i = 0; i < no_thread; i++) {
        ths[i]->join();
    }

    // print cache statistics
//    ddsm->ReportCacheStatistics();

    long t_thr = total_throughput;
    long a_thr = total_throughput;
    a_thr /= no_thread;
    long a_lat = avg_latency;
    a_lat /= no_thread;
    printf(
            "results for node_id %d: total_throughput: %ld, avg_throuhgput:%ld, avg_latency:%ld",
            node_id, t_thr, a_thr, a_lat);

    //sync with all the other workers
    //check all the benchmark are completed
    long res[3];
    res[0] = t_thr;  //total throughput for the current node
    res[1] = a_thr;  //avg throuhgput for the current node
    res[2] = a_lat;  //avg latency for the current node
    int temp = SYNC_KEY + no_node + node_id;
    ddsm.memSet((char*)&temp, sizeof(int), (char*)res, sizeof(long) * 3);
    t_thr = a_thr = a_lat = 0;
    for (int i = 1; i <= compute_num; i++) {
        memset(res, 0, sizeof(long) * 3);
        temp = SYNC_KEY + no_node + i*2;
        size_t len;
        long* ret = (long*)ddsm.memGet((char*)&temp , sizeof(int), &len);

        t_thr += ret[0];
        a_thr += ret[1];
        a_lat += ret[2];
    }
    a_thr /= compute_num;
    a_lat /= compute_num;

    if (is_master) {
        std::ofstream result;
        result.open(result_file, std::ios::app);
        result << compute_num << "," << no_thread << ","  << ","
               << shared_ratio << "," << read_ratio << "," << space_locality << ","
               << time_locality << "," << op_type << "," << memory_type << ","
               << item_size << "," << t_thr << "," << a_thr << "," << a_lat << ","
               << cache_th << "\n";
        printf(
                "results for all the nodes: "
                "compute_num: %d, no_thread: %d, shared_ratio: %d, read_ratio: %d, space_locality: %d, "
                "time_locality: %d, op_type = %d, memory_type = %d, item_size = %d, "
                "total_throughput: %ld, avg_throuhgput:%ld, avg_latency:%ld, cache_th = %f\n\n",
                compute_num, no_thread, shared_ratio, read_ratio,
                space_locality, time_locality, op_type, memory_type, item_size, t_thr,
                a_thr, a_lat, cache_th);
        result.close();
    }

#ifdef STATS_COLLECTION
    epicLog(LOG_WARNING, "shared_ratio = %lf, remote_ratio = %lf, read_ratio = %lf, space_locality = %lf, time_locality = %lf, "
      "total blocks touched %d, expected blocks touched %d\n",
      ((double)shared_access)/(ITERATION*no_thread)*100/2, ((double)remote_access)/(ITERATION*no_thread)*100/2,
      ((double)read_access)/(ITERATION*no_thread)*100/2,
      ((double)space_local_access)/(ITERATION*no_thread)*100/2, ((double)time_local_access)/(ITERATION*no_thread)*100/2,
      real_accesses.size(), gen_accesses.size());
#endif

//	long time = no_thread*compute_num*(double)(100-read_ratio)/100+1;
//	time /= 2;
//	if(time < 2) time += 1;
    long time = 1;
    printf( "sleep for %ld s\n\n", time);
    sleep(time);

    return 0;
}
