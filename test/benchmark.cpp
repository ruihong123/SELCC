#include "Timer.h"
#include "Btr.h"
#include "zipf.h"
#include "util/random.h"
//#include "util/rdma.h"

#include <city.h>
#include <stdlib.h>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <vector>

// #define USE_CORO
const int kCoroCnt = 3;

// #define BENCH_LOCK

const int kTthreadUpper = 23;

extern uint64_t cache_miss[MAX_APP_THREAD][8];
extern uint64_t cache_hit[MAX_APP_THREAD][8];
extern uint64_t invalid_counter[MAX_APP_THREAD][8];
extern uint64_t lock_fail[MAX_APP_THREAD][8];
extern uint64_t pattern[MAX_APP_THREAD][8];
extern uint64_t hot_filter_count[MAX_APP_THREAD][8];
extern uint64_t hierarchy_lock[MAX_APP_THREAD][8];
extern uint64_t handover_count[MAX_APP_THREAD][8];
extern bool Show_Me_The_Print;
const int kMaxThread = 32;

int kReadRatio;
int kThreadCount;
uint16_t ThisNodeID;
uint16_t tcp_port;
//int kComputeNodeCount;
//int kMemoryNodeCount;
bool table_scan = false;
bool use_range_query = true;

//uint64_t kKeySpace = 64 * define::MB;
uint64_t kKeySpace = 20*1024*1024; // bigdata
//uint64_t kKeySpace = 50*1024*1024; //cloudlab
double kWarmRatio = 0.8;

double zipfan = 0;

std::thread th[kMaxThread];
uint64_t tp[kMaxThread][8];

volatile bool need_stop;
extern uint64_t latency[MAX_APP_THREAD][LATENCY_WINDOWS];
uint64_t latency_th_all[LATENCY_WINDOWS];

DSMEngine::Btr *tree;
DSMEngine::RDMA_Manager *rdma_mg;

inline Key to_key(uint64_t k) {
  return (CityHash64((char *)&k, sizeof(k)) + 1) % kKeySpace;
}

class RequsetGenBench : public RequstGen {

public:
  RequsetGenBench(int coro_id, DSMEngine::RDMA_Manager *dsm, int id)
      : coro_id(coro_id), dsm(dsm), id(id) {
    seed = rdtsc();
    mehcached_zipf_init(&state, kKeySpace, zipfan,
                        (rdtsc() & (0x0000ffffffffffffull)) ^ id);
  }

  Request next() override {
    Request r;
    uint64_t dis = mehcached_zipf_next(&state);

    r.k = to_key(dis);
    r.v = 23;
    r.is_search = rand_r(&seed) % 100 < kReadRatio;

    tp[id][0]++;

    return r;
  }

private:
  int coro_id;
    DSMEngine::RDMA_Manager *dsm;
  int id;

  unsigned int seed;
  struct zipf_gen_state state;
};

RequstGen *coro_func(int coro_id, DSMEngine::RDMA_Manager *dsm, int id) {
  return new RequsetGenBench(coro_id, dsm, id);
}

Timer bench_timer;
std::atomic<int64_t> warmup_cnt{0};
std::atomic_bool ready{false};
//extern bool enable_cache;
void thread_run(int id) {
    DSMEngine::Random64 rand(id);

    bindCore(id);

//  rdma_mg->registerThread();

#ifndef BENCH_LOCK
  uint64_t all_thread = kThreadCount * rdma_mg->GetComputeNodeNum();
  uint64_t my_id = kThreadCount * (DSMEngine::RDMA_Manager::node_id)/2 + id;

  printf("I am %d\n", my_id);

  if (id == 0) {
    bench_timer.begin();
  }

  uint64_t end_warm_key = kKeySpace;
//    enable_cache = true;
  //kWarmRatio *
  for (uint64_t i = 1; i < end_warm_key; ++i) {
      // we can not sequentially pop up the data. Otherwise there will be a bug.
      if (i % all_thread == my_id) {
        tree->insert(i, i * 2);
//        tree->insert(to_key(i), i * 2);
//        tree->insert(rand.Next()%(kKeySpace), i * 2);

        }
      if (i % 1000000 == 0 && id ==0){
          printf("warm up number: %lu\n", i);
      }
  }
//    if (table_scan){
//        for (uint64_t i = 1; i < end_warm_key; ++i) {
//            // we can not sequentially pop up the data. Otherwise there will be a bug.
//            if (i % all_thread == my_id) {
//                Value v;
//                tree->search(to_key(i),v);
//            }
//            if (i % 1000000 == 0 ){
//                printf("cache warm up number: %lu\r", i);
//            }
//        }
//    }

  warmup_cnt.fetch_add(1);

  if (id == 0) {
    while (warmup_cnt.load() != kThreadCount)
      ;
    printf("node %d finish\n", rdma_mg->node_id);
      rdma_mg->sync_with_computes_Cside();

    uint64_t ns = bench_timer.end();
    printf("warmup time %lds\n", ns / 1000 / 1000 / 1000);

//    tree->index_cache_statistics();
    tree->clear_statistics();

    ready.store(true);

    warmup_cnt.store(0);
  }

  while (warmup_cnt.load() != 0)
    ;

#endif

#ifdef USE_CORO
  tree->run_coroutine(coro_func, id, kCoroCnt);

#else

  /// without coro
  unsigned int seed = rdtsc();
  struct zipf_gen_state state;
  mehcached_zipf_init(&state, kKeySpace, zipfan,
                      (rdtsc() & (0x0000ffffffffffffull)) ^ id);

  Timer timer;
  Value *value_buffer = (Value *)malloc(sizeof(Value) * 1024 * 1024);
  int print_counter = 0;
  uint64_t scan_pos = 0;

  while (true) {

    if (need_stop || id >= kTthreadUpper) {
      while (true)
        ;
    }
    // the dis range is [0, 64M]
//    uint64_t dis = mehcached_zipf_next(&state);
    uint64_t key = rand.Next()%(kKeySpace);
//    uint64_t key = to_key(dis);

    Value v;

    timer.begin();

#ifdef BENCH_LOCK
    if (rdma_mg->getMyNodeID() == 0) {
      while (true)
        ;
    }
    tree->lock_bench(key);
#else
//      if (table_scan){
//          if (use_range_query){
//              tree->range_query(scan_pos, scan_pos + 1000*1000, value_buffer);
//              scan_pos += 1000*1000;
//              if(scan_pos > kKeySpace)
//                  break;
//          }else{
//              tree->search(scan_pos,v);
//              scan_pos++;
//              if(scan_pos > kKeySpace)
//                  break;
//          }
//
//
//      }


    if (rand_r(&seed) % 100 < kReadRatio) { // GET
//        printf("Get one key");
      tree->search(key, v);

    } else {
      v = 12;
      tree->insert(key, v);
    }
    print_counter++;
    if (print_counter%100000 == 0)
    {
        printf("%d key-value pairs hase been executed\r", print_counter);
    }
//      if (print_counter%100000 == 0)
//      {
//          printf("the generated distributed key is %d\n", dis);
//      }
#endif
    auto us_10 = timer.end() / 100;
    if (us_10 >= LATENCY_WINDOWS) {
      us_10 = LATENCY_WINDOWS - 1;
    }
    latency[id][us_10]++;
      if (table_scan&&use_range_query){
          tp[id][0] += 1000*1000;
      }else{
          tp[id][0]++;
      }

  }

#endif
}

void parse_args(int argc, char *argv[]) {
  if (argc != 6) {
    printf("Usage: ./btree_bench kReadRatio kThreadCount tablescan ThisNodeID PortNum\n");
    exit(-1);
  }

//    kComputeNodeCount = atoi(argv[1]);
//    kMemoryNodeCount = atoi(argv[2]);
    kReadRatio = atoi(argv[1]);
    kThreadCount = atoi(argv[2]);

    int scan_number = atoi(argv[3]);
    ThisNodeID = atoi(argv[4]);
    tcp_port = atoi(argv[5]);
    if(scan_number == 0)
        table_scan = false;
    else
        table_scan = true;

    printf("kReadRatio %d, kThreadCount %d, tablescan %d, ThisNodeID %d, PortNum %d\n", kReadRatio, kThreadCount, table_scan, ThisNodeID, tcp_port);
}

void cal_latency() {
  uint64_t all_lat = 0;
  for (int i = 0; i < LATENCY_WINDOWS; ++i) {
    latency_th_all[i] = 0;
    for (int k = 0; k < MAX_APP_THREAD; ++k) {
      latency_th_all[i] += latency[k][i];
    }
    all_lat += latency_th_all[i];
  }

  uint64_t th50 = all_lat / 2;
  uint64_t th90 = all_lat * 9 / 10;
  uint64_t th95 = all_lat * 95 / 100;
  uint64_t th99 = all_lat * 99 / 100;
  uint64_t th999 = all_lat * 999 / 1000;

  uint64_t cum = 0;
  for (int i = 0; i < LATENCY_WINDOWS; ++i) {
    cum += latency_th_all[i];

    if (cum >= th50) {
      printf("p50 %f\t", i / 10.0);
      th50 = -1;
    }
    if (cum >= th90) {
      printf("p90 %f\t", i / 10.0);
      th90 = -1;
    }
    if (cum >= th95) {
      printf("p95 %f\t", i / 10.0);
      th95 = -1;
    }
    if (cum >= th99) {
      printf("p99 %f\t", i / 10.0);
      th99 = -1;
    }
    if (cum >= th999) {
      printf("p999 %f\n", i / 10.0);
      th999 = -1;
      return;
    }
  }
}

int main(int argc, char *argv[]) {

    std::cout << "Using Boost "
              << BOOST_VERSION / 100000     << "."  // major version
              << BOOST_VERSION / 100 % 1000 << "."  // minor version
              << BOOST_VERSION % 100                // patch level
              << std::endl;
  parse_args(argc, argv);
    DSMEngine::Cache* cache_ptr = DSMEngine::NewLRUCache(define::kIndexCacheSize*define::MB);

    struct DSMEngine::config_t config = {
            NULL,  /* dev_name */
            NULL,  /* server_name */
            tcp_port, /* tcp_port */
            1,	 /* ib_port */ //physical
            1, /* gid_idx */
            4*10*1024*1024, /*initial local buffer size*/
            ThisNodeID,
            cache_ptr
    };
//    DSMEngine::RDMA_Manager::node_id = ThisNodeID;

    rdma_mg = DSMEngine::RDMA_Manager::Get_Instance(config);
    assert(cache_ptr->GetCapacity()> 10000);
//  rdma_mg->registerThread();
  tree = new DSMEngine::Btr(rdma_mg, cache_ptr, 0);

#ifndef BENCH_LOCK
  if (DSMEngine::RDMA_Manager::node_id == 0) {
    for (uint64_t i = 1; i < 1024000; ++i) {
//        printf("insert key %d", i);
      tree->insert(to_key(i), i * 2);
//        tree->insert(i, i * 2);
    }
  }
#endif

    rdma_mg->sync_with_computes_Cside();

  for (int i = 0; i < kThreadCount; i++) {
    th[i] = std::thread(thread_run, i);
  }

#ifndef BENCH_LOCK
  while (!ready.load())
    ;
#endif
#ifndef NDEBUG
  Show_Me_The_Print  = true;
#endif
  timespec s, e;
  uint64_t pre_tp = 0;
  uint64_t pre_ths[MAX_APP_THREAD];
  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    pre_ths[i] = 0;
  }

  int count = 0;

  clock_gettime(CLOCK_REALTIME, &s);
  while (true) {
      // throutput every 10 second
    sleep(10);
    clock_gettime(CLOCK_REALTIME, &e);
    int microseconds = (e.tv_sec - s.tv_sec) * 1000000 +
                       (double)(e.tv_nsec - s.tv_nsec) / 1000;

    uint64_t all_tp = 0;
    for (int i = 0; i < kThreadCount; ++i) {
      all_tp += tp[i][0];
//      tp[i][0] = 0;
    }
    uint64_t cap = all_tp - pre_tp;
    pre_tp = all_tp;
      printf("cap is %lu\n", cap);

    for (int i = 0; i < kThreadCount; ++i) {
      auto val = tp[i][0];
      // printf("thread %d %ld\n", i, val - pre_ths[i]);
      pre_ths[i] = val;
    }

    uint64_t all = 0;
    uint64_t hit = 0;
//    uint64_t realhit = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      all += (cache_hit[i][0] + cache_miss[i][0]);
      hit += cache_hit[i][0];
      //May be we need atomic variable here.
        cache_hit[i][0] = 0;
        cache_miss[i][0] = 0;
//      realhit += invalid_counter[i][0];
    }

    uint64_t fail_locks_cnt = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      fail_locks_cnt += lock_fail[i][0];
      lock_fail[i][0] = 0;
    }
    // if (fail_locks_cnt > 500000) {
    //   // need_stop = true;
    // }

    //  pattern
    uint64_t pp[8];
    memset(pp, 0, sizeof(pp));
    for (int i = 0; i < 8; ++i) {
      for (int t = 0; t < MAX_APP_THREAD; ++t) {
        pp[i] += pattern[t][i];
        pattern[t][i] = 0;
      }
    }

    uint64_t hot_count = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      hot_count += hot_filter_count[i][0];
      hot_filter_count[i][0] = 0;
    }

    uint64_t hier_count = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      hier_count += hierarchy_lock[i][0];
      hierarchy_lock[i][0] = 0;
    }

    uint64_t ho_count = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      ho_count += handover_count[i][0];
      handover_count[i][0] = 0;
    }

    clock_gettime(CLOCK_REALTIME, &s);

    if (++count % 3 == 0 && DSMEngine::RDMA_Manager::node_id == 0) {
      cal_latency();
    }

    double per_node_tp = cap * 1.0 / microseconds;
//    uint64_t cluster_tp = rdma_mg->sum((uint64_t)(per_node_tp * 1000));
//    uint64_t cluster_tp = rdma_mg->sum((uint64_t)(per_node_tp * 1000));

    // uint64_t cluster_we = rdma_mg->sum((uint64_t)(hot_count));
    // uint64_t cluster_ho = rdma_mg->sum((uint64_t)(ho_count));

    printf("%d, throughput %.4f\n", DSMEngine::RDMA_Manager::node_id, per_node_tp);

//    if (rdma_mg->getMyNodeID() == 0) {
//      printf("cluster throughput %.3f\n", cluster_tp / 1000.0);

      // printf("WE %.3f HO %.3f\n", cluster_we * 1000000ull / 1.0 /
      // microseconds,
      //        cluster_ho * 1000000ull / 1.0 / microseconds);
        // this is the real cache hit ratge
      printf("cache hit rate: %lf\n", hit * 1.0 / all);
      // printf("ACCESS PATTERN");
      // for (int i = 0; i < 8; ++i) {
      //   printf("\t%ld", pp[i]);
      // }
      // printf("\n");
      // printf("%d fail locks: %ld %s\n", rdma_mg->getMyNodeID(), fail_locks_cnt,
      //        getIP());

      // printf("hot count %ld\t hierarchy count %ld\t handover %ld\n",
      // hot_count,
      //        hier_count, ho_count);
//    }
  }

  return 0;
}