#if !defined(_BTR_H_)
#define _BTR_H_


#include "util/rdma.h"
#include "DSMEngine/cache.h"
#include "util/page.h"
#include <atomic>
#include <city.h>
#include <functional>

#include "util/locks.h"
//#define kInternalCardinality   (kInternalPageSize - sizeof(Header) - sizeof(uint8_t) * 2) /sizeof(InternalEntry)
//#define kLeafCardinality  (kLeafPageSize - sizeof(Header) - sizeof(uint8_t) * 2) / sizeof(LeafEntry)

//#define InternalPagePadding (kInternalPageSize - sizeof(Header) - sizeof(uint8_t) * 2)%sizeof(InternalEntry)
//// InternalPagePadding is 7 for key 20 bytes value 400 bytes
//#define LeafPagePadding (kLeafPageSize - sizeof(Header) - sizeof(uint8_t) * 2)%sizeof(LeafEntry)
//// LeafPagePadding is 143 for key 20 bytes value 400 bytes.
class IndexCache;

struct Request {
    bool is_search;
    Key k;
    Value v;
};

class RequstGen {
public:
    RequstGen() = default;
    virtual Request next() { return Request{}; }
};
using CoroFunc = std::function<RequstGen *(int, DSMEngine::RDMA_Manager *, int)>;

namespace DSMEngine {








class InternalPage;
class LeafPage;
class Btr {
//friend class DSMEngine::InternalPage;

public:
  Btr(RDMA_Manager *mg, Cache *cache_ptr, uint16_t Btr_id = 0);

  void insert(const Key &k, const Value &v, CoroContext *cxt = nullptr,
              int coro_id = 0);
  bool search(const Key &k, Value &v, CoroContext *cxt = nullptr,
              int coro_id = 0);
  void del(const Key &k, CoroContext *cxt = nullptr, int coro_id = 0);

//  uint64_t range_query(const Key &from, const Key &to, Value *buffer,
//                       CoroContext *cxt = nullptr, int coro_id = 0);

  void print_and_check_tree(CoroContext *cxt = nullptr, int coro_id = 0);

  void run_coroutine(CoroFunc func, int id, int coro_cnt);

//  void lock_bench(const Key &k, CoroContext *cxt = nullptr, int coro_id = 0);

  GlobalAddress query_cache(const Key &k);
//  void index_cache_statistics();
  void clear_statistics();
    static RDMA_Manager * rdma_mg;
    static thread_local int thread_id;
    private:
  std::mutex mtx;// in case of contention
  uint64_t tree_id;
//  GlobalAddress root_ptr_ptr; // the address which stores root pointer;
// TODO: not make it as a fixed

    ibv_mr cached_root_page_mr{}; // useful when we want to reduce the hash table access in cache with id. (avoid pointer swizzling)
    InternalPage* cached_root_page_ptr;
  std::atomic<GlobalAddress> g_root_ptr = GlobalAddress::Null();
  static thread_local size_t round_robin_cur;

  // static thread_local int coro_id;
  static thread_local CoroCall worker[define::kMaxCoro];
  static thread_local CoroCall master;
  static thread_local std::shared_mutex* lock_coupling_memo[define::kMaxLevelOfTree];

  std::vector<LocalLockNode *> local_locks;

  Cache * page_cache;
  uint8_t tree_height = 0;
#ifndef NDEBUG
#endif
//    std::atomic<int> cache_invalid_counter;
  void print_verbose();

  void before_operation(CoroContext *cxt, int coro_id);

  GlobalAddress get_root_ptr_ptr();
  GlobalAddress get_root_ptr();

  void coro_worker(CoroYield &yield, RequstGen *gen, int coro_id);
//  void coro_master(CoroYield &yield, int coro_cnt);
  // broadcast the new root to all other memroy servers, if memory server and compute
  // servers are the same then the new root is know by all the compute nodes, However,
  // when we seperate the compute from the memory, the memroy node will not get notified.
  void broadcast_new_root(GlobalAddress new_root_addr, int root_level);
  bool update_new_root(GlobalAddress left, const Key &k, GlobalAddress right,
                       int level, GlobalAddress old_root, CoroContext *cxt,
                       int coro_id);
  // Insert a key and a point at a particular level (level != 0), the node is unknown
  bool insert_internal(Key &k, GlobalAddress &v, CoroContext *cxt,
                       int coro_id, int target_level);

  bool try_lock_addr(GlobalAddress lock_addr, uint64_t tag, ibv_mr *buf,
                     CoroContext *cxt, int coro_id);
  void unlock_addr(GlobalAddress lock_addr, CoroContext *cxt, int coro_id, bool async);
  void write_page_and_unlock(ibv_mr *page_buffer, GlobalAddress page_addr, int page_size, uint64_t *cas_buffer,
                             GlobalAddress remote_lock_addr, CoroContext *cxt, int coro_id, bool async);
  void lock_and_read_page(ibv_mr *page_buffer, GlobalAddress page_addr,
                          int page_size, ibv_mr *cas_buffer,
                          GlobalAddress lock_addr, uint64_t tag,
                          CoroContext *cxt, int coro_id);
    // Node ID in GLobalAddress for a tree pointer should be the id in the Memory pool
    // THis funciton will get the page by the page addr and search the pointer for the
    // next level if it is not leaf page. If it is a leaf page, just put the value in the
    // result. this funciton = fetch the page + internal page serach + leafpage search + re-read
    bool internal_page_search(GlobalAddress page_addr, const Key &k, SearchResult &result, int &level, bool isroot,
                              CoroContext *cxt, int coro_id);
    bool leaf_page_search(GlobalAddress page_addr, const Key &k, SearchResult &result, int level, CoroContext *cxt,
                          int coro_id);
//        void internal_page_search(const Key &k, SearchResult &result);

//    void leaf_page_search(LeafPage *page, const Key &k, SearchResult &result);
    // store a key and a pointer to an known internal node.
    // Note: node range [barrer1, barrer2)
        bool internal_page_store(GlobalAddress page_addr, Key &k, GlobalAddress &v, int level, CoroContext *cxt,
                                 int coro_id);
  //store a key and value to a leaf page
  bool leaf_page_store(GlobalAddress page_addr, const Key &k, const Value &v, Key &split_key,
                       GlobalAddress &sibling_addr, GlobalAddress root, int level, CoroContext *cxt, int coro_id);
  bool leaf_page_del(GlobalAddress page_addr, const Key &k, int level,
                     CoroContext *cxt, int coro_id);

  bool acquire_local_lock(GlobalAddress lock_addr, CoroContext *cxt,
                          int coro_id);
  bool acquire_shared_local_lock(GlobalAddress lock_addr, CoroContext *cxt,
                            int coro_id);
  bool acquire_exclusive_local_lock(GlobalAddress lock_addr, CoroContext *cxt,
                                   int coro_id);
  bool can_hand_over(GlobalAddress lock_addr);
  void releases_local_lock(GlobalAddress lock_addr);
};

class Btr_iter{
    // TODO: implement btree iterator for range query.
};
    static void Deallocate_MR(const Slice& key, void* value) {
        auto mr = (ibv_mr*) value;
        Btr::rdma_mg->Deallocate_Local_RDMA_Slot(mr->addr, Internal_and_Leaf);
        delete mr;
//        delete tf->table_compute;
////  delete tf->file;
//        delete tf;
    }

}
#endif // _TREE_H_
