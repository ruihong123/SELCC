#if !defined(_BTR_H_)
#define _BTR_H_


#include "storage/rdma.h"
#include "DSMEngine/cache.h"
#include "DDSM.h"
#include "storage/page.h"
#include <atomic>
#include <city.h>
#include <functional>
#include "Timer.h"
#include <algorithm>
#include <city.h>
#include <iostream>
#include <queue>
#include <utility>
#include <vector>
//#include <linux/membarrier.h>
#include "port/likely.h"
#include "utils/hash.h"
#include "port/port_posix.h"
#include "utils/Local_opt_locks.h"
//#define kInternalCardinality   (kInternalPageSize - sizeof(Header) - sizeof(uint8_t) * 2) /sizeof(InternalEntry)
//#define kLeafCardinality  (kLeafPageSize - sizeof(Header) - sizeof(uint8_t) * 2) / sizeof(LeafEntry)

//#define InternalPagePadding (kInternalPageSize - sizeof(Header) - sizeof(uint8_t) * 2)%sizeof(InternalEntry)
//// InternalPagePadding is 7 for key 20 bytes value 400 bytes
//#define LeafPagePadding (kLeafPageSize - sizeof(Header) - sizeof(uint8_t) * 2)%sizeof(LeafEntry)
//// LeafPagePadding is 143 for key 20 bytes value 400 bytes.
class IndexCache;

template<class Key, class Value>
struct Request {
    bool is_search;
    Key k;
    Value v;
};
template<class Key, class Value>
class RequstGen {
public:
    RequstGen() = default;
    virtual Request<Key,Value> next() { return Request<Key, Value>{}; }
};
//using CoroFunc = std::function<RequstGen<class Key, class Value> *(int, DSMEngine::RDMA_Manager *, int)>;
extern uint64_t cache_miss[MAX_APP_THREAD][8];
extern uint64_t cache_hit_valid[MAX_APP_THREAD][8];
extern uint64_t invalid_counter[MAX_APP_THREAD][8];
extern uint64_t lock_fail[MAX_APP_THREAD][8];
extern uint64_t pattern[MAX_APP_THREAD][8];
extern uint64_t hierarchy_lock[MAX_APP_THREAD][8];
extern uint64_t handover_count[MAX_APP_THREAD][8];
extern uint64_t hot_filter_count[MAX_APP_THREAD][8];
extern uint64_t latency[MAX_APP_THREAD][LATENCY_WINDOWS];
extern bool Show_Me_The_Print;
extern int TimePrintCounter[MAX_APP_THREAD];

namespace DSMEngine {


    template<class Key>
    class InternalPage;

    template<class Key, class Value>
    class LeafPage;

//TODO: Remove typename Value from all the template defined below.
    template<typename Key, typename Value>
    class Btr {
//friend class DSMEngine::InternalPage;

    public:
        // Assign a unique id to the tree, and allocate the root node by itself
        Btr(DDSM *dsm, Cache *cache_ptr, RecordSchema *record_scheme_ptr, uint16_t Btr_id);
        //Btree waiting for serialization. get the root node from memcached
        Btr(DDSM *dsm, Cache *cache_ptr, RecordSchema *record_scheme_ptr);

        void insert(const Key &k, const Slice &v, CoroContext *cxt = nullptr,
                    int coro_id = 0);

        bool search(const Key &k, const Slice &v, CoroContext *cxt = nullptr,
                    int coro_id = 0);

//        void del(const Key &k, CoroContext *cxt = nullptr, int coro_id = 0);

//  uint64_t range_query(const Key &from, const Key &to, Value *buffer,
//                       CoroContext *cxt = nullptr, int coro_id = 0);

//        void print_and_check_tree(CoroContext *cxt = nullptr, int coro_id = 0);

        GlobalAddress query_cache(const Key &k);

//  void index_cache_statistics();
        void clear_statistics();
        void Serialize(const char*& addr) {
            size_t off = 0;
            memcpy((char*)addr + off, &tree_id, sizeof(uint64_t));
            off += sizeof(uint64_t);
        }
        void Deserialize(const char*& addr) {
            size_t off = 0;
            memcpy(&tree_id, (char*)addr + off, sizeof(uint64_t));
            off += sizeof(uint64_t);
            //TODO: refactor the code below. need to move the attribute initialization to a separate function.
            // Some initialization of attributes which does not need to be serilizaed.
            Cache::Handle* dummy_mr;
            get_root_ptr_protected(dummy_mr);

        }
        static size_t GetSerializeSize() {
            return sizeof(uint64_t);
        }
        uint64_t GetRecordCount(){
            return num_of_record;
        }
        uint64_t GetRootRecordCount(){
            // This function is just for debugging purpose.
            Cache::Handle* handle = cached_root_page_handle.load();
            void* page_buffer;
            Header_Index<Key> * header = nullptr;
//            InternalPage<Key>* page = nullptr;
            ibv_mr* mr = (ibv_mr*)handle->value;
//            assert(mr == (ibv_mr*)handle->value);
            page_buffer = mr->addr;
            header = (Header_Index<Key> *) ((char *) page_buffer + (STRUCT_OFFSET(InternalPage<Key>, hdr)));
            // if is root, then we should always bypass the cache.
//            page = (InternalPage<Key> *)page_buffer;
            return header->last_index + 1;
        }
//    static RDMA_Manager * rdma_mg;
        // TODO: potential bug, if mulitple btrees shared the same retry counter, will it be a problem?
        //  used for the retry counter for nested function call such as sibling pointer access.
        static thread_local int nested_retry_counter;
        RecordSchema *scheme_ptr;
        uint64_t num_of_record = 0;
        uint32_t leaf_cardinality_ = 0;



    private:
        RWSpinLock root_mtx;// in case of contention in the root cache
        uint64_t tree_id;

        // The cached_root_handle_ref is to keep track of the number of reference to the root page handle.
        // this is necessary because the access of cached_root_page_handle will bypass the cache,
        // so the refs in the handle will not be increased, when accessing the root node, which can result in
        // the access on an evicted/destroyed cache handle if the root node is not accessed frequetly.
        // To solve this problem we introduce another reference mechanism in the tree for the cached root node.
        // The btree fetch the root handle from the cache (with handle refs ++ implicitly), when the cached_root_handle is invalid,
        // the cache root handle will not immediately be replaced with handle's refs --. Instead, the invaliding thread
        // will wait for the unfinished access on the old root handle, by checking the cached_root_handle_ref.
        std::atomic<Cache::Handle*> cached_root_page_handle = nullptr;
//        std::atomic<uint32_t> cached_root_handle_ref = 1;
//        std::shared_mutex root_handle_mtx;
        //    InternalPage* cached_root_page_ptr;
        std::atomic<GlobalAddress> g_root_ptr = GlobalAddress::Null();
        std::atomic<uint8_t> tree_height = 0;
        static thread_local size_t round_robin_cur;

        // static thread_local int coro_id;
        static thread_local CoroCall worker[define::kMaxCoro];
        static thread_local CoroCall master;
        static thread_local std::shared_mutex *lock_coupling_memo[define::kMaxLevelOfTree];
        static thread_local SearchResult<Key, Value> *search_result_memo;
        std::vector<LocalLockNode *> local_locks;

        Cache *page_cache;
        DDSM* ddms_ = nullptr;
        RDMA_Manager *rdma_mg = nullptr;
#ifndef NDEBUG
#endif

//    std::atomic<int> cache_invalid_counter;
        void print_verbose();

        void before_operation(CoroContext *cxt, int coro_id);

        GlobalAddress get_root_ptr_ptr();

        GlobalAddress get_root_ptr_protected(Cache::Handle *&root_hint_handle);

        GlobalAddress get_root_ptr(Cache::Handle *&root_hint_handle);

        void refetch_rootnode();

//  void coro_worker(CoroYield &yield, RequstGen<Key,Value> *gen, int coro_id);
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



        // Node ID in GLobalAddress for a tree pointer should be the id in the Memory pool
        // THis funciton will get the page by the page addr and search the pointer for the
        // next level if it is not leaf page. If it is a leaf page, just put the value in the
        // result. this funciton = fetch the page + internal page serach + leafpage search + re-read
        bool internal_page_search(GlobalAddress page_addr, const Key &k, SearchResult<Key, Value> &result, int &level,
                                  bool isroot,
                                  Cache::Handle *handle = nullptr, CoroContext *cxt = nullptr, int coro_id = 0);

        bool leaf_page_search(GlobalAddress page_addr, const Key &k, SearchResult<Key, Value> &result, int level,
                              CoroContext *cxt,
                              int coro_id);
//        void internal_page_search(const Key &k, SearchResult &result);

//    void leaf_page_search(LeafPage *page, const Key &k, SearchResult &result);
        // store a key and a pointer to an known internal node.
        // Note: node range [barrer1, barrer2)
        bool internal_page_store(GlobalAddress page_addr, Key &k, GlobalAddress &v, int level, CoroContext *cxt,
                                 int coro_id);

        //store a key and value to a leaf page [lowest, highest)
        bool leaf_page_store(GlobalAddress page_addr, const Key &k, const Slice &v, Key &split_key,
                             GlobalAddress &sibling_addr, int level, CoroContext *cxt, int coro_id);

//        bool leaf_page_del(GlobalAddress page_addr, const Key &k, int level,
//                           CoroContext *cxt, int coro_id);
//        bool try_lock_addr(GlobalAddress lock_addr, uint64_t tag, ibv_mr *buf,
//                           CoroContext *cxt, int coro_id);
//
//        void unlock_addr(GlobalAddress lock_addr, CoroContext *cxt, int coro_id, bool async);
////        void global_unlock_addr(GlobalAddress remote_lock_add, CoroContext *cxt, int coro_id, bool async);
//
////        void write_page_and_unlock(ibv_mr *page_buffer, GlobalAddress page_addr, int page_size, GlobalAddress remote_lock_addr,
////                        CoroContext *cxt, int coro_id, bool async);
////    void global_write_page_and_unlock(ibv_mr *page_buffer, GlobalAddress page_addr, int page_size,
////                                      GlobalAddress remote_lock_addr, CoroContext *cxt, int coro_id, bool async);
////  void lock_and_read_page(ibv_mr *page_buffer, GlobalAddress page_addr,
////                          int page_size, ibv_mr *cas_buffer,
////                          GlobalAddress lock_addr, uint64_t tag,
////                          CoroContext *cxt, int coro_id);
//        //Be careful, do not overwrite the global lock byte, the global lock should in a write lock state for RDMA write.
////    void global_lock_and_read_page(ibv_mr *page_buffer, GlobalAddress page_addr, int page_size, GlobalAddress lock_addr,
////                                   ibv_mr *cas_buffer, uint64_t tag, CoroContext *cxt, int coro_id);
////    uint64_t renew_swap_by_received_state_readlock(uint64_t& received_state);
////    uint64_t renew_swap_by_received_state_readunlock(uint64_t& received_state);
////    uint64_t renew_swap_by_received_state_readupgrade(uint64_t& received_state);
//        void
//        global_Rlock_and_read_page(ibv_mr *page_buffer, GlobalAddress page_addr, int page_size, GlobalAddress lock_addr,
//                                   ibv_mr *cas_buffer, uint64_t tag, CoroContext *cxt, int coro_id,
//                                   Cache::Handle *handle);
//
//        bool global_Rlock_update(GlobalAddress lock_addr, ibv_mr *cas_buffer, CoroContext *cxt, int coro_id,
//                                 Cache::Handle *handle);
//
//        void global_Wlock_and_read_page_with_INVALID(ibv_mr *page_buffer, GlobalAddress page_addr, int page_size,
//                                                     GlobalAddress lock_addr, ibv_mr *cas_buffer, uint64_t tag,
//                                                     CoroContext *cxt, int coro_id, Cache::Handle *handle);
//
//        void global_RUnlock(GlobalAddress lock_addr, ibv_mr *cas_buffer, CoroContext *cxt, int coro_id,
//                            Cache::Handle *handle);
//
//// Write unlock can share the function for the global_write_page_and_unlock.
//        void global_write_page_and_Wunlock(ibv_mr *page_buffer, GlobalAddress page_addr, int size,
//                                           GlobalAddress lock_addr,
//                                           CoroContext *cxt, int coro_id, Cache::Handle *handle, bool async);
//
//        void global_write_tuple_and_Wunlock(ibv_mr *page_buffer, GlobalAddress page_addr, int size,
//                                            GlobalAddress lock_addr,
//                                            CoroContext *cxt, int coro_id, Cache::Handle *handle, bool async);
//
//        void global_unlock_addr(GlobalAddress remote_lock_add, Cache::Handle *handle, CoroContext *cxt, int coro_id,
//                                bool async = false);
//        bool acquire_local_lock(GlobalAddress lock_addr, CoroContext *cxt,
//                                int coro_id);
//
//        bool try_lock(Local_Meta *local_lock_meta);
//
//        void unlock_lock(Local_Meta *local_lock_meta);
//
//        bool acquire_local_optimistic_lock(Local_Meta *local_lock_meta, CoroContext *cxt,
//                                           int coro_id);
//
//        bool can_hand_over(GlobalAddress lock_addr);
//
//        bool can_hand_over(Local_Meta *local_lock_meta);
//
//        void releases_local_lock(GlobalAddress lock_addr);
//
//        void releases_local_optimistic_lock(Local_Meta *local_lock_meta);
//
////        void make_page_invalidated(InternalPage<Key> *upper_page);
////        void cache_root_handle_ref(){
////            loop_back:
////            uint32_t root_handle_ref = 0;
////            while (root_handle_ref == 0){
////                root_handle_ref = cached_root_handle_ref.load();
////            }
////            if(!cached_root_handle_ref.compare_exchange_strong(root_handle_ref, root_handle_ref + 1)){
////                goto loop_back;
////            }
////        };
////        void cache_root_handle_unref(){
////            auto ret = cached_root_handle_ref.fetch_sub(1);
////            assert(ret >= 1);
////        };
//
//        // should be executed with in a local page lock.
//        void Initialize_page_invalidation(InternalPage<Key> *upper_page);
////        void invalid_root_prt(){
////            std::unique_lock<std::shared_mutex> lck(root_mtx);
////            g_root_ptr.store(GlobalAddress::Null());
////        }
    };
//
////template class Btr<int,int>;
////template class Btr<uint64_t ,uint64_t>;
//    class Btr_iter {
//        // TODO: implement btree iterator for range query.
//    };
}
#endif //BTR_H