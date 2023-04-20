#include "Btr.h"

//#include "IndexCache.h"

//#include "RdmaBuffer.h"
#include "Timer.h"
#include <algorithm>
#include <city.h>
#include <iostream>
#include <queue>
#include <utility>
#include <vector>
//#include <linux/membarrier.h>
#include "port/likely.h"
#include "util/hash.h"
#include "port/port_posix.h"

uint64_t cache_miss[MAX_APP_THREAD][8];
uint64_t cache_hit[MAX_APP_THREAD][8];
uint64_t invalid_counter[MAX_APP_THREAD][8];
uint64_t lock_fail[MAX_APP_THREAD][8];
uint64_t pattern[MAX_APP_THREAD][8];
uint64_t hierarchy_lock[MAX_APP_THREAD][8];
uint64_t handover_count[MAX_APP_THREAD][8];
uint64_t hot_filter_count[MAX_APP_THREAD][8];
uint64_t latency[MAX_APP_THREAD][LATENCY_WINDOWS];
extern bool Show_Me_The_Print;
int TimePrintCounter[MAX_APP_THREAD];


namespace DSMEngine {
bool enter_debug = false;

//struct tranverse_stack_element

thread_local int Btr::nested_retry_counter = 0;
//HotBuffer hot_buf;

volatile bool need_stop = false;
thread_local size_t Btr::round_robin_cur = 0;
thread_local CoroCall Btr::worker[define::kMaxCoro];
thread_local CoroCall Btr::master;
//thread_local GlobalAddress path_stack[define::kMaxCoro]
//                                     [define::kMaxLevelOfTree];

thread_local GlobalAddress path_stack[define::kMaxCoro]
    [define::kMaxLevelOfTree];
RDMA_Manager * Btr::rdma_mg = nullptr;
// for coroutine schedule
struct CoroDeadline {
  uint64_t deadline;
  uint16_t coro_id;

  bool operator<(const CoroDeadline &o) const {
    return this->deadline < o.deadline;
  }
};

    static inline uint32_t HashSlice(const Slice& s) {
        return Hash(s.data(), s.size(), 0);
    }

//thread_local Timer timer;
thread_local std::queue<uint16_t> hot_wait_queue;
thread_local std::priority_queue<CoroDeadline> deadline_queue;
static void Deallocate_MR(Cache::Handle *handle) {
    auto mr = (ibv_mr*) handle->value;
    if (!handle->keep_the_mr){
        Btr::rdma_mg->Deallocate_Local_RDMA_Slot(mr->addr, Internal_and_Leaf);
        delete mr;
    }

}
//TODO: make the function set cache handle as an argument, and we need to modify the remote lock status
// when unlocking the remote lock.
static void Deallocate_MR_WITH_CCP(Cache::Handle *handle) {
    // TOFIX: The code below is not protected by the lock shared mutex. Besides,
    //  the deletor may also not well protected by the cache mutex.

    // Do we need the lock during this deleter? Answer: Probably not, because it is guaratee to have only on thread comes here.
    auto mr = (ibv_mr*) handle->value;
    if (handle->strategy == 1){
        GlobalAddress lock_gptr = handle->gptr;
        lock_gptr.offset = lock_gptr.offset + STRUCT_OFFSET(LeafPage, global_lock);
        if (handle->remote_lock_status == 1){

            // RDMA read unlock
//            printf("release the read lock during the handle destroy\n ");
            Btr::rdma_mg->global_RUnlock(lock_gptr, Btr::rdma_mg->Get_local_CAS_mr());
            handle->remote_lock_status.store(0);

        }else if(handle->remote_lock_status == 2){

            // TODO: shall we not consider the global lock word when flushing back the page?

//            printf("release the write lock at %lu and write back data during the handle destroy\n ", lock_gptr.offset);
//            ibv_mr* local_mr = (ibv_mr*)value;
            assert(mr->addr!= nullptr );
            assert(((LeafPage*)mr->addr)->global_lock);
            // RDMA write unlock and write back the data.
            Btr::rdma_mg->global_write_page_and_Wunlock(mr, handle->gptr, kLeafPageSize, lock_gptr,
                                                        nullptr, 0);
            handle->remote_lock_status.store(0);
        }else{
            //An invalidated page, do nothing
        }
    }else{
        //TODO: delete the  asserts below when you implement the strategy 2.

        assert(false);
    }
//    printf("Deallocate mr for %lu\n", g_ptr.offset);
        if (!handle->keep_the_mr){
            Btr::rdma_mg->Deallocate_Local_RDMA_Slot(mr->addr, Internal_and_Leaf);
            delete mr;
        }
//    delete mr;
}
Btr::Btr(RDMA_Manager *mg, Cache *cache_ptr, uint16_t Btr_id) : tree_id(Btr_id), page_cache(cache_ptr){
    if (rdma_mg == nullptr){
        rdma_mg = mg;
    }
  for (int i = 0; i < rdma_mg->GetMemoryNodeNum(); ++i) {
    local_locks.push_back(new LocalLockNode[define::kNumOfLock]);
    for (size_t k = 0; k < define::kNumOfLock; ++k) {
      auto &n = local_locks[i][k];
      n.ticket_lock.store(0);
      n.hand_over = false;
      n.hand_time = 0;
    }
  }
    assert(sizeof(InternalPage) <= kInternalPageSize);

  print_verbose();
    assert(g_root_ptr.is_lock_free());
//    page_cache = NewLRUCache(define::kIndexCacheSize);

//  root_ptr_ptr = get_root_ptr_ptr();
    cached_root_page_mr = new ibv_mr{};
  // try to init tree and install root pointer
    rdma_mg->Allocate_Local_RDMA_Slot(*cached_root_page_mr, Internal_and_Leaf);// local allocate
    memset(cached_root_page_mr.load()->addr,0,rdma_mg->name_to_chunksize.at(Internal_and_Leaf));
    if (DSMEngine::RDMA_Manager::node_id == 0){
        // only the first compute node create the root node for index
        g_root_ptr = rdma_mg->Allocate_Remote_RDMA_Slot(Internal_and_Leaf, 2 * round_robin_cur + 1); // remote allocation.
        printf("root pointer is %d, %lu\n", g_root_ptr.load().nodeID, g_root_ptr.load().offset);
        if(++round_robin_cur == rdma_mg->memory_nodes.size()){
            round_robin_cur = 0;
        }
        auto root_page = new(cached_root_page_mr.load()->addr) LeafPage(g_root_ptr, 0);

        root_page->front_version++;
        root_page->rear_version = root_page->front_version;
        rdma_mg->RDMA_Write(g_root_ptr, cached_root_page_mr, kLeafPageSize, IBV_SEND_SIGNALED, 1, Internal_and_Leaf);
        // TODO: create a special region to store the root_ptr for every tree id.
        auto local_mr = rdma_mg->Get_local_CAS_mr(); // remote allocation.
        ibv_mr remote_mr{};
        remote_mr = *rdma_mg->global_index_table;
        // find the table enty according to the id
        remote_mr.addr = (void*) ((char*)remote_mr.addr + 8*tree_id);
        printf("Writer to remote address %p", remote_mr.addr);
        rdma_mg->RDMA_CAS(&remote_mr, local_mr, 0, g_root_ptr.load(), IBV_SEND_SIGNALED, 1, 1);
        assert(*(uint64_t*)local_mr->addr == 0);

    }else{
        rdma_mg->Allocate_Local_RDMA_Slot(*cached_root_page_mr, Internal_and_Leaf);// local allocate
        memset(cached_root_page_mr.load()->addr,0,rdma_mg->name_to_chunksize.at(Internal_and_Leaf));
//        rdma_mg->Allocate_Local_RDMA_Slot()
        ibv_mr* dummy_mr;
        get_root_ptr(dummy_mr);
    }

//  auto cas_buffer = (rdma_mg->get_rbuf(0)).get_cas_buffer();
//  bool res = rdma_mg->cas_sync(root_ptr_ptr, 0, root_addr.val, cas_buffer);
//  if (res) {
//    std::cout << "Tree root pointer value " << root_addr << std::endl;
//  } else {
//     std::cout << "fail\n";
//  }

}

void Btr::print_verbose() {

  int kLeafHdrOffset = STRUCT_OFFSET(LeafPage, hdr);
  int kInternalHdrOffset = STRUCT_OFFSET(InternalPage, hdr);
  assert(kLeafHdrOffset == kInternalHdrOffset);

  if (rdma_mg->node_id == 0) {
    std::cout << "Header size: " << sizeof(Header) << std::endl;
    std::cout << "Internal_and_Leaf Page size: " << sizeof(InternalPage) << " ["
              << kInternalPageSize << "]" << std::endl;
    std::cout << "Internal_and_Leaf per Page: " << kInternalCardinality << std::endl;
    std::cout << "Leaf Page size: " << sizeof(LeafPage) << " [" << kLeafPageSize
              << "]" << std::endl;
    std::cout << "Leaf per Page: " << kLeafCardinality << std::endl;
    std::cout << "LeafEntry size: " << sizeof(LeafEntry) << std::endl;
    std::cout << "InternalEntry size: " << sizeof(InternalEntry) << std::endl;
  }
}

inline void Btr::before_operation(CoroContext *cxt, int coro_id) {
  for (size_t i = 0; i < define::kMaxLevelOfTree; ++i) {
    path_stack[coro_id][i] = GlobalAddress::Null();
  }
}

GlobalAddress Btr::get_root_ptr_ptr() {
  GlobalAddress addr;
  addr.nodeID = 0;
  addr.offset =
      define::kRootPointerStoreOffest + sizeof(GlobalAddress) * tree_id;

  return addr;
}

//extern GlobalAddress g_root_ptr;
//extern int g_root_level;
//extern bool enable_cache;
GlobalAddress Btr::get_root_ptr(ibv_mr*& root_hint) {
    //Note it is okay if cached_root_page_mr is an older version for the g_root_ptr, because when we use the
    // page we will check whether this page is correct or not

    GlobalAddress root_ptr = g_root_ptr.load();
        root_hint = cached_root_page_mr.load();
  if (root_ptr == GlobalAddress::Null()) {
      std::unique_lock<std::mutex> l(root_mtx);

      root_ptr = g_root_ptr.load();
      root_hint = cached_root_page_mr.load();
      if (root_ptr == GlobalAddress::Null()) {
//          assert(cached_root_page_mr = nullptr);
          refetch_rootnode();
          root_ptr = g_root_ptr.load();
          root_hint = cached_root_page_mr.load();
      }
      return root_ptr;
  } else {
//      assert(((InternalPage*)cached_root_page_mr->addr)->hdr.this_page_g_ptr == root_ptr);
//      root_hint = cached_root_page_mr;
    return root_ptr;
  }

  // std::cout << "root ptr " << root_ptr << std::endl;
}
// should be protected by a mtx outside.
void Btr::refetch_rootnode() {
    // TODO: an alternative design is to insert this page into the cache. How to make sure there is no
    //  reader reading this old root note? If we do not deallocate it there will be registered memory leak
    //  we can lazy recycle this registered memory. Or we just ignore this memory leak because it will
    //  only happen at the root page.

//            rdma_mg->Deallocate_Local_RDMA_Slot(cached_root_page_mr.load()->addr, Internal_and_Leaf);
//            delete cached_root_page_mr.load();

    ibv_mr* local_mr = rdma_mg->Get_local_CAS_mr();

    ibv_mr remote_mr{};
    remote_mr = *rdma_mg->global_index_table;
    // find the table enty according to the id
    //TODO: sometimes the page search will re invalidate the g_toot_ptr after we RDMA read the new  root.
    // need to find a way to invalidate only once.
    remote_mr.addr = (void*) ((char*)remote_mr.addr + 8*tree_id);
    *(GlobalAddress*)(local_mr->addr) = GlobalAddress::Null();
    // The first compute node may not have written the root ptr to root_ptr_ptr, we need to keep polling.
    while (*(GlobalAddress*)(local_mr->addr) == GlobalAddress::Null()) {
        rdma_mg->RDMA_Read(&remote_mr, local_mr, sizeof(GlobalAddress), IBV_SEND_SIGNALED, 1, 1);
    }
    assert(*(GlobalAddress*)local_mr->addr != GlobalAddress::Null());
    GlobalAddress root_ptr = *(GlobalAddress*)local_mr->addr;

    //Try to rebuild a local mr for the new root, the old root may
    ibv_mr* temp_mr = new ibv_mr{};

    // try to init tree and install root pointer
    rdma_mg->Allocate_Local_RDMA_Slot(*temp_mr, Internal_and_Leaf);// local allocate
    memset(temp_mr->addr,0,rdma_mg->name_to_chunksize.at(Internal_and_Leaf));
    //Read a larger enough data for the root node thorugh it may oversize the page but it is ok since we only read the data.
    rdma_mg->RDMA_Read(root_ptr, temp_mr, kInternalPageSize,IBV_SEND_SIGNALED,1,Internal_and_Leaf);
    cached_root_page_mr.store(temp_mr);
    g_root_ptr.store(root_ptr);
    tree_height = ((InternalPage*) temp_mr->addr)->hdr.level;

    std::cout << "Get new root" << g_root_ptr <<std::endl;
    assert(g_root_ptr != GlobalAddress::Null());
//        root_hint = temp_mr;
}
void Btr::broadcast_new_root(GlobalAddress new_root_addr, int root_level) {
    RDMA_Request* send_pointer;
    ibv_mr send_mr = {};
//    ibv_mr receive_mr = {};
    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, Message);
    send_pointer = (RDMA_Request*)send_mr.addr;
    send_pointer->command = broadcast_root;
    send_pointer->content.root_broadcast.new_ptr = new_root_addr;
    send_pointer->content.root_broadcast.level = root_level;

//  if (root_level >= 5) {
//        enable_cache = true;
//  }
  //TODO: When we seperate the compute from the memory, how can we broad cast the new root
  // or can we wait until the compute node detect an inconsistent.

    rdma_mg->post_send<RDMA_Request>(&send_mr, 1, std::string("main"));
    ibv_wc wc[2] = {};
    //  while(wc.opcode != IBV_WC_RECV){
    //    poll_completion(&wc);
    //    if (wc.status != 0){
    //      fprintf(stderr, "Work completion status is %d \n", wc.status);
    //    }
    //
    //  }
    //  assert(wc.opcode == IBV_WC_RECV);
    if (rdma_mg->poll_completion(wc, 1, std::string("main"),
                        true, 1)){
//    assert(try_poll_completions(wc, 1, std::string("main"),true) == 0);
        fprintf(stderr, "failed to poll send for remote memory register\n");
    }
}

bool Btr::update_new_root(GlobalAddress left, const Key &k,
                           GlobalAddress right, int level,
                           GlobalAddress old_root, CoroContext *cxt,
                           int coro_id) {


    auto cas_buffer = rdma_mg->Get_local_CAS_mr();

    // TODO: recycle the olde registered memory, but we need to make sure that there
    // is no pending access over that old mr. (Temporarily not recyle it)
    ibv_mr* page_buffer = new ibv_mr{};

    // try to init tree and install root pointer
    rdma_mg->Allocate_Local_RDMA_Slot(*page_buffer, Internal_and_Leaf);// local allocate
    memset(page_buffer->addr,0,rdma_mg->name_to_chunksize.at(Internal_and_Leaf));
//  auto page_buffer = rdma_mg->Get_local_read_mr();

    assert(left != GlobalAddress::Null());
    assert(right != GlobalAddress::Null());
    assert(level < 100);
  auto new_root_addr = rdma_mg->Allocate_Remote_RDMA_Slot(Internal_and_Leaf, 2 * round_robin_cur + 1);
    if(++round_robin_cur == rdma_mg->memory_nodes.size()){
        round_robin_cur = 0;
    }
    auto new_root = new(page_buffer->addr) InternalPage(left, k, right, new_root_addr, level);

    // The code below is just for debugging
//    new_root_addr.mark = 3;
//    new_root->front_version++;
//    new_root->rear_version = new_root->front_version;
  // set local cache for root address
  g_root_ptr.store(new_root_addr,std::memory_order_seq_cst);
    cached_root_page_mr.store(page_buffer);
    tree_height = level;

  rdma_mg->RDMA_Write(new_root_addr, page_buffer, kInternalPageSize, IBV_SEND_SIGNALED, 1, Internal_and_Leaf);
  ibv_mr remote_mr = *rdma_mg->global_index_table;
    // find the table enty according to the id
    remote_mr.addr = (void*) ((char*)remote_mr.addr + 8*tree_id);
    //TODO: The new root seems not be updated by the CAS, the old root and new_root addr are the same
  if (!rdma_mg->RDMA_CAS(&remote_mr, cas_buffer, old_root, new_root_addr, IBV_SEND_SIGNALED, 1, 1)) {
      assert(*(uint64_t*)cas_buffer->addr == (uint64_t)old_root);
      printf("Update the root global buffer %p successfully", remote_mr.addr);
    broadcast_new_root(new_root_addr, level);
    std::cout << "new root level " << level << " " << new_root_addr
              << std::endl;

#ifndef NDEBUG
      usleep(10);
      ibv_wc wc[2];
      auto qp_type = std::string("default");
      assert(rdma_mg->try_poll_completions(wc, 1, qp_type, true, 1) == 0);
#endif
    return true;
  } else {
    std::cout << "cas root fail " << std::endl;
  }

  return false;
}

void Btr::print_and_check_tree(CoroContext *cxt, int coro_id) {
//  assert(rdma_mg->is_register());
    ibv_mr* page_hint;
  auto root = get_root_ptr(page_hint);
  // SearchResult result;

  GlobalAddress p = root;
  GlobalAddress levels[define::kMaxLevelOfTree];
  int level_cnt = 0;
  auto page_buffer = rdma_mg->Get_local_read_mr();
  GlobalAddress leaf_head;

next_level:

  rdma_mg->RDMA_Read(p, page_buffer, kInternalPageSize, IBV_SEND_SIGNALED, 1, Internal_and_Leaf);
  auto header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
  levels[level_cnt++] = p;
  if (header->level != 0) {
    p = header->leftmost_ptr;
    goto next_level;
  } else {
    leaf_head = p;
  }

next:
    rdma_mg->RDMA_Read(leaf_head, page_buffer, kLeafPageSize, IBV_SEND_SIGNALED, 1, Internal_and_Leaf);
//  rdma_mg->read_sync(page_buffer, , kLeafPageSize);
  auto page = (LeafPage *)page_buffer;
  for (int i = 0; i < kLeafCardinality; ++i) {
    if (page->records[i].value != kValueNull) {
    }
  }
  while (page->hdr.sibling_ptr != GlobalAddress::Null()) {
    leaf_head = page->hdr.sibling_ptr;
    goto next;
  }

  // for (int i = 0; i < level_cnt; ++i) {
  //   rdma_mg->read_sync(page_buffer, levels[i], kLeafPageSize);
  //   auto header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
  //   // std::cout << "addr: " << levels[i] << " ";
  //   // header->debug();
  //   // std::cout << " | ";
  //   while (header->sibling_ptr != GlobalAddress::Null()) {
  //     rdma_mg->read_sync(page_buffer, header->sibling_ptr, kLeafPageSize);
  //     header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
  //     // std::cout << "addr: " << header->sibling_ptr << " ";
  //     // header->debug();
  //     // std::cout << " | ";
  //   }
  //   // std::cout << "\n------------------------------------" << std::endl;
  //   // std::cout << "------------------------------------" << std::endl;
  // }
}

GlobalAddress Btr::query_cache(const Key &k) { return GlobalAddress::Null(); }

inline bool Btr::try_lock_addr(GlobalAddress lock_addr, uint64_t tag,
                               ibv_mr *buf, CoroContext *cxt, int coro_id) {
//  auto &pattern_cnt = pattern[rdma_mg->getMyThreadID()][lock_addr.nodeID];

  bool hand_over = acquire_local_lock(lock_addr, cxt, coro_id);
  if (hand_over) {
    return true;
  }

  {

    uint64_t retry_cnt = 0;
    uint64_t pre_tag = 0;
    uint64_t conflict_tag = 0;
  retry:
    retry_cnt++;
    if (retry_cnt > 3000) {
      std::cout << "Deadlock " << lock_addr << std::endl;

      std::cout << rdma_mg->GetMemoryNodeNum() << ", "
                << " locked by node  " << (conflict_tag) << std::endl;
      assert(false);
      exit(0);
    }
      *(uint64_t *)buf->addr = 0;
    rdma_mg->RDMA_CAS(lock_addr, buf, 0, tag, IBV_SEND_SIGNALED,1, LockTable);
    if ((*(uint64_t*) buf->addr) == 0){
        conflict_tag = *(uint64_t*)buf->addr;
        if (conflict_tag != pre_tag) {
            retry_cnt = 0;
            pre_tag = conflict_tag;
        }
//      lock_fail[rdma_mg->getMyThreadID()][0]++;
        goto retry;
    }
//    std::cout << "Successfully lock the " << lock_addr << std::endl;

  }

  return true;
}

inline void Btr::unlock_addr(GlobalAddress lock_addr, CoroContext *cxt, int coro_id, bool async) {

  bool hand_over_other = can_hand_over(lock_addr);
  if (hand_over_other) {
    releases_local_lock(lock_addr);
    return;
  }

  auto cas_buf = rdma_mg->Get_local_CAS_mr();
//    std::cout << "unlock " << lock_addr << std::endl;
  *(uint64_t*)cas_buf->addr = 0;
  if (async) {
      // send flag 0 means there is no flag
    rdma_mg->RDMA_Write(lock_addr, cas_buf,  sizeof(uint64_t), 0,0,LockTable);
  } else {
//      std::cout << "Unlock the remote lock" << lock_addr << std::endl;
      rdma_mg->RDMA_Write(lock_addr, cas_buf,  sizeof(uint64_t), IBV_SEND_SIGNALED,1,LockTable);
  }
//    printf( "release the remote lock at  %p\n", lock_addr);

  releases_local_lock(lock_addr);
}
    void Btr::lock_and_read_page(ibv_mr *page_buffer, GlobalAddress page_addr,
                                 int page_size, ibv_mr *cas_buffer,
                                 GlobalAddress lock_addr, uint64_t tag,
                                 CoroContext *cxt, int coro_id) {
        // Can put lock and page read in a door bell batch.
//    printf("lock %lu and read page offset %lu", lock_addr.offset, page_addr.offset);
        bool hand_over = acquire_local_lock(lock_addr, cxt, coro_id);
        if (hand_over) {
#ifdef RDMAPROCESSANALYSIS
            auto start = std::chrono::high_resolution_clock::now();
#endif
            rdma_mg->RDMA_Read(page_addr, page_buffer, page_size, IBV_SEND_SIGNALED, 1, Internal_and_Leaf);
#ifdef RDMAPROCESSANALYSIS
            if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
                auto stop = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
                printf("one RDMA READ round trip uses (%ld) ns\n", duration.count());
                TimePrintCounter[RDMA_Manager::thread_id] = 0;
            }else{
                TimePrintCounter[RDMA_Manager::thread_id]++;
            }
#endif
            return;
        }


        {
//        printf( "try to lock the %p\n", lock_addr);
            uint64_t retry_cnt = 0;
            uint64_t pre_tag = 0;
            uint64_t conflict_tag = 0;
            retry:
#ifdef RDMAPROCESSANALYSIS
            auto start = std::chrono::high_resolution_clock::now();
#endif
            retry_cnt++;
            if (retry_cnt > 10000000) {
                std::cout << "Deadlock " << lock_addr << std::endl;

                std::cout << rdma_mg->GetMemoryNodeNum() << ", "
                          << " locked by node  " << (conflict_tag) << std::endl;
                assert(false);
                exit(0);
            }
            struct ibv_send_wr sr[2];
            struct ibv_sge sge[2];

            rdma_mg->Prepare_WR_CAS(sr[0], sge[0], lock_addr, cas_buffer, 0, tag, IBV_SEND_SIGNALED, LockTable);
            rdma_mg->Prepare_WR_Read(sr[1], sge[1], page_addr, page_buffer, page_size, IBV_SEND_SIGNALED, Internal_and_Leaf);

//        rdma_mg->RDMA_CAS(lock_addr, cas_buffer, 0, tag, IBV_SEND_SIGNALED|IBV_SEND_FENCE,1, LockTable);
//        rdma_mg->RDMA_Read(page_addr, page_buffer, page_size, IBV_SEND_SIGNALED,1, Internal_and_Leaf);

//        rdma_mg->Prepare_WR_Write(sr[0], sge[0], page_addr, page_buffer, page_size, IBV_SEND_SIGNALED, Internal_and_Leaf);

            sr[0].next = &sr[1];
            *(uint64_t *)cas_buffer->addr = 0;
            assert(page_addr.nodeID == lock_addr.nodeID);
            rdma_mg->Batch_Submit_WRs(&sr[0], 2, page_addr.nodeID);
//        rdma_mg->Batch_Submit_WRs(sr, 1, page_addr.nodeID);
#ifdef RDMAPROCESSANALYSIS
            if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
                auto stop = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
                printf("RDMA write combined round trip uses (%ld) ns\n", duration.count());
                TimePrintCounter[RDMA_Manager::thread_id] = 0;
            }else{
                TimePrintCounter[RDMA_Manager::thread_id]++;
            }
#endif
            if ((*(uint64_t*) cas_buffer->addr) != 0){
                conflict_tag = *(uint64_t*)cas_buffer->addr;
                if (conflict_tag != pre_tag) {
                    retry_cnt = 0;
                    pre_tag = conflict_tag;
                }
//      lock_fail[rdma_mg->getMyThreadID()][0]++;
                goto retry;
            }
//        LeafPage* page = (LeafPage*) page_buffer->addr;
//        assert(page)

        }
//    printf( "successfully lock the %p\n", lock_addr);

//  rdma_mg->read_sync(page_buffer, page_addr, page_size, cxt);
//  pattern[rdma_mg->getMyThreadID()][page_addr.nodeID]++;
    }
// is it posisble that muliptle writer write to the same remote page at the same time (corner cases)?
    void Btr::write_page_and_unlock(ibv_mr *page_buffer, GlobalAddress page_addr, int page_size,
                                    GlobalAddress remote_lock_addr,
                                    CoroContext *cxt, int coro_id, bool async) {
//    printf("Release lock %lu and write page %lu", remote_lock_addr, page_addr);
  bool hand_over_other = can_hand_over(remote_lock_addr);
  if (hand_over_other) {

    rdma_mg->RDMA_Write(page_addr, page_buffer, page_size, IBV_SEND_SIGNALED, 1, Internal_and_Leaf);
    releases_local_lock(remote_lock_addr);
    return;
  }
    struct ibv_send_wr sr[2];
    struct ibv_sge sge[2];
    if (async){

        rdma_mg->Prepare_WR_Write(sr[0], sge[0], page_addr, page_buffer, page_size, 0, Internal_and_Leaf);
        ibv_mr* local_CAS_mr = rdma_mg->Get_local_CAS_mr();
//        *(uint64_t*) local_CAS_mr->addr = 0;
        rdma_mg->Prepare_WR_Write(sr[1], sge[1], remote_lock_addr, local_CAS_mr, sizeof(uint64_t), 0, LockTable);
        sr[0].next = &sr[1];


        *(uint64_t *)local_CAS_mr->addr = 0;
        assert(page_addr.nodeID == remote_lock_addr.nodeID);
        rdma_mg->Batch_Submit_WRs(sr, 0, page_addr.nodeID);
    }else{
#ifndef NDEBUG
        auto page = (InternalPage*) page_buffer->addr;
        if (page->hdr.level >0){
            assert(page->records[page->hdr.last_index ].ptr != GlobalAddress::Null());

        }
#endif

//        rdma_mg->RDMA_Write(page_addr, page_buffer, page_size, IBV_SEND_SIGNALED ,1, Internal_and_Leaf);

        rdma_mg->Prepare_WR_Write(sr[0], sge[0], page_addr, page_buffer, page_size, IBV_SEND_SIGNALED, Internal_and_Leaf);
        ibv_mr* local_CAS_mr = rdma_mg->Get_local_CAS_mr();
        *(uint64_t *)local_CAS_mr->addr = 0;
        //TODO: WHY the remote lock is not unlocked by this function?
//        rdma_mg->RDMA_CAS( remote_lock_addr, local_CAS_mr, 1,0, IBV_SEND_SIGNALED,1, LockTable);
//        assert(*(uint64_t *)local_CAS_mr->addr == 1);

        rdma_mg->Prepare_WR_Write(sr[1], sge[1], remote_lock_addr, local_CAS_mr, sizeof(uint64_t), IBV_SEND_SIGNALED, LockTable);
        sr[0].next = &sr[1];



        assert(page_addr.nodeID == remote_lock_addr.nodeID);
        rdma_mg->Batch_Submit_WRs(sr, 2, page_addr.nodeID);
    }
//        printf( "release the remote lock at  %p\n", remote_lock_addr);
//    std::cout << "" << remote_lock_addr << std::endl;
//  if (async) {
//    rdma_mg->write_batch(rs, 2, false);
//  } else {
//    rdma_mg->write_batch_sync(rs, 2, cxt);
//  }

  releases_local_lock(remote_lock_addr);
}
//    void Btr::global_write_page_and_unlock(ibv_mr *page_buffer, GlobalAddress page_addr, int page_size,
//                                           GlobalAddress remote_lock_addr, CoroContext *cxt, int coro_id, bool async) {
//
//        struct ibv_send_wr sr[2];
//        struct ibv_sge sge[2];
//
//        if (async){
//
//            rdma_mg->Prepare_WR_Write(sr[0], sge[0], page_addr, page_buffer, page_size, 0, Internal_and_Leaf);
//            ibv_mr* local_CAS_mr = rdma_mg->Get_local_CAS_mr();
////        *(uint64_t*) local_CAS_mr->addr = 0;
//            rdma_mg->Prepare_WR_Write(sr[1], sge[1], remote_lock_addr, local_CAS_mr, sizeof(uint64_t), IBV_SEND_FENCE, Internal_and_Leaf);
//            sr[0].next = &sr[1];
//
//
//            *(uint64_t *)local_CAS_mr->addr = 0;
//            assert(page_addr.nodeID == remote_lock_addr.nodeID);
//            rdma_mg->Batch_Submit_WRs(sr, 0, page_addr.nodeID);
//        }else{
//
//#ifndef NDEBUG
//            auto page = (InternalPage*) page_buffer->addr;
//            if (page->hdr.level >0){
//                assert(page->records[page->hdr.last_index ].ptr != GlobalAddress::Null());
//
//            }
//#endif
//
////        rdma_mg->RDMA_Write(page_addr, page_buffer, page_size, IBV_SEND_SIGNALED ,1, Internal_and_Leaf);
//
//            rdma_mg->Prepare_WR_Write(sr[0], sge[0], page_addr, page_buffer, page_size, 0, Internal_and_Leaf);
//            ibv_mr* local_CAS_mr = rdma_mg->Get_local_CAS_mr();
//            *(uint64_t *)local_CAS_mr->addr = 0;
//            //TODO: WHY the remote lock is not unlocked by this function?
////        rdma_mg->RDMA_CAS( remote_lock_addr, local_CAS_mr, 1,0, IBV_SEND_SIGNALED,1, Internal_and_Leaf);
////        assert(*(uint64_t *)local_CAS_mr->addr == 1);
//
//            rdma_mg->Prepare_WR_Write(sr[1], sge[1], remote_lock_addr, local_CAS_mr, sizeof(uint64_t), IBV_SEND_SIGNALED|IBV_SEND_FENCE, Internal_and_Leaf);
//            sr[0].next = &sr[1];
//
//
//
//            assert(page_addr.nodeID == remote_lock_addr.nodeID);
//            rdma_mg->Batch_Submit_WRs(sr, 1, page_addr.nodeID);
//        }
//#ifndef NDEBUG
////        printf("Reease global lock for %p\n", page_addr);
//
//#endif
////        releases_local_optimistic_lock(remote_lock_addr);
//    }

//    void Btr::global_unlock_addr(GlobalAddress remote_lock_add, CoroContext *cxt, int coro_id, bool async) {
//        auto cas_buf = rdma_mg->Get_local_CAS_mr();
////    std::cout << "unlock " << lock_addr << std::endl;
//        *(uint64_t*)cas_buf->addr = 0;
//        if (async) {
//            // send flag 0 means there is no flag
//            rdma_mg->RDMA_Write(remote_lock_add, cas_buf,  sizeof(uint64_t), 0,0,Internal_and_Leaf);
//        } else {
////      std::cout << "Unlock the remote lock" << lock_addr << std::endl;
//            rdma_mg->RDMA_Write(remote_lock_add, cas_buf,  sizeof(uint64_t), IBV_SEND_SIGNALED,1,Internal_and_Leaf);
////            assert(*(uint64_t*)cas_buf->addr == 1);
//        }
////        releases_local_optimistic_lock(lock_addr);
//    }
//    void Btr::global_lock_and_read_page(ibv_mr *page_buffer, GlobalAddress page_addr, int page_size, GlobalAddress lock_addr,
//                                   ibv_mr *cas_buffer, uint64_t tag, CoroContext *cxt, int coro_id) {
//
//
//            uint64_t retry_cnt = 0;
//            uint64_t pre_tag = 0;
//            uint64_t conflict_tag = 0;
//#ifndef NDEBUG
////        printf("Acquire global lock for %p\n", page_addr);
//#endif
//#ifndef NDEBUG
//        InternalPage* page = (InternalPage*)((char*)page_buffer->addr - RDMA_OFFSET);
//        assert(page->local_lock_meta.local_lock_byte == 1);
//#endif
//            retry:
//            retry_cnt++;
//            if (retry_cnt > 300000) {
//                std::cout << "Deadlock " << lock_addr << std::endl;
//
//                std::cout << rdma_mg->GetMemoryNodeNum() << ", "
//                          << " locked by node  " << (conflict_tag) << std::endl;
//                assert(false);
//                exit(0);
//            }
//            struct ibv_send_wr sr[2];
//            struct ibv_sge sge[2];
////        printf("Acquire global lock for %p\n", page_addr);
//        assert(page->local_lock_meta.local_lock_byte == 1);
//        //Only the second RDMA issue a completion
//            rdma_mg->Prepare_WR_CAS(sr[0], sge[0], lock_addr, cas_buffer, 0, tag, 0, Internal_and_Leaf);
//            rdma_mg->Prepare_WR_Read(sr[1], sge[1], page_addr, page_buffer, page_size, IBV_SEND_SIGNALED, Internal_and_Leaf);
////        rdma_mg->RDMA_CAS(lock_addr, cas_buffer, 0, tag, IBV_SEND_SIGNALED|IBV_SEND_FENCE,1, LockTable);
////        rdma_mg->RDMA_Read(page_addr, page_buffer, page_size, IBV_SEND_SIGNALED,1, Internal_and_Leaf);
//
////        rdma_mg->Prepare_WR_Write(sr[0], sge[0], page_addr, page_buffer, page_size, IBV_SEND_SIGNALED, Internal_and_Leaf);
//        assert(page->local_lock_meta.local_lock_byte == 1);
//            sr[0].next = &sr[1];
//            *(uint64_t *)cas_buffer->addr = 0;
//            assert(page_addr.nodeID == lock_addr.nodeID);
//            rdma_mg->Batch_Submit_WRs(sr, 1, page_addr.nodeID);
//
//            if ((*(uint64_t*) cas_buffer->addr) != 0){
//                conflict_tag = *(uint64_t*)cas_buffer->addr;
//                if (conflict_tag != pre_tag) {
//                    retry_cnt = 0;
//                    pre_tag = conflict_tag;
//                }
////      lock_fail[rdma_mg->getMyThreadID()][0]++;
//                goto retry;
//            }
//        assert(page->local_lock_meta.local_lock_byte == 1);
//
//    }
//
//    uint64_t Btr::renew_swap_by_received_state_readlock(uint64_t &received_state) {
//        uint64_t returned_state = 0;
//        if(received_state == 0){
//            // The first time try to lock or last time lock failed because of an unlock.
//            // read lock holder should equals 1, and fill the bitmap for this node id.
//            returned_state = 1ull << 48;
//            returned_state = returned_state | (1ull << RDMA_Manager::node_id/2);
//
//        }else if(received_state >= 1ull << 56){
//            // THere is a write lock on, we can only keep trying to read lock it.
//            returned_state = 1ull << 48;
//            returned_state = returned_state | (1ull << RDMA_Manager::node_id/2);
//        }else{
//            // There has already been a read lock holder.
//            uint64_t current_Rlock_holder_num = (received_state >> 48) % 256;
//            assert(current_Rlock_holder_num <= 255);
//            current_Rlock_holder_num++;
//            //
//            returned_state = returned_state | (current_Rlock_holder_num << 48);
//            returned_state = returned_state | (received_state << 16 >>16);
//            assert(received_state & (1ull << RDMA_Manager::node_id/2 == 0));
//
//            returned_state = returned_state | (1ull << RDMA_Manager::node_id/2);
//        }
//        return returned_state;
//    }
//    uint64_t Btr::renew_swap_by_received_state_readunlock(uint64_t &received_state) {
//        uint64_t returned_state = 0;
//        if(received_state == ((1ull << 48) |(1ull << RDMA_Manager::node_id/2))){
//            // The first time try to lock or last time lock failed because of an unlock.
//            // read lock holder should equals 1, and fill the bitmap for this node id.
//            returned_state = 1ull << 48;
//            returned_state = returned_state | (1ull << RDMA_Manager::node_id/2);
//
//        }else if(received_state >= 1ull << 56){
//            assert(false);
//            printf("read lock is on during the write lock");
//            exit(0);
//        }else{
//            // There has already been another read lock holder.
//            uint64_t current_Rlock_holder_num = (received_state >> 48) % 256;
//            assert(current_Rlock_holder_num > 0);
//            current_Rlock_holder_num--;
//            //decrease the number of lock holder
//            returned_state = returned_state | (current_Rlock_holder_num << 48);
//            returned_state = returned_state | (received_state << 16 >>16);
//            assert(received_state & (1ull << RDMA_Manager::node_id/2 == 1));
//            // clear the node id bit.
//            returned_state = returned_state & ~(1ull << RDMA_Manager::node_id/2);
//        }
//        return returned_state;
//    }
//    uint64_t Btr::renew_swap_by_received_state_readupgrade(uint64_t &received_state) {
//        return 0;
//    }
//
    void Btr::global_Rlock_and_read_page(ibv_mr *page_buffer, GlobalAddress page_addr, int page_size,
                                         GlobalAddress lock_addr,
                                         ibv_mr *cas_buffer, uint64_t tag, CoroContext *cxt, int coro_id,
                                         Cache::Handle *handle) {
        rdma_mg->global_Rlock_and_read_page(page_buffer, page_addr, page_size, lock_addr, cas_buffer,
                                            tag, cxt, coro_id);
        handle->remote_lock_status.store(1);

    }

    bool Btr::global_Rlock_update(GlobalAddress lock_addr, ibv_mr *cas_buffer, CoroContext *cxt, int coro_id,
                                  Cache::Handle *handle) {
        assert(handle->remote_lock_status.load() == 1);
        bool succfully_updated = rdma_mg->global_Rlock_update(lock_addr,cas_buffer, cxt, coro_id);
        if (succfully_updated){
            handle->remote_lock_status.store(2);
            assert(handle->gptr == (((LeafPage*)(((ibv_mr*)handle->value)->addr))->hdr.this_page_g_ptr);
            return true;
        }else{
            assert(handle->remote_lock_status.load() == 1);
            return false;

        }

    }
    void Btr::global_Wlock_and_read_page_with_INVALID(ibv_mr *page_buffer, GlobalAddress page_addr, int page_size,
                                                      GlobalAddress lock_addr, ibv_mr *cas_buffer, uint64_t tag,
                                                      CoroContext *cxt, int coro_id, Cache::Handle *handle) {
        rdma_mg->global_Wlock_and_read_page_with_INVALID(page_buffer, page_addr, page_size, lock_addr, cas_buffer,
                                                         tag, cxt, coro_id);
        assert(handle->gptr == (((LeafPage*)(((ibv_mr*)handle->value)->addr))->hdr.this_page_g_ptr);
        handle->remote_lock_status.store(2);
    }

    void Btr::global_RUnlock(GlobalAddress lock_addr, ibv_mr *cas_buffer, CoroContext *cxt, int coro_id,
                             Cache::Handle *handle) {
        rdma_mg->global_RUnlock(lock_addr, cas_buffer, cxt, coro_id);
        handle->remote_lock_status.store(0);
    }
    void Btr::global_write_page_and_Wunlock(ibv_mr *page_buffer, GlobalAddress page_addr, int size,
                                            GlobalAddress lock_addr,
                                            CoroContext *cxt, int coro_id, Cache::Handle *handle, bool async) {
        rdma_mg->global_write_page_and_Wunlock(page_buffer, page_addr, size, lock_addr, cxt,
                                               coro_id, async);
        handle->remote_lock_status.store(0);
    }
    void Btr::global_write_tuple_and_Wunlock(ibv_mr *page_buffer, GlobalAddress page_addr, int size,
                                            GlobalAddress lock_addr,
                                            CoroContext *cxt, int coro_id, Cache::Handle *handle, bool async) {
        rdma_mg->global_write_tuple_and_Wunlock(page_buffer, page_addr, size, lock_addr, cxt,
                                               coro_id, async);
        handle->remote_lock_status.store(0);
    }
    void Btr::global_unlock_addr(GlobalAddress remote_lock_add, Cache::Handle *handle, CoroContext *cxt, int coro_id,
                                 bool async) {
        rdma_mg->global_unlock_addr(remote_lock_add,cxt, coro_id, async);
        handle->remote_lock_status.store(0);
    }
    //void Btr::lock_bench(const Key &k, CoroContext *cxt, int coro_id) {
//  uint64_t lock_index = CityHash64((char *)&k, sizeof(k)) % define::kNumOfLock;
//
//  GlobalAddress lock_addr;
//  lock_addr.nodeID = 0;
//  lock_addr.offset = lock_index * sizeof(uint64_t);
//  auto cas_buffer = rdma_mg->get_rbuf(coro_id).get_cas_buffer();
//
//  // bool res = rdma_mg->cas_sync(lock_addr, 0, 1, cas_buffer, cxt);
//  try_lock_addr(lock_addr, 1, cas_buffer, cxt, coro_id);
//  unlock_addr(lock_addr, 1, cas_buffer, cxt, coro_id, true);
//}
// You need to make sure it is not the root level, this function will make sure the
// the insertion to this target level succcessful
// the target level can not be the root level.
//Note: this function will make sure the insert will definitely success. it willkeep retrying/
    bool Btr::insert_internal(Key &k, GlobalAddress &v, CoroContext *cxt,
                          int coro_id, int target_level) {

    //TODO: You need to acquire a lock when you write a page
        ibv_mr* page_hint = nullptr;
    auto root = get_root_ptr(page_hint);
    SearchResult result{0};

    GlobalAddress p = root;

    //TODO: ADD support for root invalidate and update.


    bool isroot = true;
    // this is root is to help the tree to refresh the root node because the
    // new root broadcast is not usable if physical disaggregated.
    int level = -1;
    //TODO: What if we ustilize the cache tree height for the root level?

next: // Internal_and_Leaf page search
    //TODO: What if the target_level is equal to the root level.
    if (!internal_page_search(p, k, result, level, isroot, page_hint, cxt, coro_id)) {
        if (isroot || path_stack[coro_id][result.level +1] == GlobalAddress::Null()){
            p = get_root_ptr(page_hint);
            level = -1;
        }else{
            // fall back to upper level
            assert(level == result.level|| level == -1);
            p = path_stack[coro_id][result.level +1];
            page_hint = nullptr;
            level = result.level +1;
        }
        goto next;
    }else{
        assert(level == result.level);
        isroot = false;
        page_hint = nullptr;
        // if the root and sibling are the same, it is also okay because the
        // p will not be changed

        if (level > target_level){
            if (result.slibing != GlobalAddress::Null()) { // turn right
                p = result.slibing;

            }else if (result.next_level != GlobalAddress::Null()){
                assert(result.next_level != GlobalAddress::Null());
                //Probelm here
                p = result.next_level;
                level = result.level - 1;
            }else{

            }
            if (level != target_level){
                goto next;
            }
        }else if(level < target_level){
            // Since return true will not invalidate the root node, here we manually invalidate it outside,
            // Otherwise, there will be a deadloop.
            g_root_ptr.store(GlobalAddress::Null());
            p = get_root_ptr(page_hint);
            level = -1;
            goto next;
        }else{
            //do nothing, the p and level is correct.
        }

    }
    assert(level = target_level);
    //Insert to target level
    Key split_key;
    GlobalAddress sibling_prt;
        assert(p != GlobalAddress::Null());
    bool store_success = internal_page_store(p, k, v, level, cxt, coro_id);
    if (!store_success){
        //TODO: need to understand why the result is always false.
        if (path_stack[coro_id][level + 1] != GlobalAddress::Null()){
            p = path_stack[coro_id][level + 1];
            level = level + 1;
        }
        else{
            // re-search the tree from the scratch. (only happen when root and leaf are the same.)
            p = get_root_ptr(page_hint);
            level = -1;
        }
        goto next;
    }
    return true;
//    internal_page_store(p, k, v, level, cxt, coro_id);
}

void Btr::insert(const Key &k, const Value &v, CoroContext *cxt, int coro_id) {
//  assert(rdma_mg->is_register());

  before_operation(cxt, coro_id);


    ibv_mr* page_hint = nullptr;
  auto root = get_root_ptr(page_hint);
    assert(root != GlobalAddress::Null());
//  std::cout << "The root now is " << root << std::endl;
  SearchResult result{};
  memset(&result, 0, sizeof(SearchResult));
  GlobalAddress p = root;
    bool isroot = true;
  // this is root is to help the tree to refresh the root node because the
  // new root broadcast is not usable if physical disaggregated.
    int level = -1;
    int fall_back_level = 0;
//TODO: What if we ustilize the cache tree height for the root level?

//    int target_level = 0;
#ifdef PROCESSANALYSIS
    auto start = std::chrono::high_resolution_clock::now();
#endif
//#ifndef NDEBUG
    int next_times = 0;
//#endif
next: // Internal_and_Leaf page search
//#ifndef NDEBUG
    if (next_times == 1000){
        assert(false);
    }

//#endif

    if (!internal_page_search(p, k, result, level, isroot, page_hint, cxt, coro_id)) {
        if (isroot || path_stack[coro_id][result.level +1] == GlobalAddress::Null()){
            p = get_root_ptr(page_hint);
            level = -1;
        }else{
            // fall back to upper level
            assert(level == result.level || level == -1);
            p = path_stack[coro_id][result.level +1];
            page_hint = nullptr;
            level = result.level +1;
        }

        goto next;
    }
    else{
        assert(level == result.level);
        isroot = false;
        page_hint = nullptr;
        // if the root and sibling are the same, it is also okay because the
        // p will not be changed
        if (result.slibing != GlobalAddress::Null()) { // turn right
            // this has been obsoleted, we nest the turn right page search inside the function.
            assert(false);
            p = result.slibing;

        }else if (result.next_level != GlobalAddress::Null()){
            assert(result.next_level != GlobalAddress::Null());
            p = result.next_level;
            level = result.level - 1;
        }else{
            assert(tree_height == 0);
            printf("happens when there is only one level\n");
        }

        if (level != 0){
            // level ==0 is corresponding to the corner case where the leaf node and root node are the same.
            assert(!result.is_leaf);
#ifndef NDEBUG
            next_times++;
#endif

            goto next;
        }

    }
    assert(level == 0);
    //Insert to leaf level
    Key split_key;
    GlobalAddress sibling_prt = GlobalAddress::Null();
//    if (target_level == 0){
//
//    }
#ifdef PROCESSANALYSIS
      if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
          auto stop = std::chrono::high_resolution_clock::now();
          auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//#ifndef NDEBUG
          printf("internal node tranverse uses (%ld) ns, next time is %d\n", duration.count(), next_times);
//          TimePrintCounter = 0;
      }
//#endif
#endif

//#ifdef PROCESSANALYSIS
//    start = std::chrono::high_resolution_clock::now();
//#endif
    if (!leaf_page_store(p, k, v, split_key, sibling_prt, root, 0, cxt, coro_id)){
        if (path_stack[coro_id][1] != GlobalAddress::Null()){
            p = path_stack[coro_id][1];
            level = 1;
        }
        else{
            // re-search the tree from the scratch. (only happen when root and leaf are the same.)
            p = get_root_ptr(page_hint);
            level = -1;
        }
#ifndef NDEBUG
        next_times++;
#endif
        if (next_times == 999){
            printf("break here\n");
        }
        goto next;
    }
//#ifdef PROCESSANALYSIS
//    if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
//        auto stop = std::chrono::high_resolution_clock::now();
//        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
////#ifndef NDEBUG
//        printf("leaf node store uses (%ld) ns\n", duration.count());
//        TimePrintCounter[RDMA_Manager::thread_id] = 0;
//    }else{
//        TimePrintCounter[RDMA_Manager::thread_id]++;
//    }
////#endif
//#endif
    //======================== below is about nested node split ============================//
    assert(level == 0);
    // this track means root is the leaves
//    assert(level == 0);

//    p = path_stack[coro_id][level+1];
//    //Double check the code below.
//    if (UNLIKELY(p == GlobalAddress::Null() && sibling_prt != GlobalAddress::Null())){
//        //TODO: we probably need a update root lock for the code below.
//        g_root_ptr = GlobalAddress::Null();
//        p = get_root_ptr();
//        //TODO: tHERE COULd be a bug in the get_root_ptr so that the CAS for update_new_root will be failed.
//
//        if (path_stack[coro_id][level] == p){
//            update_new_root(path_stack[coro_id][level],  split_key, sibling_prt,level+1,path_stack[coro_id][level], cxt, coro_id);
//            return;
//        }
//
//
//    }
//    level = level +1;
////internal_store:
//    // Insert to internal level if needed.
//    // the level here is equals to the current insertion level
//    while(sibling_prt != GlobalAddress::Null()){
//
//        // insert splitted key to upper level.
////        level = level + 1;
////        p = path_stack[coro_id][level];
//
//        if (!internal_page_store(p, split_key, sibling_prt, level, cxt, coro_id)){
//            // insertion failed, retry
//            if ( path_stack[coro_id][level + 1] == GlobalAddress::Null()){
////                assert(false);
//                // THis path should only happen at roof level happen.
//                // TODO: What if the node is not the root node.
//
//                // TODO: we need a root create lock to make sure there will be only on new root created. (Probably not needed)
//
//                //if we found it is on the top, Fetch the new root again to see whether there is a need for root update.
//                g_root_ptr = GlobalAddress::Null();
//                p = get_root_ptr();
//                if (p == path_stack[coro_id][level]){
//                    update_new_root(path_stack[coro_id][level-1],  split_key, sibling_prt,level,path_stack[coro_id][level-1], cxt, coro_id);
//                    break;
//                }
//                // TODO: we need to use insert internal. since the higher level is not cached.
//                level = -1;
//                insert_internal(split_key,sibling_prt, cxt, coro_id, level);
//            }else{
//                // fall back to upper level in the cache to search for the right node at this level
//                 fall_back_level = level + 1;
//
//                p = path_stack[coro_id][fall_back_level];
//                //TODO: Change it into while style.
//                // the main idea below is that when the cached internal page is stale, try to retrieve the page
//                // from remote memory from a higher level. if the higher level is still stale, then start from root.
//                // (top down)
//re_search:
//                if(!internal_page_search(p, k, result, fall_back_level, isroot, cxt, coro_id)){
//                    // if the upper level is still a stale node, just insert the node by top down method.
//                    insert_internal(split_key,sibling_prt, cxt, coro_id, level);
//                    level = level + 1; // move to upper level
//                    p = path_stack[coro_id][level];// move the pointer to upper level
//                }else{
//
//                    if (result.slibing != GlobalAddress::Null()) { // turn right for the correct node search
//                        // continue searching the sibling
//                        p = result.slibing;
//                        goto re_search;
//                    }else if (result.next_level != GlobalAddress::Null()){
//                        // the page was found successful by one step back, then we can set the p as new node.
//                        // do not need to chanve level.
//                        p = result.next_level;
////                    level = result.level - 1;
//                    }else{
//                        assert(false);
//                    }
//                }
//
//            }
//
//        }else{
//            if ( path_stack[coro_id][level + 1] == GlobalAddress::Null()){
//                //
//            }
//        }
        //check if insert success whether the split pop up to an none existing node, which means split has been propogated to the root.
//    }


//  if (res == HotResult::SUCC) {
//    hot_buf.clear(k);
//  }
}

bool Btr::search(const Key &k, Value &v, CoroContext *cxt, int coro_id) {
//  assert(rdma_mg->is_register());
    ibv_mr* page_hint = nullptr;
  auto root = get_root_ptr(page_hint);
  SearchResult result;

  GlobalAddress p = root;
    bool isroot = true;
  bool from_cache = false;
//  const CacheEntry *entry = nullptr;
//  if (enable_cache) {
//    GlobalAddress cache_addr;
//    entry = page_cache->search_from_cache(k, &cache_addr);
//    if (entry) { // cache hit
////      cache_hit[rdma_mg->getMyThreadID()][0]++;
//      from_cache = true;
//      p = cache_addr;
//      isroot = false;
//    } else {
////      cache_miss[rdma_mg->getMyThreadID()][0]++;
//    }
//  }
int level = -1;
//TODO: What if we ustilize the cache tree height for the root level?
//TODO: Change it into while style code.
#ifdef PROCESSANALYSIS
    auto start = std::chrono::high_resolution_clock::now();
#endif
//#ifndef NDEBUG
    int next_times = 0;
//#endif
    next: // Internal_and_Leaf page search
//#ifndef NDEBUG
    if (next_times++ == 1000){
        assert(false);
    }
//#endif

    if (!internal_page_search(p, k, result, level, isroot, page_hint, cxt, coro_id)) {
        //The traverser failed to move to the next level
        if (isroot || path_stack[coro_id][result.level +1] == GlobalAddress::Null()){
            p = get_root_ptr(page_hint);
            level = -1;
        }else{
            // fall back to upper level
            assert(level == result.level|| level == -1);
            p = path_stack[coro_id][result.level +1];
            page_hint = nullptr;
            level = result.level + 1;
        }
        goto next;
    }
    else{
        // The traversing moving the the next level correctly
        assert(level == result.level|| level == -1);
        isroot = false;
        page_hint = nullptr;
        // Do not need to
        if (result.slibing != GlobalAddress::Null()) { // turn right
            p = result.slibing;

        }else if (result.next_level != GlobalAddress::Null()){
            assert(result.next_level != GlobalAddress::Null());
            p = result.next_level;
            level = result.level - 1;
        }else{}

        if (level != 0 && level != -1){
            // If Level is 1 then the leaf node and root node are the same.
            assert(!result.is_leaf);

            goto next;
        }

    }
#ifdef PROCESSANALYSIS
    if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//#ifndef NDEBUG
        printf("internal node tranverse uses (%ld) ns, next time is %d\n", duration.count(), next_times);
//          TimePrintCounter = 0;
    }
//#endif
#endif
#ifdef PROCESSANALYSIS
    start = std::chrono::high_resolution_clock::now();
#endif
leaf_next:// Leaf page search

    if (!leaf_page_search(p, k, result, level, cxt, coro_id)){
        if (path_stack[coro_id][1] != GlobalAddress::Null()){
            p = path_stack[coro_id][1];
            level = 1;

        }
        else{
            p = get_root_ptr(page_hint);
            level = -1;
        }
#ifndef NDEBUG
        next_times++;
#endif
        DEBUG_PRINT_CONDITION("back off for search\n");
        goto next;
    }else{
        if (result.val != kValueNull) { // find
            v = result.val;
#ifdef PROCESSANALYSIS
            if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
                auto stop = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
                printf("leaf page fetch and search the page uses (%ld) ns\n", duration.count());
                TimePrintCounter[RDMA_Manager::thread_id] = 0;
            }else{
                TimePrintCounter[RDMA_Manager::thread_id]++;
            }
#endif

            return true;
        }
        if (result.slibing != GlobalAddress::Null()) { // turn right
            p = result.slibing;
            goto leaf_next;
        }
#ifdef PROCESSANALYSIS
        if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
            auto stop = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
            printf("leaf page fetch and search the page uses (%ld) ns\n", duration.count());
            TimePrintCounter[RDMA_Manager::thread_id] = 0;
        }else{
            TimePrintCounter[RDMA_Manager::thread_id]++;
        }
#endif

        return false; // not found
    }


//    if (result.is_leaf) {
//        if (result.val != kValueNull) { // find
//            v = result.val;
//
//            return true;
//        }
//        if (result.slibing != GlobalAddress::Null()) { // turn right
//            p = result.slibing;
//            goto next;
//        }
//        return false; // not found
//    } else {        // internal
//        p = result.slibing != GlobalAddress::Null() ? result.slibing
//                                                    : result.level;
//        goto next;
//    }
}

// TODO: Need Fix range query
//uint64_t Btr::range_query(const Key &from, const Key &to, Value *value_buffer,
//                           CoroContext *cxt, int coro_id) {
//
//  const int kParaFetch = 32;
//  thread_local std::vector<InternalPage *> result;
//  thread_local std::vector<GlobalAddress> leaves;
//
//  result.clear();
//  leaves.clear();
//  page_cache->search_range_from_cache(from, to, result);
//
//  if (result.empty()) {
//    return 0;
//  }
//
//  uint64_t counter = 0;
//  for (auto page : result) {
//    auto cnt = page->hdr.last_index + 1;
//    auto addr = page->hdr.leftmost_ptr;
//
//    // [from, to]
//    // [lowest, page->records[0].key);
//    bool no_fetch = from > page->records[0].key || to < page->hdr.lowest;
//    if (!no_fetch) {
//      leaves.push_back(addr);
//    }
//    for (int i = 1; i < cnt; ++i) {
//      no_fetch = from > page->records[i].key || to < page->records[i - 1].key;
//      if (!no_fetch) {
//        leaves.push_back(page->records[i - 1].ptr);
//      }
//    }
//
//    no_fetch = from > page->hdr.highest || to < page->records[cnt - 1].key;
//    if (!no_fetch) {
//      leaves.push_back(page->records[cnt - 1].ptr);
//    }
//  }
//
//  // printf("---- %d ----\n", leaves.size());
//  // sleep(1);
//
//  int cq_cnt = 0;
//  char *range_buffer = (rdma_mg->get_rbuf(coro_id)).get_range_buffer();
//  for (size_t i = 0; i < leaves.size(); ++i) {
//    if (i > 0 && i % kParaFetch == 0) {
//      rdma_mg->poll_rdma_cq(kParaFetch);
//      cq_cnt -= kParaFetch;
//      for (int k = 0; k < kParaFetch; ++k) {
//        auto page = (LeafPage *)(range_buffer + k * kLeafPageSize);
//        for (int i = 0; i < kLeafCardinality; ++i) {
//          auto &r = page->records[i];
//          if (r.value != kValueNull && r.f_version == r.r_version) {
//            if (r.key >= from && r.key <= to) {
//              value_buffer[counter++] = r.value;
//            }
//          }
//        }
//      }
//    }
//    rdma_mg->read(range_buffer + kLeafPageSize * (i % kParaFetch), leaves[i],
//                  kLeafPageSize, true);
//    cq_cnt++;
//  }
//
//  if (cq_cnt != 0) {
//    rdma_mg->poll_rdma_cq(cq_cnt);
//    for (int k = 0; k < cq_cnt; ++k) {
//      auto page = (LeafPage *)(range_buffer + k * kLeafPageSize);
//      for (int i = 0; i < kLeafCardinality; ++i) {
//        auto &r = page->records[i];
//        if (r.value != kValueNull && r.f_version == r.r_version) {
//          if (r.key >= from && r.key <= to) {
//            value_buffer[counter++] = r.value;
//          }
//        }
//      }
//    }
//  }
//
//  return counter;
//}

// Del needs to be rewritten
void Btr::del(const Key &k, CoroContext *cxt, int coro_id) {
//  assert(rdma_mg->is_register());
    before_operation(cxt, coro_id);


        ibv_mr* page_hint = nullptr;

    auto root = get_root_ptr(page_hint);
//  std::cout << "The root now is " << root << std::endl;
    SearchResult result;
    GlobalAddress p = root;
    bool isroot = true;
    // this is root is to help the tree to refresh the root node because the
    // new root broadcast is not usable if physical disaggregated.
    int level = -1;
//TODO: What if we ustilize the cache tree height for the root level?
    int target_level = 0;
    next: // Internal_and_Leaf page search
    if (!internal_page_search(p, k, result, level, isroot, nullptr, cxt, coro_id)) {
        if (isroot || path_stack[coro_id][result.level +1] == GlobalAddress::Null()){
            p = get_root_ptr(page_hint);
            level = -1;
        }else{
            // fall back to upper level
            assert(level == result.level|| level == -1);
            p = path_stack[coro_id][result.level +1];
            page_hint = nullptr;
            level = result.level +1;
        }
        goto next;
    }
    else{
        assert(level == result.level|| level == -1);
        isroot = false;
        page_hint = nullptr;
        // if the root and sibling are the same, it is also okay because the
        // p will not be changed
        if (result.slibing != GlobalAddress::Null()) { // turn right
            p = result.slibing;

        }else if (result.next_level != GlobalAddress::Null()){
            assert(result.next_level != GlobalAddress::Null());
            p = result.next_level;
            level = result.level - 1;
        }else{}

        if (level != target_level && level != -1){
            assert(!result.is_leaf);
//#ifndef NDEBUG
//            next_times++;
//#endif
            goto next;
        }
    }
    //Insert to leaf level
    Key split_key;
    GlobalAddress sibling_prt;
//    if (target_level == 0){
//
//    }
    //The node merge may triggered by background thread on the memory node only.
    if (!leaf_page_del(p, k,   0, cxt, coro_id)){
        if (path_stack[coro_id][1] != GlobalAddress::Null()){
            p = path_stack[coro_id][1];
            level = 1;
        }
        else{
            // re-search the tree from the scratch. (only happen when root and leaf are the same.)
            p = get_root_ptr(page_hint);
            level = -1;
        }
        goto next;
    }

  leaf_page_del(p, k, 0, cxt, coro_id);
}

/**
 * Node ID in GLobalAddress for a tree pointer should be the id in the Memory pool
 THis funciton will get the page by the page addr and search the pointer for the
 next level if it is not leaf page. If it is a leaf page, just put the value in the
 result. If this function return false then the result return nothing and we need to
 start from upper level again without cache.
 * @param page_addr
 * @param k
 * @param result
 * @param cxt
 * @param coro_id
 * @param isroot
 * @return
 */
    bool Btr::internal_page_search(GlobalAddress page_addr, const Key &k, SearchResult &result, int &level, bool isroot,
                                   ibv_mr *page_hint, CoroContext *cxt, int coro_id) {

// tothink: How could I know whether this level before I actually access this page.

        assert( page_addr.offset % kInternalPageSize == 0 );
//  auto &pattern_cnt = pattern[rdma_mg->getMyThreadID()][page_addr.nodeID];

    int counter = 0;

//    if (++counter > 100) {
//    printf("re read too many times\n");
//    sleep(1);
//    }
    // Quetion: We need to implement the lock coupling. how to avoid unnecessary RDMA for lock coupling?
    // Answer: No, see next question.
    Slice page_id((char*)&page_addr, sizeof(GlobalAddress));
    Cache::Handle* handle = nullptr;
    void* page_buffer;
    Header * header;
    InternalPage* page;
    ibv_mr* mr;
#ifdef PROCESSANALYSIS
        auto start = std::chrono::high_resolution_clock::now();
#endif
        bool skip_cache = false;
        //TODO: For the pointer swizzling, we need to clear the hdr.this_page_g_ptr when we deallocate
        // the page. Also we need a mechanism to avoid the page being deallocate during the access. if a page
        // is pointer swizzled, we need to make sure it will not be evict from the cache.
    if (page_hint != nullptr) {
        //update shoul also relect on the page_hint
        mr = page_hint;
        page_buffer = mr->addr;
        header = (Header *) ((char *) page_buffer + (STRUCT_OFFSET(InternalPage, hdr)));
        // if is root, then we should always bypass the cache.
        if (header->this_page_g_ptr == page_addr) {
            // if this page mr is in-use and is the local cache for page_addr
            skip_cache = true;
            page = (InternalPage *)page_buffer;
            memset(&result, 0, sizeof(result));
            result.is_leaf = header->leftmost_ptr == GlobalAddress::Null();
            result.level = header->level;
#ifndef NDEBUG
            if (level != -1){
                assert(level ==result.level );
            }
#endif
            level = result.level;
            assert(result.is_leaf == (level == 0));
            path_stack[coro_id][result.level] = page_addr;
            //If this is the leaf node, directly return let leaf page search to handle it.
            if (result.level == 0){
                // if the root node is the leaf node this path will happen.
                printf("root and leaf are the same 1\n");
                // assert the page is a valid page.
                assert(page->check_whether_globallock_is_unlocked());
                if (k >= page->hdr.highest){
                    std::unique_lock<std::mutex> l(root_mtx);
                    if (page_addr == g_root_ptr.load()){
                        g_root_ptr.store(GlobalAddress::Null());
                    }
                    return false;
                }
                return true;
            }
            assert(page->hdr.level < 100);
            page->check_invalidation_and_refetch_outside_lock(page_addr, rdma_mg, mr);
        }else if(isroot){
            return false;
        }

    }

    if(!skip_cache){
        assert(!isroot);
        handle = page_cache->Lookup(page_id);
#ifdef PROCESSANALYSIS
        if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
            auto stop = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//#ifndef NDEBUG
            printf("cache look up for level %d is (%ld) ns, \n", level, duration.count());
//          TimePrintCounter = 0;
        }
//#endif
#endif
        // TODO: use real pointer to bypass the cache hash table. We need overwrittened function for internal page search,
        //  given an local address (now the global address is given). Or we make it the same function with both global ptr and local ptr,
        //  and let the function to figure out the situation.



#ifndef NDEBUG
        int rdma_refetch_times = 0;
#endif
        //
        //Question: Shall we implement a shared-exclusive lock here for local contention. or we still
        // follow the optimistic latch free design?
        // Answer: Still optimistic latch free, the local read of internal node do not need local lock,
        // but the local write will write through the memory node and acquire the global lock.
        if (handle != nullptr){
            cache_hit[RDMA_Manager::thread_id][0]++;
            mr = (ibv_mr*)page_cache->Value(handle);
            page_buffer = mr->addr;
            header = (Header *)((char*)page_buffer + (STRUCT_OFFSET(InternalPage, hdr)));
#ifndef NDEBUG
            uint64_t local_meta_old = __atomic_load_n((uint64_t*)page_buffer, (int)std::memory_order_seq_cst);
//        assert(((Local_Meta*) &local_meta_new)->local_lock_byte == 0 && ((Local_Meta*) &local_meta_new)->current_ticket == ((Local_Meta*) &local_meta_new)->issued_ticket || ((Local_Meta*) &local_meta_new)->local_lock_byte == 1 && ((Local_Meta*) &local_meta_new)->current_ticket != ((Local_Meta*) &local_meta_new)->issued_ticket );
//        if (((Local_Meta*) &local_meta_new)->local_lock_byte !=0 || ((Local_Meta*) &local_meta_new)->current_ticket != current_ticket){
//            goto local_reread;
//        }
#endif
            page = (InternalPage *)page_buffer;
#ifndef NDEBUG
            uint64_t local_meta_new = __atomic_load_n((uint64_t*)&page->local_lock_meta, (int)std::memory_order_seq_cst);
//        assert(((Local_Meta*) &local_meta_new)->local_lock_byte == 0 && ((Local_Meta*) &local_meta_new)->current_ticket == ((Local_Meta*) &local_meta_new)->issued_ticket || ((Local_Meta*) &local_meta_new)->local_lock_byte == 1 && ((Local_Meta*) &local_meta_new)->current_ticket != ((Local_Meta*) &local_meta_new)->issued_ticket );
//        if (((Local_Meta*) &local_meta_new)->local_lock_byte !=0 || ((Local_Meta*) &local_meta_new)->current_ticket != current_ticket){
//            goto local_reread;
//        }
#endif
            memset(&result, 0, sizeof(result));
            result.is_leaf = header->leftmost_ptr == GlobalAddress::Null();
            result.level = header->level;
#ifndef NDEBUG
            if (level != -1){
                assert(level ==result.level );
            }

#endif
            level = result.level;
//  if(!result.is_leaf)
//      assert(result.level !=0);
            assert(result.is_leaf == (level == 0));
            path_stack[coro_id][result.level] = page_addr;
            if (result.level == 0){
                // if the root node is the leaf node this path will happen.
                printf("root and leaf are the same 2\n");
                //Why this is impossible?
                // Ans: here the level is 0 and we are internal search the page, then the index only have one leaf node
                // which is also the root node. In this case, the root page has to have a page_hint,
                // it is impossible to search a root leaf node without a page hint.
                assert(false);
                assert(page->check_whether_globallock_is_unlocked());
                if (k >= page->hdr.highest){
                    std::unique_lock<std::mutex> l(root_mtx);
                    if (page_addr == g_root_ptr.load()){
                        g_root_ptr.store(GlobalAddress::Null());
                    }
                    return false;
                }
                return true;
            }
//        printf("From cache, Page offest %lu last index is %d, page pointer is %p\n", page_addr.offset, page->hdr.last_index, page);
//        assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());

            assert(page->hdr.level < 100);

//        assert((page->local_lock_meta.current_ticket == page->local_lock_meta.issued_ticket && page->local_lock_meta.local_lock_byte == 0) || (page->local_lock_meta.current_ticket != page->local_lock_meta.issued_ticket && page->local_lock_meta.local_lock_byte == 1));
            // Note: we can not make the local lock outside the page, because in that case, the local lock
            // are aggregated but the global lock is per page, we can not do the lock handover.
            // CHANGE IT BACK
            page->check_invalidation_and_refetch_outside_lock(page_addr, rdma_mg, mr);
        }else {
            cache_miss[RDMA_Manager::thread_id][0]++;
            // TODO (potential optimization) we can use a lock when pushing the read page to cache
            // so that we can avoid install the page to cache mulitple times. But currently it is okay.
            //  pattern_cnt++;
            mr = new ibv_mr{};
            rdma_mg->Allocate_Local_RDMA_Slot(*mr, Internal_and_Leaf);

//        printf("Allocate slot for page 1, the page global pointer is %p , local pointer is  %p, hash value is %lu level is %d\n",
//               page_addr, mr->addr, HashSlice(page_id), level);

            page_buffer = mr->addr;
            header = (Header *) ((char*)page_buffer + (STRUCT_OFFSET(InternalPage, hdr)));
            page = (InternalPage *)page_buffer;
            page->global_lock = 1;

//        page->front_version = 0;
//        page->rear_version = 0;
#ifndef NDEBUG
            rdma_refetch_times = 1;
#endif
            rdma_refetch:
#ifndef NDEBUG
            if (rdma_refetch_times >= 10000){
                assert(false);
            }
#endif
            //TODO: Why the internal page read some times read an empty page?
            // THe bug could be resulted from the concurrent access by multiple threads.
            // why the last_index is always greater than the records number?
//        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
//        uint64_t lock_index =
//                CityHash64((char *)&page_addr, sizeof(page_addr)) % define::kNumOfLock;
//        GlobalAddress lock_addr;
//        lock_addr.nodeID = page_addr.nodeID;
//        lock_addr.offset = lock_index * sizeof(uint64_t);
//        lock_and_read_page(new_mr, page_addr, kInternalPageSize, cas_mr,
//                           lock_addr, 1, cxt, coro_id);
            // it can read the full page, because it has not inserted to the cache.
            rdma_mg->RDMA_Read(page_addr, mr, kInternalPageSize, IBV_SEND_SIGNALED, 1, Internal_and_Leaf);
//        DEBUG_arg("cache miss and RDMA read %p", page_addr);
            //
            assert(page->hdr.this_page_g_ptr = page_addr);
            memset(&result, 0, sizeof(result));
            result.is_leaf = header->leftmost_ptr == GlobalAddress::Null();
            result.level = header->level;
            level = result.level;
            path_stack[coro_id][result.level] = page_addr;
//        printf("From remote memory, Page offest %lu last index is %d, page pointer is %p\n", page_addr.offset, page->hdr.last_index, page);
            //check level first because the rearversion's position depends on the leaf node or internal node
            if (result.level == 0){
                // if the root node is the leaf node this path will happen.
                printf("root and leaf are the same 3\n");
                assert(false);
                assert(page->check_whether_globallock_is_unlocked());
                if (k >= page->hdr.highest){
                    std::unique_lock<std::mutex> l(root_mtx);
                    if (page_addr == g_root_ptr.load()){
                        g_root_ptr.store(GlobalAddress::Null());
                    }
                    rdma_mg->Deallocate_Local_RDMA_Slot(page_buffer, Internal_and_Leaf);

                    return false;
                }else{
                    // No need for reread.
                    rdma_mg->Deallocate_Local_RDMA_Slot(page_buffer, Internal_and_Leaf);
                    // return true and let the outside code figure out that the leaf node is the root node
//            this->unlock_addr(lock_addr, cxt, coro_id, false);
                    return true;
                }

            }
            // This consistent check should be in the path of RDMA read only.
            //TODO (OPTIMIZATION): Think about Why the last index check is necessary? can we remove it
            // If  there is no such check, the page may stay in some invalid state, where the last key-value noted by "last_index"
            // is empty. I spent 3 days to debug, but unfortunatly I did not figure it out.
            // THe weird thing is there will only be one empty in the last index, all the others are valid all the times.

//        _mm_clflush(&page->front_version);
//        _mm_clflush(&page->rear_version);
//        || page->records[page->hdr.last_index ].ptr == GlobalAddress::Null() 0x16623140000001 becomes 0x40000001
            if (!page->check_whether_globallock_is_unlocked() ) {
                //TODO: What is the other thread is modifying this page but you overwrite the buffer by a reread.
                // How to tell whether the inconsistent content is from local read-write conflict or remote
                // RDMA read and write conflict
                // TODO: What if the records are not consistent but the page version is consistent.
                //If this page is fetch from the remote memory, discard the page before insert to the cache,
                // then refetch the page by RDMA.
                //If this page read from the

#ifndef NDEBUG
                rdma_refetch_times++;
#endif
//            this->unlock_addr(lock_addr, cxt, coro_id, false);
                goto rdma_refetch;
            }
            // Initialize the local lock informations.
//        page->local_metadata_init();
            /**
             * On the responder side, contents of the RDMA write buffer are guaranteed to be fully received only if one of the following events takes place:
             * *Completion of the RDMA Write with immediate data
             * *Arrival and completion of the subsequent Send message
             * *Update of a memory element by subsequent RDMA Atomic operation
             * On the requester side, contents of the RDMA read buffer are guaranteed to be fully received only if one of the following events takes place:
             * *Completion of the RDMA Read Work Request (if completion is requested)
             * *Completion of the subsequent Work Request
             */
            assert(page->records[page->hdr.last_index ].ptr != GlobalAddress::Null());
            page->local_metadata_init();

            // if there has already been a cache entry with the same key, the old one will be
            // removed from the cache, but it may not be garbage collected right away
            //TODO(): you need to make sure this insert will not push out any cache entry,
            // because the page could hold a lock on that page and this page may miss the update.
            // However, this page's read is still a valid read and the other update may not required to
            // be seen for multiversion Transaction concurrency control, but this may critical for the 2PL algorithms.
            assert(mr != nullptr);
            handle = page_cache->Insert(page_id, mr, kInternalPageSize, Deallocate_MR);
//        assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());
//        this->unlock_addr(lock_addr, cxt, coro_id, false);
//#ifndef NDEBUG
//        usleep(10);
//        ibv_wc wc[2];
//        auto qp_type = std::string("default");
//        assert(rdma_mg->try_poll_completions(wc, 1, qp_type, true, page_addr.nodeID) == 0);
//#endif
        }
    }
        assert(mr!= nullptr);

    assert(page->hdr.level < 100);
    assert(result.level != 0);
    //          assert(!from_cache);

    //      assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());

// IN case that the local read have a conflict with the concurrent write.

local_reread:
#ifndef NDEBUG
        Key highest;
        size_t local_reread_retry = 0;
#endif
        assert(((uint64_t)&page->local_lock_meta) % 8 == 0);
        uint64_t local_meta = __atomic_load_n((uint64_t*)&page->local_lock_meta, (int)std::memory_order_seq_cst);
        while( ((Local_Meta*) &local_meta)->local_lock_byte > 0){
            local_meta = __atomic_load_n((uint64_t*)&page->local_lock_meta, (int)std::memory_order_seq_cst);
        };
        uint16_t issued_ticket = ((Local_Meta*) &local_meta)->issued_ticket;
        uint16_t current_ticket = ((Local_Meta*) &local_meta)->current_ticket;
//        if (issued_ticket != current_ticket){
//            goto local_reread;
//        }
        //        uint8_t front_v = page->front_version;
//        uint8_t rear_v = page->rear_version;
//        if(front_v != rear_v){
//                goto local_reread;
//
//        }
#ifndef NDEBUG
        highest = page->hdr.highest;
#endif
//        assert(page->records[page->hdr.last_index ].ptr != GlobalAddress::Null());

    if (k >= page->hdr.highest) { // should turn right
//            printf("should turn right ");
      // TODO: if this is the root node then we need to refresh the new root.
      if (isroot){
          // invalidate the root. Maybe we can omit the mtx here?
          std::unique_lock<std::mutex> l(root_mtx);
          if (page_addr == g_root_ptr.load()){
              g_root_ptr.store(GlobalAddress::Null());
          }

      }else if(path_stack[coro_id][result.level+1] != GlobalAddress::Null()){
          // It is possible that a staled root will result in a reread at the same level and then the upper level is null
          // Question: why none root tranverser will comes to here? If a stale root initial a sibling page read, then the k should
          // not larger than the highest this time.
          //TODO(potential bug): we need to avoid this erase because it is expensive, we can barely
          // mark the page invalidated within the page and then let the next thread access this page to refresh this page.
//          DEBUG_arg("Erase the page 1 %p\n", path_stack[coro_id][result.level+1]);
          Slice upper_node_page_id((char*)&path_stack[coro_id][result.level+1], sizeof(GlobalAddress));
          // TODO: By passing the cache access to same the cost for page invalidation, use the handle within
          // the page to check whether the page has been evicted.
          Cache::Handle* upper_layer_handle = page_cache->Lookup(upper_node_page_id);
          if(upper_layer_handle){
              InternalPage* upper_page = (InternalPage*)((ibv_mr*)upper_layer_handle->value)->addr;
              make_page_invalidated(upper_page);

              page_cache->Release(upper_layer_handle);


          }

//            page_cache->Erase(Slice((char*)&path_stack[coro_id][result.level+1], sizeof(GlobalAddress)));
      }
      //TODO: What if the Erased key is still in use by other threads? THis is very likely
      // for the upper level nodes.
    //          if (path_stack[coro_id][result.level+1] != GlobalAddress::Null()){
    //              page_cache->Erase(Slice((char*)&path_stack[coro_id][result.level+1], sizeof(GlobalAddress)))
    //          }
        if (nested_retry_counter <= 2){
//            printf("arrive here\n");
            nested_retry_counter++;
            result.slibing = page->hdr.sibling_ptr;
            assert(page->hdr.sibling_ptr != GlobalAddress::Null());
            GlobalAddress sib_ptr = page->hdr.sibling_ptr;
            // In case that the sibling pointer is invalidated
            uint64_t local_meta_new = __atomic_load_n((uint64_t*)&page->local_lock_meta, (int)std::memory_order_seq_cst);
            if (((Local_Meta*) &local_meta_new)->local_lock_byte !=0 || ((Local_Meta*) &local_meta_new)->current_ticket != current_ticket){
#ifndef NDEBUG
                if(local_reread_retry++ > 500){
                    assert(false);
                }
#endif
                goto local_reread;
            }
//            if(front_v != rear_v){
//                goto local_reread;
//            }
            // The release should always happen in the end of the function, other wise the
            // page will be overwrittened. When you run release, this means the page buffer will
            // sooner be overwritten.
            if(!skip_cache){
                page_cache->Release(handle);
            }

            isroot = false;
            page_hint = nullptr;
            return internal_page_search(sib_ptr, k, result, level, isroot, page_hint, cxt, coro_id);
        }else{
            nested_retry_counter = 0;
            if(!skip_cache){
                page_cache->Release(handle);
            }
//            DEBUG_PRINT("retry over two times place 1\n");
            return false;
        }

    }

    if (k < page->hdr.lowest) {
      if (isroot){
          // invalidate the root.
          g_root_ptr = GlobalAddress::Null();
      }else{
//          DEBUG_arg("Erase the page 2 %p\n", path_stack[coro_id][result.level+1]);
          assert(path_stack[coro_id][result.level+1] != GlobalAddress::Null());
          Slice upper_node_page_id((char*)&path_stack[coro_id][result.level+1], sizeof(GlobalAddress));
          // TODO: By passing the cache access to same the cost for page invalidation, use the handle within
          // the page to check whether the page has been evicted.
          Cache::Handle* upper_layer_handle = page_cache->Lookup(upper_node_page_id);
          if(upper_layer_handle){
              InternalPage* upper_page = (InternalPage*)((ibv_mr*)upper_layer_handle->value)->addr;

              make_page_invalidated(upper_page);
              page_cache->Release(upper_layer_handle);
          }
//          page_cache->Erase(Slice((char*)&path_stack[coro_id][result.level+1], sizeof(GlobalAddress)));
      }
    //              printf("key %ld error in level %d\n", k, page->hdr.level);
    //              sleep(10);
    //              print_and_check_tree();
    //              assert(false);
    //TODO: Maybe we can implement a invalidation instead of the erase. which will not
    // deallocate the memolry region of this cache entry.

    //          if (path_stack[coro_id][result.level+1] != GlobalAddress::Null()){
    //              page_cache->Erase(Slice((char*)&path_stack[coro_id][result.level+1], sizeof(GlobalAddress)));
    //
    //          }
        nested_retry_counter = 0;
        if(!skip_cache){
            page_cache->Release(handle);
        }
        DEBUG_PRINT_CONDITION("retry place 2\n");
      return false;
    }
    // this function will add the children pointer to the result.
    // TODO: how to make sure that a page split will not happen during you search
    //  the page.
//        assert(front_v == rear_v);
        if (!page->internal_page_search(k, result, current_ticket)){
            goto local_reread;
        }
        nested_retry_counter = 0;
#ifdef PROCESSANALYSIS
        start = std::chrono::high_resolution_clock::now();
#endif


        if(!skip_cache){
            page_cache->Release(handle);
        }

#ifdef PROCESSANALYSIS
        if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
            auto stop = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//#ifndef NDEBUG
            printf("cache release for level %d (%ld) ns\n", level,duration.count());
//          TimePrintCounter = 0;
        }
//#endif
#endif

  return true;
}
#ifdef CACHECOHERENCEPROTOCOL
    bool Btr::leaf_page_search(GlobalAddress page_addr, const Key &k, SearchResult &result, int level, CoroContext *cxt,
                               int coro_id) {
#ifdef PROCESSANALYSIS
        auto start = std::chrono::high_resolution_clock::now();
#endif
        int counter = 0;
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;

        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage,global_lock);
        // TODO: We need to implement the lock coupling. how to avoid unnecessary RDMA for lock coupling?
        //
        Slice page_id((char*)&page_addr, sizeof(GlobalAddress));
        Cache::Handle* handle = nullptr;
        void* page_buffer;
        Header * header;
        LeafPage* page;
        ibv_mr* mr = nullptr;
        assert(level == 0);
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
        handle = page_cache->LookupInsert(page_id, nullptr, kLeafPageSize, Deallocate_MR_WITH_CCP);
#ifdef PROCESSANALYSIS
        if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
            auto stop = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//#ifndef NDEBUG
            printf("cache look up for level %d is (%ld) ns, \n", level, duration.count());
//          TimePrintCounter = 0;
        }
//#endif
#endif
        // TODO: use real pointer to bypass the cache hash table. We need overwrittened function for internal page search,
        //  given an local address (now the global address is given). Or we make it the same function with both global ptr and local ptr,
        //  and let the function to figure out the situation.



#ifndef NDEBUG
        int rdma_refetch_times = 0;
#endif
//        if (handle != nullptr){
        assert(handle!= nullptr);
        std::shared_lock<std::shared_mutex> r_l(handle->rw_mtx);

        //TODO; put the mutex inside the cache. Can we guarantee the correctness of strategy.

        // First check whether the strategy is 1 and read or write lock is on, if so do nothing. If not, fetch the page
        // and read lock the page
        if(handle->strategy.load() == 1){
            //TODO: make the leaf_node_search the same as leaf_node_store?
            // No, because the read here has a optimiaziton for the double check locking

            if (handle->remote_lock_status.load() == 0){
                cache_miss[RDMA_Manager::thread_id][0]++;
                // upgrade the lock the write lock.
                //Can we use the std::call_once here?
                r_l.unlock();
                std::unique_lock<std::shared_mutex> w_l(handle->rw_mtx);

                if (handle->strategy.load() == 1 && handle->remote_lock_status.load() == 0){
                    if(handle->value) {
                        mr = (ibv_mr*)handle->value;


                    }else{
                        mr = new ibv_mr{};
                        rdma_mg->Allocate_Local_RDMA_Slot(*mr, Internal_and_Leaf);

//        printf("Allocate slot for page 1, the page global pointer is %p , local pointer is  %p, hash value is %lu level is %d\n",
//               page_addr, mr->addr, HashSlice(page_id), level);

                        //TODO: this is not guarantted to be atomic, mulitple reader can cause memory leak
                        handle->value = mr;

                    }
                    global_Rlock_and_read_page(mr, page_addr, kLeafPageSize, lock_addr, cas_mr,
                                               1, cxt, coro_id, handle);
//                handle->remote_lock_status.store(1);
                }

                w_l.unlock();
                r_l.lock();
            }else{
                cache_hit[RDMA_Manager::thread_id][0]++;
            }
            mr = (ibv_mr*)handle->value;


        }else{
            assert(handle->strategy.load() == 2);
            cache_miss[RDMA_Manager::thread_id][0]++;
            // TODO: acquire the lock and read to local buffer and remeber to release in the end.
            mr = rdma_mg->Get_local_read_mr();
            //
        }
//        if (handle->remote_lock_status.load() != 0){
//            cache_hit[RDMA_Manager::thread_id][0]++;
//            assert(handle->value != nullptr);
//            mr = (ibv_mr*)handle->value;
//        }


//        else{
//            if (handle->strategy.load() == 1)
//            assert(handle->remote_lock_status == 0);
//            // if the strategy is 2 then the page actually should not cached in the page.
//            assert(!handle->value);
//            //TODO: access it over thread local mr and do not cache it.
//
//        }

        //TODO: how to make the optimistic latch free within this funciton

        page_buffer = mr->addr;
        header = (Header *) ((char*)page_buffer + (STRUCT_OFFSET(InternalPage, hdr)));
        page = (LeafPage *)page_buffer;
        memset(&result, 0, sizeof(result));
        path_stack[coro_id][result.level] = page_addr;

//        page->global_lock = 1;


//        rdma_mg->RDMA_Read(page_addr, mr, kInternalPageSize, IBV_SEND_SIGNALED, 1, Internal_and_Leaf);
//        DEBUG_arg("cache miss and RDMA read %p", page_addr);
        //
        assert(page->hdr.this_page_g_ptr = page_addr);

        result.is_leaf = header->level == 0;
        result.level = header->level;
        level = result.level;

            assert(result.is_leaf );
            assert(result.level == 0 );
            assert(page->hdr.level < 100);
            //TODO: acquire the remote read lock. and keep trying until success.
//            page->check_invalidation_and_refetch_outside_lock(page_addr, rdma_mg, mr);
        assert(result.level == 0);
        if (k >= page->hdr.highest) { // should turn right, the highest is not included
//        printf("should turn right ");
//              if (page->hdr.sibling_ptr != GlobalAddress::Null()){
            // erase the upper level from the cache
            int last_level = 1;
            if (path_stack[coro_id][last_level] != GlobalAddress::Null()){
//            DEBUG_arg("Erase the page 3 %p\n", path_stack[coro_id][last_level]);
                Slice upper_node_page_id((char*)&path_stack[coro_id][last_level], sizeof(GlobalAddress));
                // TODO: By passing the cache access to same the cost for page invalidation, use the handle within
                // the page to check whether the page has been evicted.
                Cache::Handle* upper_layer_handle = page_cache->Lookup(upper_node_page_id);
                if(upper_layer_handle){
                    InternalPage* upper_page = (InternalPage*)((ibv_mr*)upper_layer_handle->value)->addr;
                    make_page_invalidated(upper_page);
                    page_cache->Release(upper_layer_handle);

                }
//            page_cache->Erase(Slice((char*)&path_stack[coro_id][last_level], sizeof(GlobalAddress)));

            }
            // In case that there is a long distance(num. of sibiling pointers) between current node and the target node
            if (nested_retry_counter <= 2){
                nested_retry_counter++;
                result.slibing = page->hdr.sibling_ptr;
                goto returntrue;
            }else{
                nested_retry_counter = 0;
                DEBUG_PRINT_CONDITION("retry place 3\n");
                goto returnfalse;
            }

        }
        nested_retry_counter = 0;
        if ((k < page->hdr.lowest )) { // cache is stale
            // erase the upper node from the cache and refetch the upper node to continue.
            int last_level = 1;
            if (path_stack[coro_id][last_level] != GlobalAddress::Null()){
                //TODO(POTENTIAL bug): add a lock for the page when erase it. other wise other threads may
                // modify the page based on a stale cached page.
//            DEBUG_arg("Erase the page 4 %p\n", path_stack[coro_id][last_level]);
                Slice upper_node_page_id((char*)&path_stack[coro_id][last_level], sizeof(GlobalAddress));
                // TODO: By passing the cache access to same the cost for page invalidation, use the handle within
                // the page to check whether the page has been evicted.
                Cache::Handle* upper_layer_handle = page_cache->Lookup(upper_node_page_id);
                if(upper_layer_handle){
                    InternalPage* upper_page = (InternalPage*)((ibv_mr*)upper_layer_handle->value)->addr;
                    make_page_invalidated(upper_page);
                    page_cache->Release(upper_layer_handle);
                }
//            page_cache->Erase(Slice((char*)&path_stack[coro_id][last_level], sizeof(GlobalAddress)));

            }
            DEBUG_PRINT_CONDITION("retry place 4\n");
            goto returnfalse;
        }
//#ifdef PROCESSANALYSIS
//    start = std::chrono::high_resolution_clock::now();
//#endif
        page->leaf_page_search(k, result, *mr, page_addr);
//#ifdef PROCESSANALYSIS
//    if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
//        auto stop = std::chrono::high_resolution_clock::now();
//        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//        printf("leaf page search the page uses (%ld) ns\n", duration.count());
//          TimePrintCounter[RDMA_Manager::thread_id] = 0;
//    }else{
//          TimePrintCounter[RDMA_Manager::thread_id]++;
//    }
//#endif
    returntrue:
        if (handle->strategy == 2){
            global_RUnlock(lock_addr, cas_mr, cxt, coro_id, handle);
//                    handle->remote_lock_status.store(0);
        }
        assert(handle);
//        assert(handle->refs.load() == 2);
        page_cache->Release(handle);

        return true;
    returnfalse:
        if (handle->strategy == 2){
            global_RUnlock(lock_addr, cas_mr, cxt, coro_id, handle);
//                    handle->remote_lock_status.store(0);
        }
//        assert(handle->refs.load() == 2);
        assert(handle);
        page_cache->Release(handle);
        return false;



    }
#else
    bool Btr::leaf_page_search(GlobalAddress page_addr, const Key &k, SearchResult &result, int level, CoroContext *cxt,
                           int coro_id) {
    int counter = 0;
re_read:
    if (++counter > 100) {
        printf("re read too many times\n");
        sleep(1);
    }
    // TODO: We need to implement the lock coupling. how to avoid unnecessary RDMA for lock coupling?
    //
    Slice page_id((char*)&page_addr, sizeof(GlobalAddress));
    Cache::Handle* handle = nullptr;
    void* page_buffer;
    Header * header;
    assert(level == 0);
    ibv_mr* local_mr = rdma_mg->Get_local_read_mr();
    page_buffer = local_mr->addr;
    //clear the version so that there will be no "false consistent" page
    ((LeafPage*)page_buffer)->front_version = 0;
    ((LeafPage*)page_buffer)->rear_version = 0;
    header = (Header *) ((char*)page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
//#ifdef PROCESSANALYSIS
//    auto start = std::chrono::high_resolution_clock::now();
//#endif
    rdma_mg->RDMA_Read(page_addr, local_mr, kLeafPageSize, IBV_SEND_SIGNALED, 1, Internal_and_Leaf);
//#ifdef PROCESSANALYSIS
//    if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
//        auto stop = std::chrono::high_resolution_clock::now();
//        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//        printf("leaf page search fetch RDMA uses (%ld) ns\n", duration.count());
////          TimePrintCounter[RDMA_Manager::thread_id] = 0;
//    }else{
////          TimePrintCounter[RDMA_Manager::thread_id]++;
//    }
//#endif
    memset(&result, 0, sizeof(result));
    result.is_leaf = header->leftmost_ptr == GlobalAddress::Null();
    result.level = header->level;
//  if(!result.is_leaf)
//      assert(result.level !=0);
//        assert(result.is_leaf == (level == 0));
    path_stack[coro_id][result.level] = page_addr;
    auto page = (LeafPage *)page_buffer;
    if (!page->check_consistent()) {
        goto re_read;
    }



    assert(result.level == 0);
    if (k >= page->hdr.highest) { // should turn right, the highest is not included
//        printf("should turn right ");
//              if (page->hdr.sibling_ptr != GlobalAddress::Null()){
        // erase the upper level from the cache
        int last_level = 1;
        if (path_stack[coro_id][last_level] != GlobalAddress::Null()){
//            DEBUG_arg("Erase the page 3 %p\n", path_stack[coro_id][last_level]);
            Slice upper_node_page_id((char*)&path_stack[coro_id][last_level], sizeof(GlobalAddress));
            // TODO: By passing the cache access to same the cost for page invalidation, use the handle within
            // the page to check whether the page has been evicted.
            Cache::Handle* upper_layer_handle = page_cache->Lookup(upper_node_page_id);
            if(upper_layer_handle){
                InternalPage* upper_page = (InternalPage*)((ibv_mr*)upper_layer_handle->value)->addr;
                Recieve_page_invalidation(upper_page);
                page_cache->Release(upper_layer_handle);

            }
//            page_cache->Erase(Slice((char*)&path_stack[coro_id][last_level], sizeof(GlobalAddress)));

        }
        // In case that there is a long distance(num. of sibiling pointers) between current node and the target node
        if (nested_retry_counter <= 2){
            nested_retry_counter++;
            result.slibing = page->hdr.sibling_ptr;
            return true;
        }else{
            nested_retry_counter = 0;
            DEBUG_PRINT_CONDITION("retry place 3\n");
            return false;
        }

    }
    nested_retry_counter = 0;
    if ((k < page->hdr.lowest )) { // cache is stale
        // erase the upper node from the cache and refetch the upper node to continue.
        int last_level = 1;
        if (path_stack[coro_id][last_level] != GlobalAddress::Null()){
            //TODO(POTENTIAL bug): add a lock for the page when erase it. other wise other threads may
            // modify the page based on a stale cached page.
//            DEBUG_arg("Erase the page 4 %p\n", path_stack[coro_id][last_level]);
            Slice upper_node_page_id((char*)&path_stack[coro_id][last_level], sizeof(GlobalAddress));
            // TODO: By passing the cache access to same the cost for page invalidation, use the handle within
            // the page to check whether the page has been evicted.
            Cache::Handle* upper_layer_handle = page_cache->Lookup(upper_node_page_id);
            if(upper_layer_handle){
                InternalPage* upper_page = (InternalPage*)((ibv_mr*)upper_layer_handle->value)->addr;
                Recieve_page_invalidation(upper_page);
                page_cache->Release(upper_layer_handle);
            }
//            page_cache->Erase(Slice((char*)&path_stack[coro_id][last_level], sizeof(GlobalAddress)));

        }
        DEBUG_PRINT_CONDITION("retry place 4\n");
        return false;// false means need to fall back
    }
//#ifdef PROCESSANALYSIS
//    start = std::chrono::high_resolution_clock::now();
//#endif
    page->leaf_page_search(k, result, *local_mr, page_addr);
//#ifdef PROCESSANALYSIS
//    if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
//        auto stop = std::chrono::high_resolution_clock::now();
//        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//        printf("leaf page search the page uses (%ld) ns\n", duration.count());
//          TimePrintCounter[RDMA_Manager::thread_id] = 0;
//    }else{
//          TimePrintCounter[RDMA_Manager::thread_id]++;
//    }
//#endif
    return true;
}
#endif

// This function will return true unless it found that the key is smaller than the lower bound of a searched node.
// When this function return false the upper layer should backoff in the tree.
bool Btr::internal_page_store(GlobalAddress page_addr, Key &k, GlobalAddress &v, int level, CoroContext *cxt, int coro_id) {
    assert(page_addr != GlobalAddress::Null());
        assert(v != GlobalAddress::Null());
        uint64_t lock_index =
      CityHash64((char *)&page_addr, sizeof(page_addr)) % define::kNumOfLock;
    bool need_split;
    bool insert_success;
    GlobalAddress lock_addr;
    lock_addr.nodeID = page_addr.nodeID;
    lock_addr.offset = page_addr.offset + STRUCT_OFFSET(InternalPage,global_lock);

    ibv_mr* page_mr;
    void * page_buffer;
    InternalPage* page;
    bool skip_cache = false;
    Cache::Handle* handle = nullptr;
    // TODO: also use page hint to access the internal store to bypassing the cache
    if (level == tree_height.load()) {
        cache_hit[RDMA_Manager::thread_id][0]++;

        page_mr = cached_root_page_mr.load();
        page_buffer = page_mr->addr;
        page = (InternalPage *)page_buffer;
        Header* header = (Header *) ((char *) page_buffer + (STRUCT_OFFSET(InternalPage, hdr)));
        //since tree height is modified the last, so the page must be correct. the only situation which
        // challenge the assertion below is that the root page is changed too fast or there is a long context switch above.
        assert(header->this_page_g_ptr == page_addr);
        if (header->this_page_g_ptr == page_addr) {
            assert(header->level == level);
            // if this page mr is in-use and is the local cache for page_addr
            skip_cache = true;
            page = (InternalPage *)page_buffer;
            path_stack[coro_id][level] = page_addr;
            assert(page->hdr.level < 100);
            ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
#ifndef NDEBUG
            bool valid_temp = page->hdr.valid_page;
            uint64_t local_meta_new = __atomic_load_n((uint64_t*)&page->local_lock_meta, (int)std::memory_order_seq_cst);

#endif
            bool handover = acquire_local_optimistic_lock(&page->local_lock_meta, cxt, coro_id);
#ifndef NDEBUG
//        usleep(4);
            uint8_t expected = 0;
            assert( __atomic_load_n(&page->local_lock_meta.local_lock_byte, mem_cst_seq) != 0);
            assert(!__atomic_compare_exchange_n(&page->local_lock_meta.local_lock_byte, &expected, 1, false, mem_cst_seq, mem_cst_seq));
#endif
            if (handover){
                // No need to read the page again because we handover the page as well.
//            rdma_mg->RDMA_Read(page_addr, local_buffer, kInternalPageSize, IBV_SEND_SIGNALED, 1, Internal_and_Leaf);

            }else{
                ibv_mr temp_mr = *page_mr;
                GlobalAddress temp_page_add = page_addr;
                temp_page_add.offset = page_addr.offset + RDMA_OFFSET;
                temp_mr.addr = (char*)temp_mr.addr + RDMA_OFFSET;
                temp_mr.length = temp_mr.length - RDMA_OFFSET;
                assert(page->local_lock_meta.local_lock_byte == 1);
                rdma_mg->global_Wlock_and_read_page_without_INVALID(&temp_mr, temp_page_add,
                                                                 kInternalPageSize - RDMA_OFFSET,
                                                                 lock_addr, cas_mr, 1, cxt, coro_id);
//                handle->remote_lock_status.store(2);
//            usleep(1);
            }
            assert(page->local_lock_meta.local_lock_byte == 1);


        }

    }

    if(!skip_cache){
        Slice page_id((char*)&page_addr, sizeof(GlobalAddress));

        handle = page_cache->Lookup(page_id);
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
//    int flag = 3;
        if (handle!= nullptr){
            cache_hit[RDMA_Manager::thread_id][0]++;
            // TODO: only fetch the data outside the local metadata.
            // is possible that the reader need to have a local reread, during the execution.
            page_mr = (ibv_mr*)page_cache->Value(handle);
            page_buffer = page_mr->addr;
            // you have to reread to data from the remote side to not missing update from other
            // nodes! Do not read the page from the cache!
            page = (InternalPage *)page_buffer;

            // TODO(Potential optimization): May be we can handover the cache if two writer threads modifying the same page.
            //  saving some RDMA round trips.
#ifndef NDEBUG
            bool valid_temp = page->hdr.valid_page;
            uint64_t local_meta_new = __atomic_load_n((uint64_t*)&page->local_lock_meta, (int)std::memory_order_seq_cst);

#endif
            bool handover = acquire_local_optimistic_lock(&page->local_lock_meta, cxt, coro_id);
#ifndef NDEBUG
//        usleep(4);
            uint8_t expected = 0;
            assert( __atomic_load_n(&page->local_lock_meta.local_lock_byte, mem_cst_seq) != 0);
            assert(!__atomic_compare_exchange_n(&page->local_lock_meta.local_lock_byte, &expected, 1, false, mem_cst_seq, mem_cst_seq));
#endif
            if (handover){
                // No need to read the page again because we handover the page as well.
//            rdma_mg->RDMA_Read(page_addr, local_buffer, kInternalPageSize, IBV_SEND_SIGNALED, 1, Internal_and_Leaf);

            }else{
                // need to trune the page address, to avoid local lock overwritting.
                ibv_mr temp_mr = *page_mr;
                GlobalAddress temp_page_add = page_addr;
                temp_page_add.offset = page_addr.offset + RDMA_OFFSET;
                temp_mr.addr = (char*)temp_mr.addr + RDMA_OFFSET;
                temp_mr.length = temp_mr.length - RDMA_OFFSET;
                assert(page->local_lock_meta.local_lock_byte == 1);
                rdma_mg->global_Wlock_and_read_page_without_INVALID(&temp_mr, temp_page_add,
                                                                 kInternalPageSize - RDMA_OFFSET,
                                                                 lock_addr, cas_mr, 1, cxt, coro_id);

//                handle->remote_lock_status.store(2);

//            usleep(1);
            }
            assert(page->local_lock_meta.local_lock_byte == 1);
            assert(!page->check_whether_globallock_is_unlocked());


//        lock_and_read_page(local_buffer, page_addr, kInternalPageSize, cas_mr,
//                           lock_addr, 1, cxt, coro_id);
//        printf("Existing cache entry: prepare RDMA read request global ptr is %p, local ptr is %p, level is %d\n", page_addr, page_buffer, level);

//        printf("Read page %lu over address %p, version is %u  \n", page_addr.offset, local_buffer->addr, ((InternalPage *)page_buffer)->hdr.last_index);
//        flag = 1;
        } else{
            cache_miss[RDMA_Manager::thread_id][0]++;
            //TODO: acquire the lock when trying to insert the page to the cache.
            page_mr = new ibv_mr{};
//        printf("Allocate slot for page 2 %p\n", page_addr);
            rdma_mg->Allocate_Local_RDMA_Slot(*page_mr, Internal_and_Leaf);
            page_buffer = page_mr->addr;
            page = (InternalPage *)page_buffer;
            page->local_metadata_init();
            // you have to reread to data from the remote side to not missing update from other
            // nodes! Do not read the page from the cache!
            bool handover = acquire_local_optimistic_lock(&page->local_lock_meta, cxt, coro_id);
//        assert(page->local_lock_meta.local_lock_byte == 1);
            assert(!handover);
            ibv_mr temp_mr = *page_mr;
            GlobalAddress temp_page_add = page_addr;
            temp_page_add.offset = page_addr.offset + RDMA_OFFSET;
            temp_mr.addr = (char*)temp_mr.addr + RDMA_OFFSET;
            temp_mr.length = temp_mr.length - RDMA_OFFSET;
            //Skip the local lock metadata
            //TOTHINK: The RDMA read may result in the local spin lock not work if they are in the same cache line.
            // The RDMA read may result in a false
            rdma_mg->global_Wlock_and_read_page_without_INVALID(&temp_mr, temp_page_add, kInternalPageSize - RDMA_OFFSET,
                                                             lock_addr, cas_mr, 1, cxt, coro_id);
//            handle->remote_lock_status.store(2);
            assert(page->local_lock_meta.local_lock_byte == 1);


//        lock_and_read_page(local_buffer, page_addr, kInternalPageSize, cas_mr,
//                           lock_addr, 1, cxt, coro_id);
//        printf("non-Existing cache entry: prepare RDMA read request global ptr is %p, local ptr is %p, level is %d\n", page_addr, page_buffer, level);

//        printf("Read page %lu over address %p, version is %u \n", page_addr.offset, local_buffer->addr, ((InternalPage *)page_buffer)->hdr.last_index);
            assert(page_mr != nullptr);

            handle = page_cache->Insert(page_id, page_mr, kInternalPageSize, Deallocate_MR);
            // No need for consistence check here.
//        flag = 0;
        }
    }

//    local_reread:
//        assert(((uint64_t)&page->local_lock_meta) % 8 == 0);
//        uint64_t local_meta = __atomic_load_n((uint64_t*)&page->local_lock_meta, (int)std::memory_order_seq_cst);
//        while( ((Local_Meta*) &local_meta)->local_lock_byte > 0){
//            local_meta = __atomic_load_n((uint64_t*)&page->local_lock_meta, (int)std::memory_order_seq_cst);
//        };
//        uint16_t issued_ticket = ((Local_Meta*) &local_meta)->issued_ticket;
//        uint16_t current_ticket = ((Local_Meta*) &local_meta)->current_ticket;



    assert(((char*)&page->global_lock - (char*)page) == RDMA_OFFSET);
    assert(page->hdr.level == level);
    assert(!page->check_whether_globallock_is_unlocked());
    assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());
    path_stack[coro_id][page->hdr.level] = page_addr;
    // This is the result that we do not lock the btree when search for the key.
    // Not sure whether this will still work if we have node merge
    // Why this node can not be the right most node
    if (k >= page->hdr.highest ) {
      // TODO: No need for node invalidation when inserting things because the tree tranversing is enough for invalidation (Erase)

        if (path_stack[coro_id][level+1]!= GlobalAddress::Null()){
//            DEBUG_arg("Erase the page 7 %p\n", path_stack[coro_id][level+1]);
            Slice upper_node_page_id((char*)&path_stack[coro_id][level+1], sizeof(GlobalAddress));
            // TODO: By passing the cache access to same the cost for page invalidation, use the handle within
            // the page to check whether the page has been evicted.
            Cache::Handle* upper_layer_handle = page_cache->Lookup(upper_node_page_id);
            if(upper_layer_handle){
                InternalPage* upper_page = (InternalPage*)((ibv_mr*)upper_layer_handle->value)->addr;
                make_page_invalidated(upper_page);
                page_cache->Release(upper_layer_handle);
            }
//            page_cache->Erase(Slice((char*)&path_stack[coro_id][level+1], sizeof(GlobalAddress)));
        }
        // This could be async.
//        this->unlock_addr(lock_addr, cxt, coro_id, false);



        assert(page->hdr.sibling_ptr != GlobalAddress::Null());
        if (nested_retry_counter <= 2){
            nested_retry_counter++;
            GlobalAddress sib_ptr = page->hdr.sibling_ptr;
            //Unlock this page.
            bool hand_over_other = can_hand_over(&page->local_lock_meta);
            if (hand_over_other) {
                releases_local_optimistic_lock(&page->local_lock_meta);
            }else{

                assert(page->global_lock = 1);
                rdma_mg->global_unlock_addr(lock_addr,cxt,coro_id, false);
                releases_local_optimistic_lock(&page->local_lock_meta);
            }

            insert_success = this->internal_page_store(sib_ptr, k, v, level, cxt,
                                                       coro_id);


        }else{
            nested_retry_counter = 0;
            insert_success = false;
            DEBUG_PRINT_CONDITION("retry place 5\n");
            //Unlock this page.
            bool hand_over_other = can_hand_over(&page->local_lock_meta);
            if (hand_over_other) {
                releases_local_optimistic_lock(&page->local_lock_meta);
            }else{

                assert(page->global_lock = 1);
                rdma_mg->global_unlock_addr(lock_addr,cxt,coro_id, false);
                releases_local_optimistic_lock(&page->local_lock_meta);
            }
//            return false;
        }

        if (!skip_cache){
            page_cache->Release(handle);
        }

        return insert_success;

    }
    nested_retry_counter = 0;
    if (k < page->hdr.lowest ) {
        // if key is smaller than the lower bound, the insert has to be restart from the
        // upper level. because the sibling pointer only points to larger one.

        if (path_stack[coro_id][level+1]!= GlobalAddress::Null()){
            Slice upper_node_page_id((char*)&path_stack[coro_id][level+1], sizeof(GlobalAddress));
            // TODO: By passing the cache access to same the cost for page invalidation, use the handle within
            //  the page to check whether the page has been evicted.
            Cache::Handle* upper_layer_handle = page_cache->Lookup(upper_node_page_id);
            if(upper_layer_handle){
                InternalPage* upper_page = (InternalPage*)((ibv_mr*)upper_layer_handle->value)->addr;
                make_page_invalidated(upper_page);
                page_cache->Release(upper_layer_handle);
            }
//            page_cache->Erase(Slice((char*)&path_stack[coro_id][level+1], sizeof(GlobalAddress)));
        }
//        this->unlock_addr(lock_addr, cxt, coro_id, false);
        bool hand_over_other = can_hand_over(&page->local_lock_meta);
        if (hand_over_other) {
            releases_local_optimistic_lock(&page->local_lock_meta);
        }else{

            assert(page->global_lock = 1);
            rdma_mg->global_unlock_addr(lock_addr,cxt,coro_id, false);
//            global_write_page_and_unlock(&temp_mr, temp_page_add, kInternalPageSize -RDMA_OFFSET, lock_addr, cxt, coro_id, false);
            releases_local_optimistic_lock(&page->local_lock_meta);
        }


        insert_success = false;
        DEBUG_PRINT_CONDITION("retry place 6\n");
        if (!skip_cache){
            page_cache->Release(handle);
        }
        return insert_success;// result in fall back search on the higher level.
    }

//  assert(k >= page->hdr.lowest);
    auto cnt = page->hdr.last_index + 1;
  bool is_update = false;
  uint16_t insert_index = 0;
//        printf("The last index %d 's key is %lu, this key is %lu\n", page->hdr.last_index, page->records[page->hdr.last_index].key, k);
  //TODO: Make it a binary search.
  for (int i = cnt - 1; i >= 0; --i) {
    if (page->records[i].key == k) { // find and update
//        assert(false);
//        page->front_version++;
//        __atomic_fetch_add(&page->front_version, 1, __ATOMIC_SEQ_CST);
        asm volatile ("sfence\n" : : );
      page->records[i].ptr = v;
      asm volatile ("sfence\n" : : );
//        page->rear_version++;
//        __atomic_fetch_add(&page->rear_version, 1, __ATOMIC_SEQ_CST);

        // assert(false);
      is_update = true;
      break;
    }
    if (page->records[i].key < k) {
      insert_index = i + 1;
      break;
    }
  }
  assert(cnt != kInternalCardinality);
  assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());
    Key split_key;
    GlobalAddress sibling_addr = GlobalAddress::Null();
  if (!is_update) { // insert and shift
      // The update should mark the page version change because this will make the page state in consistent.
//      __atomic_fetch_add(&page->front_version, 1, __ATOMIC_SEQ_CST);
//      page->front_version++;
      //TODO(potential bug): double check the memory fence, there could be out of order
      // execution preventing the version lock.
//      asm volatile ("sfence\n" : : );
//      asm volatile ("lfence\n" : : );
//      asm volatile ("mfence" : : : "memory");
    for (int i = cnt; i > insert_index; --i) {
      page->records[i].key = page->records[i - 1].key;
      page->records[i].ptr = page->records[i - 1].ptr;
    }


      page->records[insert_index].key = k;
      page->records[insert_index].ptr = v;
#ifndef NDEBUG
      uint16_t last_index_prev = page->hdr.last_index;
#endif
      page->hdr.last_index++;


//#ifndef NDEBUG
//      assert(last_index_memo == page->hdr.last_index);
//#endif
//      asm volatile ("sfence\n" : : );
// THe last index could be the same for several print because we may not insert to the end all the time.
//        printf("last_index of page offset %lu is %hd, page level is %d, page is %p, the last index content is %lu %p, version should be, the key is %lu\n"
//               , page_addr.offset,  page->hdr.last_index, page->hdr.level, page, page->records[page->hdr.last_index].key, page->records[page->hdr.last_index].ptr, k);
      assert(page->hdr.last_index == last_index_prev + 1);
        assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());
      assert(page->records[page->hdr.last_index].key != 0);
//  assert(page->records[page->hdr.last_index] != GlobalAddress::Null());
      cnt = page->hdr.last_index + 1;

      need_split = cnt == kInternalCardinality;

      // THe internal node is different from leaf nodes because it has the
      // leftmost_ptr. THe internal nodes has n key but n+1 global pointers.
      // the internal node split pick the middle key as split key and the middle key
      // will not existed in either of the splited node
      // THe data under this internal node [lowest, highest)

      //Both internal node and leaf nodes are [lowest, highest) except for the left most
      assert(page->local_lock_meta.local_lock_byte == 1);

      if (need_split) { // need split
          sibling_addr = rdma_mg->Allocate_Remote_RDMA_Slot(Internal_and_Leaf, 2 * round_robin_cur + 1);
          if(++round_robin_cur == rdma_mg->memory_nodes.size()){
              round_robin_cur = 0;
          }
          ibv_mr* sibling_mr = new ibv_mr{};
//          printf("Allocate slot for page 3 %p\n", sibling_addr);

          rdma_mg->Allocate_Local_RDMA_Slot(*sibling_mr, Internal_and_Leaf);

          auto sibling = new(sibling_mr->addr) InternalPage(GlobalAddress(), page->hdr.level);

//              std::cout << "addr " <<  sibling_addr << " | level " <<
//              (int)(page->hdr.level) << std::endl;
          int m = cnt / 2;
          split_key = page->records[m].key;
          assert(split_key > page->hdr.lowest);
          assert(split_key < page->hdr.highest);
          page->hdr.last_index -= (cnt - m); // this is correct. because we extract the split key to upper layer
          assert(page->hdr.last_index == m-1);
          sibling->hdr.last_index += (cnt - m - 1);
          assert(sibling->hdr.last_index == cnt - m - 1 - 1);
          for (int i = m + 1; i < cnt; ++i) { // move
              //Is this correct?
              sibling->records[i - m - 1].key = page->records[i].key;
              sibling->records[i - m - 1].ptr = page->records[i].ptr;
          }
//          page->hdr.last_index = m; // this is correct.
//          assert(page->hdr.last_index == m);
//          assert(sibling->hdr.last_index == 0);
//          sibling->hdr.last_index += (cnt - m - 1);
//          for (int i = m; i < cnt; ++i) { // move
//              //Is this correct?
//              sibling->records[i - m].key = page->records[i].key;
//              sibling->records[i - m].ptr = page->records[i].ptr;
//          }

          sibling->hdr.leftmost_ptr = page->records[m].ptr;
          sibling->hdr.lowest = page->records[m].key;
          sibling->hdr.highest = page->hdr.highest;
          page->hdr.highest = page->records[m].key;

          // link
          sibling->hdr.sibling_ptr = page->hdr.sibling_ptr;
          page->hdr.sibling_ptr = sibling_addr;
//    sibling->set_consistent();
          //the code below is just for debugging.
//    sibling_addr.mark = 2;

          rdma_mg->RDMA_Write(sibling_addr, sibling_mr, kInternalPageSize, IBV_SEND_SIGNALED, 1, Internal_and_Leaf);
          assert(sibling->records[sibling->hdr.last_index].ptr != GlobalAddress::Null());
          assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());
          k = split_key;
          v = sibling_addr;
//          printf("Create new node %p\n", v);
          // TODO (opt): we can directly add the sibling block into the cache here.


//      printf("page splitted last_index of page offset %lu is %hd, page level is %d\n", page_addr.offset,  page->hdr.last_index, page->hdr.level);

        //The code below is optional.
        Slice sibling_page_id((char*)&sibling_addr, sizeof(GlobalAddress));
          assert(page_mr!= nullptr);
        auto sib_handle = page_cache->Insert(sibling_page_id, sibling_mr, kInternalPageSize, Deallocate_MR);
          page_cache->Release(sib_handle);
//          rdma_mg->Deallocate_Local_RDMA_Slot(sibling_mr->addr, Internal_and_Leaf);
//          delete sibling_mr;
      } else{
//      k = Key ;
          // Only set the value as null is enough
          v = GlobalAddress::Null();
      }
      // Set the rear_version for the concurrent reader. The reader can tell whether
      // there is intermidated writer during its execution.
      //    page->set_consistent();
//      asm volatile ("sfence\n" : : );
//      asm volatile ("lfence\n" : : );
      asm volatile ("mfence" : : : "memory");
//      _mm_sfence();
        // The function below is the way to atomically change data without initialize the std::atomic
        // see https://gcc.gnu.org/onlinedocs/gcc/_005f_005fatomic-Builtins.html
//      __atomic_fetch_add(&page->rear_version, 1, __ATOMIC_SEQ_CST);
//      page->rear_version++;
  }

        assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());

        asm volatile ("mfence" : : : "memory");
//        printf("Can handover is %d, last index is %hd, page offset is %lu\n", can_hand_over(lock_addr),page->hdr.last_index, page_addr.offset);
        // It is posisble that the local lock implementation is not strong enough, making
        // the lock release before
        assert(page->local_lock_meta.local_lock_byte == 1);

        bool hand_over_other = can_hand_over(&page->local_lock_meta);
        if (hand_over_other) {
            //No need to write back we can handover the page as well.
//            rdma_mg->RDMA_Write(page_addr, local_buffer, kInternalPageSize, IBV_SEND_SIGNALED, 1, Internal_and_Leaf);
            releases_local_optimistic_lock(&page->local_lock_meta);
        }else{

//            ibv_mr temp_mr = *page_mr;
//            GlobalAddress temp_page_add = page_addr;
//            temp_page_add.offset = page_addr.offset + RDMA_OFFSET;
//            temp_mr.addr = (char*)temp_mr.addr + RDMA_OFFSET;
//            temp_mr.length = temp_mr.length - RDMA_OFFSET;
            assert(page->global_lock == ((uint64_t)(rdma_mg->node_id/2 +1) <<56));
            assert(page->hdr.valid_page);
            rdma_mg->global_write_page_and_Wunlock(page_mr, page_addr, kInternalPageSize, lock_addr,
                                                   cxt, coro_id, false);
            releases_local_optimistic_lock(&page->local_lock_meta);
        }
//        write_page_and_unlock(local_buffer, page_addr, kInternalPagpeSize,
//                              lock_addr, cxt, coro_id, false);
//        printf("prepare RDMA write request global ptr is %p, local ptr is %p, level is %d\n", page_addr, page_buffer, level);

        if (!skip_cache){
            page_cache->Release(handle);
        }


    // We can also say if need_split
    if (sibling_addr != GlobalAddress::Null()){
        ibv_mr* page_hint = nullptr;
        auto p = path_stack[coro_id][level+1];
        //check whether the node split is for a root node.
        if (UNLIKELY(p == GlobalAddress::Null() )){
            // First acquire local lock
            std::unique_lock<std::mutex> l(root_mtx);
            p = g_root_ptr.load();
            uint8_t height = tree_height;

            if (path_stack[coro_id][level] == p && height == level){
                //Acquire global lock for the root update.
                GlobalAddress lock_addr = {};
                // root node lock addr. but this could result in a deadlock for transaction cc.
                lock_addr.nodeID = 1;
                lock_addr.offset = 0;
                auto cas_buffer = rdma_mg->Get_local_CAS_mr();
                //aquire the global lock to avoid mulitple node creating the new  root node
acquire_global_lock:
                *(uint64_t*)cas_buffer->addr = 0;
                rdma_mg->RDMA_CAS(lock_addr, cas_buffer, 0, 1, IBV_SEND_SIGNALED,1, LockTable);
                if ((*(uint64_t*) cas_buffer->addr) != 0){
                    goto acquire_global_lock;
                }
                refetch_rootnode();
                p = g_root_ptr.load();
                height = tree_height.load();
                if (path_stack[coro_id][level] == p && height == level) {
                    update_new_root(path_stack[coro_id][level], split_key, sibling_addr, level + 1,
                                    path_stack[coro_id][level], cxt, coro_id);
                    *(uint64_t *) cas_buffer->addr = 0;

                    rdma_mg->RDMA_Write(lock_addr, cas_buffer, sizeof(uint64_t), IBV_SEND_SIGNALED, 1, LockTable);

                    return true;
                }
//                l.unlock();
                *(uint64_t *) cas_buffer->addr = 0;

                rdma_mg->RDMA_Write(lock_addr, cas_buffer, sizeof(uint64_t), IBV_SEND_SIGNALED, 1, LockTable);

            }
            l.unlock();

            {
                //find the upper level
                //TODO: shall I implement a function that search a ptr at particular level.
                printf(" rare case the tranverse during the root update\n");

                return insert_internal(split_key, sibling_addr,  cxt, coro_id, level+1);
            }


        }
        // if not a root split go ahead and insert in the upper level.
        level = level +1;
        //*****************Now it is not a root update, insert to the upper level******************
        SearchResult result{};
        memset(&result, 0, sizeof(SearchResult));
        int fall_back_level = 0;
    re_insert:
        if (UNLIKELY(!internal_page_store(p, split_key, sibling_addr, level, cxt, coro_id))){
            //this path should be a rare case.

            // fall back to upper level in the cache to search for the right node at this level
            fall_back_level = level + 1;

            p = path_stack[coro_id][fall_back_level];
            page_hint = nullptr;
            if ( p == GlobalAddress::Null()){
                // insert it top-down. this function will keep searching until it is found
                insert_internal(split_key,sibling_addr, cxt, coro_id, level);
            }else{
                    if(!internal_page_search(p, k, result, fall_back_level, false, page_hint, cxt, coro_id)){
                        // if the upper level is still a stale node, just insert the node by top down method.
                        insert_internal(split_key,sibling_addr, cxt, coro_id, level);
//                        level = level + 1; // move to upper level
//                        p = path_stack[coro_id][level];// move the pointer to upper level
                    }else{

                        if (result.next_level != GlobalAddress::Null()){
                            // the page was found successful by one step back, then we can set the p as new node.
                            // do not need to chanve level.
                            p = result.next_level;
                            page_hint = nullptr;
                            goto re_insert;
//                    level = result.level - 1;
                        }else{
                            assert(false);
                        }
                    }

            }

        }
    }
    return true;
//  if (!need_split)
//    return;
//// TODO: migrate the function outside.
//  if (root == page_addr) { // update root
//
//    if (update_new_root(page_addr, split_key, sibling_addr, level + 1, root,
//                        cxt, coro_id)) {
//      return;
//    }else{
//        //CAS the root_ptr_ptr failed. need to re-insert.
//    }
//  }
//
//  auto up_level = path_stack[coro_id][level + 1];
//    //TOTHINGK: What if the upper level has been splitted by other nodes.
//  if (up_level != GlobalAddress::Null()) {
//    internal_page_store(up_level, split_key, sibling_addr, root, level + 1, cxt,
//                        coro_id);
//  } else {
//      // internal page store means that you are running insert_internal, and there
//      // must be upper levels' node in the cache. so this path must be unreachable.
//    assert(false);
//  }
}
#ifdef CACHECOHERENCEPROTOCOL
    bool Btr::leaf_page_store(GlobalAddress page_addr, const Key &k, const Value &v, Key &split_key, GlobalAddress &sibling_addr,
                              GlobalAddress root, int level, CoroContext *cxt, int coro_id) {
#ifdef PROCESSANALYSIS
        auto start = std::chrono::high_resolution_clock::now();
#endif

        int counter = 0;
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;

        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(InternalPage,global_lock);
        // TODO: We need to implement the lock coupling. how to avoid unnecessary RDMA for lock coupling?
        //
        Slice page_id((char*)&page_addr, sizeof(GlobalAddress));
        Cache::Handle* handle = nullptr;
        void* page_buffer;
        Header * header;
        LeafPage* page;
        ibv_mr* local_mr;
        assert(level == 0);
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();

        handle = page_cache->LookupInsert(page_id, nullptr, kLeafPageSize, Deallocate_MR_WITH_CCP);
        assert(handle!= nullptr);
        std::unique_lock<std::shared_mutex> l(handle->rw_mtx);
//        assert(handle->refs.load() == 2);

        //TODO: Can I early release the local lock if the strategy is 1

        if(handle->strategy.load() == 1){
#ifndef NDEBUG
            bool hint_of_existence = false;
#endif
            if(handle->value)
            {
#ifndef NDEBUG
                hint_of_existence = true;
#endif
                // This means the page has already be in the cache.
                local_mr = (ibv_mr*)handle->value;
                //TODO: delete the line below.
//                assert(handle->remote_lock_status != 0);
            }else{
#ifndef NDEBUG
                hint_of_existence = false;
#endif
                // This means the page was not in the cache before
                local_mr = new ibv_mr{};
                rdma_mg->Allocate_Local_RDMA_Slot(*local_mr, Internal_and_Leaf);
                assert(handle->remote_lock_status == 0);

//        printf("Allocate slot for page 1, the page global pointer is %p , local pointer is  %p, hash value is %lu level is %d\n",
//               page_addr, mr->addr, HashSlice(page_id), level);
                handle->value = local_mr;

            }
            // If the remote read lock is not on, lock it
            if (handle->remote_lock_status == 0){
                global_Wlock_and_read_page_with_INVALID(local_mr, page_addr, kLeafPageSize, lock_addr, cas_mr,
                                                                 1, cxt, coro_id, handle);
//                handle->remote_lock_status.store(2);

            }else if (handle->remote_lock_status == 1){
                if (!global_Rlock_update(lock_addr, cas_mr, cxt, coro_id,handle)){
//
                    //TODO: first unlock the read lock and then acquire the write lock is not atomic. this
                    // is problematice if we want to upgrade the lock during a transaction.
                    // May be we can take advantage of the lock starvation bit to solve this problem.
                    //the Read lock has been released, we can directly acquire the write lock
                    global_Wlock_and_read_page_with_INVALID(local_mr, page_addr, kLeafPageSize, lock_addr, cas_mr,
                                                            1, cxt, coro_id,handle);
                }else{

                }
            }
        }else{
            assert(handle->strategy == 2);
            // if the strategy is 2 then the page actually should not cached in the page.
            assert(!handle->value);
            //TODO: access it over thread local mr and do not cache it.
            assert(false);
        }








        // TODO: under some situation the lock is not released
        page_buffer = local_mr->addr;
        page = (LeafPage *)page_buffer;

        assert(page->hdr.level == level);
//        assert(page->check_consistent());
        path_stack[coro_id][page->hdr.level] = page_addr;
        // It is possible that the key is larger than the highest key
        // The range of a page is [lowest,largest).
        // TODO: find out why sometimes the node is far from the target node that it need multiple times of
        //  sibling access.
        //
        //  Note that it is normal to see that the local buffer are always the same accross the nested
        //  funciton call, because they are sharing the same local buffer.
        if (k >= page->hdr.highest) {
            if (page->hdr.sibling_ptr != GlobalAddress::Null()){
//                this->unlock_addr(lock_addr, cxt, coro_id, false);
                if (path_stack[coro_id][level+1]!= GlobalAddress::Null()){
//                DEBUG_arg("Erase the page 5 %p\n", path_stack[coro_id][1]);
                    Slice upper_node_page_id((char*)&path_stack[coro_id][level+1], sizeof(GlobalAddress));
                    // TODO: By passing the cache access to same the cost for page invalidation, use the handle within
                    //  the page to check whether the page has been evicted.
                    Cache::Handle* upper_layer_handle = page_cache->Lookup(upper_node_page_id);
                    if(upper_layer_handle){
                        InternalPage* upper_page = (InternalPage*)((ibv_mr*)upper_layer_handle->value)->addr;
                        make_page_invalidated(upper_page);
                        page_cache->Release(upper_layer_handle);
                    }

//                page_cache->Erase(Slice((char*)&path_stack[coro_id][1], sizeof(GlobalAddress)));
                }
//            this->unlock_addr(lock_addr, cxt, coro_id, true);
                if (nested_retry_counter <= 2){

                    nested_retry_counter++;
                    if (handle->strategy == 2){
                        global_unlock_addr(lock_addr,handle, cxt, coro_id, false);
//                        handle->remote_lock_status.store(0);
                    }
                    page_cache->Release(handle);
                    // Has to be unlocked to avoid a deadlock.
                    l.unlock();
                    return this->leaf_page_store(page->hdr.sibling_ptr, k, v, split_key, sibling_addr, root, level, cxt, coro_id);
                }else{

//                assert(false);
                    DEBUG_PRINT_CONDITION("retry place 7");
                    nested_retry_counter = 0;
                    if (handle->strategy == 2){
                        global_unlock_addr(lock_addr,handle, cxt, coro_id, false);
                        handle->remote_lock_status.store(0);
                    }
                    //No matter what strategy it is the cache handle need to be released.
                    page_cache->Release(handle);
                    return false;
                }
            }else{
                // impossible because the right most leaf node 's max is KeyMax
                assert(false);
            }

        }
        nested_retry_counter = 0;
        if (k < page->hdr.lowest ) {
            // if key is smaller than the lower bound, the insert has to be restart from the
            // upper level. because the sibling pointer only points to larger one.

            if (path_stack[coro_id][level+1]!= GlobalAddress::Null()){
//            DEBUG_arg("Erase the page 6 %p\n", path_stack[coro_id][1]);
                Slice upper_node_page_id((char*)&path_stack[coro_id][level+1], sizeof(GlobalAddress));
                // TODO: By passing the cache access to same the cost for page invalidation, use the handle within
                //  the page to check whether the page has been evicted.
                Cache::Handle* upper_layer_handle = page_cache->Lookup(upper_node_page_id);
                if(upper_layer_handle){
                    InternalPage* upper_page = (InternalPage*)((ibv_mr*)upper_layer_handle->value)->addr;
                    make_page_invalidated(upper_page);
                    page_cache->Release(upper_layer_handle);
                }

//            page_cache->Erase(Slice((char*)&path_stack[coro_id][1], sizeof(GlobalAddress)));
            }
//            this->unlock_addr(lock_addr, cxt, coro_id, false);
            if (handle->strategy == 2){
                global_unlock_addr(lock_addr,handle, cxt, coro_id, false);
                handle->remote_lock_status.store(0);
            }
            page_cache->Release(handle);
            DEBUG_PRINT_CONDITION("retry place 8\n");
            return false;// result in fall back search on the higher level.
        }
        // Clear the retry counter, in case that there is a sibling call.

        assert(k >= page->hdr.lowest);
        assert(k < page->hdr.highest);
// TODO: Check whether the key is larger than the largest key of this node.
//  if yes, update the header.
        int cnt = 0;
        int empty_index = -1;
        char *update_addr = nullptr;
        // It is problematic to just check whether the value is empty, because it is possible
        // that the buffer is not initialized as 0
#ifdef PROCESSANALYSIS
        start = std::chrono::high_resolution_clock::now();
#endif
        // TODO: make the key-value stored with order, do not use this unordered page structure.
        //  Or use the key to check whether this holder is empty.
        page->front_version++;
        for (int i = 0; i < kLeafCardinality; ++i) {

            auto &r = page->records[i];
            if (r.value != kValueNull) {
                cnt++;
                if (r.key == k) {
                    r.value = v;
                    // ADD MORE weight for write.
//        memcpy(r.value_padding, padding, VALUE_PADDING);

//                    r.f_version++;
//                    r.r_version = r.f_version;
                    update_addr = (char *)&r;
                    break;
                }
            } else if (empty_index == -1) {
                empty_index = i;
            }
        }

        assert(cnt != kLeafCardinality);

        if (update_addr == nullptr) { // insert new item
            if (empty_index == -1) {
                printf("%d cnt\n", cnt);
                assert(false);
            }

            auto &r = page->records[empty_index];
            r.key = k;
            r.value = v;
//    memcpy(r.value_padding, padding, VALUE_PADDING);
//            r.f_version++;
//            r.r_version = r.f_version;

            update_addr = (char *)&r;

            cnt++;
        }

        bool need_split = cnt == kLeafCardinality;
        if (!need_split) {
            assert(update_addr);
            ibv_mr target_mr = *local_mr;
            int offset = (update_addr - (char *) page);
            LADD(target_mr.addr, offset);
            if (handle->strategy == 2){

                // If the page currently is in busy mode, we directly release the global lock.
                global_write_tuple_and_Wunlock(
                        &target_mr, GADD(page_addr, offset),
                        sizeof(LeafEntry), lock_addr, cxt, coro_id, handle, false);
//                handle->remote_lock_status.store(0);

            }
            page_cache->Release(handle);
//            write_page_and_unlock(
//                    &target_mr, GADD(page_addr, offset),
//                    sizeof(LeafEntry), lock_addr, cxt, coro_id, false);
//            assert(handle->refs.load() == 1);
            return true;
        } else {
            std::sort(
                    page->records, page->records + kLeafCardinality,
                    [](const LeafEntry &a, const LeafEntry &b) { return a.key < b.key; });
        }


//  Key split_key;
//  GlobalAddress sibling_addr;
        if (need_split) { // need split
//      printf("Node split\n");
            sibling_addr = rdma_mg->Allocate_Remote_RDMA_Slot(Internal_and_Leaf, 2 * round_robin_cur + 1);
            if(++round_robin_cur == rdma_mg->memory_nodes.size()){
                round_robin_cur = 0;
            }
            //TODO: use a thread local sibling memory region to reduce the allocator contention.
            ibv_mr* sibling_mr = new ibv_mr{};
//      printf("Allocate slot for page 3 %p\n", sibling_addr);
            rdma_mg->Allocate_Local_RDMA_Slot(*sibling_mr, Internal_and_Leaf);
//      memset(sibling_mr->addr, 0, kLeafPageSize);
            auto sibling = new(sibling_mr->addr) LeafPage(sibling_addr, page->hdr.level);
            //TODO: add the sibling to the local cache.
            // std::cout << "addr " <<  sibling_addr << " | level " <<
            // (int)(page->hdr.level) << std::endl;
            sibling->front_version ++;
            int m = cnt / 2;
            split_key = page->records[m].key;
            assert(split_key > page->hdr.lowest);
            assert(split_key < page->hdr.highest);

            for (int i = m; i < cnt; ++i) { // move
                sibling->records[i - m].key = page->records[i].key;
                sibling->records[i - m].value = page->records[i].value;
                page->records[i].key = 0;
                page->records[i].value = kValueNull;
            }
            //We don't care about the last index in the leaf nodes actually,
            // because we iterate all the slots to find an entry.
            page->hdr.last_index -= (cnt - m);
//      assert(page_addr == root || page->hdr.last_index == m-1);
            sibling->hdr.last_index += (cnt - m);
//      assert(sibling->hdr.last_index == cnt -m -1);
            sibling->hdr.lowest = split_key;
            sibling->hdr.highest = page->hdr.highest;
            page->hdr.highest = split_key;

            // link
            sibling->hdr.sibling_ptr = page->hdr.sibling_ptr;
            page->hdr.sibling_ptr = sibling_addr;
            sibling->rear_version =  sibling->front_version;
//    sibling->set_consistent();
            rdma_mg->RDMA_Write(sibling_addr, sibling_mr,kLeafPageSize, IBV_SEND_SIGNALED, 1, Internal_and_Leaf);
            rdma_mg->Deallocate_Local_RDMA_Slot(sibling_mr->addr, Internal_and_Leaf);
            delete sibling_mr;

        }else{
            sibling_addr = GlobalAddress::Null();
        }
        page->rear_version = page->front_version;
//  page->set_consistent();
        if (handle->strategy == 2){
            // unlock and write back the whole page because a page split has happend.
//            ibv_mr temp_mr = *local_mr;
//            GlobalAddress temp_page_add = page_addr;
//            temp_page_add.offset = page_addr.offset + RDMA_OFFSET;
//            temp_mr.addr = (char*)temp_mr.addr + RDMA_OFFSET;
//            temp_mr.length = temp_mr.length - RDMA_OFFSET;
//            assert(page->global_lock = 1);
//            assert(page->hdr.valid_page);
            // Use sync unlock.
            global_write_page_and_Wunlock(local_mr, page_addr, kLeafPageSize, lock_addr,
                                          cxt, coro_id, handle, false);
//            handle->remote_lock_status.store(0);
        }
        page_cache->Release(handle);
        //release the local lock.
        l.unlock();
//        write_page_and_unlock(local_mr, page_addr, kLeafPageSize,
//                              lock_addr, cxt, coro_id, false);

        if (sibling_addr != GlobalAddress::Null()){
            auto p = path_stack[coro_id][level+1];
            ibv_mr* page_hint = nullptr;
            //check whether the node split is for a root node.
            if (UNLIKELY(p == GlobalAddress::Null() )){
                // First acquire local lock
                std::unique_lock<std::mutex> l(root_mtx);
//                refetch_rootnode();
                // If you find the current root node does not have higher stack, and it is not a outdated root node,
                // the reason behind is that the inserted key is very small and the leaf node keep sibling shift to the right.
                // IN this case, the code will call "insert_internal"
                p = g_root_ptr.load();
                uint8_t height = tree_height;
                // Note path_stack is a global variable, be careful when debugging

                //If current store node is still the leaf (NO other thread create new root.), then we need to create a new page.
                if (path_stack[coro_id][level] == p && height == level){
                    //aquire the global lock to avoid mulitple node creating the new  root node
                    GlobalAddress lock_addr = {};
                    // root node lock addr. but this could result in a deadlock for transaction cc.
                    lock_addr.nodeID = 1;
                    lock_addr.offset = 0;
                    auto cas_buffer = rdma_mg->Get_local_CAS_mr();
                    //aquire the global lock
                    acquire_global_lock:
                    *(uint64_t*)cas_buffer->addr = 0;
                    rdma_mg->RDMA_CAS(lock_addr, cas_buffer, 0, 1, IBV_SEND_SIGNALED,1, LockTable);
                    if ((*(uint64_t*) cas_buffer->addr) != 0){
                        goto acquire_global_lock;
                    }
                    refetch_rootnode();
                    p = g_root_ptr.load();
                    height = tree_height;
                    if (path_stack[coro_id][level] == p && height == level) {
                        update_new_root(path_stack[coro_id][level], split_key, sibling_addr, level + 1,
                                        path_stack[coro_id][level], cxt, coro_id);
                        *(uint64_t *) cas_buffer->addr = 0;

                        rdma_mg->RDMA_Write(lock_addr, cas_buffer, sizeof(uint64_t), IBV_SEND_SIGNALED, 1, LockTable);

//                        page_cache->Release(handle);
                        return true;
                    }
//                l.unlock();
                    *(uint64_t *) cas_buffer->addr = 0;

                    rdma_mg->RDMA_Write(lock_addr, cas_buffer, sizeof(uint64_t), IBV_SEND_SIGNALED, 1, LockTable);

                }
                l.unlock();

                {
                    //find the upper level
                    //TODO: shall I implement a function that search a ptr at particular level.
                    printf(" rare case the tranverse during the root update\n");
                    // It is impossinle to insert the internal in level 0.
                    return insert_internal(split_key, sibling_addr,  cxt, coro_id, level+1);
                }


            }
            assert(p != GlobalAddress::Null());
            // if not a root split go ahead and insert in the upper level.
            level = level +1;
            //*****************Now it is not a root update, insert to the upper level******************
            SearchResult result{};
            memset(&result, 0, sizeof(SearchResult));
            int fall_back_level = 0;
            re_insert:

            if (UNLIKELY(!internal_page_store(p, split_key, sibling_addr, level, cxt, coro_id))){
                //this path should be a rare case.

                // fall back to upper level in the cache to search for the right node at this level
                fall_back_level = level + 1;

                p = path_stack[coro_id][fall_back_level];
                if ( p == GlobalAddress::Null()){
                    // insert it top-down. this function will keep searching until it is found
                    insert_internal(split_key,sibling_addr, cxt, coro_id, level);
                }else{
                    //fall back one step.
                    if(!internal_page_search(p, k, result, fall_back_level, false, nullptr, cxt, coro_id)){
                        // if the upper level is still a stale node, just insert the node by top down method.
                        insert_internal(split_key,sibling_addr, cxt, coro_id, level);
//                        level = level + 1; // move to upper level
//                        p = path_stack[coro_id][level];// move the pointer to upper level
                    }else{

                        if (result.next_level != GlobalAddress::Null()){
                            // the page was found successful by one step back, then we can set the p as new node.
                            // do not need to chanve level.
                            p = result.next_level;
                            goto re_insert;
//                    level = result.level - 1;
                        }else{
                            assert(false);
                        }
                    }

                }

            }
        }


        return true;
    }
#else
    bool Btr::leaf_page_store(GlobalAddress page_addr, const Key &k, const Value &v, Key &split_key, GlobalAddress &sibling_addr,
                     GlobalAddress root, int level, CoroContext *cxt, int coro_id) {


        assert(level == 0);
        uint64_t lock_index =
      CityHash64((char *)&page_addr, sizeof(page_addr)) % define::kNumOfLock;

  GlobalAddress lock_addr;


//    char padding[VALUE_PADDING];
#ifdef CONFIG_ENABLE_EMBEDDING_LOCK
  lock_addr = page_addr;
#else
  lock_addr.nodeID = page_addr.nodeID;
  lock_addr.offset = lock_index * sizeof(uint64_t);
#endif

  auto localbuf = rdma_mg->Get_local_read_mr();

  ibv_mr* cas_mr = rdma_mg->Get_local_CAS_mr();
  auto page_buffer = localbuf->addr;
  bool insert_success;
//  auto tag = rdma_mg->getThreadTag();
//  assert(tag != 0);
#ifdef PROCESSANALYSIS
    auto start = std::chrono::high_resolution_clock::now();
#endif
  lock_and_read_page(localbuf, page_addr, kLeafPageSize, cas_mr,
                     lock_addr, 1, cxt, coro_id);
#ifdef PROCESSANALYSIS
      if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
          auto stop = std::chrono::high_resolution_clock::now();
          auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
          printf("leaf page store fetch RDMA uses (%ld) ns\n", duration.count());
//          TimePrintCounter[RDMA_Manager::thread_id] = 0;
      }else{
//          TimePrintCounter[RDMA_Manager::thread_id]++;
      }
#endif
  // TODO: under some situation the lock is not released

    auto page = (LeafPage *)page_buffer;

    assert(page->hdr.level == level);
    assert(page->check_consistent());
    path_stack[coro_id][page->hdr.level] = page_addr;
    // It is possible that the key is larger than the highest key
    // The range of a page is [lowest,largest).
    // TODO: find out why sometimes the node is far from the target node that it need multiple times of
    //  sibling access.
    //
    //  Note that it is normal to see that the local buffer are always the same accross the nested
    //  funciton call, because they are sharing the same local buffer.
    if (k >= page->hdr.highest) {
        if (page->hdr.sibling_ptr != GlobalAddress::Null()){
            this->unlock_addr(lock_addr, cxt, coro_id, false);
            if (path_stack[coro_id][level+1]!= GlobalAddress::Null()){
//                DEBUG_arg("Erase the page 5 %p\n", path_stack[coro_id][1]);
                Slice upper_node_page_id((char*)&path_stack[coro_id][level+1], sizeof(GlobalAddress));
                // TODO: By passing the cache access to same the cost for page invalidation, use the handle within
                //  the page to check whether the page has been evicted.
                Cache::Handle* upper_layer_handle = page_cache->Lookup(upper_node_page_id);
                if(upper_layer_handle){
                    InternalPage* upper_page = (InternalPage*)((ibv_mr*)upper_layer_handle->value)->addr;
                    Recieve_page_invalidation(upper_page);
                    page_cache->Release(upper_layer_handle);
                }

//                page_cache->Erase(Slice((char*)&path_stack[coro_id][1], sizeof(GlobalAddress)));
            }
//            this->unlock_addr(lock_addr, cxt, coro_id, true);
            if (nested_retry_counter <= 2){

                nested_retry_counter++;
                return this->leaf_page_store(page->hdr.sibling_ptr, k, v, split_key, sibling_addr, root, level, cxt, coro_id);
            }else{

//                assert(false);
                DEBUG_PRINT_CONDITION("retry place 7");
                nested_retry_counter = 0;
                return false;
            }
        }else{
            // impossible because the right most leaf node 's max is KeyMax
            assert(false);
        }

    }
    nested_retry_counter = 0;
    if (k < page->hdr.lowest ) {
        // if key is smaller than the lower bound, the insert has to be restart from the
        // upper level. because the sibling pointer only points to larger one.

        if (path_stack[coro_id][level+1]!= GlobalAddress::Null()){
//            DEBUG_arg("Erase the page 6 %p\n", path_stack[coro_id][1]);
            Slice upper_node_page_id((char*)&path_stack[coro_id][level+1], sizeof(GlobalAddress));
            // TODO: By passing the cache access to same the cost for page invalidation, use the handle within
            //  the page to check whether the page has been evicted.
            Cache::Handle* upper_layer_handle = page_cache->Lookup(upper_node_page_id);
            if(upper_layer_handle){
                InternalPage* upper_page = (InternalPage*)((ibv_mr*)upper_layer_handle->value)->addr;
                Recieve_page_invalidation(upper_page);
                page_cache->Release(upper_layer_handle);
            }

//            page_cache->Erase(Slice((char*)&path_stack[coro_id][1], sizeof(GlobalAddress)));
        }
        this->unlock_addr(lock_addr, cxt, coro_id, false);

        insert_success = false;
        DEBUG_PRINT_CONDITION("retry place 8\n");
        return insert_success;// result in fall back search on the higher level.
    }
    // Clear the retry counter, in case that there is a sibling call.

  assert(k >= page->hdr.lowest);
    assert(k < page->hdr.highest);
// TODO: Check whether the key is larger than the largest key of this node.
//  if yes, update the header.
  int cnt = 0;
  int empty_index = -1;
  char *update_addr = nullptr;
    // It is problematic to just check whether the value is empty, because it is possible
    // that the buffer is not initialized as 0
#ifdef PROCESSANALYSIS
    start = std::chrono::high_resolution_clock::now();
#endif
    // TODO: make the key-value stored with order, do not use this unordered page structure.
    //  Or use the key to check whether this holder is empty.
    page->front_version++;
  for (int i = 0; i < kLeafCardinality; ++i) {

    auto &r = page->records[i];
    if (r.value != kValueNull) {
      cnt++;
      if (r.key == k) {
        r.value = v;
        // ADD MORE weight for write.
//        memcpy(r.value_padding, padding, VALUE_PADDING);

        r.f_version++;
        r.r_version = r.f_version;
        update_addr = (char *)&r;
        break;
      }
    } else if (empty_index == -1) {
      empty_index = i;
    }
  }

  assert(cnt != kLeafCardinality);

  if (update_addr == nullptr) { // insert new item
    if (empty_index == -1) {
      printf("%d cnt\n", cnt);
      assert(false);
    }

    auto &r = page->records[empty_index];
    r.key = k;
    r.value = v;
//    memcpy(r.value_padding, padding, VALUE_PADDING);
    r.f_version++;
    r.r_version = r.f_version;

    update_addr = (char *)&r;

    cnt++;
  }
#ifdef PROCESSANALYSIS
    if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//#ifndef NDEBUG
        printf("leaf page search and update uses (%ld) ns\n", duration.count());
//        TimePrintCounter[RDMA_Manager::thread_id] = 0;
    }else{
//        TimePrintCounter[RDMA_Manager::thread_id]++;
    }
//#endif
#endif
  bool need_split = cnt == kLeafCardinality;
  if (!need_split) {
    assert(update_addr);
    ibv_mr target_mr = *localbuf;
    int offset = (update_addr - (char *) page);
      LADD(target_mr.addr, offset);
#ifdef PROCESSANALYSIS
      start = std::chrono::high_resolution_clock::now();
#endif
      write_page_and_unlock(
              &target_mr, GADD(page_addr, offset),
              sizeof(LeafEntry), lock_addr, cxt, coro_id, false);
#ifdef PROCESSANALYSIS
      if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
          auto stop = std::chrono::high_resolution_clock::now();
          auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
          printf("leaf pages write back uses (%ld) ns\n", duration.count());
//          TimePrintCounter[RDMA_Manager::thread_id] = 0;
      }else{
//          TimePrintCounter[RDMA_Manager::thread_id]++;
      }
#endif
    return true;
  } else {
    std::sort(
        page->records, page->records + kLeafCardinality,
        [](const LeafEntry &a, const LeafEntry &b) { return a.key < b.key; });
  }


//  Key split_key;
//  GlobalAddress sibling_addr;
  if (need_split) { // need split
//      printf("Node split\n");
    sibling_addr = rdma_mg->Allocate_Remote_RDMA_Slot(Internal_and_Leaf, 2 * round_robin_cur + 1);
      if(++round_robin_cur == rdma_mg->memory_nodes.size()){
          round_robin_cur = 0;
      }
    //TODO: use a thread local sibling memory region to reduce the allocator contention.
    ibv_mr* sibling_mr = new ibv_mr{};
//      printf("Allocate slot for page 3 %p\n", sibling_addr);
      rdma_mg->Allocate_Local_RDMA_Slot(*sibling_mr, Internal_and_Leaf);
//      memset(sibling_mr->addr, 0, kLeafPageSize);
    auto sibling = new(sibling_mr->addr) LeafPage(sibling_addr, page->hdr.level);
    //TODO: add the sibling to the local cache.
    // std::cout << "addr " <<  sibling_addr << " | level " <<
    // (int)(page->hdr.level) << std::endl;
      sibling->front_version ++;
      int m = cnt / 2;
      split_key = page->records[m].key;
      assert(split_key > page->hdr.lowest);
      assert(split_key < page->hdr.highest);

      for (int i = m; i < cnt; ++i) { // move
          sibling->records[i - m].key = page->records[i].key;
          sibling->records[i - m].value = page->records[i].value;
          page->records[i].key = 0;
          page->records[i].value = kValueNull;
      }
      //We don't care about the last index in the leaf nodes actually,
      // because we iterate all the slots to find an entry.
      page->hdr.last_index -= (cnt - m);
//      assert(page_addr == root || page->hdr.last_index == m-1);
      sibling->hdr.last_index += (cnt - m);
//      assert(sibling->hdr.last_index == cnt -m -1);
      sibling->hdr.lowest = split_key;
      sibling->hdr.highest = page->hdr.highest;
      page->hdr.highest = split_key;

      // link
      sibling->hdr.sibling_ptr = page->hdr.sibling_ptr;
      page->hdr.sibling_ptr = sibling_addr;
      sibling->rear_version =  sibling->front_version;
//    sibling->set_consistent();
    rdma_mg->RDMA_Write(sibling_addr, sibling_mr,kLeafPageSize, IBV_SEND_SIGNALED, 1, Internal_and_Leaf);
      rdma_mg->Deallocate_Local_RDMA_Slot(sibling_mr->addr, Internal_and_Leaf);
      delete sibling_mr;

  }else{
      sibling_addr = GlobalAddress::Null();
  }
    page->rear_version = page->front_version;
//  page->set_consistent();
#ifdef PROCESSANALYSIS
    start = std::chrono::high_resolution_clock::now();
#endif
    write_page_and_unlock(localbuf, page_addr, kLeafPageSize,
                          lock_addr, cxt, coro_id, false);
#ifdef PROCESSANALYSIS
    if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
        printf("leaf pages write back uses (%ld) ns\n", duration.count());
        TimePrintCounter[RDMA_Manager::thread_id] = 0;
    }else{
        TimePrintCounter[RDMA_Manager::thread_id]++;
    }
#endif
    if (sibling_addr != GlobalAddress::Null()){
        auto p = path_stack[coro_id][level+1];
        ibv_mr* page_hint = nullptr;
        //check whether the node split is for a root node.
        if (UNLIKELY(p == GlobalAddress::Null() )){
            // First acquire local lock
            std::unique_lock<std::mutex> l(mtx);
            p = g_root_ptr.load();
            uint8_t height = tree_height;
            //TODO: tHERE COULd be a bug in the get_root_ptr so that the CAS for update_new_root will be failed.

            if (path_stack[coro_id][level] == p && height == level){
                //Acquire global lock for the root update.
                GlobalAddress lock_addr = {};
                // root node lock addr. but this could result in a deadlock for transaction cc.
                lock_addr.nodeID = 1;
                lock_addr.offset = 0;
                auto cas_buffer = rdma_mg->Get_local_CAS_mr();
                //aquire the global lock
                acquire_global_lock:
                *(uint64_t*)cas_buffer->addr = 0;
                rdma_mg->RDMA_CAS(lock_addr, cas_buffer, 0, 1, IBV_SEND_SIGNALED,1, LockTable);
                if ((*(uint64_t*) cas_buffer->addr) != 0){
                    goto acquire_global_lock;
                }
                refetch_rootnode();
                p = g_root_ptr.load();
                if (path_stack[coro_id][level] == p && height == level) {
                    update_new_root(path_stack[coro_id][level], split_key, sibling_addr, level + 1,
                                    path_stack[coro_id][level], cxt, coro_id);
                    *(uint64_t *) cas_buffer->addr = 0;

                    rdma_mg->RDMA_Write(lock_addr, cas_buffer, sizeof(uint64_t), IBV_SEND_SIGNALED, 1, LockTable);

                    return true;
                }
//                l.unlock();
                *(uint64_t *) cas_buffer->addr = 0;

                rdma_mg->RDMA_Write(lock_addr, cas_buffer, sizeof(uint64_t), IBV_SEND_SIGNALED, 1, LockTable);

            }
            l.unlock();

            {
                //find the upper level
                //TODO: shall I implement a function that search a ptr at particular level.
                printf(" rare case the tranverse during the root update\n");

                return insert_internal(split_key, sibling_addr,  cxt, coro_id, level);
            }


        }
        assert(p != GlobalAddress::Null());
        // if not a root split go ahead and insert in the upper level.
        level = level +1;
        //*****************Now it is not a root update, insert to the upper level******************
        SearchResult result{};
        memset(&result, 0, sizeof(SearchResult));
        int fall_back_level = 0;
        re_insert:

        if (UNLIKELY(!internal_page_store(p, split_key, sibling_addr, level, cxt, coro_id))){
            //this path should be a rare case.

            // fall back to upper level in the cache to search for the right node at this level
            fall_back_level = level + 1;

            p = path_stack[coro_id][fall_back_level];
            if ( p == GlobalAddress::Null()){
                // insert it top-down. this function will keep searching until it is found
                insert_internal(split_key,sibling_addr, cxt, coro_id, level);
            }else{
                if(!internal_page_search(p, k, result, fall_back_level, false, nullptr, cxt, coro_id)){
                    // if the upper level is still a stale node, just insert the node by top down method.
                    insert_internal(split_key,sibling_addr, cxt, coro_id, level);
//                        level = level + 1; // move to upper level
//                        p = path_stack[coro_id][level];// move the pointer to upper level
                }else{

                    if (result.next_level != GlobalAddress::Null()){
                        // the page was found successful by one step back, then we can set the p as new node.
                        // do not need to chanve level.
                        p = result.next_level;
                        goto re_insert;
//                    level = result.level - 1;
                    }else{
                        assert(false);
                    }
                }

            }

        }
    }


  return true;
}
#endif


// Need BIG FIX
    bool Btr::leaf_page_del(GlobalAddress page_addr, const Key &k, int level,
                         CoroContext *cxt, int coro_id) {
    uint64_t lock_index =
    CityHash64((char *)&page_addr, sizeof(page_addr)) % define::kNumOfLock;

    GlobalAddress lock_addr;
    lock_addr.nodeID = page_addr.nodeID;
    lock_addr.offset = lock_index * sizeof(uint64_t);

    auto rbuf = rdma_mg->Get_local_read_mr();
    ibv_mr* cas_mr = rdma_mg->Get_local_CAS_mr();
    auto page_buffer = rbuf->addr;
    //        auto cas_buffer
    bool insert_success;

    auto tag = 1;
    try_lock_addr(lock_addr, tag, cas_mr, cxt, coro_id);

    //  auto page_buffer = rdma_mg->get_rbuf(coro_id).get_page_buffer();
    rdma_mg->RDMA_Read(page_addr, rbuf, kLeafPageSize, IBV_SEND_SIGNALED, 1, Internal_and_Leaf);
    auto page = (LeafPage *)page_buffer;

    assert(page->hdr.level == level);
//    assert(page->check_consistent());
    path_stack[coro_id][page->hdr.level] = page_addr;
    if (k >= page->hdr.highest) {
        this->unlock_addr(lock_addr, cxt, coro_id, false);

        assert(page->hdr.sibling_ptr != GlobalAddress::Null());

        return this->leaf_page_del(page->hdr.sibling_ptr, k, level, cxt, coro_id);
    }
    if (k < page->hdr.lowest) {
        this->unlock_addr(lock_addr, cxt, coro_id, false);

        assert(page->hdr.sibling_ptr != GlobalAddress::Null());
        DEBUG_PRINT_CONDITION("retry place 9\n");
        return false;
    }
        page->front_version++;
    auto cnt = page->hdr.last_index + 1;

    int del_index = -1;
    for (int i = 0; i < cnt; ++i) {
        if (page->records[i].key == k) { // find and update
          del_index = i;
          break;
        }
    }

    if (del_index != -1) { // remove and shift
        for (int i = del_index + 1; i < cnt; ++i) {
          page->records[i - 1].key = page->records[i].key;
          page->records[i - 1].value = page->records[i].value;
        }

        page->hdr.last_index--;
        page->rear_version = page->front_version;
//        page->set_consistent();
        rdma_mg->RDMA_Write(page_addr, rbuf, kLeafPageSize, IBV_SEND_SIGNALED, 1, Internal_and_Leaf);
    }
    this->unlock_addr(lock_addr, cxt, coro_id, false);
    return true;
  // TODO: Merge page after the node is too small.
}

void Btr::run_coroutine(CoroFunc func, int id, int coro_cnt) {

  using namespace std::placeholders;

  assert(coro_cnt <= define::kMaxCoro);
  for (int i = 0; i < coro_cnt; ++i) {
    auto gen = func(i, rdma_mg, id);
    worker[i] = CoroCall(std::bind(&Btr::coro_worker, this, _1, gen, i));
  }

//  master = CoroCall(std::bind(&Btr::coro_master, this, _1, coro_cnt));

  master();
}

void Btr::coro_worker(CoroYield &yield, RequstGen *gen, int coro_id) {
  CoroContext ctx;
  ctx.coro_id = coro_id;
  ctx.master = &master;
  ctx.yield = &yield;

  Timer coro_timer;
//  auto thread_id = rdma_mg->getMyThreadID();

  while (true) {

    auto r = gen->next();

    coro_timer.begin();
    if (r.is_search) {
      Value v;
      this->search(r.k, v, &ctx, coro_id);
    } else {
      this->insert(r.k, r.v, &ctx, coro_id);
    }
    auto us_10 = coro_timer.end() / 100;
    if (us_10 >= LATENCY_WINDOWS) {
      us_10 = LATENCY_WINDOWS - 1;
    }
//    latency[thread_id][us_10]++;
  }
}

//void Btr::coro_master(CoroYield &yield, int coro_cnt) {
//
//  for (int i = 0; i < coro_cnt; ++i) {
//    yield(worker[i]);
//  }
//
//  while (true) {
//
//    uint64_t next_coro_id;
//
//    if (rdma_mg->poll_rdma_cq_once(next_coro_id)) {
//      yield(worker[next_coro_id]);
//    }
//
//    if (!hot_wait_queue.empty()) {
//      next_coro_id = hot_wait_queue.front();
//      hot_wait_queue.pop();
//      yield(worker[next_coro_id]);
//    }
//
//    if (!deadline_queue.empty()) {
//      auto now = timer.get_time_ns();
//      auto task = deadline_queue.top();
//      if (now > task.deadline) {
//        deadline_queue.pop();
//        yield(worker[task.coro_id]);
//      }
//    }
//  }
//}

// Local Locks
/**
 * THere is a potenttial bug if the lock is overflow and one thread will wrongly acquire the lock. need to develop a new
 * way for local lock
 * @param lock_addr
 * @param cxt
 * @param coro_id
 * @return
 */
inline bool Btr::acquire_local_lock(GlobalAddress lock_addr, CoroContext *cxt,
                                     int coro_id) {
  auto &node = local_locks[(lock_addr.nodeID -1)/2][lock_addr.offset / 8];
  bool is_local_locked = false;

  uint64_t lock_val = node.ticket_lock.fetch_add(1);
  //TOTHINK(potential bug): what if the ticket out of buffer.

  uint32_t ticket = lock_val << 32 >> 32;//clear the former 32 bit
  uint32_t current = lock_val >> 32;// current is the former 32 bit in ticket lock
//        assert(ticket - current <=4);
//    printf("lock offest %lu's ticket %x current %x, thread %u\n", lock_addr.offset, ticket, current, thread_id);
//   printf("", );
        assert((lock_val +1) <<32 != 0);
  while (ticket != current) { // lock failed
    is_local_locked = true;

//    if (cxt != nullptr) {
//      hot_wait_queue.push(coro_id);
//      (*cxt->yield)(*cxt->master);
//    }

    current = node.ticket_lock.load(std::memory_order_seq_cst) >> 32;
  }

  if (is_local_locked) {
//    hierarchy_lock[rdma_mg->getMyThreadID()][0]++;
  }

  node.hand_time++;

  return node.hand_over;
}
//    = __atomic_load_n((uint64_t*)&page->local_lock_meta, (int)std::memory_order_seq_cst);
    inline bool Btr::try_lock(Local_Meta *local_lock_meta) {
        auto currently_locked = __atomic_load_n(&local_lock_meta->local_lock_byte, __ATOMIC_SEQ_CST);
//        uint8_t currently_locked = 0;
        return !currently_locked &&
                __atomic_compare_exchange_n(&local_lock_meta->local_lock_byte, &currently_locked, 1, true, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
    }
    inline void Btr::unlock_lock(Local_Meta *local_lock_meta) {
        __atomic_store_n(&local_lock_meta->local_lock_byte, 0, mem_cst_seq);
    }

bool Btr::acquire_local_optimistic_lock(Local_Meta *local_lock_meta, CoroContext *cxt, int coro_id) {
    assert((uint64_t)local_lock_meta % 8 == 0);
    //TODO: local lock implementation over InternalPage::Local_Meta.


        __atomic_fetch_add(&local_lock_meta->issued_ticket, 1, mem_cst_seq);

//    assert(local_lock_meta->issued_ticket - local_lock_meta->current_ticket <= 16 );
    //TOTHINK(potential bug): what if the ticket out of buffer.
    uint8_t expected = 0;
#ifndef NDEBUG
    uint64_t spin_counter = 0;
    uint64_t global_static_var;

#endif
//    uint8_t lock_status =
    size_t tries = 0;
    while(1){

        if(try_lock(local_lock_meta)){
            break;
        }
        assert(local_lock_meta);
        port::AsmVolatilePause();
        if (tries++ > 100) {
            //        printf("I tried so many time I got yield\n");
            std::this_thread::yield();
        }
    }
#ifndef NDEBUG
    global_static_var = __atomic_load_n((uint64_t*)local_lock_meta, (int)std::memory_order_seq_cst);
    if(((Local_Meta*)&global_static_var)->issued_ticket - ((Local_Meta*)&global_static_var)->current_ticket == 2){
//        printf("mark here");
    }
//    printf("Acquire lock for %p, the current ticks is %d, issued ticket is%d, spin %lu times, thread %d\n", local_lock_meta,
//           ((Local_Meta*)&global_static_var)->current_ticket, ((Local_Meta*)&global_static_var)->issued_ticket, spin_counter, rdma_mg->thread_id);
#endif
    //    uint32_t ticket = lock_val << 32 >> 32;//clear the former 32 bit
//    uint8_t current = __atomic_load_n(&local_lock_addr->current_ticket, mem_cst_seq);// current is the former 32 bit in ticket lock
//    uint8_t current = local_lock_meta->current_ticket;
//        assert(ticket - current <=4);
//    printf("lock offest %lu's ticket %x current %x, thread %u\n", lock_addr.offset, ticket, current, thread_id);
//   printf("", );
//    if (current%define::kMaxHandOverTime == 0){
//        return false;
//    }else{
//        return true;
//    }
//    if (local_lock_meta->handover_times < define::kMaxHandOverTime &&)
    assert(local_lock_meta->local_lock_byte == 1);
    return local_lock_meta->hand_over;

}
inline bool Btr::can_hand_over(GlobalAddress lock_addr) {

  auto &node = local_locks[(lock_addr.nodeID-1)/2][lock_addr.offset / 8];
  uint64_t lock_val = node.ticket_lock.load(std::memory_order_relaxed);
// only when unlocking, it need to check whether it can handover to the next, so that it do not need to UNLOCK the global lock.
// It is possible that the handover is set as false but this server is still holding the lock.
  uint32_t ticket = lock_val << 32 >> 32;//
  uint32_t current = lock_val >> 32;
// if the handover in node is true, then the other thread can get the lock without any RDMAcas
// if the handover in node is false, then the other thread will acquire the lock from by RDMA cas AGAIN
  if (ticket <= current + 1) { // no pending locks
    node.hand_over = false;// if no pending thread, then it will release the remote lock and next aquir need RDMA CAS again
  } else {
    node.hand_over = node.hand_time < define::kMaxHandOverTime; // check the limit
  }
  if (!node.hand_over) {
    node.hand_time = 0;// clear the handtime.
  } else {
//    handover_count[rdma_mg->getMyThreadID()][0]++;
  }

  return node.hand_over;
}
    //Call before release the global lock
    inline bool Btr::can_hand_over(Local_Meta * local_lock_meta) {

    uint8_t issued_ticket = __atomic_load_n(&local_lock_meta->current_ticket, mem_cst_seq);
    uint8_t current_ticket = local_lock_meta->current_ticket;// current is the former 32 bit in ticket lock

// if the handover in node is true, then the other thread can get the lock without any RDMAcas
// if the handover in node is false, then the other thread will acquire the lock from by RDMA cas AGAIN
        if (issued_ticket <= current_ticket + 1) { // no pending locks
            local_lock_meta->hand_over = false;// if no pending thread, then it will release the remote lock and next aquir need RDMA CAS again
        } else {
            local_lock_meta->hand_over = local_lock_meta->hand_time < define::kMaxHandOverTime; // check the limit
        }
        if (!local_lock_meta->hand_over) {
            local_lock_meta->hand_time = 0;// clear the handtime.
        } else {
            local_lock_meta->hand_time++;

//    handover_count[rdma_mg->getMyThreadID()][0]++;
        }

        return local_lock_meta->hand_over;
    }
inline void Btr::releases_local_lock(GlobalAddress lock_addr) {

  auto &node = local_locks[(lock_addr.nodeID-1)/2][lock_addr.offset / 8];

  node.ticket_lock.fetch_add((1ull << 32));
}
inline void Btr::releases_local_optimistic_lock(Local_Meta * local_lock_meta) {

//        auto &node = local_locks[(lock_addr.nodeID-1)/2][lock_addr.offset / 8];

    local_lock_meta->current_ticket++;
    assert(local_lock_meta->local_lock_byte == 1);
//    assert((uint64_t)&local_lock_meta->local_lock_byte % 8 == 0);
    unlock_lock(local_lock_meta);
//        node.ticket_lock.fetch_add((1ull << 32));
}
void Btr::make_page_invalidated(InternalPage *upper_page) {
    //TODO invalidate page with version, may be reuse the current and issue version?
    uint8_t expected = 0;
    if(try_lock(&upper_page->local_lock_meta)){
        // if the local CAS succeed, then we set the invalidation, if not we just ignore that because,
        // either another thread is writing (so a new read is coming) or other thread has detect the invalidation and
        // already set it.
        assert(expected == 0);
        __atomic_fetch_add(&upper_page->local_lock_meta.issued_ticket,1, mem_cst_seq);
        if (upper_page->hdr.valid_page){
            upper_page->hdr.valid_page = false;
//            printf("Page invalidation %p\n", upper_page);
        }

        // keep the operation on the version.
        upper_page->local_lock_meta.current_ticket++;
        unlock_lock(&upper_page->local_lock_meta);
    }
}
    void Btr::Initialize_page_invalidation(InternalPage *upper_page) {
        // TODO: cache invalidation RPC.

    }

//void Btr::index_cache_statistics() {
//  page_cache->statistics();
//  page_cache->bench();
//}

void Btr::clear_statistics() {
  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    cache_hit[i][0] = 0;
    cache_miss[i][0] = 0;
  }
}




}