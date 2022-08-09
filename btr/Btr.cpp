#include "Btr.h"

//#include "IndexCache.h"

//#include "RdmaBuffer.h"
//#include "Timer.h"
#include <algorithm>
#include <city.h>
#include <iostream>
#include <queue>
#include <utility>
#include <vector>
namespace DSMEngine {
bool enter_debug = false;

//HotBuffer hot_buf;
uint64_t cache_miss[MAX_APP_THREAD][8];
uint64_t cache_hit[MAX_APP_THREAD][8];
uint64_t invalid_counter[MAX_APP_THREAD][8];
uint64_t lock_fail[MAX_APP_THREAD][8];
uint64_t pattern[MAX_APP_THREAD][8];
uint64_t hierarchy_lock[MAX_APP_THREAD][8];
uint64_t handover_count[MAX_APP_THREAD][8];
uint64_t hot_filter_count[MAX_APP_THREAD][8];
uint64_t latency[MAX_APP_THREAD][LATENCY_WINDOWS];
volatile bool need_stop = false;
thread_local int Btr::round_robin_cur = 0;
thread_local CoroCall Btr::worker[define::kMaxCoro];
thread_local CoroCall Btr::master;
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

//thread_local Timer timer;
thread_local std::queue<uint16_t> hot_wait_queue;
thread_local std::priority_queue<CoroDeadline> deadline_queue;

Btr::Btr(RDMA_Manager *mg, uint16_t Btr_id) : tree_id(Btr_id){
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

  print_verbose();

    page_cache = NewLRUCache(define::kIndexCacheSize);

//  root_ptr_ptr = get_root_ptr_ptr();

  // try to init tree and install root pointer
    rdma_mg->Allocate_Local_RDMA_Slot(cached_root_page_mr, Default);// local allocate
    if (rdma_mg->node_id == 1){
        // only the first compute node create the root node for index
        g_root_ptr = rdma_mg->Allocate_Remote_RDMA_Slot(Default, (round_robin_cur++)%rdma_mg->memory_nodes.size()); // remote allocation.
        auto root_page = new (cached_root_page_mr.addr) LeafPage;

        root_page->set_consistent();
        rdma_mg->RDMA_Write(g_root_ptr, &cached_root_page_mr, kLeafPageSize, 1, 1, FlushBuffer, std::string());
        // TODO: create a special region to store the root_ptr for every tree id.
        auto local_mr = rdma_mg->Get_local_CAS_mr(); // remote allocation.
        ibv_mr remote_mr{};
        remote_mr = *rdma_mg->global_index_table;
        // find the table enty according to the id
        remote_mr.addr = (void*) ((char*)remote_mr.addr + 8*tree_id);
        rdma_mg->RDMA_CAS(&remote_mr, local_mr, 0, g_root_ptr, 1, 1, 0);
    }else{
        get_root_ptr();
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
    std::cout << "Internal Page size: " << sizeof(InternalPage) << " ["
              << kInternalPageSize << "]" << std::endl;
    std::cout << "Internal per Page: " << kInternalCardinality << std::endl;
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

extern GlobalAddress g_root_ptr;
extern int g_root_level;
extern bool enable_cache;
GlobalAddress Btr::get_root_ptr() {
// TODO: Use an RPC to get the root pointer from the first node.
  if (g_root_ptr == GlobalAddress::Null()) {
      std::unique_lock<std::mutex> l(mtx);
      if (g_root_ptr == GlobalAddress::Null()) {
          ibv_mr* local_mr = rdma_mg->Get_local_CAS_mr();

          ibv_mr remote_mr{};
          remote_mr = *rdma_mg->global_index_table;
          // find the table enty according to the id
          remote_mr.addr = (void*) ((char*)remote_mr.addr + 8*tree_id);
          *(GlobalAddress*)(local_mr->addr) = GlobalAddress::Null();
          // The first compute node may not have written the root ptr to root_ptr_ptr, we need to keep polling.
          while (*(GlobalAddress*)(local_mr->addr) == GlobalAddress::Null()) {
              rdma_mg->RDMA_Read(&remote_mr, local_mr, sizeof(GlobalAddress), 1, 1, 0);
          }
          g_root_ptr = *(GlobalAddress*)local_mr->addr;
          std::cout << "Get new root" << g_root_ptr <<std::endl;

      }

    return g_root_ptr;
  } else {
    return g_root_ptr;
  }

  // std::cout << "root ptr " << root_ptr << std::endl;
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

  if (root_level >= 5) {
        enable_cache = true;
  }
  //TODO: When we seperate the compute from the memory, how can we broad cast the new root
  // or can we wait until the compute node detect an inconsistent.

    rdma_mg->post_send<RDMA_Request>(&send_mr, 0, std::string("main"));
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
                        true, 0)){
//    assert(try_poll_completions(wc, 1, std::string("main"),true) == 0);
        fprintf(stderr, "failed to poll send for remote memory register\n");
    }
}

bool Btr::update_new_root(GlobalAddress left, const Key &k,
                           GlobalAddress right, int level,
                           GlobalAddress old_root, CoroContext *cxt,
                           int coro_id) {

  auto page_buffer = rdma_mg->Get_local_read_mr();
  auto cas_buffer = rdma_mg->Get_local_CAS_mr();
    assert(left != GlobalAddress::Null());
    assert(right != GlobalAddress::Null());
  auto new_root = new (page_buffer->addr) InternalPage(left, k, right, level);

  auto new_root_addr = rdma_mg->Allocate_Remote_RDMA_Slot(Default, (round_robin_cur++)%rdma_mg->memory_nodes.size());
  // The code below is just for debugging
//    new_root_addr.mark = 3;
  new_root->set_consistent();
  // set local cache for root address
  g_root_ptr = new_root_addr;
    tree_height = level;
  rdma_mg->RDMA_Write(new_root_addr, page_buffer, kInternalPageSize,1,1, Default);
  if (rdma_mg->RDMA_CAS(&cached_root_page_mr, cas_buffer, old_root, new_root_addr, 1,1,Default)) {
    broadcast_new_root(new_root_addr, level);
    std::cout << "new root level " << level << " " << new_root_addr
              << std::endl;
    return true;
  } else {
    std::cout << "cas root fail " << std::endl;
  }

  return false;
}

void Btr::print_and_check_tree(CoroContext *cxt, int coro_id) {
//  assert(rdma_mg->is_register());

  auto root = get_root_ptr();
  // SearchResult result;

  GlobalAddress p = root;
  GlobalAddress levels[define::kMaxLevelOfTree];
  int level_cnt = 0;
  auto page_buffer = rdma_mg->Get_local_read_mr();
  GlobalAddress leaf_head;

next_level:

  rdma_mg->RDMA_Read(p, page_buffer, kInternalPageSize, 1, 1, Default);
  auto header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
  levels[level_cnt++] = p;
  if (header->level != 0) {
    p = header->leftmost_ptr;
    goto next_level;
  } else {
    leaf_head = p;
  }

next:
    rdma_mg->RDMA_Read(leaf_head, page_buffer, kLeafPageSize, 1, 1, Default);
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
    rdma_mg->RDMA_CAS(lock_addr, buf, 0, tag, 1,1, Default);
    if ((*(uint64_t*) buf->addr) == 0){
        conflict_tag = *(uint64_t*)buf->addr;
        if (conflict_tag != pre_tag) {
            retry_cnt = 0;
            pre_tag = conflict_tag;
        }
//      lock_fail[rdma_mg->getMyThreadID()][0]++;
        goto retry;
    }

  }

  return true;
}

inline void Btr::unlock_addr(GlobalAddress lock_addr, uint64_t tag,
                              uint64_t *buf, CoroContext *cxt, int coro_id,
                              bool async) {

  bool hand_over_other = can_hand_over(lock_addr);
  if (hand_over_other) {
    releases_local_lock(lock_addr);
    return;
  }

  auto cas_buf = rdma_mg->Get_local_CAS_mr();
//    std::cout << "unlock " << lock_addr << std::endl;
  *(uint64_t*)cas_buf->addr = 0;
  if (async) {
    rdma_mg->RDMA_Write(lock_addr, cas_buf,  sizeof(uint64_t), 0,0,LockTable);
  } else {
      rdma_mg->RDMA_Write(lock_addr, cas_buf,  sizeof(uint64_t), 1,1,LockTable);
  }

  releases_local_lock(lock_addr);
}

void Btr::write_page_and_unlock(ibv_mr *page_buffer, GlobalAddress page_addr,
                                int page_size, uint64_t *cas_buffer,
                                GlobalAddress lock_addr, uint64_t tag,
                                CoroContext *cxt, int coro_id, bool async) {

  bool hand_over_other = can_hand_over(lock_addr);
  if (hand_over_other) {
    rdma_mg->RDMA_Write(page_addr, page_buffer, page_size, 0,0,Default);
    releases_local_lock(lock_addr);
    return;
  }
    struct ibv_send_wr sr[2];
    struct ibv_sge sge[2];
    if (async){

        rdma_mg->Prepare_WR_Write(sr[0], sge[0], page_addr, page_buffer, page_size, 0, Default);
        ibv_mr* local_mr = rdma_mg->Get_local_CAS_mr();

        rdma_mg->Prepare_WR_Write(sr[1], sge[1], lock_addr, local_mr, sizeof(uint64_t), 0, Default);
        sr[0].next = &sr[1];


        *(uint64_t *)local_mr->addr = 0;
        assert(page_addr.nodeID == lock_addr.nodeID);
        rdma_mg->Batch_Submit_WRs(sr, 0, page_addr.nodeID);
    }else{
        rdma_mg->Prepare_WR_Write(sr[0], sge[0], page_addr, page_buffer, page_size, 1, Default);
        ibv_mr* local_mr = rdma_mg->Get_local_CAS_mr();

        rdma_mg->Prepare_WR_Write(sr[1], sge[1], lock_addr, local_mr, sizeof(uint64_t), 1, Default);
        sr[0].next = &sr[1];


        *(uint64_t *)local_mr->addr = 0;
        assert(page_addr.nodeID == lock_addr.nodeID);
        rdma_mg->Batch_Submit_WRs(sr, 2, page_addr.nodeID);
    }

//  if (async) {
//    rdma_mg->write_batch(rs, 2, false);
//  } else {
//    rdma_mg->write_batch_sync(rs, 2, cxt);
//  }

  releases_local_lock(lock_addr);
}

void Btr::lock_and_read_page(ibv_mr *page_buffer, GlobalAddress page_addr,
                             int page_size, ibv_mr *cas_buffer,
                             GlobalAddress lock_addr, uint64_t tag,
                             CoroContext *cxt, int coro_id) {
    // Can put lock and page read in a door bell batch.
    bool hand_over = acquire_local_lock(lock_addr, cxt, coro_id);
    if (hand_over) {
        rdma_mg->RDMA_Read(page_addr, page_buffer, page_size, 1,1, Default);
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
        struct ibv_send_wr sr[2];
        struct ibv_sge sge[2];

        rdma_mg->Prepare_WR_CAS(sr[0], sge[0], lock_addr, cas_buffer, 0, tag, 1, Default);
        rdma_mg->Prepare_WR_Write(sr[1], sge[1], page_addr, page_buffer, page_size, 1, Default);
        *(uint64_t *)cas_buffer->addr = 0;
        assert(page_addr.nodeID == lock_addr.nodeID);
        rdma_mg->Batch_Submit_WRs(sr, 2, page_addr.nodeID);
        if ((*(uint64_t*) cas_buffer->addr) == 0){
            conflict_tag = *(uint64_t*)cas_buffer->addr;
            if (conflict_tag != pre_tag) {
                retry_cnt = 0;
                pre_tag = conflict_tag;
            }
//      lock_fail[rdma_mg->getMyThreadID()][0]++;
            goto retry;
        }

    }

//  rdma_mg->read_sync(page_buffer, page_addr, page_size, cxt);
//  pattern[rdma_mg->getMyThreadID()][page_addr.nodeID]++;
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
// You need to make sure it is not the root level
void Btr::insert_internal(const Key &k, GlobalAddress v, CoroContext *cxt,
                           int coro_id, int level) {

    //TODO: You need to acquire a lock when you write a page
  auto root = get_root_ptr();
  SearchResult result;

  GlobalAddress p = root;
    //TODO: ADD support for root invalidate and update.
next:

  if (!page_search(p, k, result, cxt, coro_id, false)) {
    std::cout << "SEARCH WARNING insert" << std::endl;
    p = get_root_ptr();
    sleep(1);
    goto next;
  }

  assert(result.level != 0);
  if (result.slibing != GlobalAddress::Null()) {
    p = result.slibing;
    goto next;
  }

  p = result.next_level;
  if (result.level != level + 1) {
    goto next;
  }

  internal_page_store(p, k, v, root, level, cxt, coro_id);
}

void Btr::insert(const Key &k, const Value &v, CoroContext *cxt, int coro_id) {
//  assert(rdma_mg->is_register());

  before_operation(cxt, coro_id);



  auto root = get_root_ptr();
//  std::cout << "The root now is " << root << std::endl;
  SearchResult result;
  GlobalAddress p = root;
  // this is root is to help the tree to refresh the root node because the
  // new root broadcast is not usable if physical disaggregated.
  bool isroot = true;
//The page_search will be executed mulitple times if the result is not is_leaf
next:

  if (!page_search(p, k, result, cxt, coro_id, isroot)) {

    std::cout << "SEARCH WARNING insert" << std::endl;
    p = get_root_ptr();
    sleep(1);
    goto next;
  }
  isroot = false;
//The page_search will be executed mulitple times if the result is not is_leaf
// Maybe it will goes to the sibling pointer or go to the children
  if (!result.is_leaf) {
    assert(result.level != 0);
    if (result.slibing != GlobalAddress::Null()) {
      p = result.slibing;
      goto next;
    }

    p = result.next_level;
//    printf("next level pointer is %p\n", p);
    if (result.level != 1) {
      goto next;
    }
  }

  leaf_page_store(p, k, v, root, 0, cxt, coro_id);

//  if (res == HotResult::SUCC) {
//    hot_buf.clear(k);
//  }
}

bool Btr::search(const Key &k, Value &v, CoroContext *cxt, int coro_id) {
//  assert(rdma_mg->is_register());

  auto root = get_root_ptr();
  SearchResult result;

  GlobalAddress p = root;
    bool isroot = true;
  bool from_cache = false;
  const CacheEntry *entry = nullptr;
  if (enable_cache) {
    GlobalAddress cache_addr;
    entry = page_cache->search_from_cache(k, &cache_addr);
    if (entry) { // cache hit
//      cache_hit[rdma_mg->getMyThreadID()][0]++;
      from_cache = true;
      p = cache_addr;
      isroot = false;
    } else {
//      cache_miss[rdma_mg->getMyThreadID()][0]++;
    }
  }

next:
  if (!page_search(p, k, result, cxt, coro_id, isroot)) {
      if (isroot){
          p = get_root_ptr();
      }else{
          // fall back to upper level
           p = path_stack[coro_id][result.level -1];
      }
    goto next;
  }
  else{
      isroot = false;
  }

  if (result.is_leaf) {
    if (result.val != kValueNull) { // find
      v = result.val;

      return true;
    }
    if (result.slibing != GlobalAddress::Null()) { // turn right
      p = result.slibing;
      goto next;
    }
    return false; // not found
  } else {        // internal
      assert(result.slibing || result.next_level);
    p = result.slibing != GlobalAddress::Null() ? result.slibing
                                                : result.next_level;

    goto next;
  }
}

// TODO: Need Fix
uint64_t Btr::range_query(const Key &from, const Key &to, Value *value_buffer,
                           CoroContext *cxt, int coro_id) {

  const int kParaFetch = 32;
  thread_local std::vector<InternalPage *> result;
  thread_local std::vector<GlobalAddress> leaves;

  result.clear();
  leaves.clear();
  page_cache->search_range_from_cache(from, to, result);

  if (result.empty()) {
    return 0;
  }

  uint64_t counter = 0;
  for (auto page : result) {
    auto cnt = page->hdr.last_index + 1;
    auto addr = page->hdr.leftmost_ptr;

    // [from, to]
    // [lowest, page->records[0].key);
    bool no_fetch = from > page->records[0].key || to < page->hdr.lowest;
    if (!no_fetch) {
      leaves.push_back(addr);
    }
    for (int i = 1; i < cnt; ++i) {
      no_fetch = from > page->records[i].key || to < page->records[i - 1].key;
      if (!no_fetch) {
        leaves.push_back(page->records[i - 1].ptr);
      }
    }

    no_fetch = from > page->hdr.highest || to < page->records[cnt - 1].key;
    if (!no_fetch) {
      leaves.push_back(page->records[cnt - 1].ptr);
    }
  }

  // printf("---- %d ----\n", leaves.size());
  // sleep(1);

  int cq_cnt = 0;
  char *range_buffer = (rdma_mg->get_rbuf(coro_id)).get_range_buffer();
  for (size_t i = 0; i < leaves.size(); ++i) {
    if (i > 0 && i % kParaFetch == 0) {
      rdma_mg->poll_rdma_cq(kParaFetch);
      cq_cnt -= kParaFetch;
      for (int k = 0; k < kParaFetch; ++k) {
        auto page = (LeafPage *)(range_buffer + k * kLeafPageSize);
        for (int i = 0; i < kLeafCardinality; ++i) {
          auto &r = page->records[i];
          if (r.value != kValueNull && r.f_version == r.r_version) {
            if (r.key >= from && r.key <= to) {
              value_buffer[counter++] = r.value;
            }
          }
        }
      }
    }
    rdma_mg->read(range_buffer + kLeafPageSize * (i % kParaFetch), leaves[i],
                  kLeafPageSize, true);
    cq_cnt++;
  }

  if (cq_cnt != 0) {
    rdma_mg->poll_rdma_cq(cq_cnt);
    for (int k = 0; k < cq_cnt; ++k) {
      auto page = (LeafPage *)(range_buffer + k * kLeafPageSize);
      for (int i = 0; i < kLeafCardinality; ++i) {
        auto &r = page->records[i];
        if (r.value != kValueNull && r.f_version == r.r_version) {
          if (r.key >= from && r.key <= to) {
            value_buffer[counter++] = r.value;
          }
        }
      }
    }
  }

  return counter;
}

// Del needs to be rewritten
void Btr::del(const Key &k, CoroContext *cxt, int coro_id) {
  assert(rdma_mg->is_register());
  before_operation(cxt, coro_id);

  auto root = get_root_ptr();
  SearchResult result;

  GlobalAddress p = root;

next:
  if (!page_search(p, k, result, cxt, coro_id, false)) {
    std::cout << "SEARCH WARNING" << std::endl;
    goto next;
  }

  if (!result.is_leaf) {
    assert(result.level != 0);
    if (result.slibing != GlobalAddress::Null()) {
      p = result.slibing;
      goto next;
    }

    p = result.next_level;
    if (result.level != 1) {

      goto next;
    }
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
 //TODO: Replace isroot as current_search_level
    bool Btr::page_search(GlobalAddress page_addr, const Key &k, SearchResult &result, CoroContext *cxt, int coro_id,
                          bool isroot) {





//  auto &pattern_cnt = pattern[rdma_mg->getMyThreadID()][page_addr.nodeID];

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
  if (enable_cache){
            handle = page_cache->Lookup(page_id);
  }
     void* page_buffer;
     Header * header;
     //TODO: Shall we implement a shared-exclusive lock here for local contention. or we still
     // follow the optimistic latch free design?
  if (handle != nullptr){
      auto mr = (ibv_mr*)page_cache->Value(handle);
      page_buffer = mr->addr;
      header = (Header *)((char*)page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
  }else {

      //  pattern_cnt++;
      ibv_mr* new_mr = new ibv_mr{};
      rdma_mg->Allocate_Local_RDMA_Slot(*new_mr, Default);
      page_buffer = new_mr->addr;
      header = (Header *) ((char*)page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
      rdma_mg->RDMA_Read(page_addr,new_mr, kLeafPageSize, 1,1,Default);
      page_cache->Insert(page_id, new_mr, 1, Deallocate_MR);
  }

  memset(&result, 0, sizeof(result));
  result.is_leaf = header->leftmost_ptr == GlobalAddress::Null();
  result.level = header->level;
  if(!result.is_leaf)
      assert(result.level !=0);
  path_stack[coro_id][result.level] = page_addr;
  // std::cout << "level " << (int)result.level << " " << page_addr <<
  // std::endl;

  if (result.is_leaf) {
      auto page = (LeafPage *)page_buffer;
      if (!page->check_consistent()) {
          goto re_read;
      }

      if ((k < page->hdr.lowest )) { // cache is stale
            // erase the upper node from the cache and refetch the upper node to continue.
          page_cache->Erase(Slice((char*)&path_stack[coro_id][result.level+1], sizeof(GlobalAddress)));
          return false;
      }

      assert(result.level == 0);
      if (k >= page->hdr.highest) { // should turn right
//        printf("should turn right ");
//              if (page->hdr.sibling_ptr != GlobalAddress::Null()){
              // erase the upper level from the cache
              page_cache->Erase(Slice((char*)&path_stack[coro_id][result.level+1], sizeof(GlobalAddress)));
//              }
          result.slibing = page->hdr.sibling_ptr;
          return true;
      }
      page->leaf_page_search(k, result);
  } else {

      assert(result.level != 0);
//          assert(!from_cache);
      auto page = (InternalPage *)page_buffer;
//      assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());

      if (!page->check_consistent()) {
          goto re_read;
      }



      if (k >= page->hdr.highest) { // should turn right
//        printf("should turn right ");
          // TODO: if this is the root node then we need to refresh the new root.
          if (isroot){
              // invalidate the root.
              g_root_ptr = GlobalAddress::Null();
          }
          //TODO: Assert that level+1 is not beyong the top level
          page_cache->Erase(Slice((char*)&path_stack[coro_id][result.level+1], sizeof(GlobalAddress)));
          result.slibing = page->hdr.sibling_ptr;
          return true;
      }
      if (k < page->hdr.lowest) {
          if (isroot){
              // invalidate the root.
              g_root_ptr = GlobalAddress::Null();
          }
//              printf("key %ld error in level %d\n", k, page->hdr.level);
//              sleep(10);
//              print_and_check_tree();
//              assert(false);
          page_cache->Erase(Slice((char*)&path_stack[coro_id][result.level+1], sizeof(GlobalAddress)));

          return false;
      }
      // this function will add the children pointer to the result.
      page->internal_page_search(k, result);

  }


  return true;
}
// internal page serach will return the global point for the next level
//void Btr::internal_page_search(InternalPage *page, const Key &k,
//                                SearchResult &result) {
//
//  assert(k >= page->hdr.lowest);
//  assert(k < page->hdr.highest);
//
//  auto cnt = page->hdr.last_index + 1;
//  // page->debug();
//  if (k < page->records[0].key) { // this only happen when the lowest is 0
////      printf("next level pointer is  leftmost %p \n", page->hdr.leftmost_ptr);
//    result.next_level = page->hdr.leftmost_ptr;
////      result.upper_key = page->records[0].key;
//      assert(result.next_level != GlobalAddress::Null());
////      assert(page->hdr.lowest == 0);//this actually should not happen
//    return;
//  }
//
//  for (int i = 1; i < cnt; ++i) {
//    if (k < page->records[i].key) {
////        printf("next level key is %lu \n", page->records[i - 1].key);
//      result.next_level = page->records[i - 1].ptr;
//        assert(result.next_level != GlobalAddress::Null());
//        assert(page->records[i - 1].key <= k);
//        result.upper_key = page->records[i - 1].key;
//      return;
//    }
//  }
////    printf("next level pointer is  the last value %p \n", page->records[cnt - 1].ptr);
//
//    result.next_level = page->records[cnt - 1].ptr;
//    assert(result.next_level != GlobalAddress::Null());
//    assert(page->records[cnt - 1].key <= k);
////    result.upper_key = page->records[cnt - 1].key;
//}

//void Btr::leaf_page_search(LeafPage *page, const Key &k,
//                            SearchResult &result) {
//
//  for (int i = 0; i < kLeafCardinality; ++i) {
//    auto &r = page->records[i];
//    if (r.key == k && r.value != kValueNull && r.f_version == r.r_version) {
//      result.val = r.value;
//        memcpy(result.value_padding, r.value_padding, VALUE_PADDING);
////      result.value_padding = r.value_padding;
//      break;
//    }
//  }
//}

void Btr::internal_page_store(GlobalAddress page_addr, const Key &k,
                               GlobalAddress v, GlobalAddress root, int level,
                               CoroContext *cxt, int coro_id) {
  uint64_t lock_index =
      CityHash64((char *)&page_addr, sizeof(page_addr)) % define::kNumOfLock;

  GlobalAddress lock_addr;
  lock_addr.nodeID = page_addr.nodeID;
  lock_addr.offset = lock_index * sizeof(uint64_t);

  auto &rbuf = rdma_mg->get_rbuf(coro_id);
  uint64_t *cas_buffer = rbuf.get_cas_buffer();
  auto page_buffer = rbuf.get_page_buffer();

  auto tag = rdma_mg->getThreadTag();
  assert(tag != 0);

  lock_and_read_page(page_buffer, page_addr, kInternalPageSize, cas_buffer,
                     lock_addr, tag, cxt, coro_id);

  auto page = (InternalPage *)page_buffer;

  assert(page->hdr.level == level);
  assert(page->check_consistent());
  // This is the result that we do not lock the btree when search for the key.
  // Not sure whether this will still work if we have node merge
  // Why this node can not be the right most node
  if (k >= page->hdr.highest) {

    this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, true);

    assert(page->hdr.sibling_ptr != GlobalAddress::Null());

    this->internal_page_store(page->hdr.sibling_ptr, k, v, root, level, cxt,
                              coro_id);

    return;
  }
  assert(k >= page->hdr.lowest);

  auto cnt = page->hdr.last_index + 1;
  assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());
  bool is_update = false;
  uint16_t insert_index = 0;
  //TODO: Make it a binary search.
  for (int i = cnt - 1; i >= 0; --i) {
    if (page->records[i].key == k) { // find and update
      page->records[i].ptr = v;
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

  if (!is_update) { // insert and shift
    for (int i = cnt; i > insert_index; --i) {
      page->records[i].key = page->records[i - 1].key;
      page->records[i].ptr = page->records[i - 1].ptr;
    }
    page->records[insert_index].key = k;
    page->records[insert_index].ptr = v;

    page->hdr.last_index++;
  }
  assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());
  assert(page->records[page->hdr.last_index].key != 0);

  cnt = page->hdr.last_index + 1;
  bool need_split = cnt == kInternalCardinality;
  Key split_key;
  GlobalAddress sibling_addr;
  // THe internal node is different from leaf nodes because it has the
  // leftmost_ptr. THe internal nodes has n key but n+1 global pointers.
    // the internal node split pick the middle key as split key and the middle key
    // will not existed in either of the splited node
    // THe data under this internal node [lowest, highest)

    //Both internal node and leaf nodes are [lowest, highest) except for the left most
  if (need_split) { // need split
    sibling_addr = rdma_mg->alloc(kInternalPageSize);
    auto sibling_buf = rbuf.get_sibling_buffer();

    auto sibling = new (sibling_buf) InternalPage(page->hdr.level);

    //    std::cout << "addr " <<  sibling_addr << " | level " <<
    //    (int)(page->hdr.level) << std::endl;
      int m = cnt / 2;
      split_key = page->records[m].key;
      assert(split_key > page->hdr.lowest);
      assert(split_key < page->hdr.highest);
      for (int i = m + 1; i < cnt; ++i) { // move
          sibling->records[i - m - 1].key = page->records[i].key;
          sibling->records[i - m - 1].ptr = page->records[i].ptr;
      }
      page->hdr.last_index -= (cnt - m); // this is correct.
      assert(page->hdr.last_index == m-1);
      sibling->hdr.last_index += (cnt - m - 1);
      assert(sibling->hdr.last_index == cnt - m - 1 - 1);
      sibling->hdr.leftmost_ptr = page->records[m].ptr;
      sibling->hdr.lowest = page->records[m].key;
      sibling->hdr.highest = page->hdr.highest;
      page->hdr.highest = page->records[m].key;

      // link
      sibling->hdr.sibling_ptr = page->hdr.sibling_ptr;
      page->hdr.sibling_ptr = sibling_addr;
    sibling->set_consistent();
    //the code below is just for debugging.
//    sibling_addr.mark = 2;

    rdma_mg->write_sync(sibling_buf, sibling_addr, kInternalPageSize, cxt);
      assert(sibling->records[sibling->hdr.last_index].ptr != GlobalAddress::Null());
      assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());

  }
//  assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());


    page->set_consistent();
  write_page_and_unlock(page_buffer, page_addr, kInternalPageSize, cas_buffer,
                        lock_addr, tag, cxt, coro_id, need_split);

  if (!need_split)
    return;

  if (root == page_addr) { // update root

    if (update_new_root(page_addr, split_key, sibling_addr, level + 1, root,
                        cxt, coro_id)) {
      return;
    }else{
        //CAS the root_ptr_ptr failed. need to re-insert.
    }
  }

  auto up_level = path_stack[coro_id][level + 1];
    //TOTHINGK: What if the upper level has been splitted by other nodes.
  if (up_level != GlobalAddress::Null()) {
    internal_page_store(up_level, split_key, sibling_addr, root, level + 1, cxt,
                        coro_id);
  } else {
      // internal page store means that you are running insert_internal, and there
      // must be upper levels' node in the cache. so this path must be unreachable.
    assert(false);
  }
}

bool Btr::leaf_page_store(GlobalAddress page_addr, const Key &k,
                           const Value &v, GlobalAddress root, int level,
                           CoroContext *cxt, int coro_id, bool from_cache) {

  uint64_t lock_index =
      CityHash64((char *)&page_addr, sizeof(page_addr)) % define::kNumOfLock;

  GlobalAddress lock_addr;


    char padding[VALUE_PADDING];
#ifdef CONFIG_ENABLE_EMBEDDING_LOCK
  lock_addr = page_addr;
#else
  lock_addr.nodeID = page_addr.nodeID;
  lock_addr.offset = lock_index * sizeof(uint64_t);
#endif

  auto &rbuf = rdma_mg->get_rbuf(coro_id);
  uint64_t *cas_buffer = rbuf.get_cas_buffer();
  auto page_buffer = rbuf.get_page_buffer();

  auto tag = rdma_mg->getThreadTag();
  assert(tag != 0);

  lock_and_read_page(page_buffer, page_addr, kLeafPageSize, cas_buffer,

                     lock_addr, tag, cxt, coro_id);

  auto page = (LeafPage *)page_buffer;

  assert(page->hdr.level == level);
  assert(page->check_consistent());

  if (from_cache &&
      (k < page->hdr.lowest || k >= page->hdr.highest)) { // cache is stale
    this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, true);

    // if (enter_debug) {
    //   printf("cache {%lu} %lu [%lu %lu]\n", page_addr.val, k,
    //   page->hdr.lowest,
    //          page->hdr.highest);
    // }

    return false;
  }

  // if (enter_debug) {
  //   printf("{%lu} %lu [%lu %lu]\n", page_addr.val, k, page->hdr.lowest,
  //          page->hdr.highest);
  // }

  if (k >= page->hdr.highest) {

    this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, true);

    assert(page->hdr.sibling_ptr != GlobalAddress::Null());

    this->leaf_page_store(page->hdr.sibling_ptr, k, v, root, level, cxt,
                          coro_id);

    return true;
  }
  assert(k >= page->hdr.lowest);

  int cnt = 0;
  int empty_index = -1;
  char *update_addr = nullptr;
    // It is problematic to just check whether the value is empty, because it is possible
    // that the buffer is not initialized as 0
  for (int i = 0; i < kLeafCardinality; ++i) {

    auto &r = page->records[i];
    if (r.value != kValueNull) {
      cnt++;
      if (r.key == k) {
        r.value = v;
        // ADD MORE weight for write.
        memcpy(r.value_padding, padding, VALUE_PADDING);

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
    memcpy(r.value_padding, padding, VALUE_PADDING);
    r.f_version++;
    r.r_version = r.f_version;

    update_addr = (char *)&r;

    cnt++;
  }

  bool need_split = cnt == kLeafCardinality;
  if (!need_split) {
    assert(update_addr);
    write_page_and_unlock(
        update_addr, GADD(page_addr, (update_addr - (char *)page)),
        sizeof(LeafEntry), cas_buffer, lock_addr, tag, cxt, coro_id, false);

    return true;
  } else {
    std::sort(
        page->records, page->records + kLeafCardinality,
        [](const LeafEntry &a, const LeafEntry &b) { return a.key < b.key; });
  }


  Key split_key;
  GlobalAddress sibling_addr;
  if (need_split) { // need split
    sibling_addr = rdma_mg->alloc(kLeafPageSize);
    auto sibling_buf = rbuf.get_sibling_buffer();

    auto sibling = new (sibling_buf) LeafPage(page->hdr.level);

    // std::cout << "addr " <<  sibling_addr << " | level " <<
    // (int)(page->hdr.level) << std::endl;

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
    sibling->set_consistent();
    rdma_mg->write_sync(sibling_buf, sibling_addr, kLeafPageSize, cxt);
  }

  page->set_consistent();

  write_page_and_unlock(page_buffer, page_addr, kLeafPageSize, cas_buffer,
                        lock_addr, tag, cxt, coro_id, need_split);

  if (!need_split)
    return true;

  if (root == page_addr) { // update root
    if (update_new_root(page_addr, split_key, sibling_addr, level + 1, root,
                        cxt, coro_id)) {
      return true;
    }
  }

  auto up_level = path_stack[coro_id][level + 1];

  if (up_level != GlobalAddress::Null()) {
      // It is possible that two compute nodes update the same inner node without a lock.
      // we need to acquire the lock first.
    internal_page_store(up_level, split_key, sibling_addr, root, level + 1, cxt,
                        coro_id);
  } else {
    assert(from_cache);
    //If the program comes here, then it could be dangerous
    insert_internal(split_key, sibling_addr, cxt, coro_id, level + 1);
  }

  return true;
}

// Need BIG FIX
void Btr::leaf_page_del(GlobalAddress page_addr, const Key &k, int level,
                         CoroContext *cxt, int coro_id) {
  uint64_t lock_index =
      CityHash64((char *)&page_addr, sizeof(page_addr)) % define::kNumOfLock;

  GlobalAddress lock_addr;
  lock_addr.nodeID = page_addr.nodeID;
  lock_addr.offset = lock_index * sizeof(uint64_t);

  uint64_t *cas_buffer = rdma_mg->get_rbuf(coro_id).get_cas_buffer();

  auto tag = DSMEngine::RDMA_Manager::node_id;
  try_lock_addr(lock_addr, tag, cas_buffer, cxt, coro_id);

  auto page_buffer = rdma_mg->get_rbuf(coro_id).get_page_buffer();
  rdma_mg->read_sync(page_buffer, page_addr, kLeafPageSize, cxt);
  auto page = (LeafPage *)page_buffer;

  assert(page->hdr.level == level);
  assert(page->check_consistent());
  if (k >= page->hdr.highest) {
    this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, true);

    assert(page->hdr.sibling_ptr != GlobalAddress::Null());

    this->leaf_page_del(page->hdr.sibling_ptr, k, level, cxt, coro_id);
  }

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

    page->set_consistent();
    rdma_mg->write_sync(page_buffer, page_addr, kLeafPageSize, cxt);
  }
  this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, false);
}

void Btr::run_coroutine(CoroFunc func, int id, int coro_cnt) {

  using namespace std::placeholders;

  assert(coro_cnt <= define::kMaxCoro);
  for (int i = 0; i < coro_cnt; ++i) {
    auto gen = func(i, rdma_mg, id);
    worker[i] = CoroCall(std::bind(&Btr::coro_worker, this, _1, gen, i));
  }

  master = CoroCall(std::bind(&Btr::coro_master, this, _1, coro_cnt));

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

void Btr::coro_master(CoroYield &yield, int coro_cnt) {

  for (int i = 0; i < coro_cnt; ++i) {
    yield(worker[i]);
  }

  while (true) {

    uint64_t next_coro_id;

    if (rdma_mg->poll_rdma_cq_once(next_coro_id)) {
      yield(worker[next_coro_id]);
    }

    if (!hot_wait_queue.empty()) {
      next_coro_id = hot_wait_queue.front();
      hot_wait_queue.pop();
      yield(worker[next_coro_id]);
    }

    if (!deadline_queue.empty()) {
      auto now = timer.get_time_ns();
      auto task = deadline_queue.top();
      if (now > task.deadline) {
        deadline_queue.pop();
        yield(worker[task.coro_id]);
      }
    }
  }
}

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
  auto &node = local_locks[lock_addr.nodeID][lock_addr.offset / 8];
  bool is_local_locked = false;

  uint64_t lock_val = node.ticket_lock.fetch_add(1);
  //TOTHINK: what if the ticket out of buffer.

  uint32_t ticket = lock_val << 32 >> 32;//clear the former 32 bit
  uint32_t current = lock_val >> 32;// current is the former 32 bit in ticket lock

  // printf("%ud %ud\n", ticket, current);
  while (ticket != current) { // lock failed
    is_local_locked = true;

//    if (cxt != nullptr) {
//      hot_wait_queue.push(coro_id);
//      (*cxt->yield)(*cxt->master);
//    }

    current = node.ticket_lock.load(std::memory_order_relaxed) >> 32;
  }

  if (is_local_locked) {
//    hierarchy_lock[rdma_mg->getMyThreadID()][0]++;
  }

  node.hand_time++;

  return node.hand_over;
}

inline bool Btr::can_hand_over(GlobalAddress lock_addr) {

  auto &node = local_locks[lock_addr.nodeID][lock_addr.offset / 8];
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

inline void Btr::releases_local_lock(GlobalAddress lock_addr) {
  auto &node = local_locks[lock_addr.nodeID][lock_addr.offset / 8];

  node.ticket_lock.fetch_add((1ull << 32));
}

void Btr::index_cache_statistics() {
  page_cache->statistics();
  page_cache->bench();
}

void Btr::clear_statistics() {
  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    cache_hit[i][0] = 0;
    cache_miss[i][0] = 0;
  }
}
}