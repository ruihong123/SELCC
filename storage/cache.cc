// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "DSMEngine/cache.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>
//#include <infiniband/verbs.h>

#include "port/port.h"
#include "port/thread_annotations.h"
#include "utils/hash.h"
#include "utils/mutexlock.h"
#include "HugePageAlloc.h"
#include "rdma.h"
#include "storage/page.h"

// DO not enable the two at the same time otherwise there will be a bug.

#define PARALLEL_DEGREE 16
#define STARVATION_THRESHOLD 16
#define STARV_SPIN_BASE 8
uint64_t cache_miss[MAX_APP_THREAD][8];
uint64_t cache_hit_valid[MAX_APP_THREAD][8];
uint64_t invalid_counter[MAX_APP_THREAD][8];

uint64_t lock_fail[MAX_APP_THREAD][8];
uint64_t pattern[MAX_APP_THREAD][8];
uint64_t hierarchy_lock[MAX_APP_THREAD][8];
uint64_t handover_count[MAX_APP_THREAD][8];
uint64_t hot_filter_count[MAX_APP_THREAD][8];
uint64_t latency[MAX_APP_THREAD][LATENCY_WINDOWS];
extern bool Show_Me_The_Print;
#ifdef TIMEPRINT
uint64_t cache_lookup_total[MAX_APP_THREAD] = {0};
uint64_t cache_lookup_times[MAX_APP_THREAD] = {0};
void Reset_cache_counters(){
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
        cache_lookup_total[i] = 0;
        cache_lookup_times[i] = 0;
    }

}

uint64_t Calculate_cache_counters(){
    uint64_t sum = 0;
    uint64_t times = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
        sum += cache_lookup_total[i];
        times += cache_lookup_times[i];
    }
    return sum/times;

}
#endif
int TimePrintCounter[MAX_APP_THREAD];
namespace DSMEngine {
//std::atomic<uint64_t> LRUCache::counter = 0;
RDMA_Manager* Cache::Handle::rdma_mg = nullptr;
Cache::~Cache() {}







LRUCache::LRUCache() : capacity_(0), usage_(0) {
  // Make empty circular linked lists.
  lru_.next = &lru_;
  lru_.prev = &lru_;
  in_use_.next = &in_use_;
  in_use_.prev = &in_use_;
}

LRUCache::~LRUCache() {
  assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle
  for (LRUHandle* e = lru_.next; e != &lru_;) {
    LRUHandle* next = e->next;
    assert(e->in_cache);
    e->in_cache = false;
    assert(e->refs == 1);  // Invariant of lru_ list.
      Unref(e, nullptr);
    e = next;
  }
}
//Can we use the lock within the handle to reduce the conflict here so that the critical seciton
// of the cache shard lock will be minimized.
    void LRUCache::Ref(LRUHandle* e) {
        if (e->refs == 1 && e->in_cache) {  // If on lru_ list, move to in_use_ list.
            LRU_Remove(e);
            LRU_Append(&in_use_, e);
        }
        e->refs++;
        assert(e->refs <=100);
    }


void LRUCache::Unref(LRUHandle *e, SpinLock *spin_l) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs == 0) {  // Deallocate.
      //Finish erase will only goes here, or directly return. it will never goes to next if clause
//        mutex_.unlock();
#ifdef EARLY_LOCK_RELEASE
      if (spin_l!= nullptr){
          //Early releasing the lock to avoid the RDMA lock releasing in the critical section.
          assert(spin_l->check_own() == true);
          spin_l->Unlock();
      }
#endif
      assert(!e->in_cache);
    (*e->deleter)(e);
//    free(e);
    delete e;
//      if (spin_l!= nullptr ){
//          spin_l->Lock();
//      }
  } else if (e->in_cache && e->refs == 1) {
//#ifndef NDEBUG
//      if (e->gptr.offset < 9480863232){
//          printf("page of %lu is removed from the inuse list and apped to LRU list\n", e->gptr.offset);
//      }
//#endif
    // No longer in use; move to lru_ list.
    LRU_Remove(e);// remove from in_use list move to LRU list.
    LRU_Append(&lru_, e);
  }
}

    void LRUCache::Unref_Inv(LRUHandle *e) {
        assert(e->refs > 0);
        e->refs--;
        if (e->refs == 0) {  // Deallocate.

            assert(false);
            assert(!e->in_cache);
            (*e->deleter)(e);
            delete e;

        } else if (e->in_cache && e->refs == 1) {

            // No longer in use; move to lru_ list.
            LRU_Remove(e);// remove from in_use list move to LRU list.
            LRU_Append(lru_.next, e);
        }
    }

//void DSMEngine::LRUCache::Unref_WithoutLock(LRUHandle *e) {
//    assert(e->refs > 0);
////    e->refs--;
//    if (e->refs.load() == 1) {  // Deallocate.
//        WriteLock l(&mutex_);
//        if (e->refs.fetch_sub(1) == 1){
//            //Finish erase will only goes here, or directly return. it will neve goes to next if clause
//            assert(!e->in_cache);
//            (*e->deleter)(e->key(), e->value);
//            free(e);
//        }
//        return;
//
//    } else if (e->in_cache && e->refs.load() == 2) {
//
//        WriteLock l(&mutex_);
//        if (e->in_cache && e->refs.fetch_sub(1) == 2){
//            // No longer in use; move to lru_ list.
//            LRU_Remove(e);// remove from in_use list move to LRU list.
//            LRU_Append(&lru_, e);
//        }
//        return;
//
//    }
//    e->refs.fetch_sub(1);
//}


void LRUCache::LRU_Remove(LRUHandle* e) {
#ifndef NDEBUG
//    if (e->gptr.offset < 10480863232){
//        printf("page %lu is being remove from a list", e->gptr.offset);
//    }
#endif
  e->next.load()->prev = e->prev.load();
  e->prev.load()->next = e->next.load();
}

void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e) {
  // Make "e" newest entry by inserting just before *list
  e->next = list;
  e->prev.store(list->prev);
  e->prev.load()->next = e;
  e->next.load()->prev = e;
}

//Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
//    //TODO: WHEN there is a miss, directly call the RDMA refetch and put it into the
//    // cache.
////  MutexLock l(&mutex_);
//    LRUHandle *e;
//    {
//        mutex_.ReadLock();
//        assert(usage_ <= capacity_);
//        //TOTHINK(ruihong): should we update the lru list after look up a key?
//        //  Answer: Ref will refer this key and later, the outer function has to call
//        // Unref or release which will update the lRU list.
//        e = table_.Lookup(key, hash);
//        if (e != nullptr) {
//            Ref_in_LookUp(e);
//        }else{
//            mutex_.ReadUnlock();
//        }
//    }
//
//  return reinterpret_cast<Cache::Handle*>(e);
//}
Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
//  MutexLock l(&mutex_);
    SpinLock l(&mutex_);
    //TOTHINK(ruihong): shoul we update the lru list after look up a key?
    //  Answer: Ref will refer this key and later, the outer function has to call
    // Unref or release which will update the lRU list.
    LRUHandle* e = table_.Lookup(key, hash);
    if (e != nullptr) {
        assert(e->refs >= 1);
        Ref(e);
    }
    return reinterpret_cast<Cache::Handle*>(e);
}
Cache::Handle *DSMEngine::LRUCache::LookupInsert(const Slice &key, uint32_t hash, void *value, size_t charge,
                                                 void (*deleter)(Cache::Handle* handle)) {
    assert(!SpinLock::check_own());
    SpinLock l(&mutex_);
    //TOTHINK(ruihong): shoul we update the lru list after look up a key?
    //  Answer: Ref will refer this key and later, the outer function has to call
    // Unref or release which will update the lRU list.
    LRUHandle* e = table_.Lookup(key, hash);
    if (e != nullptr) {
        assert(e->refs >= 1);
        Ref(e);
//        assert(e->refs <=2);
//        DEBUG_PRINT("cache hit when searching the leaf node");
        return reinterpret_cast<Cache::Handle*>(e);
    }else{
//        fprintf(stdout, "Did not find cache entry for %lu\n", (*(GlobalAddress*)key.data()).offset);
        // This LRU handle is not initialized.
        // TODO: get the LRU handle from the free list.
        e = new LRUHandle();
//                reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size()));

        e->value = value;
        e->remote_lock_status = 0;
        e->remote_lock_urged = 0;
        e->gptr = *(GlobalAddress*)key.data();

        e->deleter = deleter;
        e->charge = charge;
        e->key_length = key.size();
        e->hash = hash;
        e->in_cache = false;
        e->refs = 1;  // for the returned handle.
//        std::memcpy(e->key_data, key.data(), key.size());
        if (capacity_ > 0) {
            e->refs++;  // for the table_cache's reference. refer here and unrefer outside
            e->in_cache = true;
            LRU_Append(&in_use_, e);// Finally it will be pushed into LRU list
            usage_ += charge;
            FinishErase(table_.Insert(e), &l);//table_.Insert(e) will return LRUhandle with duplicate key as e, and then delete it by FinishErase
//#ifndef NDEBUG
//            if (e->gptr.offset < 9480863232){
//                printf("page of %lu is inserted into the cache", e->gptr.offset);
//            }
//#endif
        } else {  // don't do caching. (capacity_==0 is supported and turns off caching.)
            // next is read by key() in an assert, so it must be initialized
            e->next = nullptr;
        }
#ifdef EARLY_LOCK_RELEASE
        //In case that insert a handle which has already exisit, the old handle need to be deallocated
        if (!l.check_own()){
            assert(false);
            l.Lock();
        }
#endif
        assert(usage_ <= capacity_ + kLeafPageSize + kInternalPageSize);
        // This will remove some entry from LRU if the table_cache over size.
#ifdef BUFFER_HANDOVER
        bool already_foward_the_mr = false;
#endif
//        if (counter.fetch_add(1) == 100000){
//            printf("capacity is %zu, usage is %zu\n", capacity_, usage_);
//            counter = 0;
//        }
        int counter = 0;
        while (usage_ > capacity_ && lru_.next != &lru_) {
            assert(counter == 0);
            LRUHandle* old = lru_.next;
            assert(old->refs == 1);
//#ifndef NDEBUG
//            if (old->gptr.offset < 9480863232){
//                printf("page of %lu is extracted from the LRUlist", e->gptr.offset);
//            }
//#endif
            // Directly reuse the mr if the evicted cache entry is the same size as the new inserted on.
#ifdef BUFFER_HANDOVER
//            bool rw_locked = false;

            if (value == nullptr && !already_foward_the_mr && ((ibv_mr*)old->value)->length == charge){
                old->keep_the_mr = true;
                e->value = old->value;
                already_foward_the_mr = true;
//                e->rw_mtx.lock();
//                rw_locked = true;
            }
            std::unique_lock<RWSpinLock> lck(e->rw_mtx);

            //If there is early lock release, then the handle may be accessed by other threads,
            // before the mr has been dirty flushed. We need to make sure the following accessor,
            // can not access the mr before we finish the old mr deallocating (flush the dirty page)
            //TODO: ACQUIRE THE READ-write LATCH IN the cache handle
#endif
            assert(l.check_own());
            bool erased = FinishErase(table_.Remove(old->key(), old->hash), &l);
#ifdef EARLY_LOCK_RELEASE
//            if (rw_locked){
//                e->rw_mtx.unlock();
//            }
            if (!l.check_own()){
                l.Lock();
            }
#endif
            if (!erased) {  // to avoid unused variable when compiled NDEBUG
                assert(erased);
            }
            counter++;
        }
//        assert(usage_ <= capacity_);

        return reinterpret_cast<Cache::Handle*>(e);
    }
}
void LRUCache::Release(Cache::Handle* handle) {
//  MutexLock l(&mutex_);
//  WriteLock l(&mutex_);
  SpinLock l(&mutex_);
    Unref(reinterpret_cast<LRUHandle *>(handle), &l);
//    assert(reinterpret_cast<LRUHandle*>(handle)->refs != 0);
}
void LRUCache::Release_Inv(Cache::Handle* handle) {

    SpinLock l(&mutex_);
    Unref_Inv(reinterpret_cast<LRUHandle *>(handle));
}
//If the inserted key has already existed, then the old LRU handle will be removed from
// the cache, but it may not garbage-collected right away.
Cache::Handle* LRUCache::Insert(const Slice& key, uint32_t hash, void* value,
                                size_t charge,
                                void (*deleter)(Cache::Handle* handle)) {
//  MutexLock l(&mutex_);

  //TODO: set the LRUHandle within the page, so that we can check the reference, during the direct access, or we reserver
  // a place hodler for the address pointer to the LRU handle of the page.
  LRUHandle* e = new LRUHandle();
//      reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size()));

    e->remote_lock_status = 0;
    e->remote_lock_urged = 0;
    e->gptr = *(GlobalAddress*)key.data();

  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->in_cache = false;
  e->refs = 1;  // for the returned handle.
//  std::memcpy(e->key_data, key.data(), key.size());
//  WriteLock l(&mutex_);
  SpinLock l(&mutex_);
  if (capacity_ > 0) {
    e->refs++;  // for the table_cache's reference. refer here and unrefer outside
    e->in_cache = true;
    LRU_Append(&in_use_, e);// Finally it will be pushed into LRU list
    usage_ += charge;
      FinishErase(table_.Insert(e), &l);//table_.Insert(e) will return LRUhandle with duplicate key as e, and then delete it by FinishErase
  } else {  // don't do caching. (capacity_==0 is supported and turns off caching.)
    // next is read by key() in an assert, so it must be initialized
    e->next = nullptr;
  }
#ifdef EARLY_LOCK_RELEASE

    if (!l.check_own()){
        l.Lock();
    }
#endif
        assert(usage_ <= capacity_ + kLeafPageSize + kInternalPageSize);
  // This will remove some entry from LRU if the table_cache over size.
#ifdef BUFFER_HANDOVER
            bool already_foward_the_mr = false;
#endif

    while (usage_ > capacity_ && lru_.next != &lru_) {

    LRUHandle* old = lru_.next;
    assert(old->refs == 1);
#ifdef BUFFER_HANDOVER
//    bool rw_locked = false;
      if (value == nullptr && !already_foward_the_mr && ((ibv_mr*)old->value)->length == charge){
          old->keep_the_mr = true;
          e->value = old->value;
          assert(((ibv_mr*)e->value)->addr != nullptr);
          already_foward_the_mr = true;
//          e->rw_mtx.lock();
      }
      std::unique_lock<RWSpinLock> lck(e->rw_mtx);
#endif
        assert(l.check_own());
    bool erased = FinishErase(table_.Remove(old->key(), old->hash), &l);
    //some times the finsih Erase will release the spinlock to let other threads working during the RDMA lock releasing.
    //We need to regain the lock here in case that there is another cache entry eviction.
#ifdef EARLY_LOCK_RELEASE
//        if (rw_locked){
//            e->rw_mtx.unlock();
//        }
      if (!l.check_own()){
          l.Lock();
      }
#endif
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

// If e != nullptr, finish removing *e from the table_cache;
// it must have already been removed from the hash table.  Return whether e != nullptr.
// Remove the handle from LRU and change the usage.
bool LRUCache::FinishErase(LRUHandle *e, SpinLock *spin_l) {

  if (e != nullptr) {
//#ifndef NDEBUG
//      if (e->gptr.offset < 9480863232){
//          printf("page of %lu is removed from the cache\n", e->gptr.offset);
//      }
//#endif
    assert(e->in_cache);
    LRU_Remove(e);
    e->in_cache = false;
    usage_ -= e->charge;
  // decrease the reference of cache, making it not pinned by cache, but it
  // can still be pinned outside the cache.
//      assert(e->refs == 1);
      Unref(e, spin_l);

  }
  return e != nullptr;
}

void LRUCache::Erase(const Slice& key, uint32_t hash) {
//  MutexLock l(&mutex_);
//  WriteLock l(&mutex_);
  SpinLock l(&mutex_);
    FinishErase(table_.Remove(key, hash), &l);
}

void LRUCache::Prune() {
//  MutexLock l(&mutex_);
//  WriteLock l(&mutex_);
    SpinLock l(&mutex_);
  while (lru_.next != &lru_) {
    LRUHandle* e = lru_.next;
    assert(e->refs == 1);
    bool erased = FinishErase(table_.Remove(e->key(), e->hash), nullptr);
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }
}




static const int kNumShardBits = 7;
static const int kNumShards = 1 << kNumShardBits;

class ShardedLRUCache : public Cache {
 private:
  LRUCache shard_[kNumShards];
  port::Mutex id_mutex_;
  uint64_t last_id_;
  size_t capacity_;

  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  static uint32_t Shard(uint32_t hash) { return hash >> (32 - kNumShardBits); }

 public:
  explicit ShardedLRUCache(size_t capacity) : last_id_(0) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    capacity_ = capacity;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }
  ~ShardedLRUCache() override {}
  size_t GetCapacity() override{
      return capacity_;
  }
    // if there has already been a cache entry with the same key, the old one will be
    // removed from the cache, but it may not be garbage collected right away
  Handle* Insert(const Slice& key, void* value, size_t charge,
                 void (*deleter)(Cache::Handle* handle)) override {
#ifndef NDEBUG
        assert(capacity_ >= 1000);
        if (TotalCharge() > 0.9 * capacity_ ){
            for (int i = 0; i < kNumShards - 1; ++i) {
                if (shard_[i+1].TotalCharge() >0 && shard_[i].TotalCharge()/shard_[i+1].TotalCharge() >= 2){
//                    printf("Uneven cache distribution\n");
                    assert(false);
                    break;
                }
            }
        }

#endif
    const uint32_t hash = HashSlice(key);

//        auto handle = shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
//        printf("Insert: refer to handle %p\n", handle);

        return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }
  //TODO: Change the search key to GlobalAddress.
  Handle* Lookup(const Slice& key) override {
      assert(capacity_ >= 1000);
    const uint32_t hash = HashSlice(key);

//    auto handle = shard_[Shard(hash)].Lookup(key, hash);
//      printf("Look up: refer to handle %p\n", handle);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
    Handle* LookupInsert(const Slice& key,  void* value,
                         size_t charge,
                         void (*deleter)(Cache::Handle* handle)) override{

                assert(capacity_ >= 1000);
                const uint32_t hash = HashSlice(key);
                return shard_[Shard(hash)].LookupInsert(key, hash, value, charge, deleter);
  };
  void Release(Handle* handle) override {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
//      printf("release handle %p\n", handle);
    shard_[Shard(h->hash)].Release(handle);
  }
    void Release_Inv(Handle* handle) override {
        LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
//      printf("release handle %p\n", handle);
        shard_[Shard(h->hash)].Release_Inv(handle);
    }
  void Erase(const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  void* Value(Handle* handle) override {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  uint64_t NewId() override {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
  void Prune() override {
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].Prune();
    }
  }
  size_t TotalCharge() const override {
    size_t total = 0;
    for (int s = 0; s < kNumShards; s++) {
      total += shard_[s].TotalCharge();
    }
    return total;
  }
};


Cache* NewLRUCache(size_t capacity) { return new ShardedLRUCache(capacity); }


LocalBuffer::LocalBuffer(const CacheConfig &cache_config) {
        size = cache_config.cacheSize;
        data = (uint64_t)hugePageAlloc(size * define::GB);
    if (data == 0){
        data = (int64_t) malloc(size * define::GB);
    }
}

    void Cache_Handle::invalidate_current_entry(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr,
                                                 ibv_mr *mr, ibv_mr* cas_mr) {

        if (this->remote_lock_status == 1){
            rdma_mg->global_RUnlock(lock_addr, cas_mr, false, nullptr, nullptr, 0);
            remote_lock_status.store(0);
            //todo: spin wait to avoid write lock starvation.
            if (remote_lock_urged.load() > 1){
                spin_wait_us(10);
            }
        }else if (this->remote_lock_status == 2){
            rdma_mg->global_write_page_and_Wunlock(mr, page_addr, page_size, lock_addr);
            remote_lock_status.store(0);
        }else{
            assert(false);
        }
    }


    void Cache_Handle::reader_pre_access(GlobalAddress page_addr, size_t page_size,
                                          GlobalAddress lock_addr, ibv_mr*& mr) {
        assert(page_addr == gptr);
        if (rdma_mg == nullptr){
            rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        }

        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
        if (remote_lock_urged.load() > 0){
            lock_pending_num.fetch_add(1);
            uint16_t handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
            while (handover_degree > STARVATION_THRESHOLD || timer_alarmed.load()){
                //wait here by no ops
                handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
                asm volatile("pause\n": : :"memory");
            }
            rw_mtx.lock_shared();
            lock_pending_num.fetch_sub(1);
//            read_lock_holder_num.fetch_add(1);
            read_lock_counter.fetch_add(1);
        }else{
            rw_mtx.lock_shared();
        }
//TODO: (OPTIMIZATION) we can first check what state is the cache in if it is invalid, we can
// directly acquire the write local lock.

#ifdef LOCAL_LOCK_DEBUG
        {
            std::unique_lock<std::mutex> lck(holder_id_mtx);
            holder_ids.insert(RDMA_Manager::thread_id);
        }
#endif

            //TODO: make the leaf_node_search the same as leaf_node_store?
            // No, because the read here has a optimiaziton for the double check locking

            if (remote_lock_status.load() == 0){
                // upgrade the lock the write lock.
                //Can we use the std::call_once here?
#ifdef LOCAL_LOCK_DEBUG
                {
                    std::unique_lock<std::mutex> lck(holder_id_mtx);
                    holder_ids.erase(RDMA_Manager::thread_id);

                }
#endif
                rw_mtx.unlock_shared();
                asm volatile ("sfence\n" : : );
                asm volatile ("lfence\n" : : );
                asm volatile ("mfence\n" : : );
//                std::unique_lock<std::shared_mutex> w_l(rw_mtx);
                rw_mtx.lock(RDMA_Manager::thread_id+384);
#ifdef LOCAL_LOCK_DEBUG

                {
                    std::unique_lock<std::mutex> lck(holder_id_mtx);
                    holder_ids.insert(RDMA_Manager::thread_id);

                }
#endif
                if (remote_lock_status.load() == 0){
                    cache_miss[RDMA_Manager::thread_id][0]++;
                    if(value) {
                        mr = (ibv_mr*)value;
                    }else{
                        mr = new ibv_mr{};
                        rdma_mg->Allocate_Local_RDMA_Slot(*mr, Regular_Page);
//                        printf("value mr is null for globale page nodeid %lu, offset %lu\n", page_addr.nodeID, page_addr.offset);

//        printf("Allocate slot for page 1, the page global pointer is %p , local pointer is  %p, hash value is %lu level is %d\n",
//               page_addr, mr->addr, HashSlice(page_id), level);

                        //TODO: this is not guarantted to be atomic, mulitple reader can cause memory leak
                        value = mr;

                    }
                    rdma_mg->global_Rlock_and_read_page_with_INVALID(mr, page_addr, page_size, lock_addr, cas_mr);
                    remote_lock_status.store(1);
                }else{
                    cache_hit_valid[RDMA_Manager::thread_id][0]++;
                }
#ifdef LOCAL_LOCK_DEBUG

                {
                    std::unique_lock<std::mutex> lck(holder_id_mtx);
                    holder_ids.erase(RDMA_Manager::thread_id);

                }
#endif
//                w_l.unlock();
                rw_mtx.unlock();
                asm volatile ("sfence\n" : : );
                asm volatile ("lfence\n" : : );
                asm volatile ("mfence\n" : : );
                rw_mtx.lock_shared();
#ifdef LOCAL_LOCK_DEBUG
                {
                    std::unique_lock<std::mutex> lck(holder_id_mtx);
                    holder_ids.insert(RDMA_Manager::thread_id);
                }
#endif

            }else{
                cache_hit_valid[RDMA_Manager::thread_id][0]++;
            }
            mr = (ibv_mr*)value;



    }
    bool Cache_Handle::try_reader_pre_access(GlobalAddress page_addr, size_t page_size,
                                          GlobalAddress lock_addr, ibv_mr*& mr) {
        assert(page_addr == gptr);
        if (rdma_mg == nullptr){
            rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        }

        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
        if (remote_lock_urged.load() > 0){
            lock_pending_num.fetch_add(1);
            //TODO: pontential bug below, if there is try lock then the read write counter may not be updated but neve cleared.
//            uint16_t handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
//            while (handover_degree > STARVATION_THRESHOLD || timer_alarmed.load()){
//                //wait here by no ops
//                handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
//                asm volatile("pause\n": : :"memory");
//            }
            if (!rw_mtx.try_shared_lock()) {
                lock_pending_num.fetch_sub(1);
                return false;
            }
            read_lock_counter.fetch_add(1);
            lock_pending_num.fetch_sub(1);

//            read_lock_holder_num.fetch_add(1);
        }else{
            if (!rw_mtx.try_shared_lock()){
                return false;
            }
        }


#ifdef LOCAL_LOCK_DEBUG
        {
            std::unique_lock<std::mutex> lck(holder_id_mtx);
            holder_ids.insert(RDMA_Manager::thread_id);
        }
#endif

            //TODO: make the leaf_node_search the same as leaf_node_store?
            // No, because the read here has a optimiaziton for the double check locking

            if (remote_lock_status.load() == 0){
                cache_miss[RDMA_Manager::thread_id][0]++;
                // upgrade the lock the write lock.
                //Can we use the std::call_once here?
#ifdef LOCAL_LOCK_DEBUG
                {
                    std::unique_lock<std::mutex> lck(holder_id_mtx);
                    holder_ids.erase(RDMA_Manager::thread_id);

                }
#endif
                rw_mtx.unlock_shared();
                asm volatile ("sfence\n" : : );
                asm volatile ("lfence\n" : : );
                asm volatile ("mfence\n" : : );
//                std::unique_lock<std::shared_mutex> w_l(rw_mtx);
                if (!rw_mtx.try_lock()){

                    return false;
                }
//                rw_mtx.lock(RDMA_Manager::thread_id+384);
#ifdef LOCAL_LOCK_DEBUG

                {
                    std::unique_lock<std::mutex> lck(holder_id_mtx);
                    holder_ids.insert(RDMA_Manager::thread_id);

                }
#endif
                if ( remote_lock_status.load() == 0){
                    if(value) {
                        mr = (ibv_mr*)value;
                    }else{
                        mr = new ibv_mr{};
                        rdma_mg->Allocate_Local_RDMA_Slot(*mr, Regular_Page);
//                        printf("value mr is null for globale page nodeid %lu, offset %lu\n", page_addr.nodeID, page_addr.offset);

//        printf("Allocate slot for page 1, the page global pointer is %p , local pointer is  %p, hash value is %lu level is %d\n",
//               page_addr, mr->addr, HashSlice(page_id), level);

                        //TODO: this is not guarantted to be atomic, mulitple reader can cause memory leak
                        value = mr;

                    }
                    if (!rdma_mg->global_Rlock_and_read_page_with_INVALID(mr, page_addr, page_size, lock_addr, cas_mr,5)){
                        rw_mtx.unlock();
                        return false;
                    }
                    remote_lock_status.store(1);
                }
#ifdef LOCAL_LOCK_DEBUG

                {
                    std::unique_lock<std::mutex> lck(holder_id_mtx);
                    holder_ids.erase(RDMA_Manager::thread_id);

                }
#endif
//                w_l.unlock();
                rw_mtx.unlock();
                asm volatile ("sfence\n" : : );
                asm volatile ("lfence\n" : : );
                asm volatile ("mfence\n" : : );
                if (!rw_mtx.try_shared_lock()){

                    return false;
                }
//                rw_mtx.lock_shared();
#ifdef LOCAL_LOCK_DEBUG
                {
                    std::unique_lock<std::mutex> lck(holder_id_mtx);
                    holder_ids.insert(RDMA_Manager::thread_id);
                }
#endif

            }else{
                cache_hit_valid[RDMA_Manager::thread_id][0]++;
            }
            mr = (ibv_mr*)value;
//        if (remote_lock_urged.load() > 0){
//            read_lock_counter.fetch_add(1);
//        }

        return true;
    }

    void Cache_Handle::reader_post_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *mr) {
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();


//        assert(handle->refs.load() == 2);
#ifdef LOCAL_LOCK_DEBUG
        {
            std::unique_lock<std::mutex> lck(holder_id_mtx);
            holder_ids.erase(RDMA_Manager::thread_id);

        }
#endif
        if (remote_lock_urged.load() > 0) {
            uint16_t handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
//            printf("Lock starvation prevention code was executed stage 1\n");
            if(lock_pending_num.load() > 0 && !timer_on){
                timer_begin = std::chrono::high_resolution_clock::now();
            }
            if ( handover_degree > STARVATION_THRESHOLD || timer_alarmed.load()){
                printf("Lock starvation prevention code was executed stage 2, page_adr is %p\n", page_addr);
                fflush(stdout);
                // make sure only one thread release the global latch successfully by double check lock.
                rw_mtx.unlock_shared();
                rw_mtx.lock(RDMA_Manager::thread_id+256);
                Invalid_local_by_cached_mes(page_addr, page_size, lock_addr, mr, true);
                rw_mtx.unlock();
                return;
            }
        }
        rw_mtx.unlock_shared();



    }

    void Cache_Handle::updater_pre_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *&mr) {

        if (rdma_mg == nullptr){
            rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        }
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
        if (remote_lock_urged.load() > 0){
            lock_pending_num.fetch_add(1);
            uint16_t handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
            while (handover_degree > STARVATION_THRESHOLD || timer_alarmed.load()){
                //wait here by no ops
                handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
                asm volatile("pause\n": : :"memory");
            }
            rw_mtx.lock(RDMA_Manager::thread_id+512);
            lock_pending_num.fetch_sub(1);
            write_lock_counter.fetch_add(1);
        }else{
            rw_mtx.lock(RDMA_Manager::thread_id+512);
        }
//        lock_pending_num.fetch_add(1);
//        rw_mtx.lock();
#ifdef LOCAL_LOCK_DEBUG
        {
            std::unique_lock<std::mutex> lck(holder_id_mtx);
            holder_ids.insert(RDMA_Manager::thread_id);
        }
#endif
#ifndef NDEBUG
            bool hint_of_existence = false;
#endif
            if(value)
            {
#ifndef NDEBUG
                hint_of_existence = true;
#endif
                // This means the page has already be in the cache.
                mr = (ibv_mr*)value;
                //TODO: delete the line below.
//                assert(handle->remote_lock_status != 0);
            }else{
#ifndef NDEBUG
                hint_of_existence = false;
#endif
                // This means the page was not in the cache before
                mr = new ibv_mr{};
                rdma_mg->Allocate_Local_RDMA_Slot(*mr, Regular_Page);
                assert(remote_lock_status == 0);

//        printf("Allocate slot for page 1, the page global pointer is %p , local pointer is  %p, hash value is %lu level is %d\n",
//               page_addr, mr->addr, HashSlice(page_id), level);
                value = mr;

            }
            // If the remote read lock is not on, lock it
            if (remote_lock_status == 0){
                cache_miss[RDMA_Manager::thread_id][0]++;
                rdma_mg->global_Wlock_and_read_page_with_INVALID(mr, page_addr, page_size, lock_addr, cas_mr);
                remote_lock_status.store(2);
//                handle->remote_lock_status.store(2);

            }else if (remote_lock_status == 1){
                cache_miss[RDMA_Manager::thread_id][0]++;
//                cache_hit_valid[RDMA_Manager::thread_id][0]++;
                if (!global_Rlock_update(mr, lock_addr, cas_mr)){
                    remote_lock_status.store(0);
                    //TODO: first unlock the read lock and then acquire the write lock is not atomic. this
                    // is problematice if we want to upgrade the lock during a transaction.
                    // May be we can take advantage of the lock starvation bit to solve this problem.
                    //the Read lock has been released, we can directly acquire the write lock
                    rdma_mg->global_Wlock_and_read_page_with_INVALID(mr, page_addr, page_size, lock_addr, cas_mr);
                    remote_lock_status.store(2);
                }else{
                    assert( remote_lock_status.load() == 2);
                    //TODO:
                }
            }else{
                cache_hit_valid[RDMA_Manager::thread_id][0]++;

            }

    }
    bool Cache_Handle::try_upgrade_pre_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *&mr) {

        if (rdma_mg == nullptr){
            rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        }
        assert(rw_mtx.issharelocked());
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
        if (remote_lock_urged.load() > 0){
            lock_pending_num.fetch_add(1);
//            uint16_t handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
//            while (handover_degree > STARVATION_THRESHOLD || timer_alarmed.load()){
//                //wait here by no ops
//                handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
//                asm volatile("pause\n": : :"memory");
//            }
            if (!rw_mtx.try_upgrade()){
                lock_pending_num.fetch_sub(1);
                return false;
            }
//            rw_mtx.lock(RDMA_Manager::thread_id+512);
            write_lock_counter.fetch_add(1);
            lock_pending_num.fetch_sub(1);
            //The continous access counter can not be added here, because the lock may be abandoned later.
        }else{
            if (!rw_mtx.try_upgrade()){
                return false;
            }
//            rw_mtx.lock(RDMA_Manager::thread_id+512);
        }
//        lock_pending_num.fetch_add(1);
//        rw_mtx.lock();
#ifdef LOCAL_LOCK_DEBUG
        {
            std::unique_lock<std::mutex> lck(holder_id_mtx);
            holder_ids.insert(RDMA_Manager::thread_id);
        }
#endif
#ifndef NDEBUG
        bool hint_of_existence = false;
#endif
        if(value)
        {
#ifndef NDEBUG
            hint_of_existence = true;
#endif
            // This means the page has already be in the cache.
            mr = (ibv_mr*)value;
            //TODO: delete the line below.
//                assert(handle->remote_lock_status != 0);
        }else{
#ifndef NDEBUG
            hint_of_existence = false;
#endif
            // This means the page was not in the cache before
            mr = new ibv_mr{};
            rdma_mg->Allocate_Local_RDMA_Slot(*mr, Regular_Page);
            assert(remote_lock_status == 0);

//        printf("Allocate slot for page 1, the page global pointer is %p , local pointer is  %p, hash value is %lu level is %d\n",
//               page_addr, mr->addr, HashSlice(page_id), level);
            value = mr;

        }
        // If the remote read lock is not on, lock it
        if (remote_lock_status == 0){
            cache_miss[RDMA_Manager::thread_id][0]++;
            if (!rdma_mg->global_Wlock_and_read_page_with_INVALID(mr, page_addr, page_size, lock_addr, cas_mr, 5)){
                rw_mtx.unlock();
                return false;
            }
            remote_lock_status.store(2);
//                handle->remote_lock_status.store(2);

        }else if (remote_lock_status == 1){
            cache_miss[RDMA_Manager::thread_id][0]++;
//                cache_hit_valid[RDMA_Manager::thread_id][0]++;
            if (!global_Rlock_update(mr, lock_addr, cas_mr)){
                rw_mtx.unlock();
                remote_lock_status.store(0);
                return false;
                //TODO: first unlock the read lock and then acquire the write lock is not atomic. this
                // is problematice if we want to upgrade the lock during a transaction.
                // May be we can take advantage of the lock starvation bit to solve this problem.
                //the Read lock has been released, we can directly acquire the write lock
                rdma_mg->global_Wlock_and_read_page_with_INVALID(mr, page_addr, page_size, lock_addr, cas_mr);
                remote_lock_status.store(2);
            }else{

                assert( remote_lock_status.load() == 2);

            }
        }else{
            cache_hit_valid[RDMA_Manager::thread_id][0]++;

        }
//        if(remote_lock_urged.load() > 0){
//            write_lock_counter.fetch_add(1);
//        }

        return true;
    }
    bool Cache_Handle::try_updater_pre_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *&mr) {

        if (rdma_mg == nullptr){
            rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        }
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
        if (remote_lock_urged.load() > 0){
            lock_pending_num.fetch_add(1);
            // TODO: delete the lcok pending and the waiting mechanims below. if there is a blocking.
//            uint16_t handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
//            while (handover_degree > STARVATION_THRESHOLD || timer_alarmed.load()){
//                //wait here by no ops
//                handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
//                asm volatile("pause\n": : :"memory");
//            }
            if (!rw_mtx.try_lock()){
                lock_pending_num.fetch_sub(1);
                return false;
            }
//            rw_mtx.lock(RDMA_Manager::thread_id+512);
            write_lock_counter.fetch_add(1);
            lock_pending_num.fetch_sub(1);
        }else{
            if (!rw_mtx.try_lock()){
                return false;
            }
//            rw_mtx.lock(RDMA_Manager::thread_id+512);
        }
//        lock_pending_num.fetch_add(1);
//        rw_mtx.lock();
#ifdef LOCAL_LOCK_DEBUG
        {
            std::unique_lock<std::mutex> lck(holder_id_mtx);
            holder_ids.insert(RDMA_Manager::thread_id);
        }
#endif
#ifndef NDEBUG
            bool hint_of_existence = false;
#endif
            if(value)
            {
#ifndef NDEBUG
                hint_of_existence = true;
#endif
                // This means the page has already be in the cache.
                mr = (ibv_mr*)value;
                //TODO: delete the line below.
//                assert(handle->remote_lock_status != 0);
            }else{
#ifndef NDEBUG
                hint_of_existence = false;
#endif
                // This means the page was not in the cache before
                mr = new ibv_mr{};
                rdma_mg->Allocate_Local_RDMA_Slot(*mr, Regular_Page);
                assert(remote_lock_status == 0);

//        printf("Allocate slot for page 1, the page global pointer is %p , local pointer is  %p, hash value is %lu level is %d\n",
//               page_addr, mr->addr, HashSlice(page_id), level);
                value = mr;

            }
            // If the remote read lock is not on, lock it
            if (remote_lock_status == 0){
                cache_miss[RDMA_Manager::thread_id][0]++;
                if (!rdma_mg->global_Wlock_and_read_page_with_INVALID(mr, page_addr, page_size, lock_addr, cas_mr, 5)){
                    rw_mtx.unlock();
                    return false;
                }
                remote_lock_status.store(2);
//                handle->remote_lock_status.store(2);

            }else if (remote_lock_status == 1){
                cache_miss[RDMA_Manager::thread_id][0]++;
//                cache_hit_valid[RDMA_Manager::thread_id][0]++;
                if (!global_Rlock_update(mr, lock_addr, cas_mr)){
                    rw_mtx.unlock();
                    remote_lock_status.store(0);
                    return false;
                    //TODO: first unlock the read lock and then acquire the write lock is not atomic. this
                    // is problematice if we want to upgrade the lock during a transaction.
                    // May be we can take advantage of the lock starvation bit to solve this problem.
                    //the Read lock has been released, we can directly acquire the write lock
                    rdma_mg->global_Wlock_and_read_page_with_INVALID(mr, page_addr, page_size, lock_addr, cas_mr);
                    remote_lock_status.store(2);
                }else{

                    assert( remote_lock_status.load() == 2);

                }
            }else{
                cache_hit_valid[RDMA_Manager::thread_id][0]++;

            }
            //TODO: If the code below not work, delete the waiting code before try the local latch.
//        if (remote_lock_urged.load() > 0){
//            write_lock_counter.fetch_add(1);
//        }
        return true;
    }
    void Cache_Handle::upgrade_pre_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *&mr) {
        assert(rw_mtx.issharelocked());
        assert(remote_lock_status == 1);
        if (rdma_mg == nullptr){
            rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        }
        rw_mtx.unlock_shared();
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
        if (remote_lock_urged.load() > 0){
            lock_pending_num.fetch_add(1);
            uint16_t handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
            while (handover_degree > STARVATION_THRESHOLD || timer_alarmed.load()){
                //wait here by no ops
                handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
                asm volatile("pause\n": : :"memory");
            }
            rw_mtx.lock(RDMA_Manager::thread_id+640);
            lock_pending_num.fetch_sub(1);
            write_lock_counter.fetch_add(1);
        }else{
            rw_mtx.lock(RDMA_Manager::thread_id+640);
        }
//        lock_pending_num.fetch_add(1);
//        rw_mtx.lock();
#ifdef LOCAL_LOCK_DEBUG
        {
            std::unique_lock<std::mutex> lck(holder_id_mtx);
            holder_ids.insert(RDMA_Manager::thread_id);
        }
#endif
#ifndef NDEBUG
            bool hint_of_existence = false;
#endif
            if(value)
            {
#ifndef NDEBUG
                hint_of_existence = true;
#endif
                // This means the page has already be in the cache.
                mr = (ibv_mr*)value;
                //TODO: delete the line below.
//                assert(handle->remote_lock_status != 0);
            }else{
#ifndef NDEBUG
                hint_of_existence = false;
#endif
                // This means the page was not in the cache before
                mr = new ibv_mr{};
                rdma_mg->Allocate_Local_RDMA_Slot(*mr, Regular_Page);
                assert(remote_lock_status == 0);

//        printf("Allocate slot for page 1, the page global pointer is %p , local pointer is  %p, hash value is %lu level is %d\n",
//               page_addr, mr->addr, HashSlice(page_id), level);
                value = mr;

            }
            // If the remote read lock is not on, lock it
            if (remote_lock_status == 0){
                cache_miss[RDMA_Manager::thread_id][0]++;
                rdma_mg->global_Wlock_and_read_page_with_INVALID(mr, page_addr, page_size, lock_addr, cas_mr);
                remote_lock_status.store(2);
//                handle->remote_lock_status.store(2);

            }else if (remote_lock_status == 1){
                cache_miss[RDMA_Manager::thread_id][0]++;
//                cache_hit_valid[RDMA_Manager::thread_id][0]++;
                if (!global_Rlock_update(mr, lock_addr, cas_mr)){
                    remote_lock_status.store(0);
                    //TODO: first unlock the read lock and then acquire the write lock is not atomic. this
                    // is problematice if we want to upgrade the lock during a transaction.
                    // May be we can take advantage of the lock starvation bit to solve this problem.
                    //the Read lock has been released, we can directly acquire the write lock
                    rdma_mg->global_Wlock_and_read_page_with_INVALID(mr, page_addr, page_size, lock_addr, cas_mr);
                    remote_lock_status.store(2);
                }else{
                    assert( remote_lock_status.load() == 2);
                    //TODO:
                }
            }else{
                cache_hit_valid[RDMA_Manager::thread_id][0]++;

            }

    }

    void Cache_Handle::updater_writer_post_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *&mr) {

//        if (strategy == 1){
#ifdef LOCAL_LOCK_DEBUG
        {
            std::unique_lock<std::mutex> lck(holder_id_mtx);
            holder_ids.erase(RDMA_Manager::thread_id);
        }
#endif
        if (remote_lock_urged.load() > 0) {
//            printf("Lock starvation prevention code was executed stage 1\n");
            uint16_t handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
            if(lock_pending_num.load() > 0 && !timer_on){
                timer_begin = std::chrono::high_resolution_clock::now();
                timer_on.store(true);
            }
            assert(remote_lock_status == 2);
            if ( handover_degree > STARVATION_THRESHOLD || timer_alarmed.load()){
                Invalid_local_by_cached_mes(page_addr, page_size, lock_addr, mr, true);
            }
        }
        rw_mtx.unlock();
    }
    //This function should be used with caution. only used when there on the new allocated cache line. and used only once per page.
    void Cache::Handle::writer_pre_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *&mr) {

        if (rdma_mg == nullptr){
                rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        }
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
        if (remote_lock_urged.load() > 0){
            lock_pending_num.fetch_add(1);
            uint16_t handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
            while (handover_degree > STARVATION_THRESHOLD || timer_alarmed.load()){
                //wait here by no ops
                handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
                asm volatile("pause\n": : :"memory");
            }
            rw_mtx.lock(RDMA_Manager::thread_id+768);
            lock_pending_num.fetch_sub(1);
            write_lock_counter.fetch_add(1);
        }else{
            rw_mtx.lock(RDMA_Manager::thread_id+768);
        }
#ifdef LOCAL_LOCK_DEBUG

        {
            std::unique_lock<std::mutex> lck(holder_id_mtx);
            holder_ids.insert(RDMA_Manager::thread_id);
        }
#endif
#ifndef NDEBUG
            bool hint_of_existence = false;
#endif
            if(value)
            {
#ifndef NDEBUG
                hint_of_existence = true;
#endif
                // This means the page has already be in the cache.
                mr = (ibv_mr*)value;
                //TODO: delete the line below.
//                assert(handle->remote_lock_status != 0);
            }else{
#ifndef NDEBUG
                hint_of_existence = false;
#endif
                // This means the page was not in the cache before
                mr = new ibv_mr{};
                rdma_mg->Allocate_Local_RDMA_Slot(*mr, Regular_Page);
                assert(remote_lock_status == 0);

//        printf("Allocate slot for page 1, the page global pointer is %p , local pointer is  %p, hash value is %lu level is %d\n",
//               page_addr, mr->addr, HashSlice(page_id), level);
                value = mr;

            }
            // If the remote read lock is not on, lock it
            if (remote_lock_status == 0){
                cache_miss[RDMA_Manager::thread_id][0]++;
                rdma_mg->global_Wlock_with_INVALID(mr, page_addr, page_size, lock_addr, cas_mr);
                remote_lock_status.store(2);
//                handle->remote_lock_status.store(2);

            }else if (remote_lock_status == 1){
                cache_miss[RDMA_Manager::thread_id][0]++;

//                cache_hit_valid[RDMA_Manager::thread_id][0]++;
                if (!global_Rlock_update(mr, lock_addr, cas_mr)){
//
                    //TODO: first unlock the read lock and then acquire the write lock is not atomic. this
                    // is problematice if we want to upgrade the lock during a transaction.
                    // May be we can take advantage of the lock starvation bit to solve this problem.
                    //the Read lock has been released, we can directly acquire the write lock
                    rdma_mg->global_Wlock_with_INVALID(mr, page_addr, page_size, lock_addr, cas_mr);
                    remote_lock_status.store(2);
                }else{
//                    cache_hit_valid[RDMA_Manager::thread_id][0]++;
                    assert( remote_lock_status.load() == 2);
                    //TODO:
                }
            }else{
                cache_hit_valid[RDMA_Manager::thread_id][0]++;
            }
    }

    bool
    Cache_Handle::global_Rlock_update(ibv_mr * local_mr, GlobalAddress lock_addr, ibv_mr *cas_buffer, CoroContext *cxt,
                                       int coro_id) {
        assert(remote_lock_status.load() == 1);
        bool succfully_updated = rdma_mg->global_Rlock_update(local_mr, lock_addr, cas_buffer, cxt, coro_id);
        if (succfully_updated){
            remote_lock_status.store(2);
//            assert(gptr == (((LeafPage<uint64_t ,uint64_t>*)(((ibv_mr*)value)->addr))->hdr.this_page_g_ptr));
            return true;
        }else{
            assert(remote_lock_status.load() == 1);
            return false;

        }
    }
    //Deprecated temporarilly.
    void
    Cache_Handle::writer_post_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *&mr) {
        assert(false);



#ifdef LOCAL_LOCK_DEBUG
        {
            std::unique_lock<std::mutex> lck(holder_id_mtx);
            holder_ids.insert(RDMA_Manager::thread_id);
        }
#endif
        //the mechanism to avoid global lock starvation be local latch
        if (remote_lock_urged.load() > 0) {
            uint16_t handover_degree = write_lock_counter.load() + read_lock_counter.load()/PARALLEL_DEGREE;
            if(lock_pending_num.load() > 0 && !timer_on){
                timer_begin = std::chrono::high_resolution_clock::now();
            }
            assert(remote_lock_status == 2);
            if ( handover_degree > STARVATION_THRESHOLD || timer_alarmed.load()){
                Invalid_local_by_cached_mes(page_addr, page_size, lock_addr, mr, true);
            }
        }
        rw_mtx.unlock();
    }
//shall be protected by latch outside
    void Cache_Handle::Invalid_local_by_cached_mes(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr,
                                                    ibv_mr *mr, bool need_spin) {
        state_mtx.lock();
        if (this->remote_lock_status == 1){
            printf("High pririty invalidation message receive, target gcl is %p\n", page_addr);
            fflush(stdout);
            rdma_mg->global_RUnlock(lock_addr, rdma_mg->Get_local_CAS_mr(), false, nullptr, nullptr, 0);
            remote_lock_status.store(0);
            //spin wait to delay the global latch acquire for other thread and then to prevent write lock starvation.
            // However, it is possible the other thread has already enter the critical section and see the global latch is 0.
            // In this case, that thread can immediately issue  a global read lacth request. Then the remote writer is still starved.
            // Since this scenario will happen rarely, then we think this method can relieve the latch starvation to some extense.
//                    if (remote_lock_urged.load() > 1){
            if (need_spin){
                spin_wait_us(STARV_SPIN_BASE* (1 + starvation_priority.load()));
            }
//                    }
        }else if (this->remote_lock_status == 2){
            if (remote_lock_urged == 2){
                //cache downgrade from Modified to Shared rather than release the lock.
                rdma_mg->global_write_page_and_WdowntoR(mr, page_addr, page_size, lock_addr);
                remote_lock_status.store(1);
            }else{
//                        printf("Lock starvation prevention code was executed stage 3\n");
                if (starvation_priority == 0 || next_holder_id == Invalid_Node_ID){
                    // lock release to a specific writer
                    rdma_mg->global_write_page_and_Wunlock(mr, page_addr, page_size, lock_addr);
                    remote_lock_status.store(0);
                    if (need_spin){
                        spin_wait_us(STARV_SPIN_BASE* (1 + starvation_priority.load()));
                    }

                }else{
#ifdef GLOBAL_HANDOVER
                    assert(next_holder_id != RDMA_Manager::node_id);
                    printf("Global lock for page %p handover from node %u to node %u part 2, starvation level is %d\n", page_addr, rdma_mg->node_id, next_holder_id.load(), starvation_priority.load());
                    fflush( stdout );
                    rdma_mg->global_write_page_and_WHandover(mr, page_addr, page_size, next_holder_id.load(), lock_addr,
                                                             false, nullptr);
                    remote_lock_status.store(0);
#else
                    rdma_mg->global_write_page_and_Wunlock(mr, page_addr, page_size, lock_addr);
                            remote_lock_status.store(0);
                            spin_wait_us(STARV_SPIN_BASE* (1 + starvation_priority.load()));
#endif
//                            spin_wait_us(STARV_SPIN_BASE* (1 + starvation_priority.load()));
                }

            }

        }else{
            //The lock has been released by other threads.
        }
        clear_states();
        state_mtx.unlock();
    }

}  // namespace DSMEngine
