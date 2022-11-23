// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "DSMEngine/cache.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>

#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/hash.h"
#include "util/mutexlock.h"
#include "HugePageAlloc.h"
namespace DSMEngine {

Cache::~Cache() {}

namespace {



// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class HandleTable {
 public:
  HandleTable() : length_(0), elems_(0), list_(nullptr) { Resize(); }
  ~HandleTable() { delete[] list_; }

  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }
    //
  LRUHandle* Insert(LRUHandle* h) {
    LRUHandle** ptr = FindPointer(h->key(), h->hash);
    LRUHandle* old = *ptr;
    // if we find a LRUhandle whose key is same as h, we replace that LRUhandle
    // with h, if not find, we just put the h at the end of the list.
    h->next_hash = (old == nullptr ? nullptr : old->next_hash);
    *ptr = h;
    if (old == nullptr) {
      ++elems_;
      if (elems_ > length_) { // length_ is the size limit for current cache.
        // Since each table_cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }
    return old;
  }

  LRUHandle* Remove(const Slice& key, uint32_t hash) {
#ifndef NDEBUG
      GlobalAddress gprt = (*(GlobalAddress*)(key.data()));
      if (gprt.offset < 9480863232){
          printf("page of %lu is removed from the cache table", gprt.offset);
      }
#endif
    LRUHandle** ptr = FindPointer(key, hash);
    LRUHandle* result = *ptr;
    //TODO: only erase those lru handles which has been accessed. THis can prevent
    // a index block being invalidated multiple times.
    if (result != nullptr) {
        //*ptr belongs to the Handle previous to the result.
      *ptr = result->next_hash;// ptr is the "next_hash" in the handle previous to the result
      --elems_;
    }
      assert(result!= nullptr);
    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of table_cache entries that hash into the bucket.
  uint32_t length_;
  uint32_t elems_;
  LRUHandle** list_;

  // Return a pointer to slot that points to a table_cache entry that
  // matches key/hash.  If there is no such table_cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = &list_[hash & (length_ - 1)];
    while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    // This iterator will stop at the LRUHandle whose next_hash is nullptr or its nexthash's
    // key and hash value is the target.
    return ptr;
  }

  void Resize() {
    uint32_t new_length = 4;// it originally is 4
    while (new_length < elems_) {
      new_length *= 2;
    }
    LRUHandle** new_list = new LRUHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    //TOTHINK: will each element in list_ be supposed to have only one element?
    // Probably yes.
    for (uint32_t i = 0; i < length_; i++) {
      LRUHandle* h = list_[i];
      while (h != nullptr) {
        LRUHandle* next = h->next_hash;
        uint32_t hash = h->hash;
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

// A single shard of sharded table_cache.
class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, uint32_t hash, void* value,
                        size_t charge,
                        void (*deleter)(const GlobalAddress, void* value, int strategy, int lock_mode));
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  Cache::Handle* LookupInsert(const Slice& key, uint32_t hash, void* value,
                              size_t charge,
                              void (*deleter)(const GlobalAddress, void* value, int strategy, int lock_mode));
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);
  void Prune();
  size_t TotalCharge() const {
//    MutexLock l(&mutex_);
//    ReadLock l(&mutex_);
    SpinLock l(&mutex_);
    return usage_;
  }

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle* list, LRUHandle* e);
  void Ref(LRUHandle* e);
//    void Ref_in_LookUp(LRUHandle* e);
    void Unref(LRUHandle *e, SpinLock *spin_l);
//    void Unref_WithoutLock(LRUHandle* e);
    bool FinishErase(LRUHandle *e, SpinLock *spin_l) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Initialized before use.
  size_t capacity_;

  // mutex_ protects the following state.
//  mutable port::RWMutex mutex_;
    mutable SpinMutex mutex_;
  size_t usage_ GUARDED_BY(mutex_);

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // Entries have refs==1 and in_cache==true.
  LRUHandle lru_ GUARDED_BY(mutex_);

  // Dummy head of in-use list.
  // Entries are in use by clients, and have refs >= 2 and in_cache==true.
  LRUHandle in_use_ GUARDED_BY(mutex_);

  HandleTable table_ GUARDED_BY(mutex_);
};

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
    void LRUCache::Ref(LRUHandle* e) {
        if (e->refs == 1 && e->in_cache) {  // If on lru_ list, move to in_use_ list.
            LRU_Remove(e);
            LRU_Append(&in_use_, e);
        }
        e->refs++;
    }

// THere should be no lock outside
//void LRUCache::Ref_in_LookUp(LRUHandle* e) {
//    //TODO: Update the read lock to a write lock within the if predicate
//  if (e->refs.load() == 1 && e->in_cache.load()) {  // If on lru_ list, move to in_use_ list.
//      mutex_.ReadUnlock();
//      mutex_.WriteLock();
//      if (e->refs.load() == 1 && e->in_cache.load()) {
//          LRU_Remove(e);
//          LRU_Append(&in_use_, e);
//          e->refs.fetch_add(1);
//          mutex_.WriteUnlock();
//          return;
//      }
//      e->refs.fetch_add(1);
//      assert(e->in_cache.load());
//      mutex_.WriteUnlock();
//      return;
//  }
////  e->refs++;
//    e->refs.fetch_add(1);
//    assert(e->in_cache.load());
//    mutex_.ReadUnlock();
//}

void LRUCache::Unref(LRUHandle *e, SpinLock *spin_l) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs == 0) {  // Deallocate.
      //Finish erase will only goes here, or directly return. it will never goes to next if clause
//        mutex_.unlock();

      if (spin_l!= nullptr){
          //Early releasing the lock to avoid the RDMA lock releasing in the critical section.
          spin_l->Unlock();
      }
      assert(!e->in_cache);
    (*e->deleter)(e->gptr, e->value, e->strategy, e->remote_lock_status);
//    free(e);
    delete e;
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
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e) {
//#ifndef NDEBUG
//    if (e->gptr.offset < 9480863232){
//        printf("page %lu is being append to a LRU list", e->gptr.offset);
//    }
//#endif
  // Make "e" newest entry by inserting just before *list
  e->next = list;
  e->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
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
        Ref(e);
    }
    return reinterpret_cast<Cache::Handle*>(e);
}
Cache::Handle *DSMEngine::LRUCache::LookupInsert(const Slice &key, uint32_t hash, void *value, size_t charge,
                                                 void (*deleter)(const GlobalAddress, void* value, int strategy, int lock_mode)) {
    SpinLock l(&mutex_);
    //TOTHINK(ruihong): shoul we update the lru list after look up a key?
    //  Answer: Ref will refer this key and later, the outer function has to call
    // Unref or release which will update the lRU list.
    LRUHandle* e = table_.Lookup(key, hash);
    if (e != nullptr) {
        Ref(e);
//        assert(e->refs <=2);
        DEBUG_PRINT("cache hit when searching the leaf node");
        return reinterpret_cast<Cache::Handle*>(e);
    }else{
        // This LRU handle is not initialized.
        e = new LRUHandle();
//                reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size()));

        e->value = value;
        e->remote_lock_status = 0;
        e->remote_lock_urge = false;
        e->strategy = 1;
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
        if (!l.check_own()){
            l.Lock();
        }
        assert(usage_ <= capacity_ + kLeafPageSize + kInternalPageSize);
        // This will remove some entry from LRU if the table_cache over size.
        while (usage_ > capacity_ && lru_.next != &lru_) {
            LRUHandle* old = lru_.next;
            assert(old->refs == 1);
//#ifndef NDEBUG
//            if (old->gptr.offset < 9480863232){
//                printf("page of %lu is extracted from the LRUlist", e->gptr.offset);
//            }
//#endif
            bool erased = FinishErase(table_.Remove(old->key(), old->hash), &l);
            if (!l.check_own()){
                l.Lock();
            }
            if (!erased) {  // to avoid unused variable when compiled NDEBUG
                assert(erased);
            }

        }

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
//If the inserted key has already existed, then the old LRU handle will be removed from
// the cache, but it may not garbage-collected right away.
Cache::Handle* LRUCache::Insert(const Slice& key, uint32_t hash, void* value,
                                size_t charge,
                                void (*deleter)(const GlobalAddress, void* value, int strategy, int lock_mode)) {
//  MutexLock l(&mutex_);

  //TODO: set the LRUHandle within the page, so that we can check the reference, during the direct access, or we reserver
  // a place hodler for the address pointer to the LRU handle of the page.
  LRUHandle* e = new LRUHandle();
//      reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size()));

    e->remote_lock_status = 0;
    e->remote_lock_urge = false;
    e->strategy = 1;
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
    if (!l.check_own()){
        l.Lock();
    }
        assert(usage_ <= capacity_ + kLeafPageSize + kInternalPageSize);
  // This will remove some entry from LRU if the table_cache over size.
  while (usage_ > capacity_ && lru_.next != &lru_) {

    LRUHandle* old = lru_.next;
    assert(old->refs == 1);
    bool erased = FinishErase(table_.Remove(old->key(), old->hash), &l);
    //some times the finsih Erase will release the spinlock to let other threads working during the RDMA lock releasing.
      if (!l.check_own()){
          l.Lock();
      }
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
//      assert(e->refs <=2);
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




        static const int kNumShardBits = 6;
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
                 void (*deleter)(const GlobalAddress, void* value, int strategy, int lock_mode)) override {
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
  Handle* Lookup(const Slice& key) override {
      assert(capacity_ >= 1000);
    const uint32_t hash = HashSlice(key);

//    auto handle = shard_[Shard(hash)].Lookup(key, hash);
//      printf("Look up: refer to handle %p\n", handle);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
    Handle* LookupInsert(const Slice& key,  void* value,
                         size_t charge,
                         void (*deleter)(const GlobalAddress, void* value, int strategy, int lock_mode)) override{

                assert(capacity_ >= 1000);
                const uint32_t hash = HashSlice(key);
                return shard_[Shard(hash)].LookupInsert(key, hash, value, charge, deleter);
  };
  void Release(Handle* handle) override {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
//      printf("release handle %p\n", handle);
    shard_[Shard(h->hash)].Release(handle);
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

}  // end anonymous namespace

Cache* NewLRUCache(size_t capacity) { return new ShardedLRUCache(capacity); }


LocalBuffer::LocalBuffer(const CacheConfig &cache_config) {
        size = cache_config.cacheSize;
        data = (uint64_t)hugePageAlloc(size * define::GB);
    if (data == 0){
        data = (int64_t) malloc(size * define::GB);
    }
}

}  // namespace DSMEngine
