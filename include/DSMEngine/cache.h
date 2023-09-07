// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Cache is an interface that maps keys to values.  It has internal
// synchronization and may be safely accessed concurrently from
// multiple threads.  It may automatically evict entries to make room
// for new entries.  Values have a specified charge against the table_cache
// capacity.  For example, a table_cache where the values are variable
// length strings, may use the length of the string as the charge for
// the string.
//
// A builtin table_cache implementation with a least-recently-used eviction
// policy is provided.  Clients may use their own implementations if
// they want something more sophisticated (like scan-resistance, a
// custom eviction policy, variable table_cache sizing, etc.)

#ifndef STORAGE_DSMEngine_INCLUDE_CACHE_H_
#define STORAGE_DSMEngine_INCLUDE_CACHE_H_

#include <cstdint>

#include "DSMEngine/export.h"
#include "DSMEngine/slice.h"
#include <shared_mutex>

#include "Config.h"
#include "util/rdma.h"

namespace DSMEngine {
//class RDMA_Manager;
//class ibv_mr;


    // LRU table_cache implementation
//
// Cache entries have an "in_cache" boolean indicating whether the table_cache has a
// reference on the entry.  The only ways that this can become false without the
// entry being passed to its "deleter" are via Erase(), via Insert() when
// an element with a duplicate key is inserted, or on destruction of the table_cache.
//
// The table_cache keeps two linked lists of items in the table_cache.  All items in the
// table_cache are in one list or the other, and never both.  Items still referenced
// by clients but erased from the table_cache are in neither list.  The lists are:
// - in-use:  contains the items currently referenced by clients, in no
//   particular order.  (This list is used for invariant checking.  If we
//   removed the check, elements that would otherwise be on this list could be
//   left as disconnected singleton lists.)
// - LRU:  contains the items not currently referenced by clients, in LRU order
// Elements are moved between these lists by the Ref() and Unref() methods,
// when they detect an element in the table_cache acquiring or losing its only
// external reference.

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.

class DSMEngine_EXPORT Cache;

// Create a new table_cache with a fixed size capacity.  This implementation
// of Cache uses a least-recently-used eviction policy.
DSMEngine_EXPORT Cache* NewLRUCache(size_t capacity);
class DSMEngine_EXPORT Cache {
 public:
  Cache() = default;
    struct Handle {
    public:
        void* value = nullptr;
        std::atomic<uint32_t> refs;     // References, including table_cache reference, if present.
        //TODO: the internal node may not need the rw_mtx below, maybe we can delete them.
        std::atomic<bool> remote_lock_urge;
        std::atomic<int> remote_lock_status = 0; // 0 unlocked, 1 read locked, 2 write lock
        GlobalAddress gptr = GlobalAddress::Null();
        std::atomic<int> strategy = 1; // strategy 1 normal read write locking without releasing, strategy 2. Write lock with release, optimistic latch free read.
        bool keep_the_mr = false;
        std::shared_mutex rw_mtx;
        RDMA_Manager* rdma_mg = nullptr;
        void (*deleter)(Cache::Handle* handle);
        ~Handle(){}
        void reader_pre_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *&mr);
        void reader_post_access(GlobalAddress lock_addr);
        void writer_pre_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *&mr);
        void writer_post_access(GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr, ibv_mr *&mr);
        bool global_Rlock_update(GlobalAddress lock_addr, ibv_mr *cas_buffer, CoroContext *cxt = nullptr, int coro_id = 0);
    };
  Cache(const Cache&) = delete;
  Cache& operator=(const Cache&) = delete;

  // Destroys all existing entries by calling the "deleter"
  // function that was passed to the constructor.
  virtual ~Cache();

  // Opaque handle to an entry stored in the table_cache.

  virtual size_t GetCapacity() = 0;

  //TODO: change the slice key to GlobalAddress& key.

  // Insert a mapping from key->value into the table_cache and assign it
  // the specified charge against the total table_cache capacity.
  //
  // Returns a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  //
  // When the inserted entry is no longer needed, the key and
  // value will be passed to "deleter".
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(Cache::Handle* handle)) = 0;

  // If the table_cache has no mapping for "key", returns nullptr.
  //
  // Else return a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  virtual Handle* Lookup(const Slice& key) = 0;
  //Atomic cache look up and Insert a new handle atomically for the key if not found.
  virtual Handle* LookupInsert(const Slice& key, void* value,
                                size_t charge,
                                void (*deleter)(Cache::Handle* handle)) = 0;
  // Release a mapping returned by a previous Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void Release(Handle* handle) = 0;

  // Return the value encapsulated in a handle returned by a
  // successful Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void* Value(Handle* handle) = 0;

  // If the table_cache contains entry for key, erase it.  Note that the
  // underlying entry will be kept around until all existing handles
  // to it have been released.
  virtual void Erase(const Slice& key) = 0;
//TODO: a new function which pop out a least recent used entry. which will be reused later.

  // Return a new numeric id.  May be used by multiple clients who are
  // sharing the same table_cache to partition the key space.  Typically the
  // client will allocate a new id at startup and prepend the id to
  // its table_cache keys.
  virtual uint64_t NewId() = 0;

  // Remove all table_cache entries that are not actively in use.  Memory-constrained
  // applications may wish to call this method to reduce memory usage.
  // Internal_and_Leaf implementation of Prune() does nothing.  Subclasses are strongly
  // encouraged to override the default implementation.  A future release of
  // DSMEngine may change Prune() to a pure abstract method.
  virtual void Prune() {}

  // Return an estimate of the combined charges of all elements stored in the
  // table_cache.
  virtual size_t TotalCharge() const = 0;

 private:
  void LRU_Remove(Handle* e);
  void LRU_Append(Handle* e);
  void Unref(Handle* e);

  struct Rep;
  Rep* rep_;
};
    struct LRUHandle : Cache::Handle {


        LRUHandle* next_hash;// Next LRUhandle in the hash
        LRUHandle* next;
        LRUHandle* prev;
        size_t charge;  // TODO(opt): Only allow uint32_t?
        size_t key_length;
        std::atomic<bool> in_cache;     // Whether entry is in the table_cache.
        uint32_t hash;     // Hash of key(); used for fast sharding and comparisons
//        char key_data[1];  // Beginning of key

        Slice key() const {
            // next_ is only equal to this if the LRU handle is the list head of an
            // empty list. List heads never have meaningful keys.
            assert(next != this);
            assert(key_length == 8);
            return Slice((char*)&gptr, key_length);
        }
    };
class LocalBuffer {

    public:
    LocalBuffer(const CacheConfig &cache_config);

        uint64_t data;
        uint64_t size;

    private:
    };

}  // namespace DSMEngine

#endif  // STORAGE_DSMEngine_INCLUDE_CACHE_H_
