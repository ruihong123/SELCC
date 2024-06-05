// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_DSMEngine_UTIL_MUTEXLOCK_H_
#define STORAGE_DSMEngine_UTIL_MUTEXLOCK_H_
#include <assert.h>
#include <atomic>
#include <functional>
#include <mutex>
#include <thread>

#include "port/port.h"

namespace DSMEngine {

// Helper class that locks a mutex on construction and unlocks the mutex when
// the destructor of the MutexLock object is invoked.
//
// Typical usage:
//
//   void MyClass::MyMethod() {
//     MutexLock l(&mu_);       // mu_ is an instance variable
//     ... some complex code, possibly with multiple return paths ...
//   }

class MutexLock {
 public:
  explicit MutexLock(port::Mutex *mu) : mu_(mu) {
    this->mu_->Lock();
  }
  // No copying allowed
  MutexLock(const MutexLock &) = delete;
  void operator=(const MutexLock &) = delete;

  ~MutexLock() { this->mu_->Unlock(); }

 private:
  port::Mutex *const mu_;
};
//
// SpinMutex has very low overhead for low-contention cases.  Method names
// are chosen so you can use std::unique_lock or std::lock_guard with it.
//
class SpinMutex {
 public:
  SpinMutex() : locked_(false) {}
  //TODO this spinmutex have problem, some time 2 thread can walk into the protected code.
  bool try_lock() {
    auto currently_locked = locked_.load(std::memory_order_relaxed);
    return !currently_locked &&
           locked_.compare_exchange_weak(currently_locked, true,
                                         std::memory_order_acquire,
                                         std::memory_order_relaxed);
  }
  void lock() {
    for (size_t tries = 0;; ++tries) {
      if (try_lock()) {
        // success
        break;
      }
        port::AsmVolatilePause();
      if (tries > 10000) {
        //        printf("I tried so many time I got yield\n");
        std::this_thread::yield();
      }
    }
  }

  void unlock() {
    locked_.store(false, std::memory_order_release);
    //    printf("spin mutex unlocked. \n");
  }

  // private:
  std::atomic<bool> locked_;
};
// TODO: This read-write implementation may not 100% guarantee the atomicity, is it?
//  If it is not, we can follow the RDMA shared exclusive latch to implement an local atomic read-write lock.
// Implement a shared-exclusive spin lock with lock, try_lock, unlock, lock_shared, try_lock_shared, and unlock_shared.
class RWSpinLock{
    std::atomic<int> readers_count{0};
    std::atomic<bool> write_now{false};
    size_t thread_id = 0;
public:
    void lock(size_t thread_ID = 128) {
        while (write_now.exchange(true, std::memory_order_acquire)){
            port::AsmVolatilePause();
            std::this_thread::yield();
        }

        // wait for readers to exit
        while (readers_count != 0 ){
            port::AsmVolatilePause();
            std::this_thread::yield();
        }
        thread_id = thread_ID + 1;
    }

    void lock_shared() {
        // unique_lock have priority
        while(true) {
            while (write_now) {     // wait for unlock
                port::AsmVolatilePause();
                std::this_thread::yield();
            }

            readers_count.fetch_add(1, std::memory_order_acquire);

            if (write_now){
                // locked while transaction? Fallback. Go another round
                readers_count.fetch_sub(1, std::memory_order_release);
            } else {
                // all ok
                return;
            }
        }
    }
    // Try atomic upgrade to exclusive latch, if failed, then return false and still hold the shared latch.
    bool try_upgrade(){
        auto currently_locked = false;
        assert(readers_count.load() > 0);
        if (write_now.compare_exchange_strong(currently_locked, true,
                                              std::memory_order_acquire,
                                              std::memory_order_relaxed)){
            if (readers_count.load() == 1){
                readers_count.fetch_sub(1, std::memory_order_release);
                return true;
            }else{
                write_now.store(false, std::memory_order_release);
//                readers_count.fetch_sub(1, std::memory_order_release);
                return false;
            }

        }else{
//            readers_count.fetch_sub(1, std::memory_order_release);
            return false;
        }
    }
    bool try_lock(size_t thread_ID = 128) {
        auto currently_locked = false;

//        auto currently_locked = write_now.load(std::memory_order_relaxed);
//        auto currently_readers = readers_count.load(std::memory_order_relaxed);
//        if (!currently_locked && currently_readers == 0){
        if( write_now.compare_exchange_strong(currently_locked, true,
                                              std::memory_order_acquire,
                                              std::memory_order_relaxed)){
            if (readers_count.load() == 0){
                thread_id = 64+thread_ID;
                return true;
            }else{
                write_now.store(false);
                return false;
            }
        }else{
            return false;
        }
//        }else{
//            return false;
//        }

    }
    bool try_shared_lock() {
        auto currently_locked = write_now.load(std::memory_order_relaxed);
//        auto currently_readers = readers_count.load(std::memory_order_relaxed);
        if (!currently_locked){
            readers_count.fetch_add(1);
            if (write_now.load()){
                readers_count.fetch_sub(1, std::memory_order_release);
                return false;
            }else{
                return true;
            }
        }else{
            return false;
        }

    }

    void unlock() {
        assert(write_now.load(std::memory_order_relaxed) == true);
//        assert(readers_count.load(std::memory_order_relaxed) == 0);
        thread_id = 0;
        write_now.store(false, std::memory_order_release);
    }
    bool islocked(){
        return write_now.load(std::memory_order_relaxed);
    }
    bool issharelocked(){
        return readers_count.load(std::memory_order_relaxed) > 0;
    }


    void unlock_shared() {
        assert(readers_count.load(std::memory_order_relaxed) > 0);
        readers_count.fetch_sub(1, std::memory_order_release);
    }
};

class SpinLock {
 public:

    explicit SpinLock(SpinMutex *mu) : mu_(mu) {
        assert(owns == false);

        this->mu_->lock();
          owns = true;
  }
  // No copying allowed
  SpinLock(const SpinLock &) = delete;
  void operator=(const SpinLock &) = delete;
    void Lock(){
        this->mu_->lock();
        owns = true;
    }
    //THis logic for checking whether lockis on is not correct. if you want to use this you need to make sure there is only one spinmutex hold at the same time.
    static bool check_own(){
//        if (owns == false){
//            printf("break here.");
//        }
        return owns;
    }
    //THis logic for checking whether lockis on is not correct. if you want to use this you need to make sure there is only one spinmutex hold at the same time.
    void Unlock(){
        assert(owns == true);

        this->mu_->unlock();
        owns = false;
    }
  ~SpinLock() {
//      assert(owns == true);
      if(owns){
          this->mu_->unlock();
          owns = false;
      }
      assert(owns==false);
  }

 private:
  SpinMutex *const mu_;
  thread_local static bool owns;


};
//
// Acquire a ReadLock on the specified RWMutex.
// The Lock will be automatically released then the
// object goes out of scope.
//
class ReadLock {
 public:
  explicit ReadLock(port::RWMutex *mu) : mu_(mu) {
    this->mu_->ReadLock();
  }
  // No copying allowed
  ReadLock(const ReadLock &) = delete;
  void operator=(const ReadLock &) = delete;

  ~ReadLock() { this->mu_->ReadUnlock(); }

 private:
  port::RWMutex *const mu_;
};

//
// Automatically unlock a locked mutex when the object is destroyed
//
class ReadUnlock {
 public:
  explicit ReadUnlock(port::RWMutex *mu) : mu_(mu) { mu->AssertHeld(); }
  // No copying allowed
  ReadUnlock(const ReadUnlock &) = delete;
  ReadUnlock &operator=(const ReadUnlock &) = delete;

  ~ReadUnlock() { mu_->ReadUnlock(); }

 private:
  port::RWMutex *const mu_;
};

//
// Acquire a WriteLock on the specified RWMutex.
// The Lock will be automatically released then the
// object goes out of scope.
//
class WriteLock {
 public:
  explicit WriteLock(port::RWMutex *mu) : mu_(mu) {
    this->mu_->WriteLock();
  }
  // No copying allowed
  WriteLock(const WriteLock &) = delete;
  void operator=(const WriteLock &) = delete;

  ~WriteLock() { this->mu_->WriteUnlock(); }

 private:
  port::RWMutex *const mu_;
};


//TODO: implement a spin read wirte lock to control some short synchronization more
// efficiently.


// We want to prevent false sharing
template <class T>
struct ALIGN_AS(CACHE_LINE_SIZE) LockData {
  T lock_;
};

//
// Inspired by Guava: https://github.com/google/guava/wiki/StripedExplained
// A striped Lock. This offers the underlying lock striping similar
// to that of ConcurrentHashMap in a reusable form, and extends it for
// semaphores and read-write locks. Conceptually, lock striping is the technique
// of dividing a lock into many <i>stripes</i>, increasing the granularity of a
// single lock and allowing independent operations to lock different stripes and
// proceed concurrently, instead of creating contention for a single lock.
//
template <class T, class P>
class Striped {
 public:
  Striped(size_t stripes, std::function<uint64_t(const P &)> hash)
      : stripes_(stripes), hash_(hash) {

    locks_ = reinterpret_cast<LockData<T> *>(
        port::cacheline_aligned_alloc(sizeof(LockData<T>) * stripes));
    for (size_t i = 0; i < stripes; i++) {
      new (&locks_[i]) LockData<T>();
    }

  }

  virtual ~Striped() {
    if (locks_ != nullptr) {
      assert(stripes_ > 0);
      for (size_t i = 0; i < stripes_; i++) {
        locks_[i].~LockData<T>();
      }
      port::cacheline_aligned_free(locks_);
    }
  }

  T *get(const P &key) {
    uint64_t h = hash_(key);
    size_t index = h % stripes_;
    return &reinterpret_cast<LockData<T> *>(&locks_[index])->lock_;
  }

 private:
  size_t stripes_;
  LockData<T> *locks_;
  std::function<uint64_t(const P &)> hash_;
};

}  // namespace DSMEngine

#endif  // STORAGE_DSMEngine_UTIL_MUTEXLOCK_H_
