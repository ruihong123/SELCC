//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// See port_example.h for documentation for the following types/functions.

#pragma once

#include <thread>


// size_t printf formatting named in the manner of C99 standard formatting
// strings such as PRIu64
// in fact, we could use that one
#define ROCKSDB_PRIszt "zu"

#define __declspec(S)

#define ROCKSDB_NOEXCEPT noexcept

#undef PLATFORM_IS_LITTLE_ENDIAN
#if defined(OS_MACOSX)
  #include <machine/endian.h>
  #if defined(__DARWIN_LITTLE_ENDIAN) && defined(__DARWIN_BYTE_ORDER)
    #define PLATFORM_IS_LITTLE_ENDIAN \
        (__DARWIN_BYTE_ORDER == __DARWIN_LITTLE_ENDIAN)
  #endif
#elif defined(OS_SOLARIS)
  #include <sys/isa_defs.h>
  #ifdef _LITTLE_ENDIAN
    #define PLATFORM_IS_LITTLE_ENDIAN true
  #else
    #define PLATFORM_IS_LITTLE_ENDIAN false
  #endif
  #include <alloca.h>
#elif defined(OS_AIX)
  #include <sys/types.h>
  #include <arpa/nameser_compat.h>
  #define PLATFORM_IS_LITTLE_ENDIAN (BYTE_ORDER == LITTLE_ENDIAN)
  #include <alloca.h>
#elif defined(OS_FREEBSD) || defined(OS_OPENBSD) || defined(OS_NETBSD) || \
    defined(OS_DRAGONFLYBSD) || defined(OS_ANDROID)
  #include <sys/endian.h>
  #include <sys/types.h>
  #define PLATFORM_IS_LITTLE_ENDIAN (_BYTE_ORDER == _LITTLE_ENDIAN)
#else
  #include <endian.h>
#endif
#include <pthread.h>

#include <stdint.h>
#include <string.h>
#include <limits>
#include <string>

#ifndef PLATFORM_IS_LITTLE_ENDIAN
#define PLATFORM_IS_LITTLE_ENDIAN (__BYTE_ORDER == __LITTLE_ENDIAN)
#endif

#if defined(OS_MACOSX) || defined(OS_SOLARIS) || defined(OS_FREEBSD) ||\
    defined(OS_NETBSD) || defined(OS_OPENBSD) || defined(OS_DRAGONFLYBSD) ||\
    defined(OS_ANDROID) || defined(CYGWIN) || defined(OS_AIX)
// Use fread/fwrite/fflush on platforms without _unlocked variants
#define fread_unlocked fread
#define fwrite_unlocked fwrite
#define fflush_unlocked fflush
#endif

#if defined(OS_MACOSX) || defined(OS_FREEBSD) ||\
    defined(OS_OPENBSD) || defined(OS_DRAGONFLYBSD)
// Use fsync() on platforms without fdatasync()
#define fdatasync fsync
#endif

#if defined(OS_ANDROID) && __ANDROID_API__ < 9
// fdatasync() was only introduced in API level 9 on Android. Use fsync()
// when targeting older platforms.
#define fdatasync fsync
#endif
extern bool Show_Me_The_Print;
#ifdef NDEBUG
#define DEBUG_PRINT_arg(x,y)
#define DEBUG_PRINT(x)
#define DEBUG_PRINT_CONDITION(x)
#define DEBUG_PRINT_CONDITION_arg(x,y)
#else
#define DEBUG_PRINT_arg(x,y) printf(x,y)
#define DEBUG_PRINT(x) printf(x)
#define DEBUG_PRINT_CONDITION(x) if(Show_Me_The_Print) {printf(x);}
#define DEBUG_PRINT_CONDITION_arg(x,y) if(Show_Me_The_Print) {printf(x,y);}
#define DEBUG_ASSERT_CONDITION(x) if(Show_Me_The_Print) {assert(x);}
#endif



//Debugging macro
//#define TIMEPRINT
//#define PROCESSANALYSIS
//#define RDMAPROCESSANALYSIS

//Configuration macro
#define NUM_QP_ACCROSS_COMPUTE 1
#define RDMAFAAFORREADLOCK
#define CACHECOHERENCEPROTOCOL
#define DYNAMIC_ANALYSE_PAGE
#define TIMEPRINTGAP 100000
//#define GETANALYSIS
//#define TIMEPRINT
#define ROCKSDB_PTHREAD_ADAPTIVE_MUTEX
#define RECEIVE_OUTSTANDING_SIZE 128 // used to be 1024
#define SEND_OUTSTANDING_SIZE_XCOMPUTE 32 //todo: enlarge this
#define ATOMIC_OUTSTANDING_SIZE 16
#define MAX_INLINE_SIZE 256
//#define ASYNC_UNLOCK
#define INVALIDATION_STATISTICS
#define ACCESS_MODE 1 // 0 No cache no sharding, 1 cache with CC protocol, 2 sharding.
#define SYNC_XALL_OFFSET (16384*2)
#define GLOBAL_HANDOVER
#define PARALLEL_INVALIDATION
#define PAGE_FREE_LIST
#define FREELIST_RATIO 0.002
#define FREELIST_THREAD_NUM 1
#define DIRTY_ONLY_FLUSH
#define ASYNC_PLAN 1

#define PARALLEL_DEGREE 8
#define STARVATION_THRESHOLD 256 // todo: we can try 1, 8 64 and UINT64_MAX
#define WRITER_STARV_SPIN_BASE 16 // disable WRITER_STARV_SPIN_BASE and STARV_REVENGE when testing the impact of STARVATION_THRESHOLD
#define STARV_REVENGE
#define STARV_BACKOFF

#include "port/thread_annotations.h"

namespace DSMEngine {

extern const bool kDefaultToAdaptiveMutex;
enum class CpuPriority {
  kIdle = 0,
  kLow = 1,
  kNormal = 2,
  kHigh = 3,
};

namespace port {

// For use at db/file_indexer.h kLevelMaxIndex
const uint32_t kMaxUint32 = std::numeric_limits<uint32_t>::max();
const int kMaxInt32 = std::numeric_limits<int32_t>::max();
const int kMinInt32 = std::numeric_limits<int32_t>::min();
const uint64_t kMaxUint64 = std::numeric_limits<uint64_t>::max();
const int64_t kMaxInt64 = std::numeric_limits<int64_t>::max();
const int64_t kMinInt64 = std::numeric_limits<int64_t>::min();
const size_t kMaxSizet = std::numeric_limits<size_t>::max();

constexpr bool kLittleEndian = PLATFORM_IS_LITTLE_ENDIAN;
#undef PLATFORM_IS_LITTLE_ENDIAN

class CondVar;

class Mutex {
 public:
  explicit Mutex(bool adaptive = kDefaultToAdaptiveMutex);
  // No copying
  Mutex(const Mutex&) = delete;
  void operator=(const Mutex&) = delete;

  ~Mutex();

  void Lock();
  void Unlock();
  // this will assert if the mutex is not locked
  // it does NOT verify that mutex is held by a calling thread
  void AssertHeld();
  void AssertNotHeld();
 private:
  friend class CondVar;
  pthread_mutex_t mu_;
#ifndef NDEBUG
  bool locked_ = false;
#endif
};

class RWMutex {
 public:
  RWMutex();
  // No copying allowed
  RWMutex(const RWMutex&) = delete;
  void operator=(const RWMutex&) = delete;

  ~RWMutex();

  void ReadLock();
  void WriteLock();
  void ReadUnlock();
  void WriteUnlock();
  void AssertHeld() { }

 private:
  pthread_rwlock_t mu_; // the underlying platform mutex
#ifndef NDEBUG
  int lock_state_;
#endif
};

class CondVar {
 public:
  explicit CondVar(Mutex* mu);
  ~CondVar();
  void Wait();
  // Timed condition wait.  Returns true if timeout occurred.
  bool TimedWait(uint64_t abs_time_us);
  void Signal();
  void SignalAll();
 private:
  pthread_cond_t cv_;
  Mutex* mu_;
};
inline bool Snappy_Compress(const char* input, size_t length,
                            std::string* output) {
#if HAVE_SNAPPY
  output->resize(snappy::MaxCompressedLength(length));
  size_t outlen;
  snappy::RawCompress(input, length, &(*output)[0], &outlen);
  output->resize(outlen);
  return true;
#else
  // Silence compiler warnings about unused arguments.
  (void)input;
  (void)length;
  (void)output;
#endif  // HAVE_SNAPPY

  return false;
}

inline bool Snappy_GetUncompressedLength(const char* input, size_t length,
                                         size_t* result) {
#if HAVE_SNAPPY
  return snappy::GetUncompressedLength(input, length, result);
#else
  // Silence compiler warnings about unused arguments.
  (void)input;
  (void)length;
  (void)result;
  return false;
#endif  // HAVE_SNAPPY
}

inline bool Snappy_Uncompress(const char* input, size_t length, char* output) {
#if HAVE_SNAPPY
  return snappy::RawUncompress(input, length, output);
#else
  // Silence compiler warnings about unused arguments.
  (void)input;
  (void)length;
  (void)output;
  return false;
#endif  // HAVE_SNAPPY
}

inline bool GetHeapProfile(void (*func)(void*, const char*, int), void* arg) {
  // Silence compiler warnings about unused arguments.
  (void)func;
  (void)arg;
  return false;
}

inline uint32_t AcceleratedCRC32C(uint32_t crc, const char* buf, size_t size) {
#if HAVE_CRC32C
  return ::crc32c::Extend(crc, reinterpret_cast<const uint8_t*>(buf), size);
#else
  // Silence compiler warnings about unused arguments.
  (void)crc;
  (void)buf;
  (void)size;
  return 0;
#endif  // HAVE_CRC32C
}
using Thread = std::thread;

static inline void AsmVolatilePause() {
#if defined(__i386__) || defined(__x86_64__)
  asm volatile("pause");
#elif defined(__aarch64__)
  asm volatile("wfe");
#elif defined(__powerpc64__)
  asm volatile("or 27,27,27");
#endif
  // it's okay for other platforms to be no-ops
}

// Returns -1 if not available on this platform
extern int PhysicalCoreID();

typedef pthread_once_t OnceType;
#define DSMEngine_ONCE_INIT PTHREAD_ONCE_INIT
extern void InitOnce(OnceType* once, void (*initializer)());

#ifndef CACHE_LINE_SIZE
// To test behavior with non-native table_cache line size, e.g. for
// Bloom filters, set TEST_CACHE_LINE_SIZE to the desired test size.
// This disables ALIGN_AS to keep it from failing compilation.
#ifdef TEST_CACHE_LINE_SIZE
#define CACHE_LINE_SIZE TEST_CACHE_LINE_SIZE
#define ALIGN_AS(n) /*empty*/
#else
#if defined(__s390__)
#define CACHE_LINE_SIZE 256U
#elif defined(__powerpc__) || defined(__aarch64__)
#define CACHE_LINE_SIZE 128U
#else
#define CACHE_LINE_SIZE 64U
#endif
#define ALIGN_AS(n) alignas(n)
#endif
#endif

static_assert((CACHE_LINE_SIZE & (CACHE_LINE_SIZE - 1)) == 0,
              "Cache line size must be a power of 2 number of bytes");

extern void *cacheline_aligned_alloc(size_t size);

extern void cacheline_aligned_free(void *memblock);

#define PREFETCH(addr, rw, locality) __builtin_prefetch(addr, rw, locality)

extern void Crash(const std::string& srcfile, int srcline);

extern int GetMaxOpenFiles();

extern const size_t kPageSize;

using ThreadId = pid_t;

extern void SetCpuPriority(ThreadId id, CpuPriority priority);

} // namespace port
}  // namespace ROCKSDB_NAMESPACE
