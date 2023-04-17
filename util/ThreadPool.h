//
// Created by ruihong on 7/29/21.
//

#ifndef DSMEngine_THREADPOOL_H
#define DSMEngine_THREADPOOL_H
#include <condition_variable>
#include <deque>
#include <mutex>
#include <functional>
#include <vector>
#include <atomic>
#include <port/port_posix.h>
#include <assert.h>
#include <boost/lockfree/spsc_queue.hpp>
namespace DSMEngine {
class DBImpl;
enum ThreadPoolType{FlushThreadPool, CompactionThreadPool, SubcompactionThreadPool};
struct BGItem {
  //  void* tag = nullptr;
  std::function<void(void* args)> function;
  void* args;
  //  std::function<void()> unschedFunction;
};
struct BGThreadMetadata {
  void* rdma_mg;
  void* func_args;
};
//TODO: need the thread pool to be lightweight so that the invalidation message overhead will be minimum.
class ThreadPool{
 public:

//  ThreadPool(std::mutex* mtx, std::condition_variable* signal);
  std::vector<port::Thread> bgthreads_;
    std::vector<boost::lockfree::spsc_queue<BGItem, boost::lockfree::capacity<R_SIZE>>*> queue_pool;
  ThreadPoolType Type_;
//  std::mutex mu_;
//  std::condition_variable bgsignal_;
//  std::mutex RDMA_notify_mtx;
//  std::condition_variable RDMA_signal;
  int total_threads_limit_;
//  std::atomic_uint queue_len_ = 0;
  std::atomic<bool> exit_all_threads_ = false;
std::atomic<bool> wait_for_jobs_to_complete_;
//  void WakeUpAllThreads() { bgsignal_.notify_all();
//  }
  void BGThread(uint32_t thread_id) {
//    bool low_io_priority = false;
     uint32_t  miss_poll_counter = 0;
    while (true) {
      // Wait until there is an item that is ready to run
//      std::unique_lock<std::mutex> lock(mu_);
      // Stop waiting if the thread needs to do work or needs to terminate.
      while (!exit_all_threads_.load() && queue_pool[thread_id]->empty() ) {
//        bgsignal_.wait(lock);
          if(++miss_poll_counter < 1024){
              continue;
          }
          if(++miss_poll_counter < 2048){
              usleep(1);
              continue ;
          }
          if(++miss_poll_counter < 4096){
              usleep(16);

              continue;
          }else{
              usleep(512);
              continue;
          }
      }

      if (exit_all_threads_.load()) {  // mechanism to let BG threads exit safely

        if (!wait_for_jobs_to_complete_.load() ||
            queue_pool.empty()) {
          break;
        }
      }


      auto func = std::move(queue_pool[thread_id]->front().function);
      void* args = std::move(queue_pool[thread_id]->front().args);
      queue_pool[thread_id]->pop();

//      queue_len_.store(static_cast<unsigned int>(queue_pool.size()),
//                       std::memory_order_relaxed);

//      lock.unlock();


      func(args);
    }
  }
  void StartBGThreads() {
    // Start background thread if necessary

      for (int i = 0; i < total_threads_limit_; ++i) {
          queue_pool.push_back(new boost::lockfree::spsc_queue<BGItem, boost::lockfree::capacity<R_SIZE>>());

      }
      for (int i = 0; i < total_threads_limit_; ++i) {
          port::Thread p_t(&ThreadPool::BGThread, this, i);
          bgthreads_.push_back(std::move(p_t));
      }

  }
  void Schedule(std::function<void(void *args)> &&func, void *args, uint32_t thread_id) {

//    std::lock_guard<std::mutex> lock(mu_);
    if (exit_all_threads_.load()) {
      return;
    }
//    printf("schedule a work request!\n");
    BGItem item = BGItem();
      //    item.tag = tag;
   item.function = std::move(func);
   item.args = std::move(args);
    // Add to priority queue
    queue_pool[thread_id]->push(item);

//    auto& item = queue_pool.back();


//    queue_len_.store(static_cast<unsigned int>(queue_pool.size()),
//                     std::memory_order_relaxed);

    //    if (!HasExcessiveThread()) {
    //      // Wake up at least one waiting thread.
    //      bgsignal_.notify_one();
    //    } else {
    //      // Need to wake up all threads to make sure the one woken
    //      // up is not the one to terminate.
    //      WakeUpAllThreads();
    //    }
//    WakeUpAllThreads();
  }
  void JoinThreads(bool wait_for_jobs_to_complete) {

//    std::unique_lock<std::mutex> lock(mu_);
    assert(!exit_all_threads_);

    wait_for_jobs_to_complete_.store(wait_for_jobs_to_complete);
    exit_all_threads_.store(true);
    // prevent threads from being recreated right after they're joined, in case
    // the user is concurrently submitting jobs.
    total_threads_limit_ = 0;

//    lock.unlock();

//    bgsignal_.notify_all();

    for (auto& th : bgthreads_) {
      th.join();
    }

    bgthreads_.clear();
    for(auto iter : queue_pool){
        delete iter;
    }
    exit_all_threads_.store(false);
    wait_for_jobs_to_complete_.store(false);
  }
  void SetBackgroundThreads(int num){
    total_threads_limit_ = num;
      StartBGThreads();

  }
  //  void Schedule(std::function<void(void* args)>&& schedule, void* args);

};




}


#endif  // DSMEngine_THREADPOOL_H
