// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_TXN_TRANSACTION_MANAGER_H__
#define __DATABASE_TXN_TRANSACTION_MANAGER_H__

#include <iostream>
#include <vector>

#include "Meta.h"
#include "StorageManager.h"
#include "Record.h"
#include "Records.h"
#include "TxnParam.h"
#include "CharArray.h"
#include "TxnContext.h"
#include "TxnAccess.h"
#include "Profiler.h"
#include "env_posix.h"
//#include "log.h"

namespace DSMEngine {
class TransactionManager {
 public:
  TransactionManager(StorageManager *storage_manager, size_t thread_count, size_t thread_id, bool wal_log = false)
      : storage_manager_(storage_manager),
        thread_count_(thread_count),
        thread_id_(thread_id),
        log_enabled_(wal_log){
      if(wal_log){
          if (!log_file_){
              Status ret = NewWritableFile("logdump.txt", &log_file_);
              if (!ret.ok()){
                  printf("cannot create log file\n");
                  fflush(stdout);
              }
          }

      }

  }
  ~TransactionManager() {
        if(log_enabled_){
            delete log_file_;
        }
  }
    Status NewWritableFile(const std::string& filename,
                           WritableFile** result)  {
        int fd = ::open(filename.c_str(),
                        O_TRUNC | O_WRONLY | O_CREAT | 0, 0644);
        if (fd < 0) {
            *result = nullptr;
            return PosixError(filename, errno);
        }

        *result = new PosixWritableFile(filename, fd);
        return Status::OK();
    }
    bool AllocateNewRecord(TxnContext *context, size_t table_id, Cache::Handle *&handle,
                           GlobalAddress &data_addr, Record*& tuple);

  bool InsertRecord(TxnContext* context, size_t table_id, const IndexKey* keys,
                    size_t key_num, Record *record, Cache::Handle* handle, const GlobalAddress tuple_gaddr);
  // Merge the Latch and unlatch request for tuples within the same global cache line.
  bool AcquireLatchForTuple(char*& tuple_buffer,GlobalAddress tuple_gaddr, AccessType access_type);
  bool AcquireXLatchForTuple(char *&tuple_buffer, GlobalAddress tuple_gaddr, Cache::Handle*& handle);
  bool AcquireSLatchForTuple(char*& tuple_buffer,GlobalAddress tuple_gaddr,  Cache::Handle*& handle);
  void ReleaseLatchForTuple(GlobalAddress tuple_addr, Cache::Handle *handle);
    void ReleaseLatchForGCL(GlobalAddress page_gaddr, Cache::Handle *handle);
    bool ClearAllLatches();
  bool SearchRecord(TxnContext* context, size_t table_id,
                    const IndexKey& primary_key, Record*& record,
                    AccessType access_type) {
    PROFILE_TIME_START(thread_id_, INDEX_READ);
    GlobalAddress data_addr = storage_manager_->tables_[table_id]->SearchPriIndex(
            primary_key);
//      assert(TOPAGE(data_addr).offset != data_addr.offset);
//      printf("target data address is %p\n", data_addr);
//      fflush(stdout);
      PROFILE_TIME_END(thread_id_, INDEX_READ);
    if (data_addr != GlobalAddress::Null()) {
      bool ret = SelectRecordCC(context, table_id, record, data_addr,
                                access_type);
      return ret;
    } else {
//      printf("table_id=%d cannot find the record with  key=%lx\n",
//          table_id, primary_key);
//        fflush(stdout);
      //Not found return true, and let the caller to handle check whetehr record is still null to figure out
      // whether the tuple is found or not.
      return true;
    }
  }

  bool SearchRecords(TxnContext* context, size_t table_id, size_t index_id,
                     const IndexKey& secondary_key, Records *records,
                     AccessType access_type) {
    printf("not supported for now\n");
    return true;
  }

  bool CommitTransaction(TxnContext* context, TxnParam* param,
                         CharArray& ret_str);

  void AbortTransaction();

  size_t GetThreadId() const {
    return thread_id_;
  }

 private:
  bool SelectRecordCC(TxnContext* context, size_t table_id,
                      Record *&record, const GlobalAddress &tuple_gaddr,
                      AccessType access_type);

 public:
  StorageManager* storage_manager_;
 protected:
//  Env* env_;
  size_t thread_id_;
  size_t thread_count_;
  AccessList<kMaxAccessLimit> access_list_;
    static WritableFile* log_file_;
    bool log_enabled_ = false;

//    std::map<uint64_t, Access*> access_list_;
#if defined(TO)
  uint64_t start_timestamp_ = 0;
  bool is_first_access_ = true;
  std::map<uint64_t , std::pair<Cache::Handle*, int>> locked_handles_;
#endif
  // lock handles shall also be used for non-lock based algorithm to avoid acquire the same latch twice during the execution.
#if defined(LOCK) || defined(OCC)
  std::unordered_map<uint64_t , std::pair<Cache::Handle*, AccessType>> locked_handles_;

#endif

    };
}

#endif
