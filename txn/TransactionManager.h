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
#include "TpccParams.h"
//#include "TpccConstants.h"
//#include "log.h"
#define TWO_PHASE_COMMIT

namespace DSMEngine {
//extern TpccBenchmark::TpccScaleParams tpcc_scale_params;

class TransactionManager {
 public:
  TransactionManager(StorageManager *storage_manager, size_t thread_count, size_t thread_id, bool wal_log = false, bool sharding = false)
      : storage_manager_(storage_manager),
        thread_id_(thread_id),
        thread_count_(thread_count),
        log_enabled_(wal_log),
        sharding_(sharding),
        warehouse_start_(TpccBenchmark::tpcc_scale_params.starting_warehouse_),
        warehouse_end_(TpccBenchmark::tpcc_scale_params.ending_warehouse_),
        warehouse_bit(TpccBenchmark::kWarehouseBits),
        num_warehouse_per_par_(TpccBenchmark::num_wh_per_par){
      env_ = Env::Default();
      if(wal_log){
          if (!log_file){
              Status ret = env_->NewWritableFile("logdump.txt", &log_file);
              if (!ret.ok()){
                  printf("cannot create log file\n");
                  fflush(stdout);
              }
          }

      }

  }
  ~TransactionManager() {
        if(log_enabled_){
            delete log_file;
        }
  }
//    static void Two_phase_commit_worker(uint16_t targe){
//        TransactionManager *txn_manager = new TransactionManager(nullptr, 0, 0);
//    };
//    Status NewWritableFile(const std::string& filename,
//                           WritableFile** result)  {
//        int fd = ::open(filename.c_str(),
//                        O_TRUNC | O_WRONLY | O_CREAT | 0, 0644);
//        if (fd < 0) {
//            *result = nullptr;
//            return PosixError(filename, errno);
//        }
//
//        *result = new PosixWritableFile(filename, fd);
//        return Status::OK();
//    }
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
    bool IsRecordLocal(IndexKey primary_key, uint16_t& target_node_id){
            int warehouse_id = primary_key >> TpccBenchmark::kWarehouseBits;
            target_node_id = ((warehouse_id -1) / num_warehouse_per_par_)*2;
            return warehouse_id >= warehouse_start_ && warehouse_id <= warehouse_end_;

    }
    void EnableLog(){
        log_enabled_ = true;
    }
    void DisableLog(){
        log_enabled_ = false;
    }
  bool SearchRecord(TxnContext* context, size_t table_id,
                    const IndexKey& primary_key, Record*& record,
                    AccessType access_type) {
      PROFILE_TIME_START(thread_id_, INDEX_READ);
      uint16_t target_node_id;
      if (sharding_ && !IsRecordLocal(primary_key, target_node_id)){
          RecordSchema *schema_ptr = storage_manager_->tables_[table_id]->GetSchema();

          char* tuple_buffer;
          //Send message to the corresponding node to search the record.
          if (default_gallocator->rdma_mg->Tuple_Read_2PC_RPC(target_node_id, primary_key, table_id,
                                                              schema_ptr->GetSchemaSize(), tuple_buffer, log_enabled_)){
              record = new Record(schema_ptr, tuple_buffer);
              Access* access = access_list_.NewAccess();
              access->access_type_ = access_type;
              access->access_global_record_ = record;
              if (participants.find(target_node_id) == participants.end()){
                  participants.insert(target_node_id);
              }
              return true;
          } else{
//              printf("Abort at remote tuple read\n");
//                fflush(stdout);
              AbortTransaction();
              return false;
          }

      }
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
    void WriteCommitLog(){
        if (log_enabled_){
            std::string ret_str_temp("Commit\n");
            Slice log_record = Slice(ret_str_temp.c_str(), ret_str_temp.size());
            log_file->Append(log_record);
            log_file->Flush();
            log_file->Sync();
        }


    }
    void WriteAbortLog(){

        std::string ret_str_temp("Abort\n");
        Slice log_record = Slice(ret_str_temp.c_str(), ret_str_temp.size());
        log_file->Append(log_record);
        log_file->Flush();
        log_file->Sync();

    }
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
    Env* env_;
    static WritableFile* log_file;
 protected:
//  Env* env_;
  size_t thread_id_;
  size_t thread_count_;
  AccessList<kMaxAccessLimit> access_list_;

    bool log_enabled_ = false;
    bool sharding_ = false;
//    bool require_2pc = false;
    int warehouse_start_ = 0;
    int warehouse_end_ = 0;
    int warehouse_bit = 0;
    int num_warehouse_per_par_ = 0;
    std::set<uint16_t> participants;
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
