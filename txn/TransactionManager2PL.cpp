// NOTICE: this file is adapted from Cavalia
#if defined(LOCK)
#include "TransactionManager.h"

namespace DSMEngine {
    bool TransactionManager::AllocateNewRecord(TxnContext *context, size_t table_id,
                                           Cache::Handle* &handle, GlobalAddress &tuple_gaddr, char* &tuple_buffer) {
        Table* table = storage_manager_->tables_[table_id];
        void* page_buffer;
        GlobalAddress* g_addr = table->GetOpenedBlock();
        if ( g_addr == nullptr){
            *g_addr = default_gallocator->Allocate_Remote(Regular_Page);
            table->SetOpenedBlock(g_addr);
        }
        if (locked_handles_.find(*g_addr) == locked_handles_.end()){
            default_gallocator->PrePage_Update(page_buffer, *g_addr, handle);
            locked_handles_[*g_addr] = std::pair(handle,INSERT_ONLY);
        }
        else{
            handle = locked_handles_.at(*g_addr).first;
            //TODO: update the hierachical lock atomically, if the lock is shared lock
//            if (locked_handles_[g_addr].second < INSERT_ONLY){
//                locked_handles_[g_addr].second = INSERT_ONLY;
//            }
            assert(locked_handles_.at(*g_addr).second >= INSERT_ONLY);
//            default_gallocator->PrePage_Write(page_buffer, g_addr, handle);
            page_buffer = handle->value;
        }
//        default_gallocator->PrePage_Write(page_buffer, g_addr, handle);
        assert(handle != nullptr);
        assert(page_buffer != nullptr);
        uint64_t cardinality = 8ull*(kLeafPageSize - STRUCT_OFFSET(DataPage, data_[0]) - 8) / (8ull*table->GetSchema()->GetSchemaSize() +1);
        auto* page = new(page_buffer) DataPage(*g_addr, cardinality, table_id);
        int cnt = 0;
        bool ret = page->AllocateRecord(cnt, table->GetSchema() , tuple_gaddr, tuple_buffer);
        assert(ret);
        // if the cache line is full, set the thread local ptr as null, and allocate a new page next time.
        if(cnt == page->hdr.kDataCardinality){
            table->SetOpenedBlock(nullptr);
        }
    }
    //The hierachy lock shall be acquired outside of this function.
  bool TransactionManager::InsertRecord(TxnContext* context, 
      size_t table_id, const IndexKey* keys, 
      size_t key_num, Record *record, Cache::Handle* handle, const GlobalAddress tuple_gaddr) {
    PROFILE_TIME_START(thread_id_, CC_INSERT);
//    RecordSchema *schema_ptr = storage_manager_->tables_[table_id]->GetSchema();
    record->SetVisible(true);
    Access* access = access_list_.NewAccess();
    access->access_type_ = INSERT_ONLY;
    access->access_record_ = record;
//    access->access_handle_ = hadndle;
    access->access_addr_ = tuple_gaddr;
    PROFILE_TIME_START(thread_id_, INDEX_INSERT);
    bool ret = storage_manager_->tables_[table_id]->InsertPriIndex(keys, key_num, tuple_gaddr);
    PROFILE_TIME_END(thread_id_, INDEX_INSERT);
    PROFILE_TIME_END(thread_id_, CC_INSERT);
    return true;
  }
    //TODO: Implement the delete record function.

  bool TransactionManager::SelectRecordCC(
      TxnContext* context, size_t table_id, 
      Record *&record, const GlobalAddress &tuple_gaddr,
      AccessType access_type) {
//    printf(LOG_DEBUG, "thread_id=%u,table_id=%u,access_type=%u,data_addr=%lx, start SelectRecordCC",
//        thread_id_, table_id, access_type, data_addr);
    PROFILE_TIME_START(thread_id_, CC_SELECT);
    GlobalAddress g_addr = TOPAGE(tuple_gaddr);
      assert(g_addr.offset - tuple_gaddr.offset > STRUCT_OFFSET(DataPage, data_));
    RecordSchema *schema_ptr = storage_manager_->tables_[table_id]->GetSchema();
    void*  page_buff;
    Cache::Handle* handle;
      char* tuple_buffer;
      if (locked_handles_.find(g_addr) == locked_handles_.end()){
          if (access_type == READ_ONLY) {
              PROFILE_TIME_START(thread_id_, LOCK_READ);
              default_gallocator->PrePage_Read(page_buff, TOPAGE(tuple_gaddr), handle);
              assert((tuple_gaddr.offset - handle->gptr.offset) > STRUCT_OFFSET(DataPage, data_));
              tuple_buffer = (char*)page_buff + (tuple_gaddr.offset - handle->gptr.offset);
              locked_handles_[g_addr] = std::pair(handle,access_type);

              PROFILE_TIME_END(thread_id_, LOCK_READ);
          }
          else {
              // DELETE_ONLY, READ_WRITE
              PROFILE_TIME_START(thread_id_, LOCK_WRITE);
              default_gallocator->PrePage_Update(page_buff, TOPAGE(tuple_gaddr), handle);
              assert((tuple_gaddr.offset - handle->gptr.offset) > STRUCT_OFFSET(DataPage, data_));
              tuple_buffer = (char*)page_buff + (tuple_gaddr.offset - handle->gptr.offset);
              locked_handles_[g_addr] = std::pair(handle,access_type);
              PROFILE_TIME_END(thread_id_, LOCK_WRITE);
          }
      }
      else{
          handle = locked_handles_.at(g_addr).first;
          //TODO: update the hierachical lock atomically, if the lock is shared lock
//            if (locked_handles_[g_addr].second < INSERT_ONLY){
//                locked_handles_[g_addr].second = INSERT_ONLY;
//            }
          assert(locked_handles_.at(g_addr).second >= access_type);
//            default_gallocator->PrePage_Write(page_buffer, g_addr, handle);
          page_buff = handle->value;
          tuple_buffer = (char*)page_buff + (tuple_gaddr.offset - handle->gptr.offset);
      }


//    if (lock_success) {
      record = new Record(schema_ptr, tuple_buffer);
//      record->Deserialize(data_addr, gallocators[thread_id_]);
      Access* access = access_list_.NewAccess();
      access->access_type_ = access_type;
      access->access_record_ = record;
//      access->access_handle_ = handlde;
      access->access_addr_ = tuple_gaddr;
      if (access_type == DELETE_ONLY) {
        record->SetVisible(false);
      }
      PROFILE_TIME_END(thread_id_, CC_SELECT);
      return true;
//    }
//    else { // fail to acquire lock
//      PROFILE_TIME_END(thread_id_, CC_SELECT);
//      epicLog(LOG_DEBUG, "thread_id=%u,table_id=%u,access_type=%u,data_addr=%lx,lock fail, abort",
//          thread_id_, table_id, access_type,data_addr);
//      this->AbortTransaction();
//      return false;
//    }
  }

  bool TransactionManager::CommitTransaction(TxnContext* context, 
      TxnParam* param, CharArray& ret_str) {
//    epicLog(LOG_DEBUG, "thread_id=%u,txn_type=%d,commit", thread_id_, context->txn_type_);
    PROFILE_TIME_START(thread_id_, CC_COMMIT);
    for (auto iter : locked_handles_){
        assert(iter.second.second == READ_ONLY ||
           iter.second.second == DELETE_ONLY ||
           iter.second.second == INSERT_ONLY ||
           iter.second.second == READ_WRITE);
        if (iter.second.second == READ_ONLY){
            default_gallocator->PostPage_Read(iter.second.first->gptr, iter.second.first);
        }
        else {
            default_gallocator->PostPage_Update(iter.second.first->gptr, iter.second.first);
        }
      // unlock
//      this->UnLockRecord(access->access_addr_, record->GetSchemaSize());
    }
    //GC
    for (size_t i = 0; i < access_list_.access_count_; ++i) {
      Access* access = access_list_.GetAccess(i);
      if (access->access_type_ == DELETE_ONLY) {
          //TODO: implement the delete function.
//        gallocators[thread_id_]->Free(access->access_addr_);
//        access->access_addr_ = Gnullptr;
      }
      delete access->access_record_;
      access->access_record_ = nullptr;
      access->access_addr_ = GlobalAddress::Null();
    }
    access_list_.Clear();
    PROFILE_TIME_END(thread_id_, CC_COMMIT);
    return true;
  }

  void TransactionManager::AbortTransaction() {
//    epicLog(LOG_DEBUG, "thread_id=%u,abort", thread_id_);
    PROFILE_TIME_START(thread_id_, CC_ABORT);
      for (auto iter : locked_handles_){
          assert(iter.second.second == READ_ONLY ||
                 iter.second.second == DELETE_ONLY ||
                 iter.second.second == INSERT_ONLY ||
                 iter.second.second == READ_WRITE);
          if (iter.second.second == READ_ONLY){
              default_gallocator->PostPage_Read(iter.second.first->gptr, iter.second.first);
          }
          else {
              default_gallocator->PostPage_Update(iter.second.first->gptr, iter.second.first);
          }
          // unlock
//      this->UnLockRecord(access->access_addr_, record->GetSchemaSize());
      }
    //GC
    for (size_t i = 0; i < access_list_.access_count_; ++i) {
      Access* access = access_list_.GetAccess(i);
        if (access->access_type_ == INSERT_ONLY) {
            access->access_record_->SetVisible(false);
            //todo: Deallcoate the space of inserted tuples.
        }
      delete access->access_record_;
      access->access_record_ = nullptr;
      access->access_addr_ = GlobalAddress::Null();
    }
    access_list_.Clear();
    PROFILE_TIME_END(thread_id_, CC_ABORT);
  }


}
#endif
