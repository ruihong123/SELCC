#ifndef __DATABASE_STORAGE_TABLE_H__
#define __DATABASE_STORAGE_TABLE_H__

#include <iostream>

#include "Record.h"
#include "RecordSchema.h"
#include "Meta.h"
#include "HashIndex.h"
#include "Btr.h"
//#include "Profiler.h"

namespace DSMEngine {
    static RecordSchema* GetPrimaryIndexSchema() {
        static RecordSchema* index_schema_ptr = nullptr;
        if (index_schema_ptr == nullptr){
            index_schema_ptr = new RecordSchema(65535);
            std::vector<DSMEngine::ColumnInfo*> columns;
            columns.push_back(new DSMEngine::ColumnInfo("primary_id", DSMEngine::ValueType::UINT64));
            columns.push_back(new DSMEngine::ColumnInfo("gptr", DSMEngine::ValueType::UINT64));
            index_schema_ptr->InsertColumns(columns);
            size_t column_ids[1] = {0};
            index_schema_ptr->SetPrimaryColumns(column_ids,1);
        }

        return index_schema_ptr;
    }
class Table{
public:
  Table() {
    schema_ptr_ = nullptr;
    primary_index_ = nullptr;
//    secondary_indexes_ = nullptr;
    opened_block_ == GlobalAddress::Null();
  }
  ~Table() {
    if (primary_index_) {
//        delete primary_index_->scheme_ptr;
        delete primary_index_;
      primary_index_ = nullptr;
    }
  }
    // Only the master node (node-0) can create the new table and new index.
  void Init(size_t table_id, RecordSchema* schema_ptr, DDSM* ddsm) {
    table_id_ = table_id;
    schema_ptr_ = schema_ptr;
    secondary_count_ = 0;
//    secondary_indexes_ = nullptr;
    // the index init shall be deprecated, since we can init the index in the constructor. If the index does not need init
    // we can tell that by the number of constructor arguments.
      DSMEngine::RecordSchema* index_schema_ptr = GetPrimaryIndexSchema();
    primary_index_ = new Btr<IndexKey, uint64_t>(ddsm, ddsm->rdma_mg->page_cache_, index_schema_ptr, DDSM::GetNextIndexID());
//    primary_index_->Init(kHashIndexBucketHeaderNum, gallocator);
  }

  // return false if the key exists in primary index already
  //TODO: rename it
  bool InsertPriIndex(const IndexKey* keys, size_t key_num, GlobalAddress tuple_gaddr) {
    assert(key_num == secondary_count_ + 1);
    char key_value_pair[16] = {0};
    Slice inserted_slice(key_value_pair, 8);
    memcpy(key_value_pair, &keys[0], 8);
    memcpy(key_value_pair + 8, &tuple_gaddr, 8);
    primary_index_->insert(keys[0], inserted_slice);
      return true;
  }

  GlobalAddress SearchRecord(const IndexKey& key) {
      GlobalAddress tuple_gaddr = GlobalAddress::Null();
      char key_value_pair[16] = {0};
      Slice retrieved_slice(key_value_pair, 8);
    bool find = primary_index_->search(key, retrieved_slice);
      if (find){
            memcpy(&tuple_gaddr, key_value_pair + 8, 8);
            return tuple_gaddr;
      } else {
          tuple_gaddr = GlobalAddress::Null();
          return tuple_gaddr;
      }

  }

  void ReportTableSize() const {
    uint64_t size = primary_index_->GetRecordCount()
        * schema_ptr_->GetSchemaSize();
    std::cout << "table_id=" << table_id_ << ", size="
              << size * 1.0 / 1000 / 1000 << "MB" << std::endl;
  }

  size_t GetTableId() const {
    return table_id_;
  }
  size_t GetSecondaryCount() const {
    return secondary_count_;
  }
  RecordSchema* GetSchema() {
    return schema_ptr_;
  }
  size_t GetSchemaSize() const {
    return schema_ptr_->GetSchemaSize();
  }
  GlobalAddress GetOpenedBlock() const {

    return opened_block_;
  }
    void SetOpenedBlock(const GlobalAddress& opened_block) {
        opened_block_ = opened_block;
    }
  Btr<IndexKey, uint64_t>* GetPrimaryIndex() {
    return primary_index_;
  }

  virtual void Serialize(const char*& addr){
    size_t off = 0;
    memcpy((void *) (addr + off), &table_id_, sizeof(size_t));
    off += sizeof(size_t);
    memcpy((void*)(addr+off), &secondary_count_, sizeof(size_t));
    off += sizeof(size_t);
    const char* cur_addr = (addr + off);
    schema_ptr_->Serialize(cur_addr);
    cur_addr = cur_addr + schema_ptr_->GetSerializeSize() ;
    primary_index_->Serialize(cur_addr);
  }
  
  virtual void Deserialize(const char*& addr) {
    size_t off = 0;
    memcpy(&table_id_, (void *) (addr + off), sizeof(size_t));
    off += sizeof(size_t);
    memcpy(&secondary_count_, addr+off, sizeof(size_t));
    off += sizeof(size_t);
    const char* cur_addr = addr + off;
    schema_ptr_ = new RecordSchema(table_id_);
    schema_ptr_->Deserialize(cur_addr);
    cur_addr = cur_addr + schema_ptr_->GetSerializeSize();
    RecordSchema* index_schema_ptr = GetPrimaryIndexSchema();
    primary_index_ = new Btr<IndexKey, uint64_t>(default_gallocator, default_gallocator->rdma_mg->page_cache_, index_schema_ptr);
    primary_index_->Deserialize(cur_addr);
  }
    
  static size_t GetSerializeSize() {
    size_t ret = sizeof(size_t) * 2;
    ret += RecordSchema::GetSerializeSize();
    ret += Btr<IndexKey, uint64_t>::GetSerializeSize();
    return ret;
  }
  void AllocateNewTuple(char*& tuple_data_, GlobalAddress &tuple_gaddr, Cache::Handle* &handle, DDSM *gallocator ) {
        void* page_buffer;
        GlobalAddress g_addr = GetOpenedBlock();
      DataPage *page = nullptr;
        if ( g_addr == GlobalAddress::Null()){
            g_addr = gallocator->Allocate_Remote(Regular_Page);
            SetOpenedBlock(g_addr);
            gallocator->PrePage_Write(page_buffer, g_addr, handle);
            uint64_t cardinality = 8ull*(kLeafPageSize - STRUCT_OFFSET(DataPage, data_[0])) / (8ull*schema_ptr_->GetSchemaSize() +1);
            page = new(page_buffer) DataPage(g_addr, cardinality, table_id_);
        } else {
            gallocator->PrePage_Update(page_buffer, g_addr, handle);
            assert(((DataPage*)page_buffer)->hdr.kTableID == table_id_);
            page = reinterpret_cast<DataPage*>(page_buffer);

        }
        assert(handle != nullptr);
        assert(page_buffer != nullptr);
        // TODO: if this is a new cache line, we need to initialize the header correctly.
        int cnt = 0;
        bool ret = page->AllocateRecord(cnt, GetSchema() , tuple_gaddr, tuple_data_);
        assert(ret);
        // always open a new page when current page is full.
        if(cnt == page->hdr.kDataCardinality){
            SetOpenedBlock(gallocator->Allocate_Remote(Regular_Page));
        }
  }

 private:
  size_t table_id_;
  size_t secondary_count_;

  RecordSchema *schema_ptr_;
//  HashIndex *primary_index_;
  Btr<IndexKey, uint64_t>* primary_index_;
//  HashIndex **secondary_indexes_; // Currently disabled
  static thread_local GlobalAddress opened_block_;
};

}
#endif
