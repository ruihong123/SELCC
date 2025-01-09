//
// Created by wang4996 on 12/27/24.
//

#ifndef SELCC_DELTASECTION_H
#define SELCC_DELTASECTION_H


#include <infiniband/verbs.h>
#include "Common.h"
#include "Record.h"
#include "rdma.h"

namespace DSMEngine {
#if defined(MVOCC)
//    class DeltaSection{
//        uint64_t head_;
//        uint64_t tail_;
//        bool is_full_;
//        char local_seg_addr_;
//
//    } attribute((aligned(64)));
    class DeltaSectionWrap {
    public:
        uint64_t head_;
        uint64_t tail_;
        // is_empty is necessary because we can not tell whether the ring  buffer is full or empty
        // merely by checking the head and tail pointer.
        bool is_empty_;
        uint8_t owner_compute_node_id_;
        GlobalAddress seg_addr_;
        ibv_mr *seg_local_mr_;
        char* local_seg_addr_;
        size_t seg_size_;
        RDMA_Manager *rdma_mg_;
        std::shared_mutex ds_mtx_;



        DeltaSectionWrap(uint8_t compute_node_id, GlobalAddress seg_addr, size_t seg_size, ibv_mr *seg_local_mr) {
            head_ = 0;
            tail_ = 0;
            is_empty_ = true;
            owner_compute_node_id_ = compute_node_id;
            seg_addr_ = seg_addr;
            seg_size_ = seg_size;
            seg_local_mr_ = seg_local_mr;
            local_seg_addr_ = (char *) seg_local_mr_->addr;
            rdma_mg_ = RDMA_Manager::Get_Instance();
        }
        ~DeltaSectionWrap() {
            //TODO: need to deallocate the remote memory.
            rdma_mg_->Deallocate_Local_RDMA_Slot(seg_local_mr_->addr, DeltaChunk);
            delete seg_local_mr_;
        }
        // new_record is the local copy and the old_record is the global copy. Later the local copy will be written to the global copy.
        void fill_in_delta_record(Record *new_record, Record *old_record, GlobalAddress& delta_gadd, size_t& delta_size) {

            // fill in the delta record, according to the dirty_col_ids and old_record.
            size_t field_size = 0;
            // calculate the size for serializing the dirty fields
            for (auto col_id: new_record->dirty_col_ids) {
                size_t column_size = new_record->schema_ptr_->GetColumnSize(col_id);
                size_t column_offset = new_record->schema_ptr_->GetColumnOffset(col_id);
                if (memcmp(new_record->data_ptr_ + column_offset, old_record->data_ptr_ + column_offset, column_size) != 0) {
                    field_size += sizeof(size_t) * 2 + column_size;
                }
            }
            delta_size = field_size + STRUCT_OFFSET(DeltaRecord, data_);

            std::unique_lock<std::shared_mutex> lck(ds_mtx_);

            while (!is_empty_ && (head_ + seg_size_ - tail_) % seg_size_ <= delta_size) {
                // wait until there is enough space for the new delta record.
                // if full then we clear the whole delta section. (will be changed later)
                tail_ = head_;
                is_empty_ = true;
            }
            MetaColumn meta_col = old_record->GetMeta();
            DeltaRecord *delta_record = new(local_seg_addr_ + tail_) DeltaRecord(
                    meta_col.Wts_, meta_col.prev_delta_, meta_col.ov_size_, delta_size);
            char *start = delta_record->data_;
            for (auto col_id: new_record->dirty_col_ids) {
                size_t column_size = new_record->schema_ptr_->GetColumnSize(col_id);
                size_t column_offset = new_record->schema_ptr_->GetColumnOffset(col_id);
                memcpy(start, &col_id, sizeof(size_t));
                start += sizeof(size_t);
                memcpy(start, &column_size, sizeof(size_t));
                start += sizeof(size_t);
                memcpy(start, new_record->data_ptr_ + column_offset, column_size);
                start += column_size;
            }
            delta_gadd = seg_addr_;
            delta_gadd.offset += tail_;
            tail_ += delta_size;
            if (is_empty_){
                is_empty_ = false;
            }
        }

    };
#endif
}

#endif //SELCC_DELTASECTION_H
