//
// Created by wang4996 on 12/27/24.
//

#ifndef SELCC_DELTASECTION_H
#define SELCC_DELTASECTION_H


#include <infiniband/verbs.h>
#include <pmmintrin.h>
#include "Common.h"
#include "Record.h"
#include "rdma.h"

namespace DSMEngine {
#if defined(MVOCC)
    class DeltaSection{
    public:
        uint64_t head_;
        uint64_t tail_;
        bool is_empty_;
        char local_seg_addr_[1];

    }__attribute__((packed));

    class DeltaSectionWrap {
    public:
//        uint64_t head_;
//        uint64_t tail_;
//        // is_empty is necessary because we can not tell whether the ring  buffer is full or empty
//        // merely by checking the head and tail pointer.
//        bool is_empty_;
        uint8_t owner_compute_node_id_;
        GlobalAddress seg_addr_;
        ibv_mr *seg_local_mr_;
//        char* local_seg_addr_;
        size_t seg_avail_size_;
        RDMA_Manager *rdma_mg_;
        std::shared_mutex ds_mtx_;
        uint64_t max_ts;
        DeltaSection* inner_section;
        DeltaSectionWrap(uint8_t compute_node_id, GlobalAddress seg_addr, size_t seg_size, ibv_mr *seg_local_mr) {
            inner_section = (DeltaSection *) seg_local_mr_->addr;
            inner_section->head_ = 0;
            inner_section->tail_ = 0;
            inner_section->is_empty_ = true;
            owner_compute_node_id_ = compute_node_id;
            seg_addr_ = seg_addr;
            seg_avail_size_ = seg_size - STRUCT_OFFSET(DeltaSection, local_seg_addr_) - 1; // 1 is for the RDMA write polling.
            seg_local_mr_ = seg_local_mr;

            rdma_mg_ = RDMA_Manager::Get_Instance();
            max_ts = 0;
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

            while (!inner_section->is_empty_ && (inner_section->head_ + seg_avail_size_ - inner_section->tail_) % seg_avail_size_ <= delta_size) {
                // wait until there is enough space for the new delta record.
                // if full then we clear the whole delta section. (will be changed later)
                inner_section->tail_ = inner_section->head_;
                inner_section->is_empty_ = true;
            }
            MetaColumn meta_col = old_record->GetMeta();
            // update the max time stamp.
            if (max_ts < meta_col.Wts_){
                max_ts = meta_col.Wts_;
            }
            DeltaRecord *delta_record = new(inner_section->local_seg_addr_ + inner_section->tail_) DeltaRecord(
                    meta_col.Wts_, meta_col.prev_delta_, meta_col.prev_delta_wts_, meta_col.prev_delta_size_, delta_size);
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
            delta_gadd.offset += inner_section->tail_ + STRUCT_OFFSET(DeltaSection, local_seg_addr_);
            inner_section->tail_ += delta_size;
            if (inner_section->is_empty_){
                inner_section->is_empty_ = false;
            }
        }
        uint64_t GetMaxTimestamp(){
            return max_ts;
        }
        void PullUpdates(){
            RDMA_Manager *rdma_mg = RDMA_Manager::Get_Instance();
            // pull the updates from the remote node.
            RDMA_Request* send_pointer;
            ibv_mr* send_mr = rdma_mg->Get_local_send_message_mr();
            ibv_mr* recv_mr = seg_local_mr_;
            char* tuple_buffer = (char*)recv_mr->addr;
            send_pointer = (RDMA_Request*)send_mr->addr;
            send_pointer->command = pull_delta_section;
            send_pointer->content.pull_ds.ds_gaddr = seg_addr_;
            send_pointer->content.pull_ds.old_head = seg_addr_;
            send_pointer->content.pull_ds.old_tail = seg_addr_;
            send_pointer->content.pull_ds.old_max_ts = seg_addr_;
            send_pointer->buffer = recv_mr->addr;
            send_pointer->rkey = recv_mr->rkey;


            uint8_t * receive_pointer = (uint8_t*)((uint8_t*)recv_mr->addr + rdma_mg->delta_section_size - 1);
            //Clear the reply buffer for the polling.
            *receive_pointer = 0;
//        *receive_pointer = {};

            int qp_id = rdma_mg->qp_inc_ticket++ % NUM_QP_ACCROSS_COMPUTE;

            rdma_mg->post_send_xcompute(send_mr, owner_compute_node_id_, qp_id, sizeof(RDMA_Request));
            ibv_wc wc[2] = {};
            assert(send_pointer->command!= create_qp_);
//        printf("Tuple read request sent from %u to %u\n", node_id, target_node_id);
//        fflush(stdout);
////         Check the completion outside this function
//        if (poll_completion_xcompute(wc, 1, std::string("main"), true, target_node_id, qp_id)){
//            fprintf(stderr, "failed to poll send for remote memory register\n");
//            return false;
//        }
            asm volatile ("sfence\n" : : );
            asm volatile ("lfence\n" : : );
            asm volatile ("mfence\n" : : );
            volatile uint8_t * check_byte = (uint8_t*)receive_pointer;
            while(!*check_byte){
                _mm_clflush((const void *) check_byte);
                asm volatile ("sfence\n" : : );
                asm volatile ("lfence\n" : : );
                asm volatile ("mfence\n" : : );
            }

        }

    };
#endif
}

#endif //SELCC_DELTASECTION_H
