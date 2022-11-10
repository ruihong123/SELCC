//
// Created by wang4996 on 22-8-8.
//

//#include <infiniband/verbs.h>
#include "page.h"
#include "Btr.h"
namespace DSMEngine{
    bool InternalPage::internal_page_search(const Key &k, SearchResult &result, uint16_t current_ticket) {

        assert(k >= hdr.lowest);
        assert(k < hdr.highest);

        Key highest_buffer = 0;
        highest_buffer = hdr.highest;
        // optimistically latch free.
        //TODO (potential bug) what will happen if the record version is not consistent?

        // It is necessary to have reread in this function because the interanl page cache can be
        // updated by a concurrent writer. THe writer will pull the updates from the remote memory.
        //
        //TODO(Potential bug): the optimistic latch free is not fully correct because the orginal
        // algorithm first check the lock state then check the verison again when the read operation end.
        // the front and rear verison can guarantee the consistency between remote writer and reader but can not guarantee the
        // consistency between local writer and reader.
        // THe front and rear versions are necessary.
        // choice1: Maybe the lock check is necessary (either in the page or outside)
        // choice2: or we check whether the front verison equals the rear version to check wehther there is a
        // concurrent writer (check lock).
//    re_read:
        GlobalAddress target_global_ptr_buff;

//        uint64_t local_meta_new = __atomic_load_n((uint64_t*)&local_meta, (int)std::memory_order_seq_cst);
//        if (((uint16_t*) &local_meta_new)[2] != current_ticket){
//            return false;
//        }
//        uint8_t front_v = front_version;
//        uint8_t rear_v = rear_version;
//        if(front_v != current_ticket){
//            return false;
//        }
//          asm volatile ("sfence\n" : : );
//          asm volatile ("lfence\n" : : );
//          asm volatile ("mfence\n" : : );

          //TOTHINK: how to make sure that concurrent write will not result in segfault,
          // such as out of buffer for cnt.
        auto cnt = hdr.last_index + 1;
        // page->debug();
        if (k < records[0].key) {
//      printf("next level pointer is  leftmost %p \n", page->hdr.leftmost_ptr);
            target_global_ptr_buff = hdr.leftmost_ptr;

//      result.upper_key = page->records[0].key;
            // check front verison here because a writer will change the front version at the beggining of a write op
            // if this has not changed, we can guarntee that there is not writer interfere.

            // TODO: maybe we need memory fence here either.
            // TOTHINK: There is no need for local reread because the data will be modified in a copy on write manner.


            result.next_level = target_global_ptr_buff;
#ifndef NDEBUG
            result.later_key = records[0].key;
#endif

//            asm volatile ("sfence\n" : : );
//            asm volatile ("lfence\n" : : );
//            asm volatile ("mfence\n" : : );
            uint64_t local_meta_new = __atomic_load_n((uint64_t*)&local_lock_meta, (int)std::memory_order_seq_cst);
            if (((Local_Meta*) &local_meta_new)->local_lock_byte !=0 || ((Local_Meta*) &local_meta_new)->current_ticket != current_ticket){
                return false;
            }

            assert(k < result.later_key);
            assert(result.next_level != GlobalAddress::Null());
            return true;
        }
        //binary search the btree node.
        uint16_t left = 0;
        uint16_t right = hdr.last_index;
        uint16_t mid = 0;
        while (left < right) {
            mid = (left + right + 1) / 2;

            if (k >= records[mid].key) {
                // Key at "mid" is smaller than "target".  Therefore all
                // blocks before "mid" are uninteresting.
                left = mid;
            } else {
                // Key at "mid" is >= "target".  Therefore all blocks at or
                // after "mid" are uninteresting.
                right = mid - 1;
            }
        }
        assert(left == right);
        target_global_ptr_buff = records[right].ptr;
        result.next_level = target_global_ptr_buff;
#ifndef NDEBUG
        result.this_key = records[right].key;
#endif
        uint64_t local_meta_new = __atomic_load_n((uint64_t*)&local_lock_meta, (int)std::memory_order_seq_cst);
        if (((Local_Meta*) &local_meta_new)->local_lock_byte !=0 || ((Local_Meta*) &local_meta_new)->current_ticket != current_ticket){
            return false;
        }
#ifndef NDEBUG
        if (right != hdr.last_index){
            result.later_key = records[right + 1].key;
            assert(k < result.later_key);
        }

#endif
        assert(result.this_key <= k);

        assert(result.next_level != GlobalAddress::Null());
        return true;
//        for (int i = 1; i < cnt; ++i) {
//            if (k < records[i].key) {
////        printf("next level key is %lu \n", page->records[i - 1].key);
//
//                target_global_ptr_buff = records[i - 1].ptr;
//
//                assert(records[i - 1].key <= k);
////                result.upper_key = records[i - 1].key;
//
//
//
//                result.next_level = target_global_ptr_buff;
//#ifndef NDEBUG
//                result.later_key = records[i].key;
//#endif
//
//                uint64_t local_meta_new = __atomic_load_n((uint64_t*)&local_lock_meta, (int)std::memory_order_seq_cst);
//                if (((Local_Meta*) &local_meta_new)->local_lock_byte !=0 || ((Local_Meta*) &local_meta_new)->current_ticket != current_ticket){
//                    return false;
//                }
//                assert(k < result.later_key);
//                assert(result.next_level != GlobalAddress::Null());
//                return true;
//            }
//        }
////    printf("next level pointer is  the last value %p \n", page->records[cnt - 1].ptr);
//
//        target_global_ptr_buff = records[cnt - 1].ptr;
//
//        assert(records[cnt - 1].key <= k);
//
//
//
//        result.next_level = target_global_ptr_buff;
//#ifndef NDEBUG
//        result.later_key = hdr.highest;
//#endif
//
//        uint64_t local_meta_new = __atomic_load_n((uint64_t*)&local_lock_meta, (int)std::memory_order_seq_cst);
//        if (((Local_Meta*) &local_meta_new)->local_lock_byte !=0 || ((Local_Meta*) &local_meta_new)->current_ticket != current_ticket){
//            return false;
//        }
//        assert(k < result.later_key);
//        assert(result.next_level != GlobalAddress::Null());
//        assert(result.next_level.offset >= 1024*1024);
//        return true;
    }
     bool InternalPage::try_lock() {
        auto currently_locked = __atomic_load_n(&local_lock_meta.local_lock_byte, __ATOMIC_RELAXED);
        return !currently_locked &&
               __atomic_compare_exchange_n(&local_lock_meta.local_lock_byte, &currently_locked, 1, true, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED);
    }
    inline void  InternalPage::unlock_lock() {
        __atomic_store_n(&local_lock_meta.local_lock_byte, 0, mem_cst_seq);
    }
    // THe local concurrency control optimization to reduce RDMA bandwidth, is worthy of writing in the paper
    void InternalPage::check_invalidation_and_refetch_outside_lock(GlobalAddress page_addr, RDMA_Manager *rdma_mg, ibv_mr *page_mr) {
        uint8_t expected = 0;
        assert(page_mr->addr == this);

#ifndef NDEBUG
        uint8_t lock_temp = __atomic_load_n(&local_lock_meta.local_lock_byte,mem_cst_seq);
        uint8_t issued_temp = __atomic_load_n(&local_lock_meta.issued_ticket,mem_cst_seq);
        uint16_t retry_counter = 0;
#endif
        if (!hdr.valid_page && try_lock()){
            // when acquiring the lock, check the valid bit again, so that we can save unecessssary bandwidth.
            if(!hdr.valid_page){
//                printf("Page refetch %p\n", this);
                __atomic_fetch_add(&local_lock_meta.issued_ticket, 1, mem_cst_seq);
                ibv_mr temp_mr = *page_mr;
                GlobalAddress temp_page_add = page_addr;
                temp_page_add.offset = page_addr.offset + RDMA_OFFSET;
                temp_mr.addr = (char*)temp_mr.addr + RDMA_OFFSET;
                temp_mr.length = temp_mr.length - RDMA_OFFSET;
                invalidation_reread:
                rdma_mg->RDMA_Read(temp_page_add, &temp_mr, kInternalPageSize-RDMA_OFFSET, IBV_SEND_SIGNALED, 1, Internal_and_Leaf);
                assert(hdr.level < 100);
                // If the global lock is in use, then this read page should be in a inconsistent state.
                if (global_lock != 0){
#ifndef NDEBUG
                    assert(++retry_counter<1000000);
#endif
                    goto invalidation_reread;
                }
                // TODO: think whether we need to reset the global lock to 1 because the RDMA write need to make sure
                //  that the global lock is 1.
                //  Answer, we only need to reset it when we write back the data.

                hdr.valid_page = true;
                local_lock_meta.current_ticket++;

            }
            unlock_lock();
        }
    }
    void InternalPage::check_invalidation_and_refetch_inside_lock(GlobalAddress page_addr, RDMA_Manager *rdma_mg, ibv_mr *page_mr) {
        uint8_t expected = 0;
        assert(page_mr->addr == this);
        if (!hdr.valid_page ){

            ibv_mr temp_mr = *page_mr;
            GlobalAddress temp_page_add = page_addr;
            temp_page_add.offset = page_addr.offset + RDMA_OFFSET;
            temp_mr.addr = (char*)temp_mr.addr + RDMA_OFFSET;
            temp_mr.length = temp_mr.length - RDMA_OFFSET;
        invalidation_reread:
            rdma_mg->RDMA_Read(temp_page_add, &temp_mr, kInternalPageSize-RDMA_OFFSET, IBV_SEND_SIGNALED, 1, Internal_and_Leaf);
            // If the global lock is in use, then this read page should be in a inconsistent state.
            if (global_lock != 1){
                // with a lock the remote side can not be inconsistent.
                assert(false);
                goto invalidation_reread;
            }
            __atomic_store_n(&hdr.valid_page, false, (int)std::memory_order_seq_cst);


        }
    }

    void LeafPage::leaf_page_search(const Key &k, SearchResult &result, ibv_mr local_mr_copied, GlobalAddress g_page_ptr) {
//    re_read:
        Value target_value_buff{};
//        uint8_t front_v = front_version;
        asm volatile ("sfence\n" : : );
        asm volatile ("lfence\n" : : );
        asm volatile ("mfence\n" : : );
        //TODO: If record verisons are not consistent, we need to reread the page.
        // or refetch the record. or we just remove the byteaddressable write and then do not
        // use record level version.
        for (int i = 0; i < kLeafCardinality; ++i) {
            auto &r = records[i];
            while (r.f_version != r.r_version){
//                ibv_mr target_mr = *local_mr_copied;
                exit(0);
                int offset = ((char*)&r - (char *) this);
                LADD(local_mr_copied.addr, offset);
                Btr::rdma_mg->RDMA_Read(GADD(g_page_ptr, offset), &local_mr_copied, sizeof(LeafEntry),IBV_SEND_SIGNALED,1, Internal_and_Leaf);

            }
            if (r.key == k && r.value != kValueNull ) {
                assert(r.f_version == r.r_version);
                target_value_buff = r.value;
                asm volatile ("sfence\n" : : );
                asm volatile ("lfence\n" : : );
                asm volatile ("mfence\n" : : );
//                uint8_t rear_v = rear_version;
//                if (front_v!= rear_v)// version checking
//                    //TODO: reread from the remote side.
//                    goto re _read;

//                memcpy(result.value_padding, r.value_padding, VALUE_PADDING);
//      result.value_padding = r.value_padding;
                break;
            }
        }
        result.val = target_value_buff;
    }
}