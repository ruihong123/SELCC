//
// Created by wang4996 on 22-8-8.
//

//#include <infiniband/verbs.h>
#include "page.h"
#include "Btr.h"
namespace DSMEngine {
    template class LeafPage<uint64_t, GlobalAddress>;
    template class LeafPage<uint64_t, uint64_t>;
    template class InternalPage<uint64_t>;
    template<class Key>
    bool InternalPage<Key>::internal_page_search(const Key &k, void *result_ptr) {
        SearchResult<Key,GlobalAddress>& result = *(SearchResult<Key,GlobalAddress>*)result_ptr;
        assert(k >= hdr.lowest);
//        assert(k < hdr.highest);
//        uint64_t local_meta_new = __atomic_load_n((uint64_t*)&local_lock_meta, (int)std::memory_order_seq_cst);
//        if (((Local_Meta*) &local_meta_new)->local_lock_byte !=0 || ((Local_Meta*) &local_meta_new)->current_ticket != current_ticket){
//            return false;
//        }

//        Key highest_buffer = 0;
//        highest_buffer = hdr.highest;
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



            assert(k < result.later_key);
            assert(result.next_level != GlobalAddress::Null());
            return true;
        }
        //binary search the btree node.
        uint16_t left = 0;
        uint16_t right = hdr.last_index;
        uint16_t mid = 0;
        // THe binary search algorithm below will stop at the largest value that smaller than the target key.
        // tHE pointer after a split key is for [splitkey, next splitkey). So the binary search is correct.
        while (left < right) {
            mid = (left + right + 1) / 2;
            //TODO: extract the condition of equal into another if branch, just like what I did for internal store.
            if (k >= records[mid].key) {
                // Key at "mid" is smaller than "target".  Therefore all
                // blocks before "mid" are uninteresting.
                left = mid;
            } else {
                // Key at "mid" is >= "target".  Therefore all blocks at or
                // after "mid" are uninteresting.
                right = mid - 1; // why mid -1 rather than mid
            }
        }

        assert(left == right);
        target_global_ptr_buff = records[right].ptr;
        result.next_level = target_global_ptr_buff;
#ifndef NDEBUG
        if (right < hdr.last_index){
            result.this_key = records[right].key;
            result.later_key = records[right + 1].key;
        }else{
            result.this_key = 0;
            result.later_key = hdr.highest;
        }

#endif

#ifndef NDEBUG
//        if (right < hdr.last_index){

        assert(k < result.later_key);
//        }
#endif

        assert(result.this_key <= k);

        assert(result.next_level != GlobalAddress::Null());
        return true;

    }


    template<class Key>
    bool InternalPage<Key>::internal_page_store(GlobalAddress page_addr, const Key &k, GlobalAddress value, int level,
                                                CoroContext *cxt,
                                                int coro_id) {
        auto cnt = hdr.last_index + 1;
        bool is_update = false;
        uint16_t insert_index = 0;
//--------------------------------------------------
        //binary search the btree node.
        uint16_t left = 0;
        uint16_t right = hdr.last_index;
        uint16_t mid = 0;
        if (k < records[0].key) {
            insert_index = 0;
        }else{
            while (left < right) {
                mid = (left + right + 1) / 2;

                if (k > records[mid].key) {
                    // Key at "mid" is smaller than "target".  Therefore all
                    // blocks before "mid" are uninteresting.
                    left = mid;
                } else if (k < records[mid].key) {
                    // Key at "mid" is >= "target".  Therefore all blocks at or
                    // after "mid" are uninteresting.
                    right = mid - 1;
                }else{
                    // internal node entry shall never get updated
                    assert(false);
                    records[mid].ptr = value;
                    is_update = true;

                }
            }
            assert(left == right);
            if (BOOST_LIKELY(k!=records[left].key)){
                insert_index = left +1;
            }else{
                // internal node entry shall never get updated
                assert(false);
                records[left].ptr = value;
                is_update = true;
            }


        }


//        printf("The last index %d 's key is %lu, this key is %lu\n", page->hdr.last_index, page->records[page->hdr.last_index].key, k);
        // ---------------------------------------------------------
        //TODO: Make it a binary search.
//        for (int i = cnt - 1; i >= 0; --i) {
//            if (records[i].key == k) { // find and update
//
////                asm volatile ("sfence\n" : : );
//                records[i].ptr = value;
////                asm volatile ("sfence\n" : : );
//
//                is_update = true;
//                break;
//            }
//            if (records[i].key < k) {
//                insert_index = i + 1;
//                break;
//            }
//        }
        //--------------------------------------------
        assert(cnt != kInternalCardinality);
        assert(records[hdr.last_index].ptr != GlobalAddress::Null());
//        Key split_key;
//        GlobalAddress sibling_addr = GlobalAddress::Null();
        if (!is_update) { // insert and shift
//            if ((k & ((1ull << 40) -1)) == 0){
//                printf("Internal Node Insert position for key %p is %d, this node_id is %lu\n", k, insert_index,
//                       RDMA_Manager::node_id);
//                fflush(stdout);
//            }
            // The update should mark the page version change because this will make the page state in consistent.
//      __atomic_fetch_add(&page->front_version, 1, __ATOMIC_SEQ_CST);
//      page->front_version++;
            //TODO(potential bug): double check the memory fence, there could be out of order
            // execution preventing the version lock.
//      asm volatile ("sfence\n" : : );
//      asm volatile ("lfence\n" : : );
//      asm volatile ("mfence" : : : "memory");
            for (int i = cnt; i > insert_index; --i) {
                records[i].key = records[i - 1].key;
                records[i].ptr = records[i - 1].ptr;
            }


            records[insert_index].key = k;
            records[insert_index].ptr = value;
#ifndef NDEBUG
            uint16_t last_index_prev = hdr.last_index;
#endif
            hdr.last_index++;


//#ifndef NDEBUG
//      assert(last_index_memo == page->hdr.last_index);
//#endif
//      asm volatile ("sfence\n" : : );
// THe last index could be the same for several print because we may not insert to the end all the time.
//        printf("last_index of page offset %lu is %hd, page level is %d, page is %p, the last index content is %lu %p, version should be, the key is %lu\n"
//               , page_addr.offset,  page->hdr.last_index, page->hdr.level, page, page->records[page->hdr.last_index].key, page->records[page->hdr.last_index].ptr, k);
            assert(hdr.last_index == last_index_prev + 1);
            assert(records[hdr.last_index].ptr != GlobalAddress::Null());
            assert(records[hdr.last_index].key != 0);
//  assert(page->records[page->hdr.last_index] != GlobalAddress::Null());
            cnt = hdr.last_index + 1;
        }

        return cnt == kInternalCardinality;
    }

#ifdef CACHECOHERENCEPROTOCOL
    //TODO: make it ordered and ty not use the Sherman write amplification optimization.
    template<class Key, class Value>
    void LeafPage<Key,Value>::leaf_page_search(const Key &k, SearchResult<Key, Value> &result, GlobalAddress g_page_ptr,
                                               RecordSchema *record_scheme) {

#ifdef DYNAMIC_ANALYSE_PAGE
//        int kLeafCardinality = record_scheme->GetLeafCardi();
        size_t tuple_length = record_scheme->GetSchemaSize();
        char* tuple_start = data_;
        uint16_t left = 0;
        uint16_t right = hdr.last_index;
        uint16_t mid = 0;
#ifndef NDEBUG
        std::vector<std::pair<uint16_t, uint16_t>> binary_history;
        char* tuple_last = data_ + hdr.last_index*tuple_length;
        auto r_last = Record(record_scheme,tuple_last);
        Key last_key;
        r_last.GetPrimaryKey(&last_key);
        assert(k < hdr.highest );
        assert(last_key < hdr.highest);
#endif
        while (left < right) {
            mid = (left + right + 1) / 2;
            tuple_start = data_ + mid*tuple_length;
            auto r = Record(record_scheme,tuple_start);
            Key temp_key;
            r.GetPrimaryKey(&temp_key);
//            binary_history.push_back(std::make_pair(left, right));
            if (k > temp_key) {
                // Key at "mid" is smaller than "target".  Therefore all
                // blocks before "mid" are uninteresting.
                left = mid;
            } else if (k < temp_key) {
                // Key at "mid" is >= "target".  Therefore all blocks at or
                // after "mid" are uninteresting.
                right = mid - 1;
            } else{
                //Find the value.
                memcpy((void*)result.val.data(),r.data_ptr_, r.GetRecordSize());
                result.find_value = true;
                return;
            }
        }
        // Not find
        assert(right == left);
        tuple_start = data_ + right*tuple_length;
        auto r = Record(record_scheme,tuple_start);
        Key temp_key;
        r.GetPrimaryKey(&temp_key);
        if (k == temp_key){
//            assert(false);
            assert(result.val.size() == r.GetRecordSize());
            memcpy((void*)result.val.data(),r.data_ptr_, r.GetRecordSize());
            result.find_value = true;
        }else{
            assert(k >temp_key || right == 0);
            assert(false);
        }
        return;


//        for (int i = 0; i < kLeafCardinality; ++i) {
//            tuple_start = data_ + i*tuple_length;
//
//            auto r = Record(record_scheme,tuple_start);
//            Key temp_key;
//            r.GetPrimaryKey(&temp_key);
//            if (temp_key == k && temp_key != kValueNull<Key> ) {
//                assert(result.val.size() == r.GetRecordSize());
//                memcpy(result.val.data(),r.data_ptr_, r.GetRecordSize());
//                result.find_value = true;
//                asm volatile ("sfence\n" : : );
//                asm volatile ("lfence\n" : : );
//                asm volatile ("mfence\n" : : );
////                uint8_t rear_v = rear_version;
////                if (front_v!= rear_v)// version checking
////                    //TODO: reread from the remote side.
////                    goto re _read;
//
////                memcpy(result.value_padding, r.value_padding, VALUE_PADDING);
////      result.value_padding = r.value_padding;
//                break;
//            }
//        }
//        result.val = target_value_buff;
#else
        Value target_value_buff{};
//        uint8_t front_v = front_version;
        asm volatile ("sfence\n" : : );
        asm volatile ("lfence\n" : : );
        asm volatile ("mfence\n" : : );

        for (int i = 0; i < kLeafCardinality; ++i) {
            auto &r = records[i];

            if (r.key == k && r.value != kValueNull<Key> ) {
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
#endif

        //        records =
//        data_
//    re_read:

    }
    // [lowest, highest)
    template<class TKey, class Value>
    bool
    LeafPage<TKey,Value>::leaf_page_store(const TKey &k, const Slice &v, int &cnt,
                                          RecordSchema *record_scheme) {

        // It is problematic to just check whether the value is empty, because it is possible
        // that the buffer is not initialized as 0
#ifdef DYNAMIC_ANALYSE_PAGE
        // TODO: make the key-value stored with order, do not use this unordered page structure.
        //  Or use the key to check whether this holder is empty.
        cnt = hdr.last_index + 1;
        bool is_update = false;
        uint16_t insert_index = 0;
        assert(hdr.kLeafCardinality > 0);
        int tuple_length = record_scheme->GetSchemaSize();

//#ifndef NDEBUG
//        if (hdr.last_index >= 0){
//            char* tuple_last = data_ + hdr.last_index*tuple_length;
//            auto r_last = Record(record_scheme,tuple_last);
//            TKey last_key;
//            r_last.GetPrimaryKey(&last_key);
//            assert(k < hdr.highest );
//            assert(last_key < hdr.highest);
//        }
//
//#endif
//        int kLeafCardinality = record_scheme->GetLeafCardi();
        char* tuple_start;
        tuple_start = data_ + 0*tuple_length;

        auto r_temp = Record(record_scheme,tuple_start);
        TKey temp_key1;
        r_temp.GetPrimaryKey((char*)&temp_key1);
        if (k < temp_key1 || hdr.last_index == -1) {
            // this branc can only happen when the page is empty or the leafpage is the left most leaf page
//            assert(hdr.last_index == -1);
            insert_index = 0;
        }else{
            assert(hdr.last_index >= 0);
            uint16_t left = 0;
            uint16_t right = hdr.last_index;
            uint16_t mid = 0;
            while (left < right) {
                mid = (left + right + 1) / 2;
                tuple_start = data_ + mid*tuple_length;
                auto r = Record(record_scheme,tuple_start);
                TKey temp_key;
                r.GetPrimaryKey(&temp_key);
                if (k > temp_key) {
                    // Key at "mid" is smaller than "target".  Therefore all
                    // blocks before "mid" are uninteresting.
                    left = mid;
                } else if (k < temp_key) {
                    // Key at "mid" is >= "target".  Therefore all blocks at or
                    // after "mid" are uninteresting.
                    right = mid - 1; // why mid -1 rather than mid
                } else{
                    //Find the value.
                    assert(v.size() == r.GetRecordSize());
                    memcpy(r.data_ptr_, v.data(), r.GetRecordSize());
                    is_update = true;
                    return cnt == hdr.kLeafCardinality;
                }
            }
            assert(left == right);
            tuple_start = data_ + left*tuple_length;
            auto r = Record(record_scheme,tuple_start);
            TKey temp_key;
            r.GetPrimaryKey(&temp_key);
            if ((k != temp_key )){
                insert_index = left +1;
            }else{
//                assert(false);
                assert(v.size() == r.GetRecordSize());
                memcpy(r.data_ptr_, v.data(), r.GetRecordSize());
                is_update = true;
                return cnt == hdr.kLeafCardinality;
            }




        }

        assert(cnt != hdr.kLeafCardinality);
        assert(!is_update);
//        if (!is_update) { // insert new item

        tuple_start = data_ + insert_index*tuple_length;
        if (insert_index <= hdr.last_index){
            // Move all the tuples at and after the insert_index,use memmove to avoid undefined behavior for overlapped address.
            memmove(tuple_start + tuple_length, tuple_start, (hdr.last_index - insert_index+1)*tuple_length);
            auto r = Record(record_scheme,tuple_start);
            assert(v.size() == r.GetRecordSize());
            r.ReSetRecord(v.data_reference(), v.size());
        }else{
            assert(insert_index < hdr.kLeafCardinality );
            auto r = Record(record_scheme,tuple_start);
            assert(v.size() == r.GetRecordSize());
            r.ReSetRecord(v.data_reference(), v.size());
        }

//    memcpy(r.value_padding, padding, VALUE_PADDING);
//            r.f_version++;
//            r.r_version = r.f_version;
        cnt++;
        hdr.last_index++;
        assert(hdr.last_index < hdr.kLeafCardinality);
//        }
//#ifndef NDEBUG
//        auto tuple_last = data_ + insert_index*tuple_length;
//        if ((k & ((1ull << 40) -1)) == 0){
//            printf("Leafnode Insert position for key %p is %d, this node id %lu \n", k, insert_index, RDMA_Manager::node_id);
//            fflush(stdout);
//        }
//        auto r_last2 = Record(record_scheme,tuple_last);
//        TKey last_key;
//        r_last2.GetPrimaryKey(&last_key);
////        assert(k < hdr.highest  );
//        assert(k == last_key);
//#endif
        return cnt == hdr.kLeafCardinality;
#else
        for (int i = 0; i < kLeafCardinality; ++i) {

            auto &r = records[i];
            if (r.value != kValueNull<Key>) {
                cnt++;
                if (r.key == k) {
                    r.value = v;
                    // ADD MORE weight for write.
//        memcpy(r.value_padding, padding, VALUE_PADDING);

//                    r.f_version++;
//                    r.r_version = r.f_version;
                    update_addr = (char *)&r;
                    break;
                }
            } else if (empty_index == -1) {
                empty_index = i;
            }
        }

        assert(cnt != kLeafCardinality);

        if (update_addr == nullptr) { // insert new item
            if (empty_index == -1) {
                printf("%d cnt\n", cnt);
                assert(false);
            }

            auto &r = records[empty_index];
            r.key = k;
            r.value = v;
//    memcpy(r.value_padding, padding, VALUE_PADDING);
//            r.f_version++;
//            r.r_version = r.f_version;

            update_addr = (char *)&r;

            cnt++;
        }

        return cnt == kLeafCardinality;
#endif
    }

    bool DataPage::InsertRecord(const Slice &tuple, int &cnt, RecordSchema *record_scheme, GlobalAddress& g_addr) {
        int tuple_length = record_scheme->GetSchemaSize();
        uint32_t bitmap_size = (hdr.kDataCardinality + 63) / 64;
        auto* bitmap = (uint64_t*)data_;
        char* data_start = data_ + bitmap_size*8;
        size_t empty_slot = find_empty_spot_from_bitmap(bitmap, hdr.kDataCardinality);
        if (empty_slot == -1){
            //Need to allcoate a new page
            return false;
        }
        size_t offset = empty_slot*tuple_length;
        memcpy(data_start + empty_slot*tuple_length, tuple.data(), tuple.size());
        set_bitmap(bitmap, empty_slot);
        g_addr = GADD(hdr.this_page_g_ptr, STRUCT_OFFSET(DataPage, data_) + bitmap_size*8 + offset);
        hdr.number_of_records++;
        cnt = hdr.number_of_records;
        return true;
    }

    bool DataPage::AllocateRecord(int &cnt, RecordSchema *record_scheme, GlobalAddress &g_addr, char *&data_buffer) {
        int tuple_length = record_scheme->GetSchemaSize();
        uint32_t bitmap_size = (hdr.kDataCardinality + 63) / 64;
        bitmap_size*=8;
        auto* bitmap = (uint64_t*)data_;
        int empty_slot = find_empty_spot_from_bitmap(bitmap, hdr.kDataCardinality);
        if (empty_slot == -1){
            assert(hdr.number_of_records == hdr.kDataCardinality);
            //Need to allcoate a new page
            return false;
        }
        size_t offset = empty_slot*tuple_length;
        set_bitmap(bitmap, empty_slot);
        g_addr = GADD(hdr.this_page_g_ptr, STRUCT_OFFSET(DataPage, data_) + bitmap_size + offset);
        data_buffer = data_ + bitmap_size + offset;
        hdr.number_of_records++;
        cnt = hdr.number_of_records;
        return true;
    }
    //todo: maybe we need to reimplement it when we change the server from little edian to big edian.
    int DataPage::find_empty_spot_from_bitmap(uint64_t* bitmap, uint32_t number_of_bits){
        uint32_t number_of_64 = (number_of_bits + 63) / 64;
        uint32_t number_left = number_of_bits;
        uint64_t last_result = 0;
        uint64_t last_j = 0;
        for (uint32_t i = 0; i < number_of_64; ++i) {
            if (bitmap[i] != 0xFFFFFFFFFFFFFFFF){

                for (uint32_t j = 0; j < (number_left>64?64:number_left); ++j) {
                    last_result = bitmap[i] & (1ull<<j);
                    last_j = j;
                    if (last_result == 0){
                        assert(i*64 + j < number_of_bits);
                        return i*64 + j;
                    }
                }
            }
            number_left -= 64;
        }
        assert(hdr.number_of_records == hdr.kDataCardinality);
        return -1;
    }
    void DataPage::set_bitmap(uint64_t *bitmap, size_t index) {
        bitmap[index / 64] |= (1ull << (index % 64));
    }
    void DataPage::reset_bitmap(uint64_t *bitmap, size_t index) {
        bitmap[index / 64] &= ~(1ull << (index % 64));
    }

    bool DataPage::DeleteRecord(GlobalAddress g_addr, RecordSchema *record_scheme) {
        assert(g_addr.nodeID == hdr.this_page_g_ptr.nodeID);
        int tuple_length = record_scheme->GetSchemaSize();
        uint32_t bitmap_size = (hdr.kDataCardinality + 63) / 64;
        bitmap_size*=8;
        size_t page_offset = g_addr.offset - hdr.this_page_g_ptr.offset - bitmap_size - STRUCT_OFFSET(DataPage, data_);

        size_t index = page_offset / tuple_length;
        assert(page_offset% tuple_length == 0);
        uint64_t* bitmap = (uint64_t*)data_;
        reset_bitmap(bitmap, index);
        return true;

    }




}
#else
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
//                exit(0);
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
#endif
