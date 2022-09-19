//
// Created by wang4996 on 22-8-8.
//

#include "page.h"
namespace DSMEngine{
    void InternalPage::internal_page_search(const Key &k, SearchResult &result) {

        assert(k >= hdr.lowest);
        assert(k < hdr.highest);
        // optimistically latch free.
    re_read:
        GlobalAddress target_global_ptr_buff;
        uint8_t front_v = front_version;
          asm volatile ("sfence\n" : : );
          asm volatile ("lfence\n" : : );
          asm volatile ("mfence\n" : : );
        auto cnt = hdr.last_index + 1;
        // page->debug();
        if (k < records[0].key) {
//      printf("next level pointer is  leftmost %p \n", page->hdr.leftmost_ptr);
            target_global_ptr_buff = hdr.leftmost_ptr;
            asm volatile ("sfence\n" : : );
            asm volatile ("lfence\n" : : );
            asm volatile ("mfence\n" : : );
//      result.upper_key = page->records[0].key;
            assert(result.next_level != GlobalAddress::Null());
            uint8_t rear_v = rear_version;
            // TODO: maybe we need memory fence here either.

            if (front_v!= rear_v){
                goto re_read;
            }
            result.next_level = target_global_ptr_buff;
            return;
        }

        for (int i = 1; i < cnt; ++i) {
            if (k < records[i].key) {
//        printf("next level key is %lu \n", page->records[i - 1].key);

                target_global_ptr_buff = records[i - 1].ptr;
                assert(result.next_level != GlobalAddress::Null());
                assert(records[i - 1].key <= k);
                result.upper_key = records[i - 1].key;
                uint8_t rear_v = rear_version;
                if (front_v!= rear_v){
                    goto re_read;
                }

                result.next_level = target_global_ptr_buff;
                return;
            }
        }
//    printf("next level pointer is  the last value %p \n", page->records[cnt - 1].ptr);

        target_global_ptr_buff = records[cnt - 1].ptr;

        assert(records[cnt - 1].key <= k);
        uint8_t rear_v = rear_version;
        if (front_v!= rear_v)// version checking
            goto re_read;

        result.next_level = target_global_ptr_buff;
        assert(result.next_level != GlobalAddress::Null());
    }

    void LeafPage::leaf_page_search(const Key &k, SearchResult &result) {
    re_read:
        Value target_value_buff{};
        uint8_t front_v = front_version;
        asm volatile ("sfence\n" : : );
        asm volatile ("lfence\n" : : );
        asm volatile ("mfence\n" : : );
        for (int i = 0; i < kLeafCardinality; ++i) {
            auto &r = records[i];
            if (r.key == k && r.value != kValueNull && r.f_version == r.r_version) {
                target_value_buff = r.value;
                asm volatile ("sfence\n" : : );
                asm volatile ("lfence\n" : : );
                asm volatile ("mfence\n" : : );
                uint8_t rear_v = rear_version;
                if (front_v!= rear_v)// version checking
                    goto re_read;

//                memcpy(result.value_padding, r.value_padding, VALUE_PADDING);
//      result.value_padding = r.value_padding;
                break;
            }
        }
        result.val = target_value_buff;
    }
}