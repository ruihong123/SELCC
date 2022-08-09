//
// Created by wang4996 on 22-8-8.
//

#include "page.h"
namespace DSMEngine{

    void InternalPage::internal_page_search(const Key &k, SearchResult &result) {

        assert(k >= hdr.lowest);
        assert(k < hdr.highest);

        auto cnt = hdr.last_index + 1;
        // page->debug();
        if (k < records[0].key) { // this only happen when the lowest is 0
//      printf("next level pointer is  leftmost %p \n", page->hdr.leftmost_ptr);
            result.next_level = hdr.leftmost_ptr;
//      result.upper_key = page->records[0].key;
            assert(result.next_level != GlobalAddress::Null());
//      assert(page->hdr.lowest == 0);//this actually should not happen
            return;
        }

        for (int i = 1; i < cnt; ++i) {
            if (k < records[i].key) {
//        printf("next level key is %lu \n", page->records[i - 1].key);
                result.next_level = records[i - 1].ptr;
                assert(result.next_level != GlobalAddress::Null());
                assert(records[i - 1].key <= k);
                result.upper_key = records[i - 1].key;
                return;
            }
        }
//    printf("next level pointer is  the last value %p \n", page->records[cnt - 1].ptr);

        result.next_level = records[cnt - 1].ptr;
        assert(result.next_level != GlobalAddress::Null());
        assert(records[cnt - 1].key <= k);
    }

    void LeafPage::leaf_page_search(const Key &k, SearchResult &result) {
        for (int i = 0; i < kLeafCardinality; ++i) {
            auto &r = records[i];
            if (r.key == k && r.value != kValueNull && r.f_version == r.r_version) {
                result.val = r.value;
                memcpy(result.value_padding, r.value_padding, VALUE_PADDING);
//      result.value_padding = r.value_padding;
                break;
            }
        }
    }
}