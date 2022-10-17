//
// Created by wang4996 on 22-8-8.
//

#ifndef MEMORYENGINE_PAGE_H
#define MEMORYENGINE_PAGE_H
#include "Common.h"
#include <iostream>
namespace DSMEngine{
    struct SearchResult {
        bool is_leaf;
        uint8_t level;
        GlobalAddress slibing;
        GlobalAddress next_level;
#ifndef NDEBUG
        Key upper_key;

        char key_padding[KEY_PADDING];
#endif
        Value val;
        char value_padding[VALUE_PADDING];

    };
    class Header {
    private:
        GlobalAddress leftmost_ptr;
        GlobalAddress sibling_ptr;
        uint8_t level;
        // the last index is initialized as -1 in leaf node and internal nodes,
        // only 0 in the root node.
        int16_t last_index;
        Key lowest;
        Key highest;

        friend class InternalPage;
        friend class LeafPage;
        friend class Btr;
        friend class IndexCache;

    public:
        Header() {
            leftmost_ptr = GlobalAddress::Null();
            sibling_ptr = GlobalAddress::Null();
            last_index = -1;
            lowest = kKeyMin;
            highest = kKeyMax;
        }

        void debug() const {
            std::cout << "leftmost=" << leftmost_ptr << ", "
                      << "sibling=" << sibling_ptr << ", "
                      << "level=" << (int)level << ","
                      << "cnt=" << last_index + 1 << ",";
//              << "range=[" << lowest << " - " << highest << "]";
        }
    } __attribute__((packed));
    ;

    class InternalEntry {
    public:
        Key key = {};
        char key_padding[KEY_PADDING] = "";
        GlobalAddress ptr = GlobalAddress::Null();

        InternalEntry() {
//            ptr = GlobalAddress::Null();
//    key = 0;
//            key = {};
        }
    } __attribute__((packed));

    class LeafEntry {
    public:
        uint8_t f_version : 4;
        Key key = {};
        char key_padding[KEY_PADDING] = "";
        Value value = {};
        char value_padding[VALUE_PADDING] = "";
        uint8_t r_version : 4;

        LeafEntry() {
            f_version = 0;
            r_version = 0;
            value = kValueNull;
            key = 0;
//      key = {};
        }
    } __attribute__((packed));

    constexpr int kInternalCardinality =
            (kInternalPageSize - sizeof(Header) - sizeof(uint8_t) * 2 - 8) /
            sizeof(InternalEntry);

    constexpr int kLeafCardinality =
            (kLeafPageSize - sizeof(Header) - sizeof(uint8_t) * 2 - 8) / sizeof(LeafEntry);


    class InternalPage {
        // private:
        union {
            uint32_t crc;
            uint64_t embedding_lock;
            uint64_t index_cache_freq;
        };

        uint8_t front_version;
        Header hdr;
        InternalEntry records[kInternalCardinality] = {};

//  uint8_t padding[InternalPagePadding];
        uint8_t rear_version;

        friend class Btr;
        friend class Cache;

    public:
        // this is called when tree grows
        InternalPage(GlobalAddress left, const Key &key, GlobalAddress right,
                     uint32_t level = 0) {
            hdr.leftmost_ptr = left;
            hdr.level = level;
            records[0].key = key;
            records[0].ptr = right;
            records[1].ptr = GlobalAddress::Null();

            hdr.last_index = 0;

            front_version = 0;
            rear_version = 0;
        }

        InternalPage(uint32_t level = 0) {
            hdr.level = level;
            records[0].ptr = GlobalAddress::Null();

            front_version = 0;
            rear_version = 0;

            embedding_lock = 0;
        }

//        void set_consistent() {
//            front_version++;
//            rear_version = front_version;
//#ifdef CONFIG_ENABLE_CRC
//            this->crc =
//        CityHash32((char *)&front_version, (&rear_version) - (&front_version));
//#endif
//        }


        bool check_consistent() const {

            bool succ = true;
#ifdef CONFIG_ENABLE_CRC
            auto cal_crc =
        CityHash32((char *)&front_version, (&rear_version) - (&front_version));
    succ = cal_crc == this->crc;
#endif
            succ = succ && (rear_version == front_version);
            if (!succ) {
                // this->debug();
            }
            return succ;
        }

        void debug() const {
            std::cout << "InternalPage@ ";
            hdr.debug();
            std::cout << "version: [" << (int)front_version << ", " << (int)rear_version
                      << "]" << std::endl;
        }

        void verbose_debug() const {
            this->debug();
            for (int i = 0; i < this->hdr.last_index + 1; ++i) {
                printf("[%lu %lu] ", this->records[i].key, this->records[i].ptr.val);
            }
            printf("\n");
        }
        bool internal_page_search(const Key &k, SearchResult &result, Key version);
        void internal_page_store(GlobalAddress page_addr, const Key &k,
                                 GlobalAddress value, GlobalAddress root, int level,
                                 CoroContext *cxt, int coro_id);
    } __attribute__((packed));

    class LeafPage {
    private:
        union {
            uint32_t crc;
            uint64_t embedding_lock;
        };
        uint8_t front_version;
        Header hdr;
        LeafEntry records[kLeafCardinality] = {};

//  uint8_t padding[LeafPagePadding];
        uint8_t rear_version;

        friend class Btr;

    public:
        LeafPage(uint32_t level = 0) {
            hdr.level = level;
            records[0].value = kValueNull;

            front_version = 0;
            rear_version = 0;

            embedding_lock = 0;
        }

//        void set_consistent() {
//            front_version++;
//            rear_version = front_version;
//#ifdef CONFIG_ENABLE_CRC
//            this->crc =
//        CityHash32((char *)&front_version, (&rear_version) - (&front_version));
//#endif
//        }

        bool check_consistent() const {

            bool succ = true;
#ifdef CONFIG_ENABLE_CRC
            auto cal_crc =
        CityHash32((char *)&front_version, (&rear_version) - (&front_version));
    succ = cal_crc == this->crc;
#endif

            succ = succ && (rear_version == front_version);
            if (!succ) {
                // this->debug();
            }

            return succ;
        }

        void debug() const {
            std::cout << "LeafPage@ ";
            hdr.debug();
            std::cout << "version: [" << (int)front_version << ", " << (int)rear_version
                      << "]" << std::endl;
        }
        void leaf_page_search(const Key &k, SearchResult &result, ibv_mr local_mr_copied, GlobalAddress g_page_ptr);
    } __attribute__((packed));

}


#endif //MEMORYENGINE_PAGE_H
