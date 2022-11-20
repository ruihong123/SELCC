//
// Created by wang4996 on 22-8-8.
//

#ifndef MEMORYENGINE_PAGE_H
#define MEMORYENGINE_PAGE_H
#include "Common.h"
#include "rdma.h"
#include <iostream>

namespace DSMEngine{

    struct SearchResult {
        bool is_leaf;
        uint8_t level;
        GlobalAddress slibing;
        GlobalAddress next_level;
        // for future pointer swizzling design
        ibv_mr* page_hint = nullptr;
#ifndef NDEBUG
        Key this_key;
        Key later_key;

        char key_padding[KEY_PADDING];
#endif
        Value val;
        char value_padding[VALUE_PADDING];

    };
    class Header {
    private:
        GlobalAddress leftmost_ptr;
        GlobalAddress sibling_ptr;
        GlobalAddress this_page_g_ptr;
        // the last index is initialized as -1 in leaf node and internal nodes,
        // only 0 in the root node.

        Key lowest;
        Key highest;
        //TODO: reserve struture or pointer for the invalidation bit (if not valid, a RDMA read is required),
        // globale address (for checking whether this page is what we want) cache handle pointer.
        bool valid_page;
        int16_t last_index;
        uint8_t level;
        friend class InternalPage;
        friend class LeafPage;
        friend class Btr;
        friend class IndexCache;

    public:
        Header() {
            leftmost_ptr = GlobalAddress::Null();
            sibling_ptr = GlobalAddress::Null();
            last_index = -1;
            valid_page = true;
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
    } __attribute__ ((aligned (8)));

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
#ifdef CACHECOHERENCEPROTOCOL
    class LeafEntry {
    public:
//        uint8_t f_version : 4;
        Key key = {};
        char key_padding[KEY_PADDING] = "";
        Value value = {};
        char value_padding[VALUE_PADDING] = "";
//        uint8_t r_version : 4;

        LeafEntry() {
//            f_version = 0;
//            r_version = 0;
            value = kValueNull;
            key = 0;
//      key = {};
        }
    } __attribute__((packed));
#else
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
#endif
    //TODO (potential bug): recalcuclate the kInternalCardinality, if we take alignment into consideration
    // the caculation below may not correct.
    struct Local_Meta {


        uint8_t issued_ticket;
        uint8_t current_ticket;
        uint8_t local_lock_byte;
        uint8_t hand_time;
        uint32_t hand_over;//can be only 1 byte.
    };
//        constexpr int RDMA_OFFSET  = 64; // cache line offset.
    constexpr int RDMA_OFFSET  = sizeof(Local_Meta);
    constexpr int kInternalCardinality =
            (kInternalPageSize - sizeof(Header) - sizeof(uint8_t) * 2 - 8 - sizeof(uint64_t) -RDMA_OFFSET) /
            sizeof(InternalEntry);

    constexpr int kLeafCardinality =
            (kLeafPageSize - sizeof(Header) - sizeof(uint8_t) * 2 - 8 - sizeof(uint64_t) - RDMA_OFFSET) / sizeof(LeafEntry);


    class InternalPage {
        // private:
        //TODO: we can make the local lock metaddata outside the page.
    public:

        Local_Meta local_lock_meta;
//        std::atomic<uint8_t> front_version;
//        uint8_t front_version;
        alignas(8) uint64_t global_lock;
        uint8_t busy;;
        uint8_t front_version = 0;
        Header hdr;
        InternalEntry records[kInternalCardinality] = {};

//  uint8_t padding[InternalPagePadding];
//        alignas(8) uint8_t rear_version;

        friend class Btr;
        friend class Cache;

    public:
        // this is called when tree grows
        InternalPage(GlobalAddress left, const Key &key, GlobalAddress right, GlobalAddress this_page_g_ptr,
                     uint32_t level = 0) {
            hdr.leftmost_ptr = left;
            hdr.level = level;
            hdr.valid_page = true;
            global_lock = 0;
            records[0].key = key;
            records[0].ptr = right;
            records[1].ptr = GlobalAddress::Null();

            hdr.last_index = 0;
            hdr.this_page_g_ptr = this_page_g_ptr;
            local_metadata_init();
//            front_version = 0;
//            rear_version = 0;
//            local_lock_bytes = 0;
//            current_ticket = 0;
//            issued_ticket = 0;

//            embedding_lock = 1;
        }

        InternalPage(GlobalAddress this_page_g_ptr, uint32_t level = 0) {
            hdr.level = level;
            global_lock = 0;
            records[0].ptr = GlobalAddress::Null();
            local_metadata_init();
            hdr.this_page_g_ptr = this_page_g_ptr;
//            front_version = 0;
//            rear_version = 0;

//            embedding_lock = 1;
        }

//        void set_consistent() {
//            front_version++;
//            rear_version = front_version;
//#ifdef CONFIG_ENABLE_CRC
//            this->crc =
//        CityHash32((char *)&front_version, (&rear_version) - (&front_version));
//#endif
//        }
        void local_metadata_init(){
            __atomic_store_n(&local_lock_meta.local_lock_byte, 0, mem_cst_seq);
            local_lock_meta.current_ticket = 0;
            local_lock_meta.issued_ticket = 0;

            local_lock_meta.hand_over = 0;
            local_lock_meta.hand_time = 0;
        }
        void set_global_address(GlobalAddress g_ptr){

        }
        bool try_lock();
        void unlock_lock();
        void check_invalidation_and_refetch_outside_lock(GlobalAddress page_addr, RDMA_Manager *rdma_mg, ibv_mr *page_mr);
        void check_invalidation_and_refetch_inside_lock(GlobalAddress page_addr, RDMA_Manager *rdma_mg, ibv_mr *page_mr);

        bool check_lock_state() const {

            bool succ = global_lock ==0;
#ifdef CONFIG_ENABLE_CRC
            auto cal_crc =
        CityHash32((char *)&front_version, (&rear_version) - (&front_version));
    succ = cal_crc == this->crc;
#endif


//            succ = succ && (rear_version == front_version);
            if (!succ) {
                // this->debug();
            }
//#ifndef NDEBUG
//            if (front_version == 0 && succ){
//                printf("check version pass, with 0\n");
//            }
//#endif
            return succ;
        }

        void debug() const {
            std::cout << "InternalPage@ ";
            hdr.debug();
//            std::cout << "version: [" << (int)front_version << ", " << (int)rear_version
//                      << "]" << std::endl;
        }

        void verbose_debug() const {
            this->debug();
            for (int i = 0; i < this->hdr.last_index + 1; ++i) {
                printf("[%lu %lu] ", this->records[i].key, this->records[i].ptr.val);
            }
            printf("\n");
        }
        bool internal_page_search(const Key &k, SearchResult &result, uint16_t current_ticket);
        void internal_page_store(GlobalAddress page_addr, const Key &k,
                                 GlobalAddress value, GlobalAddress root, int level,
                                 CoroContext *cxt, int coro_id);
    };
#ifdef CACHECOHERENCEPROTOCOL
    class LeafPage {
//    private:
        Local_Meta local_lock_meta;
        // if busy we will not cache it in cache, switch back to the Naive
        alignas(8) uint64_t global_lock;
        uint8_t busy;
//        uint8_t front_version;
        Header hdr;
        LeafEntry records[kLeafCardinality] = {};

//  uint8_t padding[LeafPagePadding];
        uint8_t rear_version;

        friend class Btr;

    public:
        LeafPage(GlobalAddress this_page_g_ptr, uint32_t level = 0) {
            hdr.level = level;
            hdr.this_page_g_ptr = this_page_g_ptr;
            records[0].value = kValueNull;

            front_version = 0;
            rear_version = 0;
            local_lock_meta.local_lock_byte = 0;
            local_lock_meta.current_ticket = 0;
            local_lock_meta.issued_ticket = 0;
            local_lock_meta.hand_over = 0;
            local_lock_meta.hand_time = 0;

//            embedding_lock = 1;
        }

//        void set_consistent() {
//            front_version++;
//            rear_version = front_version;
//#ifdef CONFIG_ENABLE_CRC
//            this->crc =
//        CityHash32((char *)&front_version, (&rear_version) - (&front_version));
//#endif
//        }
        void local_metadata_init(){
            __atomic_store_n(&local_lock_meta.local_lock_byte, 0, mem_cst_seq);
            local_lock_meta.current_ticket = 0;
            local_lock_meta.issued_ticket = 0;

            local_lock_meta.hand_over = 0;
            local_lock_meta.hand_time = 0;
        }


        void debug() const {
            std::cout << "LeafPage@ ";
            hdr.debug();
            std::cout << "version: [" << (int)front_version << ", " << (int)rear_version
                      << "]" << std::endl;
        }
        void leaf_page_search(const Key &k, SearchResult &result, ibv_mr local_mr_copied, GlobalAddress g_page_ptr);


    };
#else
    class LeafPage {
//    private:
        Local_Meta local_lock_meta;
        alignas(8) uint64_t global_lock;
        alignas(8) uint64_t reference_bitmap;
        uint8_t front_version;
        Header hdr;
        LeafEntry records[kLeafCardinality] = {};

//  uint8_t padding[LeafPagePadding];
        uint8_t rear_version;

        friend class Btr;

    public:
        LeafPage(GlobalAddress this_page_g_ptr, uint32_t level = 0) {
            hdr.level = level;
            hdr.this_page_g_ptr = this_page_g_ptr;
            records[0].value = kValueNull;

            front_version = 0;
            rear_version = 0;
            local_lock_meta.local_lock_byte = 0;
            local_lock_meta.current_ticket = 0;
            local_lock_meta.issued_ticket = 0;
            local_lock_meta.hand_over = 0;
            local_lock_meta.hand_time = 0;

//            embedding_lock = 1;
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
#endif
}


#endif //MEMORYENGINE_PAGE_H
