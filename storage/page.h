
//
// Created by wang4996 on 22-8-8.
//

#ifndef MEMORYENGINE_PAGE_H
#define MEMORYENGINE_PAGE_H
#include "Common.h"
#include "rdma.h"
#include "DSMEngine/slice.h"
#include "storage/ColumnInfo.h"
#include "RecordSchema.h"
#include "Record.h"
#include <iostream>

namespace DSMEngine{
    enum Page_Type { P_Plain = 0, P_Internal, P_Leaf, P_Data};
    template<class Key, class Value>
    struct SearchResult {
        bool is_leaf;
        uint8_t level;
        GlobalAddress slibing;
        GlobalAddress next_level;
        // for future pointer swizzling design
        ibv_mr* page_hint = nullptr;
        bool find_value = false;
#ifndef NDEBUG
        Key this_key;
        Key later_key;

        char key_padding[KEY_PADDING];
#endif
//        Value val;
        Slice val;
//        char* val[kMaxAttributeLength];
//        size_t val_size;
//        char value_padding[VALUE_PADDING];
        void Reset(){
            is_leaf = false;
            level = 0;
            slibing = GlobalAddress::Null();
            next_level = GlobalAddress::Null();
            page_hint = nullptr;
            find_value = false;
#ifndef NDEBUG
            this_key = 0;
            later_key = 0;
#endif
        }
    };


    template<typename T>
    class Header_Index {
    public:
        Page_Type p_type = P_Plain;
        GlobalAddress this_page_g_ptr;
        GlobalAddress leftmost_ptr;
        GlobalAddress sibling_ptr;
        // the last index is initialized as -1 in leaf node and internal nodes,
        // only 0 in the root node.
        //TODO: reserve struture or pointer for the invalidation bit (if not valid, a RDMA read is required),
        // globale address (for checking whether this page is what we want) cache handle pointer.
        bool valid_page;
        int16_t last_index;
        uint8_t level;

        template<class K> friend class InternalPage;
        friend class RDMA_Manager;

        template<class K, class V> friend class LeafPage;

        template<class K, class V> friend class Btr;
//        friend class IndexCache;

        uint32_t kLeafCardinality;
        //        T* try_var{};
        T lowest{};
        T highest{};
        Header_Index() {
            leftmost_ptr = GlobalAddress::Null();
            sibling_ptr = GlobalAddress::Null();
            last_index = -1;
            valid_page = true;
            lowest = kKeyMin<T>;
            highest = kKeyMax<T>;
        }

        void debug() const {
            std::cout << "leftmost=" << leftmost_ptr << ", "
                      << "sibling=" << sibling_ptr << ", "
                      << "level=" << (int)level << ","
                      << "cnt=" << last_index + 1 << ",";
//              << "range=[" << lowest << " - " << highest << "]";
        }

    } __attribute__ ((aligned (8)));

    template<class Key>
    class InternalEntry {
    public:
        Key key = {};
//        char key_padding[KEY_PADDING] = "";
        GlobalAddress ptr = GlobalAddress::Null();
        InternalEntry() {
//            ptr = GlobalAddress::Null();
//    key = 0;
//            key = {};
        }
    } __attribute__((packed));
#ifdef CACHECOHERENCEPROTOCOL

    template<class Key, class Value>
    class LeafEntry {
    public:
//        uint8_t f_version : 4;
        Key key{};
        char key_padding[KEY_PADDING] = "";
        Value value{};
        char value_padding[VALUE_PADDING] = "";
//        uint8_t r_version : 4;

        LeafEntry() {
//            f_version = 0;
//            r_version = 0;
            value = kValueNull<Key>;
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
//        constexpr int RDMA_OFFSET  = 64; // local lock offset.
    constexpr int RDMA_OFFSET  = sizeof(Local_Meta);
//    constexpr int kInternalCardinality =
//            (kInternalPageSize - sizeof(Header) - sizeof(uint8_t) * 2 - 8 - sizeof(uint64_t) -RDMA_OFFSET) /
//            sizeof(InternalEntry);
//
//    constexpr int kLeafCardinality =
//            (kLeafPageSize - sizeof(Header) - sizeof(uint8_t) * 2 - 8 - sizeof(uint64_t) - RDMA_OFFSET) / sizeof(LeafEntry);
    template<class Key>
    class InternalPage {
        // private:
        //TODO: we can make the local lock metaddata outside the page.
    public:
        constexpr static int kInternalCardinality = (kInternalPageSize - sizeof(Header_Index<Key>) - sizeof(uint8_t) * 2 - 8 - sizeof(uint64_t) - RDMA_OFFSET) /
                                                    sizeof(InternalEntry<Key>);
        Local_Meta local_lock_meta;
//        std::atomic<uint8_t> front_version;
//        uint8_t front_version;
        alignas(8) uint64_t global_lock;
//        uint8_t busy;
//        uint8_t front_version = 0;
        Header_Index<Key> hdr;
        InternalEntry<Key> records[kInternalCardinality] = {};
//        char data[1] = {};
//  uint8_t padding[InternalPagePadding];
//        alignas(8) uint8_t rear_version;

        template<class K, class V> friend class Btr;
        friend class Cache;

    public:
        // this is called when tree grows
        InternalPage(GlobalAddress left, const Key &key, GlobalAddress right, GlobalAddress this_page_g_ptr,
                     uint32_t level = 0) {
            assert(STRUCT_OFFSET(InternalPage<Key>, local_lock_meta) == 0);
            hdr.p_type = P_Internal;
            hdr.leftmost_ptr = left;
            hdr.level = level;
            hdr.valid_page = true;
            global_lock = 0;
            records[0].key = key;
            records[0].ptr = right;
            records[1].ptr = GlobalAddress::Null();

            hdr.last_index = 0;
            assert(this_page_g_ptr!= GlobalAddress::Null());
            hdr.this_page_g_ptr = this_page_g_ptr;
            local_metadata_init();

        }

        explicit InternalPage(GlobalAddress this_page_g_ptr, uint32_t level = 0) {
            hdr.level = level;
            global_lock = 0;
            records[0].ptr = GlobalAddress::Null();
            local_metadata_init();
            assert(this_page_g_ptr!= GlobalAddress::Null());
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

        bool try_lock() {
            auto currently_locked = __atomic_load_n(&local_lock_meta.local_lock_byte, __ATOMIC_RELAXED);
            return !currently_locked &&
                   __atomic_compare_exchange_n(&local_lock_meta.local_lock_byte, &currently_locked, 1, true, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED);
        }
        inline void  unlock_lock() {
            __atomic_store_n(&local_lock_meta.local_lock_byte, 0, mem_cst_seq);
        }
        // THe local concurrency control optimization to reduce RDMA bandwidth, is worthy of writing in the paper

        void check_invalidation_and_refetch_outside_lock(GlobalAddress page_addr, RDMA_Manager *rdma_mg, ibv_mr *page_mr) {
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
//                printf("Internal page refresh\n");
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


        void check_invalidation_and_refetch_inside_lock(GlobalAddress page_addr, RDMA_Manager *rdma_mg, ibv_mr *page_mr) {
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
        bool check_whether_globallock_is_unlocked() const {

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
        bool internal_page_search(const Key &k, void *result_ptr, uint16_t current_ticket);
        bool
        internal_page_store(GlobalAddress page_addr, const Key &k, GlobalAddress value, int level, CoroContext *cxt,
                            int coro_id);
    };
#ifdef CACHECOHERENCEPROTOCOL
    template<typename TKey, typename Value>
    class LeafPage {
    public:
//        constexpr static int kLeafCardinality = (kLeafPageSize - sizeof(Header<TKey>) - sizeof(uint8_t) * 2 - 8 - sizeof(uint64_t) - RDMA_OFFSET) / sizeof(LeafEntry<TKey, Value>);
        Local_Meta local_lock_meta;
        // if busy we will not cache it in cache, switch back to the Naive
        alignas(8) uint64_t global_lock = 0;
//        uint8_t busy;
//        uint8_t front_version;
        Header_Index<TKey> hdr;
#ifdef DYNAMIC_ANALYSE_PAGE
        char data_[1];// The data segment is beyond this class.
#else
                LeafEntry<TKey, Value> records[kLeafCardinality] = {};
#endif
//  uint8_t padding[LeafPagePadding];
//        uint8_t rear_version;

        template<class K, class V> friend class Btr;

    public:
        LeafPage(GlobalAddress this_page_g_ptr, uint32_t leaf_cardinality,uint32_t level = 0) {
            assert(level == 0);
            hdr.p_type = P_Leaf;
            hdr.level = level;
            hdr.this_page_g_ptr = this_page_g_ptr;
            hdr.kLeafCardinality = leaf_cardinality;
            global_lock = 0;
//            records[0].value = {0};

//            front_version = 0;
//            rear_version = 0;
            local_lock_meta.local_lock_byte = 0;
            local_lock_meta.current_ticket = 0;
            local_lock_meta.issued_ticket = 0;
            local_lock_meta.hand_over = 0;
            local_lock_meta.hand_time = 0;

//            embedding_lock = 1;
        }

        void local_metadata_init(){
            __atomic_store_n(&local_lock_meta.local_lock_byte, 0, mem_cst_seq);
            local_lock_meta.current_ticket = 0;
            local_lock_meta.issued_ticket = 0;

            local_lock_meta.hand_over = 0;
            local_lock_meta.hand_time = 0;
        }

        void leaf_page_search(const TKey &k, SearchResult <TKey, Value> &result, ibv_mr local_mr_copied,
                              GlobalAddress g_page_ptr, RecordSchema *record_scheme);
        bool leaf_page_store(const TKey &k, const Slice &v, int &cnt, RecordSchema *record_scheme);

    };
    class Header {
    public:
        Page_Type p_type = P_Data;
        GlobalAddress this_page_g_ptr;
        int32_t number_of_records;
        friend class RDMA_Manager;
        friend class DataPage;
        uint32_t kDataCardinality;
        uint32_t table_id;
        Header() {
            number_of_records = 0;
            table_id = 0;
        }
    } __attribute__ ((aligned (8)));


    class DataPage {
    public:
//        constexpr static int kLeafCardinality = (kLeafPageSize - sizeof(Header<TKey>) - sizeof(uint8_t) * 2 - 8 - sizeof(uint64_t) - RDMA_OFFSET) / sizeof(LeafEntry<TKey, Value>);
        Local_Meta local_lock_meta;
        // if busy we will not cache it in cache, switch back to the Naive
        alignas(8) uint64_t global_lock = 0;
//        uint8_t busy;
//        uint8_t front_version;
        Header hdr;
#ifdef DYNAMIC_ANALYSE_PAGE
        char data_[1];// The data segment is beyond this class.
#else
        LeafEntry<TKey, Value> records[kLeafCardinality] = {};
#endif
//  uint8_t padding[LeafPagePadding];
//        uint8_t rear_version;

        template<class K, class V> friend class Btr;

    public:
        DataPage(GlobalAddress this_page_g_ptr, uint32_t data_cardinality, uint32_t id) {
            hdr.p_type = P_Data;
            hdr.this_page_g_ptr = this_page_g_ptr;
            hdr.kDataCardinality = data_cardinality;
            hdr.table_id = id;
            global_lock = 0;
//            records[0].value = {0};

//            front_version = 0;
//            rear_version = 0;
            local_lock_meta.local_lock_byte = 0;
            local_lock_meta.current_ticket = 0;
            local_lock_meta.issued_ticket = 0;
            local_lock_meta.hand_over = 0;
            local_lock_meta.hand_time = 0;

//            embedding_lock = 1;
        }

        void local_metadata_init(){
            __atomic_store_n(&local_lock_meta.local_lock_byte, 0, mem_cst_seq);
            local_lock_meta.current_ticket = 0;
            local_lock_meta.issued_ticket = 0;

            local_lock_meta.hand_over = 0;
            local_lock_meta.hand_time = 0;
        }

//        void Data_page_search(const TKey &k, SearchResult <TKey, Value> &result, ibv_mr local_mr_copied,
//                              GlobalAddress g_page_ptr, RecordSchema *record_scheme);
        bool InsertRecord(const Slice &tuple, int &cnt, RecordSchema *record_scheme, GlobalAddress& g_addr);
        bool AllocateRecord(int &cnt, RecordSchema *record_scheme, GlobalAddress& g_addr, char*& data_buffer);

        bool DeleteRecord(GlobalAddress g_addr, RecordSchema *record_scheme);
        bool Data_page_delete(char* local_addr);
        bool Data_page_delete(Record* out_side_record);
        static int find_empty_spot_from_bitmap(uint64_t* bitmap, uint32_t number_of_bits);
        void set_bitmap(uint64_t* bitmap, size_t index);
        void reset_bitmap(uint64_t* bitmap, size_t index);
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
