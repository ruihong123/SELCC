
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
    enum Page_Type { P_Plain = 0, P_Internal, P_Leaf};
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
    class Header {
    private:
        Page_Type p_type = P_Plain;
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

    public:
        GlobalAddress this_page_g_ptr;

        //        T* try_var{};
        T lowest{};
        T highest{};
        Header() {
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
        char key_padding[KEY_PADDING] = "";
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
        constexpr static int kInternalCardinality = (kInternalPageSize - sizeof(Header<Key>) - sizeof(uint8_t) * 2 - 8 - sizeof(uint64_t) -RDMA_OFFSET) /
                                                    sizeof(InternalEntry<Key>);
        Local_Meta local_lock_meta;
//        std::atomic<uint8_t> front_version;
//        uint8_t front_version;
        alignas(8) uint64_t global_lock;
//        uint8_t busy;
//        uint8_t front_version = 0;
        Header<Key> hdr;
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

        InternalPage(GlobalAddress this_page_g_ptr, uint32_t level = 0) {
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

        void verbose_debug() const {
            this->debug();
            for (int i = 0; i < this->hdr.last_index + 1; ++i) {
                printf("[%lu %lu] ", this->records[i].key, this->records[i].ptr.val);
            }
            printf("\n");
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
        Header<TKey> hdr;
#ifdef DYNAMIC_ANALYSE_PAGE
        char data_[1];// The data segment is beyond this class.
#else
                LeafEntry<TKey, Value> records[kLeafCardinality] = {};
#endif
//  uint8_t padding[LeafPagePadding];
//        uint8_t rear_version;

        template<class K, class V> friend class Btr;

    public:
        LeafPage(GlobalAddress this_page_g_ptr, uint32_t level = 0) {
            hdr.p_type = P_Leaf;
            hdr.level = level;
            hdr.this_page_g_ptr = this_page_g_ptr;
            global_lock = 0;
//            records[0].value = {0};

            front_version = 0;
//            rear_version = 0;
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


//        void debug() const {
//            std::cout << "LeafPage@ ";
//            hdr.debug();
//            std::cout << "version: [" << (int)front_version << ", " << (int)rear_version
//                      << "]" << std::endl;
//        }
        void leaf_page_search(const TKey &k, SearchResult <TKey, Value> &result, ibv_mr local_mr_copied,
                              GlobalAddress g_page_ptr, RecordSchema *record_scheme);
        bool leaf_page_store(const TKey &k, const Slice &v, int &cnt, RecordSchema *record_scheme);

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
    template<class Key>
    bool InternalPage<Key>::internal_page_search(const Key &k, void *result_ptr, uint16_t current_ticket) {
        SearchResult<Key,GlobalAddress>& result = *(SearchResult<Key,GlobalAddress>*)result_ptr;
        assert(k >= hdr.lowest);
//        assert(k < hdr.highest);
        uint64_t local_meta_new = __atomic_load_n((uint64_t*)&local_lock_meta, (int)std::memory_order_seq_cst);
        if (((Local_Meta*) &local_meta_new)->local_lock_byte !=0 || ((Local_Meta*) &local_meta_new)->current_ticket != current_ticket){
            return false;
        }

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

//            asm volatile ("sfence\n" : : );
//            asm volatile ("lfence\n" : : );
//            asm volatile ("mfence\n" : : );
            local_meta_new = __atomic_load_n((uint64_t*)&local_lock_meta, (int)std::memory_order_seq_cst);
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
        local_meta_new = __atomic_load_n((uint64_t*)&local_lock_meta, (int)std::memory_order_seq_cst);
        if (((Local_Meta*) &local_meta_new)->local_lock_byte !=0 || ((Local_Meta*) &local_meta_new)->current_ticket != current_ticket){
            return false;
        }
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
                    records[mid].ptr = value;
                    is_update = true;

                }
            }
            assert(left == right);
            if (BOOST_LIKELY(k!=records[left].key)){
                insert_index = left +1;
            }else{
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
//            need_split =

        // THe internal node is different from leaf nodes because it has the
        // leftmost_ptr. THe internal nodes has n key but n+1 global pointers.
        // the internal node split pick the middle key as split key and the middle key
        // will not existed in either of the splited node
        // THe data under this internal node [lowest, highest)

        //Both internal node and leaf nodes are [lowest, highest) except for the left most
        assert(local_lock_meta.local_lock_byte == 1);
        return cnt == kInternalCardinality;
    }

#ifdef CACHECOHERENCEPROTOCOL
    //TODO: make it ordered and ty not use the Sherman write amplification optimization.
    template<class Key, class Value>
    void LeafPage<Key,Value>::leaf_page_search(const Key &k, SearchResult <Key, Value> &result, ibv_mr local_mr_copied,
                                               GlobalAddress g_page_ptr, RecordSchema *record_scheme) {

#ifdef DYNAMIC_ANALYSE_PAGE
        int kLeafCardinality = record_scheme->GetLeafCardi();
        size_t tuple_length = record_scheme->GetSchemaSize();
        char* tuple_start = data_;
        uint16_t left = 0;
        uint16_t right = hdr.last_index;
        uint16_t mid = 0;
        while (left < right) {
            mid = (left + right + 1) / 2;
            tuple_start = data_ + mid*tuple_length;
            auto r = Record(record_scheme,tuple_start);
            Key temp_key;
            r.GetPrimaryKey(&temp_key);
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
        assert(right == left);
        tuple_start = data_ + right*tuple_length;
        auto r = Record(record_scheme,tuple_start);
        Key temp_key;
        r.GetPrimaryKey(&temp_key);
        if (k == temp_key){
            assert(result.val.size() == r.GetRecordSize());
            memcpy((void*)result.val.data(),r.data_ptr_, r.GetRecordSize());
            result.find_value = true;
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
        int kLeafCardinality = record_scheme->GetLeafCardi();
        int tuple_length = record_scheme->GetSchemaSize();
        char* tuple_start;
        tuple_start = data_ + 0*tuple_length;

        auto r_temp = Record(record_scheme,tuple_start);
        TKey temp_key1;
        r_temp.GetPrimaryKey((char*)&temp_key1);
        if (k < temp_key1 || hdr.last_index == -1) {
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
                    memcpy(r.data_ptr_,v.data(), r.GetRecordSize());
                    is_update = true;
                    return cnt == kLeafCardinality;
                }
            }
            assert(left == right);
            tuple_start = data_ + left*tuple_length;
            auto r = Record(record_scheme,tuple_start);
            TKey temp_key;
            r.GetPrimaryKey(&temp_key);
            if ((k != temp_key )){
//                DEBUG_ASSERT_CONDITION(false);
                insert_index = left +1;
            }else{
                assert(v.size() == r.GetRecordSize());
                memcpy(r.data_ptr_,v.data(), r.GetRecordSize());
                is_update = true;
                return cnt == kLeafCardinality;
            }




        }

        assert(cnt != kLeafCardinality);
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
                assert(insert_index < kLeafCardinality );
                auto r = Record(record_scheme,tuple_start);
                assert(v.size() == r.GetRecordSize());
                r.ReSetRecord(v.data_reference(), v.size());
            }

//    memcpy(r.value_padding, padding, VALUE_PADDING);
//            r.f_version++;
//            r.r_version = r.f_version;
            cnt++;
            hdr.last_index++;
        assert(hdr.last_index < kLeafCardinality);
//        }

        return cnt == kLeafCardinality;
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
#endif //MEMORYENGINE_PAGE_H
