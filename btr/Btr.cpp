#include "Btr.h"
#include <atomic>
namespace DSMEngine {
//    template class btree_iterator<uint64_t , uint64_t>;

    template class Btr<uint64_t>;
    template class Btr<Secondary_Key<uint64_t, uint64_t>>;
//    template class Btr<uint64_t , char[100]>;
    template<class Key>
    class InternalPage;

    template<class Key>
    class LeafPage;



//    bool enter_debug = false;

//struct tranverse_stack_element
    template <typename Key>
    thread_local int Btr<Key>::nested_retry_counter = 0;
//HotBuffer hot_buf;

//    volatile bool need_stop = false;
    template <typename Key>
    thread_local size_t Btr<Key>::round_robin_cur = 0;
    // template <typename Key, typename Value>
    // thread_local CoroCall Btr<Key,Value>::worker[define::kMaxCoro];
    // template <typename Key, typename Value>
    // thread_local CoroCall Btr<Key,Value>::master;
//thread_local GlobalAddress path_stack[define::kMaxCoro]
//                                     [define::kMaxLevelOfTree];
    template <typename Key>
    thread_local SearchResult<Key>* Btr<Key>::search_result_memo = nullptr;
    extern thread_local GlobalAddress path_stack[define::kMaxCoro][define::kMaxLevelOfTree];
    template <typename Key>
//RDMA_Manager * Btr<Key,Value>::rdma_mg = nullptr;
// for coroutine schedule
    struct CoroDeadline {
        uint64_t deadline;
        uint16_t coro_id;

        bool operator<(const CoroDeadline &o) const {
            return this->deadline < o.deadline;
        }
    };

    static inline uint32_t HashSlice(const Slice& s) {
        return Hash(s.data(), s.size(), 0);
    }

//thread_local Timer timer;
//    thread_local std::queue<uint16_t> hot_wait_queue;
//thread_local std::priority_queue<CoroDeadline> deadline_queue;
//    static void Deallocate_MR(Cache::Handle *handle) {
//        assert(handle->refs.load() == 0);
//        auto rdma_mg = RDMA_Manager::Get_Instance(nullptr);
//        auto mr = (ibv_mr*) handle->value;
//        if (!handle->keep_the_mr){
//            rdma_mg->Deallocate_Local_RDMA_Slot(mr->addr, Regular_Page);
//            delete mr;
//        }
//        assert(handle->refs.load() == 0);
//
//
//    }
//TODO: make the function set cache handle as an argument, and we need to modify the remote lock status
// when unlocking the remote lock.
    template <typename Key>
    Btr<Key>::Btr(DDSM *dsm, Cache *cache_ptr, RecordSchema *record_scheme_ptr)
            : scheme_ptr(record_scheme_ptr), page_cache(cache_ptr), ddms_(dsm){
        assert(sizeof(LeafPage<Key>) < kLeafPageSize);
        assert(sizeof(InternalPage<Key>) < kInternalPageSize);
        assert(STRUCT_OFFSET(LeafPage<char>,hdr) == STRUCT_OFFSET(LeafPage<uint64_t>,hdr));
        if (rdma_mg == nullptr){
            rdma_mg = ddms_->rdma_mg;
        }
        assert(sizeof(InternalPage<Key>) <= kInternalPageSize);
//        leaf_cardinality_ = (kLeafPageSize - STRUCT_OFFSET(LeafPage<Key COMMA Value>, data_[0])) / scheme_ptr->GetSchemaSize();
        leaf_cardinality_ = LeafPage<Key>::calculate_cardinality(kLeafPageSize, scheme_ptr->GetSchemaSize());
        print_verbose();
        assert(g_root_ptr.is_lock_free());
        cached_root_page_handle.store(nullptr);
    }
    template <typename Key>
    Btr<Key>::Btr(DDSM *dsm, Cache *cache_ptr, RecordSchema *record_scheme_ptr, uint16_t Btr_id, bool secondary)
            : scheme_ptr(record_scheme_ptr), tree_id(Btr_id), page_cache(cache_ptr), ddms_(dsm), secondary_(secondary){
        assert(sizeof(LeafPage<Key>) < kLeafPageSize);
        assert(sizeof(InternalPage<Key>) < kInternalPageSize);
        // the secondary index type here is deprecated. If secondary key is needed we need to define the Key in
        // the template a compound key, containing both attibute value and tupleID/primary key.
        assert(!secondary_);
        assert(STRUCT_OFFSET(LeafPage<char>,hdr) == STRUCT_OFFSET(LeafPage<uint64_t>,hdr));
        if (rdma_mg == nullptr){
            rdma_mg = ddms_->rdma_mg;
        }
        assert(sizeof(InternalPage<Key>) <= kInternalPageSize);
        // The end of page is the page forward check pointer.
//        leaf_cardinality_ = (kLeafPageSize - STRUCT_OFFSET(LeafPage<Key COMMA Value>, data_[0]) - sizeof(uint8_t)) / scheme_ptr->GetSchemaSize();
        leaf_cardinality_ = LeafPage<Key>::calculate_cardinality(kLeafPageSize, scheme_ptr->GetSchemaSize());
        print_verbose();
        assert(g_root_ptr.is_lock_free());
        //TODO: simplify the code below by SELCC APIs.
        if (DSMEngine::RDMA_Manager::node_id == 0){
            // only the first compute node create the root node for index
            g_root_ptr = rdma_mg->Allocate_Remote_RDMA_Slot(Regular_Page, 2 * round_robin_cur + 1); // remote allocation.
            assert(g_root_ptr.load().nodeID == 2 * round_robin_cur + 1);
            printf("root pointer is %d, %lu\n", g_root_ptr.load().nodeID, g_root_ptr.load().offset);
            if(++round_robin_cur == rdma_mg->memory_nodes.size()){
                round_robin_cur = 0;
            }
            void* root_page_buf = nullptr;
            GlobalAddress Gptr = g_root_ptr.load();
            left_most_leaf = Gptr; // THis will be unchanged.
            Slice page_id((char *) &Gptr, sizeof(GlobalAddress));
            std::unique_lock<RWSpinLock> lck(root_mtx);
            // TODO: make it utilize SELCC APIs.
            // Remember to release the handle when the root page has been changed.
            assert((Gptr.offset % 1ULL*1024ULL*1024ULL*1024ULL)% kLeafPageSize == 0);
            auto temp_handle = page_cache->LookupInsert(page_id, nullptr, kLeafPageSize, Deallocate_MR_WITH_CCP);
            cached_root_page_handle.store(temp_handle);
            auto mr = new ibv_mr{};
            rdma_mg->Allocate_Local_RDMA_Slot(*mr, Regular_Page);
            memset(mr->addr,0,rdma_mg->name_to_chunksize.at(Regular_Page));
            cached_root_page_handle.load()->value = mr;
            assert(cached_root_page_handle.load()->remote_lock_status == 0);
            root_page_buf = mr->addr;
            assert(root_page_buf);
            auto root_page = new(root_page_buf) LeafPage<Key>(g_root_ptr, leaf_cardinality_, scheme_ptr->GetSchemaSize());

//            root_page->front_version++;
//            root_page->rear_version = root_page->front_version;
            rdma_mg->RDMA_Write(g_root_ptr, (ibv_mr*)cached_root_page_handle.load()->value, kLeafPageSize, IBV_SEND_SIGNALED, 1, Regular_Page);
            auto local_mr = rdma_mg->Get_local_CAS_mr(); // remote allocation.
            ibv_mr remote_mr{};
            remote_mr = *rdma_mg->global_index_table;
            // find the table enty according to the id
            remote_mr.addr = (void*) ((char*)remote_mr.addr + 8*tree_id);
            printf("Writer to remote address %p", remote_mr.addr);
            rdma_mg->RDMA_CAS(&remote_mr, local_mr, 0, g_root_ptr.load(), IBV_SEND_SIGNALED, 1, 1);
            assert(*(uint64_t*)local_mr->addr == 0);

        }else{
//            memset(cached_root_page_mr.load()->addr,0,rdma_mg->name_to_chunksize.at(Regular_Page));
//        rdma_mg->Allocate_Local_RDMA_Slot()
            Cache::Handle* dummy_hd;
            get_root_ptr_protected(dummy_hd);
        }

//  auto cas_buffer = (rdma_mg->get_rbuf(0)).get_cas_buffer();
//  bool res = rdma_mg->cas_sync(root_ptr_ptr, 0, root_addr.val, cas_buffer);
//  if (res) {
//    std::cout << "Tree root pointer value " << root_addr << std::endl;
//  } else {
//     std::cout << "fail\n";
//  }

    }
    template <typename Key>
    void Btr<Key>::print_verbose() {

        int kInternalHdrOffset = STRUCT_OFFSET(InternalPage<Key>, hdr);
        int kLeafHdrOffset = (char *)&((LeafPage<Key> *)(0))->hdr - (char *)((LeafPage<Key> *)(0));
//            STRUCT_OFFSET(LeafPage<Key,Value>, hdr);

        assert(kLeafHdrOffset == kInternalHdrOffset);

        if (rdma_mg->node_id == 0) {
            std::cout << "Header size: " << sizeof(Header_Index<Key>) << std::endl;
            std::cout << "Internal_and_Leaf Page size: " << sizeof(InternalPage<Key>) << " ["
                      << kInternalPageSize << "]" << std::endl;
            std::cout << "Internal_and_Leaf per Page: " << InternalPage<Key>::kInternalCardinality << std::endl;
            std::cout << "Leaf Page size: " << sizeof(LeafPage<Key>) << " [" << kLeafPageSize
                      << "]" << std::endl;
            std::cout << "Leaf per Page: " << leaf_cardinality_ << std::endl;
//            std::cout << "LeafEntry size: " << sizeof(LeafEntry<Key>) << std::endl;
            std::cout << "InternalEntry size: " << sizeof(InternalEntry<Key>) << std::endl;
        }
    }
    template <typename Key>
    inline void Btr<Key>::before_operation(CoroContext *cxt, int coro_id) {
        for (size_t i = 0; i < define::kMaxLevelOfTree; ++i) {
            path_stack[coro_id][i] = GlobalAddress::Null();
        }
    }
    template <typename Key>
    GlobalAddress Btr<Key>::get_root_ptr_ptr() {
        GlobalAddress addr;
        addr.nodeID = 0;
        addr.offset =
                define::kRootPointerStoreOffest + sizeof(GlobalAddress) * tree_id;

        return addr;
    }




//extern GlobalAddress g_root_ptr;
//extern int g_root_level;
//extern bool enable_cache;
    template <typename Key>
    GlobalAddress Btr<Key>::get_root_ptr_protected(Cache::Handle *&root_hint_handle) {
        //Note it is okay if cached_root_page_mr is an older version for the g_root_ptr, because when we use the
        // page we will check whether this page is correct or not

        GlobalAddress root_ptr = g_root_ptr.load();
        root_hint_handle = cached_root_page_handle.load();
        if (root_ptr == GlobalAddress::Null()) {
            std::unique_lock<RWSpinLock> l(root_mtx);

            root_ptr = g_root_ptr.load();
            root_hint_handle = cached_root_page_handle.load();
            if (root_ptr == GlobalAddress::Null()) {
//          assert(cached_root_page_mr = nullptr);
                refetch_rootnode();
                root_ptr = g_root_ptr.load();
                root_hint_handle = cached_root_page_handle.load();
            }
            return root_ptr;
        } else {
//      assert(((InternalPage*)cached_root_page_mr->addr)->hdr.this_page_g_ptr == root_ptr);
//      root_hint = cached_root_page_mr;
            return root_ptr;
        }

        // std::cout << "root ptr " << root_ptr << std::endl;
    }
    template <typename Key>
    GlobalAddress Btr<Key>::get_root_ptr(Cache::Handle *&root_hint_handle) {
        //Note it is okay if cached_root_page_mr is an older version for the g_root_ptr, because when we use the
        // page we will check whether this page is correct or not

        GlobalAddress root_ptr = g_root_ptr.load();
        root_hint_handle = cached_root_page_handle.load();
        if (root_ptr == GlobalAddress::Null()) {

            refetch_rootnode();
            root_ptr = g_root_ptr.load();
            root_hint_handle = cached_root_page_handle.load();
            return root_ptr;
        } else {
            return root_ptr;
        }

        // std::cout << "root ptr " << root_ptr << std::endl;
    }
// should be protected by a mtx outside.
    template <typename Key>
    void Btr<Key>::refetch_rootnode() {
        // TODO: an alternative design is to insert this page into the cache. How to make sure there is no
        //  reader reading this old root note? If we do not deallocate it there will be registered memory leak
        //  we can lazy recycle this registered memory. Or we just ignore this memory leak because it will
        //  only happen at the root page.

//            rdma_mg->Deallocate_Local_RDMA_Slot(cached_root_page_mr.load()->addr, Internal_and_Leaf);
//            delete cached_root_page_mr.load();

        ibv_mr* local_mr = rdma_mg->Get_local_CAS_mr();

        ibv_mr remote_mr{};
        remote_mr = *rdma_mg->global_index_table;
        // find the table enty according to the id
        remote_mr.addr = (void*) ((char*)remote_mr.addr + 8*tree_id);
        *(GlobalAddress*)(local_mr->addr) = GlobalAddress::Null();
        // The first compute node may not have written the root ptr to root_ptr_ptr, we need to keep polling.
        while (*(GlobalAddress*)(local_mr->addr) == GlobalAddress::Null()) {
            rdma_mg->RDMA_Read(&remote_mr, local_mr, sizeof(GlobalAddress), IBV_SEND_SIGNALED, 1, 1);
        }
        assert(*(GlobalAddress*)local_mr->addr != GlobalAddress::Null());
        GlobalAddress root_ptr = *(GlobalAddress*)local_mr->addr;
//        printf("cached_root_page_handle is %p", cached_root_page_handle.load());
        if (cached_root_page_handle.load() != nullptr &&
        root_ptr == cached_root_page_handle.load()->gptr){
            g_root_ptr.store(root_ptr);
            return;
        }
        uint8_t last_level = tree_height.load();
        GlobalAddress last_root = g_root_ptr.load();
        Slice page_id((char *) &root_ptr, sizeof(GlobalAddress));
        // We assume the old root page will not be quickly evicted from the local cache, so we can release the handle immediately
        // after a new root is detected and the old root buffer can still be valid.
        // TODO: What if the assumption is not correct?
        ibv_mr* temp_mr = nullptr;
        assert((root_ptr.offset % 1ULL*1024ULL*1024ULL*1024ULL)% kLeafPageSize == 0);
        // Remember to release the handle when the root page has been changed.
        Cache::Handle* temp_handle = page_cache->LookupInsert(page_id, nullptr, kLeafPageSize, Deallocate_MR_WITH_CCP);
        // TODO: need to have some mechanisms to gurantee the integraty of fetched root page, either optimistic way or pessimistic way.
        if(temp_handle->value == nullptr){
            //Try to rebuild a local mr for the new root, the old root may
            temp_mr = new ibv_mr{};

            // try to init tree and install root pointer
            rdma_mg->Allocate_Local_RDMA_Slot(*temp_mr, Regular_Page);// local allocate
            memset(temp_mr->addr,0,rdma_mg->name_to_chunksize.at(Regular_Page));
            temp_handle->value = temp_mr;

        }else{
            temp_mr = (ibv_mr*)temp_handle->value;
        }
        //Read the tree height below
        ibv_mr* local_buffer = rdma_mg->Get_local_CAS_mr();
        GlobalAddress level_fetch_addr = root_ptr;
        level_fetch_addr.offset = root_ptr.offset + STRUCT_OFFSET(InternalPage<uint64_t>, hdr.level);
        rdma_mg->RDMA_Read(level_fetch_addr, local_buffer, sizeof(uint8_t), IBV_SEND_SIGNALED, 1, Regular_Page);

//        assert(((DataPage*)((ibv_mr*)temp_handle->value)->addr)->hdr.this_page_g_ptr == root_ptr);
//        std::unique_lock<std::shared_mutex> lck(root_handle_mtx);
        if (cached_root_page_handle.load() != nullptr){

            page_cache->Release(cached_root_page_handle.load());
        }
        // todo: how can we know current tree height if we do not read the page content.
        auto height_temp = *(uint8_t*)local_buffer->addr;
        assert(height_temp >= tree_height.load());
        cached_root_page_handle.store(temp_handle);
        g_root_ptr.store(root_ptr);

        tree_height.store(height_temp);
        assert(last_level <=  height_temp);
        printf("Get new root node id is %u, offset is %lu, tree id is %lu, this node_id is %hu, tree height is %hhu\n", g_root_ptr.load().nodeID, g_root_ptr.load().offset, tree_id, DSMEngine::RDMA_Manager::node_id, tree_height.load());
//        if (last_level > 0){
//            assert(last_level != tree_height.load());
//        }
        assert(g_root_ptr != GlobalAddress::Null());
//        root_hint = temp_mr;
    }
    template <typename Key>
    void Btr<Key>::broadcast_new_root(GlobalAddress new_root_addr, int root_level) {
        RDMA_Request* send_pointer;
        ibv_mr send_mr = {};
//    ibv_mr receive_mr = {};
        rdma_mg->Allocate_Local_RDMA_Slot(send_mr, Message);
        send_pointer = (RDMA_Request*)send_mr.addr;
        send_pointer->command = broadcast_root;
        send_pointer->content.root_broadcast.new_ptr = new_root_addr;
        send_pointer->content.root_broadcast.level = root_level;

        //TODO: When we seperate the compute from the memory, how can we broad cast the new root
        // or can we wait until the compute node detect an inconsistent.

        rdma_mg->post_send<RDMA_Request>(&send_mr, 1, std::string("main"));
        ibv_wc wc[2] = {};
        //  while(wc.opcode != IBV_WC_RECV){
        //    poll_completion(&wc);
        //    if (wc.status != 0){
        //      fprintf(stderr, "Work completion status is %d \n", wc.status);
        //    }
        //
        //  }
        //  assert(wc.opcode == IBV_WC_RECV);
        if (rdma_mg->poll_completion(wc, 1, std::string("main"),
                                     true, 1)){
//    assert(try_poll_completions(wc, 1, std::string("main"),true) == 0);
            fprintf(stderr, "failed to poll send for remote memory register\n");
        }
    }
    template <typename Key>
    bool Btr<Key>::update_new_root(GlobalAddress left, const Key &k,
                                         GlobalAddress right, int level,
                                         GlobalAddress old_root, CoroContext *cxt,
                                         int coro_id) {

        assert(level > 0);
        auto cas_buffer = rdma_mg->Get_local_CAS_mr();

        // TODO: recycle the olde registered memory, but we need to make sure that there
        // is no pending access over that old mr. (Temporarily not recyle it)
        ibv_mr* page_mr = new ibv_mr{};

        // try to init tree and install root pointer
        rdma_mg->Allocate_Local_RDMA_Slot(*page_mr, Regular_Page);// local allocate
        memset(page_mr->addr, 0, rdma_mg->name_to_chunksize.at(Regular_Page));
//  auto page_buffer = rdma_mg->Get_local_read_mr();

        assert(left != GlobalAddress::Null());
        assert(right != GlobalAddress::Null());
        assert(level < 100);
        auto new_root_addr = rdma_mg->Allocate_Remote_RDMA_Slot(Regular_Page, 2 * round_robin_cur + 1);
        if(++round_robin_cur == rdma_mg->memory_nodes.size()){
            round_robin_cur = 0;
        }
        assert(level >0);
        auto new_root = new(page_mr->addr) InternalPage<Key>(left, k, right, new_root_addr, secondary_, level);



        Slice page_id((char *) &new_root_addr, sizeof(GlobalAddress));
        // Remember to release the handle when the root page has been changed.
        Cache::Handle* temp_handle = page_cache->Insert(page_id, page_mr, kLeafPageSize, Deallocate_MR_WITH_CCP);
        assert(temp_handle->value == page_mr);
            //Try to rebuild a local mr for the new root, the old root may
//        temp_handle->value = page_buffer;

        if (cached_root_page_handle.load() != nullptr){
            page_cache->Release(cached_root_page_handle.load());
        }
        cached_root_page_handle.store(temp_handle);
        // set local cache for root address
        g_root_ptr.store(new_root_addr,std::memory_order_seq_cst);
        assert(level>=tree_height.load());
        tree_height.store(level);
        assert(new_root->hdr.level == level);
        rdma_mg->RDMA_Write(new_root_addr, page_mr, kInternalPageSize, IBV_SEND_SIGNALED, 1, Regular_Page);
        ibv_mr remote_mr = *rdma_mg->global_index_table;
        // find the table enty according to the id
        remote_mr.addr = (void*) ((char*)remote_mr.addr + 8*tree_id);
        //TODO: The new root seems not be updated by the CAS, the old root and new_root addr are the same
        if (!rdma_mg->RDMA_CAS(&remote_mr, cas_buffer, old_root, new_root_addr, IBV_SEND_SIGNALED, 1, 1)) {
            assert(*(uint64_t*)cas_buffer->addr == (uint64_t)old_root);
            printf("Update the root global buffer %p successfully new root node is %d, offset is %llu, level is %u tree id is %llu, this node id is %lu\n",remote_mr.addr, new_root_addr.nodeID, new_root_addr.offset , level, tree_id, rdma_mg->node_id);
            broadcast_new_root(new_root_addr, level);
//#ifndef NDEBUG
//            usleep(10);
//            ibv_wc wc[2];
//            auto qp_type = std::string("default");
//            assert(rdma_mg->try_poll_completions(wc, 1, qp_type, true, 1) == 0);
//#endif
            return true;
        } else {
            std::cout << "cas root fail " << std::endl;
        }

        return false;
    }
//    template <typename Key, typename Value>
//    void Btr<Key,Value>::print_and_check_tree(CoroContext *cxt, int coro_id) {
////  assert(rdma_mg->is_register());
//        ibv_mr* page_hint;
//        auto root = get_root_ptr_protected(page_hint);
//        // SearchResult result;
//
//        GlobalAddress p = root;
//        GlobalAddress levels[define::kMaxLevelOfTree];
//        int level_cnt = 0;
//        auto page_buffer = rdma_mg->Get_local_read_mr();
//        GlobalAddress leaf_head;
//
//        next_level:
//
//        rdma_mg->RDMA_Read(p, page_buffer, kInternalPageSize, IBV_SEND_SIGNALED, 1, Internal_and_Leaf);
//        auto header = (Header<Key> *)((char*)page_buffer->addr + (STRUCT_OFFSET(LeafPage<Key COMMA Value>, hdr)));
//        levels[level_cnt++] = p;
//        if (header->level != 0) {
//            p = header->leftmost_ptr;
//            goto next_level;
//        } else {
//            leaf_head = p;
//        }
//
//        next:
//        rdma_mg->RDMA_Read(leaf_head, page_buffer, kLeafPageSize, IBV_SEND_SIGNALED, 1, Internal_and_Leaf);
////  rdma_mg->read_sync(page_buffer, , kLeafPageSize);
//        auto page = (LeafPage<Key,Value> *)page_buffer;
//        for (int i = 0; i < LeafPage<Key, Value>::kLeafCardinality; ++i) {
//            if (page->records[i].value != kValueNull<Key>) {
//            }
//        }
//        while (page->hdr.sibling_ptr != GlobalAddress::Null()) {
//            leaf_head = page->hdr.sibling_ptr;
//            goto next;
//        }
//
//        // for (int i = 0; i < level_cnt; ++i) {
//        //   rdma_mg->read_sync(page_buffer, levels[i], kLeafPageSize);
//        //   auto header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
//        //   // std::cout << "addr: " << levels[i] << " ";
//        //   // header->debug();
//        //   // std::cout << " | ";
//        //   while (header->sibling_ptr != GlobalAddress::Null()) {
//        //     rdma_mg->read_sync(page_buffer, header->sibling_ptr, kLeafPageSize);
//        //     header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
//        //     // std::cout << "addr: " << header->sibling_ptr << " ";
//        //     // header->debug();
//        //     // std::cout << " | ";
//        //   }
//        //   // std::cout << "\n------------------------------------" << std::endl;
//        //   // std::cout << "------------------------------------" << std::endl;
//        // }
//    }

//Note: this function will make sure the insert will definitely success. it willkeep retrying/
    template<typename Key>
    bool Btr<Key>::insert_internal(Key &k, GlobalAddress &v, CoroContext *cxt,
                                         int coro_id, int target_level) {

        //TODO: You need to acquire a lock when you write a page
        Cache::Handle* page_hint = nullptr;
        auto root = get_root_ptr_protected(page_hint);
        assert(target_level <= tree_height.load());
        SearchResult<Key> result;

        GlobalAddress p = root;

        //TODO: ADD support for root invalidate and update.


        bool isroot = true;
        // this is root is to help the tree to refresh the root node because the
        // new root broadcast is not usable if physical disaggregated.
        int level = -1;
        //TODO: What if we ustilize the cache tree height for the root level?

        next: // Internal_and_Leaf page search
        //TODO: What if the target_level is equal to the root level.
        assert(target_level <= tree_height.load());
        if (!internal_page_search(p, k, result, level, isroot, page_hint, cxt, coro_id)) {
            if (isroot || path_stack[coro_id][result.level +1] == GlobalAddress::Null()){
                p = get_root_ptr_protected(page_hint);
                level = -1;
            }else{
                // fall back to upper level
                assert(level == result.level|| level == -1);
                p = path_stack[coro_id][result.level +1];
                page_hint = nullptr;
                level = result.level +1;
            }
            goto next;
        }else{
            assert(level == result.level);
            isroot = false;
            page_hint = nullptr;
            // if the root and sibling are the same, it is also okay because the
            // p will not be changed

            if (level > target_level){
                if (result.slibing != GlobalAddress::Null()) { // turn right
                    p = result.slibing;

                }else if (result.next_level != GlobalAddress::Null()){
                    assert(result.next_level != GlobalAddress::Null());
                    //Probelm here
                    p = result.next_level;
                    level = result.level - 1;
                }else{

                }
                if (level != target_level){
                    goto next;
                }
            }else if(level < target_level){
                // Since return true will not invalidate the root node, here we manually invalidate it outside,
                // Otherwise, there will be a deadloop.
                {
                    std::unique_lock<RWSpinLock> l(root_mtx);
                    g_root_ptr.store(GlobalAddress::Null());
                }

                p = get_root_ptr_protected(page_hint);
                level = -1;
                goto next;
            }else{
                //do nothing, the p and level is correct.
            }

        }
        assert(level == target_level);
        //Insert to target level
        Key split_key;
        GlobalAddress sibling_prt;
        assert(p != GlobalAddress::Null());
        bool store_success = internal_page_store(p, k, v, level, cxt, coro_id);
        if (!store_success){
            //TODO: need to understand why the result is always false.
            if (path_stack[coro_id][level + 1] != GlobalAddress::Null()){
                p = path_stack[coro_id][level + 1];
                level = level + 1;
            }
            else{
                // re-search the tree from the scratch. (only happen when root and leaf are the same.)
                p = get_root_ptr_protected(page_hint);
                level = -1;
            }
            goto next;
        }
        return true;
//    internal_page_store(p, k, v, level, cxt, coro_id);
    }
    template<typename Key>
    void Btr<Key>::insert(const Key &k, const Slice &v, CoroContext *cxt, int coro_id) {
//  assert(rdma_mg->is_register());
#ifndef NDEBUG
        //check whether the primary key equal to the k.
        assert(k == *(Key*)v.data());
        Record record = Record(scheme_ptr, const_cast<char *>(v.data()));
        Key pri_k;
        record.GetPrimaryKey(&pri_k);
        assert(pri_k == k);
#endif
        before_operation(cxt, coro_id);


        Cache::Handle* page_hint = nullptr;
        auto root = get_root_ptr_protected(page_hint);
        assert(root != GlobalAddress::Null());
        GlobalAddress p = root;
//  std::cout << "The root now is " << root << std::endl;
        SearchResult<Key> result{0};
        char result_buff[16];
        result.val.Reset(result_buff, 16);
//        memset(&result, 0, sizeof(SearchResult<Key, Value>));
        bool isroot = true;
        // this is root is to help the tree to refresh the root node because the
        // new root broadcast is not usable if physical disaggregated.
        int level = -1;
        int fall_back_level = 0;
//TODO: What if we ustilize the cache tree height for the root level?

//    int target_level = 0;
#ifdef PROCESSANALYSIS
        auto start = std::chrono::high_resolution_clock::now();
#endif
        int next_times = 0;

    next: // Internal_and_Leaf page search

        if (next_times++ == 100000){
            if (next_times%10 == 0){
                printf("this result level is %d\n", result.level);
            }
            assert(false);
        }
#if ACCESS_MODE == 0
        uint8_t tree_h = this->tree_height.load();
        if (tree_h > 0){
            spin_wait_us(next_times/(2*tree_h));
        }
#endif

//#endif

        if (!isroot){
//            assert(p != root);
        }
        assert(level <= tree_height.load());
        if (!internal_page_search(p, k, result, level, isroot, page_hint, cxt, coro_id)) {
            if (isroot || path_stack[coro_id][result.level +1] == GlobalAddress::Null()){
                isroot = true;
                p = get_root_ptr_protected(page_hint);
                printf("revisit the root, this nodeid is %lu\n", RDMA_Manager::node_id);
                fflush(stdout);
                level = -1;
            }else{
                // fall back to upper level
                assert(level == result.level || level == -1);
#ifndef NDEBUG
                printf("fall back to the upper level, this nodeid is %lu, this thread is %d, This gptr %p, upper gptr is %p\n", RDMA_Manager::node_id, RDMA_Manager::thread_id, p, path_stack[coro_id][result.level +1]);
                fflush(stdout);
#endif
                p = path_stack[coro_id][result.level +1];
                if (p == root){
                    isroot = true;
                }
                page_hint = nullptr;
                level = result.level +1;
            }

            goto next;
        }
        else{
            assert(level == result.level);
            isroot = false;
            page_hint = nullptr;
            // if the root and sibling are the same, it is also okay because the
            // p will not be changed
            if (result.slibing != GlobalAddress::Null()) { // turn right
                // this has been obsoleted, we nest the turn right page search inside the function.
                assert(false);
                p = result.slibing;

            }else if (result.next_level != GlobalAddress::Null()){
                assert(result.next_level != p);

                p = result.next_level;
                level = result.level - 1;
//                printf("move to the next level this level %d, next level %d, this gaddr node id %lu, offset %lu, next nodeid %lu offset %lu this nodeid is %lu\n",
//                       result.level, result.level - 1, p.nodeID, p.offset, result.next_level.nodeID, result.next_level.offset, RDMA_Manager::node_id);
//                fflush(stdout);

                assert(result.next_level != GlobalAddress::Null());
            }else{
//                assert(tree_height == 0);
//                printf("happens when there is only one level, tree height is %d\n", tree_height.load());
//                fflush(stdout);
            }

            if (level != 0){
                // level ==0 is corresponding to the corner case where the leaf node and root node are the same.
                assert(!result.is_leaf);

                goto next;
            }

        }
        assert(level == 0);
        //Insert to leaf level
        Key split_key;
        GlobalAddress sibling_prt = GlobalAddress::Null();
//    if (target_level == 0){
//
//    }
#ifdef PROCESSANALYSIS
        if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
          auto stop = std::chrono::high_resolution_clock::now();
          auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//#ifndef NDEBUG
          printf("internal node tranverse uses (%ld) ns, next time is %d\n", duration.count(), next_times);
//          TimePrintCounter = 0;
      }
//#endif
#endif

//#ifdef PROCESSANALYSIS
//    start = std::chrono::high_resolution_clock::now();
//#endif
        if (!leaf_page_store(p, k, v, split_key, sibling_prt, 0, cxt, coro_id)){
            if (path_stack[coro_id][1] != GlobalAddress::Null()){

                p = path_stack[coro_id][1];
                if (p == root){
                    isroot = true;
                }
                level = 1;
                printf("Fall back to the level 1\n");
                fflush(stdout);

            }
            else{

                // re-search the tree from the scratch. (only happen when root and leaf are the same.)
                p = get_root_ptr_protected(page_hint);
                isroot = true;
                level = -1;
                printf("Fall back to root\n");
                fflush(stdout);


            }
#ifndef NDEBUG
            next_times++;
#endif
            if (next_times++ == 999){
                printf("break here\n");
            }
            goto next;
        }
//#ifdef PROCESSANALYSIS
//    if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
//        auto stop = std::chrono::high_resolution_clock::now();
//        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
////#ifndef NDEBUG
//        printf("leaf node store uses (%ld) ns\n", duration.count());
//        TimePrintCounter[RDMA_Manager::thread_id] = 0;
//    }else{
//        TimePrintCounter[RDMA_Manager::thread_id]++;
//    }
////#endif
//#endif
        //======================== below is about nested node split ============================//
        assert(level == 0);

    }

    template <typename Key>
    bool Btr<Key>::search(const Key &k, const Slice &value_buff, CoroContext *cxt, int coro_id) {
//  assert(rdma_mg->is_register());
        Cache::Handle* page_hint = nullptr;
        auto root = get_root_ptr_protected(page_hint);
        SearchResult<Key> result;
        memset(&result, 0, sizeof(SearchResult<Key>));
//        if(!search_result_memo){
//            search_result_memo = new SearchResult<Key,Value>();
//        }
        result.val = value_buff;

        GlobalAddress p = root;
        bool isroot = true;
        bool from_cache = false;
//  const CacheEntry *entry = nullptr;
//  if (enable_cache) {
//    GlobalAddress cache_addr;
//    entry = page_cache->search_from_cache(k, &cache_addr);
//    if (entry) { // cache hit
////      cache_hit_valid[rdma_mg->getMyThreadID()][0]++;
//      from_cache = true;
//      p = cache_addr;
//      isroot = false;
//    } else {
////      cache_miss[rdma_mg->getMyThreadID()][0]++;
//    }
//  }
        int level = -1;
//TODO: What if we ustilize the cache tree height for the root level?
//TODO: Change it into while style code.
#ifdef PROCESSANALYSIS
        auto start = std::chrono::high_resolution_clock::now();
#endif
//#ifndef NDEBUG
        int next_times = 0;
//#endif
        next: // Internal_and_Leaf page search
//#ifndef NDEBUG
        if (next_times++ == 1000){
            assert(false);
        }
//#endif

        if (!internal_page_search(p, k, result, level, isroot, page_hint, cxt, coro_id)) {
            //The traverser failed to move to the next level
            if (isroot || path_stack[coro_id][result.level +1] == GlobalAddress::Null()){
                p = get_root_ptr_protected(page_hint);
                level = -1;
            }else{
                // fall back to upper level
                assert(level == result.level|| level == -1);
                p = path_stack[coro_id][result.level +1];
                page_hint = nullptr;
                level = result.level + 1;
            }
            goto next;
        }
        else{
            // The traversing moving the the next level correctly
            assert(level == result.level|| level == -1);
            isroot = false;
            page_hint = nullptr;
            // Do not need to
            if (result.slibing != GlobalAddress::Null()) { // turn right
                p = result.slibing;

            }else if (result.next_level != GlobalAddress::Null()){
                assert(result.next_level != GlobalAddress::Null());
                p = result.next_level;
                level = result.level - 1;
            }else{}

            if (level != 0 && level != -1){
                // If Level is 1 then the leaf node and root node are the same.
                assert(!result.is_leaf);

                goto next;
            }

        }
#ifdef PROCESSANALYSIS
        if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//#ifndef NDEBUG
        printf("internal node tranverse uses (%ld) ns, next time is %d\n", duration.count(), next_times);
//          TimePrintCounter = 0;
    }
//#endif
#endif
#ifdef PROCESSANALYSIS
        start = std::chrono::high_resolution_clock::now();
#endif
        leaf_next:// Leaf page search

        assert(result.val.data()!= nullptr);
        if (!leaf_page_search(p, k, result, level, cxt, coro_id)){
            if (path_stack[coro_id][1] != GlobalAddress::Null()){
                p = path_stack[coro_id][1];
                level = 1;

            }
            else{
                p = get_root_ptr_protected(page_hint);
                level = -1;
            }
#ifndef NDEBUG
            next_times++;
#endif
            DEBUG_PRINT_CONDITION("back off for search\n");
            goto next;
        }else{
            if (result.find_value) { // find
//                value_buff = result.val;
#ifdef PROCESSANALYSIS
                if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
                auto stop = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
                printf("leaf page fetch and search the page uses (%ld) ns\n", duration.count());
                TimePrintCounter[RDMA_Manager::thread_id] = 0;
            }else{
                TimePrintCounter[RDMA_Manager::thread_id]++;
            }
#endif

                return true;
            }
            if (result.slibing != GlobalAddress::Null()) { // turn right
                p = result.slibing;
                assert(result.val.data()!= nullptr);
                goto leaf_next;
            }
#ifdef PROCESSANALYSIS
            if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
            auto stop = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
            printf("leaf page fetch and search the page uses (%ld) ns\n", duration.count());
            TimePrintCounter[RDMA_Manager::thread_id] = 0;
        }else{
            TimePrintCounter[RDMA_Manager::thread_id]++;
        }
#endif
//            assert(false);
            return false; // not found
        }
    }
    template <typename Key>
    bool Btr<Key>::remove(const Key &k, CoroContext *cxt, int coro_id) {
//  assert(rdma_mg->is_register());
        Cache::Handle* page_hint = nullptr;
        auto root = get_root_ptr_protected(page_hint);
        SearchResult<Key> result;
        memset(&result, 0, sizeof(SearchResult<Key>));
//        if(!search_result_memo){
//            search_result_memo = new SearchResult<Key,Value>();
//        }
//        result.val = value_buff;

        GlobalAddress p = root;
        bool isroot = true;
        bool from_cache = false;
        int level = -1;
//TODO: What if we ustilize the cache tree height for the root level?
//TODO: Change it into while style code.
#ifdef PROCESSANALYSIS
        auto start = std::chrono::high_resolution_clock::now();
#endif
//#ifndef NDEBUG
        int next_times = 0;
//#endif
        next: // Internal_and_Leaf page search
//#ifndef NDEBUG
        if (next_times++ == 1000){
            assert(false);
        }
//#endif

        if (!internal_page_search(p, k, result, level, isroot, page_hint, cxt, coro_id)) {
            //The traverser failed to move to the next level
            if (isroot || path_stack[coro_id][result.level +1] == GlobalAddress::Null()){
                p = get_root_ptr_protected(page_hint);
                level = -1;
            }else{
                // fall back to upper level
                assert(level == result.level|| level == -1);
                p = path_stack[coro_id][result.level +1];
                page_hint = nullptr;
                level = result.level + 1;
            }
            goto next;
        }
        else{
            // The traversing moving the the next level correctly
            assert(level == result.level|| level == -1);
            isroot = false;
            page_hint = nullptr;
            // Do not need to
            if (result.slibing != GlobalAddress::Null()) { // turn right
                p = result.slibing;

            }else if (result.next_level != GlobalAddress::Null()){
                assert(result.next_level != GlobalAddress::Null());
                p = result.next_level;
                level = result.level - 1;
            }else{}

            if (level != 0 && level != -1){
                // If Level is 1 then the leaf node and root node are the same.
                assert(!result.is_leaf);

                goto next;
            }

        }
#ifdef PROCESSANALYSIS
        if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//#ifndef NDEBUG
        printf("internal node tranverse uses (%ld) ns, next time is %d\n", duration.count(), next_times);
//          TimePrintCounter = 0;
    }
//#endif
#endif
#ifdef PROCESSANALYSIS
        start = std::chrono::high_resolution_clock::now();
#endif
        leaf_next:// Leaf page search
        if (!leaf_page_delete(p, k, result, level)){
            if (path_stack[coro_id][1] != GlobalAddress::Null()){
                p = path_stack[coro_id][1];
                level = 1;

            }
            else{
                p = get_root_ptr_protected(page_hint);
                level = -1;
            }
#ifndef NDEBUG
            next_times++;
#endif
            DEBUG_PRINT_CONDITION("back off for search\n");
            goto next;
        }else{
            if (result.find_value) { // find
//                value_buff = result.val;
#ifdef PROCESSANALYSIS
                if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
                auto stop = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
                printf("leaf page fetch and search the page uses (%ld) ns\n", duration.count());
                TimePrintCounter[RDMA_Manager::thread_id] = 0;
            }else{
                TimePrintCounter[RDMA_Manager::thread_id]++;
            }
#endif

                return true;
            }
            if (result.slibing != GlobalAddress::Null()) { // turn right
                p = result.slibing;
                goto leaf_next;
            }
#ifdef PROCESSANALYSIS
            if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
            auto stop = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
            printf("leaf page fetch and search the page uses (%ld) ns\n", duration.count());
            TimePrintCounter[RDMA_Manager::thread_id] = 0;
        }else{
            TimePrintCounter[RDMA_Manager::thread_id]++;
        }
#endif
//            assert(false);
            return false; // not found
        }
    }
    template<typename Key>
    typename Btr<Key>::iterator Btr<Key>::begin() {

        void* page_buffer;
        Cache_Handle* handle;
        ddms_->SELCC_Shared_Lock(page_buffer, left_most_leaf, handle);
        auto * node = (LeafPage<Key> *)page_buffer;
        return iterator(node, handle, 0, scheme_ptr, ddms_);
    }
    template<typename Key>
    typename Btr<Key>::iterator Btr<Key>::lower_bound(const Key &key) {
        Cache::Handle *page_hint = nullptr;
        auto root = get_root_ptr_protected(page_hint);
        SearchResult<Key> result = {0};
        GlobalAddress p = root;
        bool isroot = true;
        bool from_cache = false;

        int level = -1;
//TODO: What if we ustilize the cache tree height for the root level?
//TODO: Change it into while style code.
#ifdef PROCESSANALYSIS
        auto start = std::chrono::high_resolution_clock::now();
#endif
//#ifndef NDEBUG
        int next_times = 0;
//#endif
        next: // Internal_and_Leaf page search
//#ifndef NDEBUG
        if (next_times++ == 1000) {
            assert(false);
        }
//#endif

        if (!internal_page_search(p, key, result, level, isroot, page_hint)) {
            //The traverser failed to move to the next level
            if (isroot || path_stack[0][result.level + 1] == GlobalAddress::Null()) {
                p = get_root_ptr_protected(page_hint);
                level = -1;
            } else {
                // fall back to upper level
                assert(level == result.level || level == -1);
                p = path_stack[0][result.level + 1];
                page_hint = nullptr;
                level = result.level + 1;
            }
            goto next;
        } else {
            // The traversing moving the next level correctly
            assert(level == result.level || level == -1);
            isroot = false;
            page_hint = nullptr;
            // Do not need to
            if (result.slibing != GlobalAddress::Null()) { // turn right
                p = result.slibing;

            } else if (result.next_level != GlobalAddress::Null()) {
                assert(result.next_level != GlobalAddress::Null());
                p = result.next_level;
                level = result.level - 1;
            } else {}

            if (level != 0 && level != -1) {
                // If Level is 1 then the leaf node and root node are the same.
                assert(!result.is_leaf);

                goto next;
            }

        }
#ifdef PROCESSANALYSIS
        if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//#ifndef NDEBUG
        printf("internal node tranverse uses (%ld) ns, next time is %d\n", duration.count(), next_times);
//          TimePrintCounter = 0;
    }
//#endif
#endif
#ifdef PROCESSANALYSIS
        start = std::chrono::high_resolution_clock::now();
#endif
        Btr<Key>::iterator iter;
    leaf_next:// Leaf page search
        if (!leaf_page_find(p, key, result, iter, level)) {
            if (path_stack[0][1] != GlobalAddress::Null()) {
                p = path_stack[0][1];
                level = 1;

            } else {
                p = get_root_ptr_protected(page_hint);
                level = -1;
            }
#ifndef NDEBUG
            next_times++;
#endif
            DEBUG_PRINT_CONDITION("back off for search\n");
            goto next;
        } else {
#ifndef NDEBUG
            Key k;
            char buff[16];
            iter.Get(k,buff);
            assert(k >= key);
#endif
            // we have move constructor for btree iterator, so this should be faster than before.
            return iter;
        }
    }


/**
 * Node ID in GLobalAddress for a tree pointer should be the id in the Memory pool
 THis funciton will get the page by the page addr and search the pointer for the
 next level if it is not leaf page. If it is a leaf page, just put the value in the
 result. If this function return false then the result return nothing and we need to
 start from upper level again without cache.
 * @param page_addr
 * @param k
 * @param result
 * @param cxt
 * @param coro_id
 * @param isroot
 * @return
 */
    template <typename Key>
    bool Btr<Key>::internal_page_search(GlobalAddress page_addr, const Key &k, SearchResult<Key> &result, int &level, bool isroot,
                                              Cache::Handle *handle, CoroContext *cxt, int coro_id) {

// tothink: How could I know whether this level before I actually access this page.

//        assert( page_addr.offset % kInternalPageSize == 0 );
//  auto &pattern_cnt = pattern[rdma_mg->getMyThreadID()][page_addr.nodeID];

        int counter = 0;

        // Quetion: We need to implement the lock coupling. how to avoid unnecessary RDMA for lock coupling?
        // Answer: No, see next question.
        Slice page_id((char*)&page_addr, sizeof(GlobalAddress));
//        Cache::Handle* handle = nullptr;
        void* page_buffer;
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<Key>,global_lock);
        Header_Index<Key> * header = nullptr;
        InternalPage<Key>* page = nullptr;
        ibv_mr* mr;
#ifdef PROCESSANALYSIS
        auto start = std::chrono::high_resolution_clock::now();
#endif
        bool skip_cache = false;
#if ACCESS_MODE == 1
        //TODO: For the pointer swizzling, we need to clear the hdr.this_page_g_ptr when we deallocate
        // the page. Also we need a mechanism to avoid the page being deallocate during the access. if a page
        // is pointer swizzled, we need to make sure it will not be evict from the cache.
        if (isroot) {
            //TODO: there is deadlock for root_mtx and the rw_mtx in the cached handle.
            root_mtx.lock_shared();
            handle = cached_root_page_handle.load();


            if (handle->gptr == page_addr) {

                handle->reader_pre_access(page_addr, kInternalPageSize, lock_addr, mr);
                // No need to acquire root mtx here, because if we got an outdated child ptr, the optimistic lock coupling can
                // handle it.
                assert(mr == (ibv_mr*)handle->value);
                page_buffer = mr->addr;
                header = (Header_Index<Key> *) ((char *) page_buffer + (STRUCT_OFFSET(InternalPage<Key>, hdr)));
                // if is root, then we should always bypass the cache.
                skip_cache = true;
                page = (InternalPage<Key> *)page_buffer;

//                memset(&result, 0, sizeof(result));
                result.Reset();
                result.is_leaf = header->leftmost_ptr == GlobalAddress::Null();
                result.level = header->level;
#ifndef NDEBUG
                if (level != -1){
                    assert(level ==result.level );
                }
#endif
                level = result.level;
                assert(result.is_leaf == (level == 0));
                path_stack[coro_id][result.level] = page_addr;
                //If this is the leaf node, directly return let leaf page search to handle it.
                if (result.level == 0){
#ifndef NDEBUG
                    // if the root node is the leaf node this path will happen.
//                    printf("root and leaf are the same 1, this tree id is %lu, this node id is %lu\n", tree_id, RDMA_Manager::node_id);
#endif
                    // assert the page is a valid page.
//                    assert(page->check_whether_globallock_is_unlocked());
                    if (k >= page->hdr.highest){
                        root_mtx.unlock_shared();
                        handle->reader_post_access(page_addr, kInternalPageSize, lock_addr, mr);
                        std::unique_lock<RWSpinLock> l(root_mtx);
                        if (page_addr == g_root_ptr.load()){
                            g_root_ptr.store(GlobalAddress::Null());
                        }
//                        root_mtx.unlock_shared();
                        return false;
                    }

                    handle->reader_post_access(page_addr, kInternalPageSize, lock_addr, mr);
                    root_mtx.unlock_shared();
                    return true;
                }
                assert(page->hdr.level < 100);
            }else {
//                //
//                std::unique_lock<std::shared_mutex> l(root_mtx);
//                if (page_addr == g_root_ptr.load()){
//                    g_root_ptr.store(GlobalAddress::Null());
//                }
                printf("page_addr node id %lu, offset is %lu, cache handles shows node id %lu, offset is %lu\n", page_addr.nodeID, page_addr.offset, handle->gptr.nodeID, handle->gptr.offset);
//                handle->reader_post_access(page_addr, kInternalPageSize, lock_addr, mr);
                root_mtx.unlock_shared();
                return false;
            }

        }
#endif

        if(!skip_cache){
            // Can be root if the original root ptr is invalid and this funciton is entered again bby the node fall back, because we do not have
            // page_hint this time.
            ddms_->SELCC_Shared_Lock(page_buffer, page_addr, handle);
            mr = (ibv_mr*)handle->value;
#if ACCESS_MODE == 1
            assert(page_buffer == mr->addr);
#elif ACCESS_MODE == 0
            assert(page_buffer == handle->value);
#endif
            header = (Header_Index<Key> *) ((char *) page_buffer + (STRUCT_OFFSET(InternalPage<Key>, hdr)));

            page = (InternalPage<Key> *)page_buffer;
#ifndef NDEBUG
            if (level != -1){
                assert(level ==header->level );
            }
#endif
            result.Reset();
            result.is_leaf = header->leftmost_ptr == GlobalAddress::Null();
            result.level = header->level;

            level = result.level;
            assert(result.is_leaf == (level == 0));
            path_stack[coro_id][result.level] = page_addr;
            //If this is the leaf node, directly return let leaf page search to handle it.
            if (result.level == 0){
                //THis path shall not happen
#if ACCESS_MODE == 1 || ACCESS_MODE == 2
                assert(false);
#endif
                // if the root node is the leaf node this path will happen.
#ifndef NDEBUG
//                printf("root and leaf are the same 1, this tree id is %lu, this node id is %lu\n", tree_id, RDMA_Manager::node_id);
#endif                // assert the page is a valid page.
//                    assert(page->check_whether_globallock_is_unlocked());
                if (k >= page->hdr.highest){
                    ddms_->SELCC_Shared_UnLock(page_addr, handle);
                    std::unique_lock<RWSpinLock> l(root_mtx);
                    if (page_addr == g_root_ptr.load()){
                        g_root_ptr.store(GlobalAddress::Null());
                    }
                    return false;
                }
                ddms_->SELCC_Shared_UnLock(page_addr, handle);
                return true;
            }

        }
        assert(mr!= nullptr);

        assert(page->hdr.level < 100);
        assert(result.level != 0);
        if(secondary_ && page->hdr.highest == page->hdr.lowest && k == page->hdr.highest){
            //do nothing, this node is the correct node.
        }else {
            if (k >= page->hdr.highest) { // should turn right
//            printf("should turn right ");
                // (1) If this node is the root node then the g_root_ptr is invalidated.
                // (2) if this node is from the level = (the root level) - 1 then the cached root page should be invalidated.
                // Note that the root page is not stored in LRU cache.
                // (3) If other level, then the upper level page in the LRU cache should be invalidated.
                GlobalAddress sib_ptr = page->hdr.sibling_ptr;
                if (!skip_cache) {
                    ddms_->SELCC_Shared_UnLock(page_addr, handle);
                } else {
                    handle->reader_post_access(page_addr, kInternalPageSize, lock_addr, mr);
                    root_mtx.unlock_shared();
                }
                if (isroot || path_stack[coro_id][result.level + 1] == GlobalAddress::Null()) {
                    // only invalidate the upper layer if we did not acquire shared root_mtx.
                    //since the mtx below is just to avoid muliptle refreshes.


                    if (!skip_cache) {
                        // invalidate the root. Maybe we can omit the mtx here?
                        std::unique_lock<RWSpinLock> l(root_mtx);
                        if (page_addr == g_root_ptr.load()) {
                            g_root_ptr.store(GlobalAddress::Null());
                        }
                    } else {
                        g_root_ptr.store(GlobalAddress::Null());
                    }


                }


                //TODO: What if the Erased key is still in use by other threads? THis is very likely
                // for the upper level nodes.
                //          if (path_stack[coro_id][result.level+1] != GlobalAddress::Null()){
                //              page_cache->Erase(Slice((char*)&path_stack[coro_id][result.level+1], sizeof(GlobalAddress)))
                //          }
                if (nested_retry_counter <= 4) {
//            printf("arrive here\n");
                    nested_retry_counter++;
//                result.slibing = page->hdr.sibling_ptr;
//                assert(page->hdr.sibling_ptr != GlobalAddress::Null());
                    // The release should always happen in the end of the function, otherwise the
                    // page will be overwrittened. When you run release, this means the page buffer will
                    // sooner be overwritten.
                    isroot = false;
                    handle = nullptr;
//                printf("Right turn from Page nodeid %lu, offset %lu\n", page_addr.nodeID, page_addr.offset);
                    return internal_page_search(sib_ptr, k, result, level, isroot, handle, cxt, coro_id);
                } else {
                    nested_retry_counter = 0;
#ifndef NDEBUG
                    printf("retry over two times place 1, key is %d, highest is %d, this level is %d\n", k,
                           page->hdr.highest, level);
#endif
                    return false;
                }

            }
        }

        if (k < page->hdr.lowest) {
            assert(false);
            if(!skip_cache){
                ddms_->SELCC_Shared_UnLock(page_addr, handle);
            }else{
                handle->reader_post_access(page_addr, kInternalPageSize, lock_addr, mr);
                root_mtx.unlock_shared();
            }
            if (isroot || path_stack[coro_id][result.level + 1] == GlobalAddress::Null()){
                if (!skip_cache){
                    // invalidate the root.
                    std::unique_lock<RWSpinLock> l(root_mtx);
                    if (page_addr == g_root_ptr.load()){
                        g_root_ptr.store(GlobalAddress::Null());
                    }
                }else{
                    g_root_ptr.store(GlobalAddress::Null());
                }

            }

            nested_retry_counter = 0;

            DEBUG_PRINT("retry place 2\n");
            return false;
        }
        nested_retry_counter = 0;
        // The second template parameter of SearchResult shall not influence the space oganization, so we can
        // dynamic cast the types.
        assert(STRUCT_OFFSET(SearchResult<Key>, later_key) == STRUCT_OFFSET(SearchResult<Key>, later_key));
        page->internal_page_search(k, &result);
        assert(result.next_level != page_addr);
#ifdef PROCESSANALYSIS
        start = std::chrono::high_resolution_clock::now();
#endif


        if(!skip_cache){
            ddms_->SELCC_Shared_UnLock(page_addr, handle);
        }else{
            handle->reader_post_access(page_addr, kInternalPageSize, lock_addr, mr);
            root_mtx.unlock_shared();
        }

#ifdef PROCESSANALYSIS
        if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
            auto stop = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//#ifndef NDEBUG
            printf("cache release for level %d (%ld) ns\n", level,duration.count());
//          TimePrintCounter = 0;
        }
//#endif
#endif

        return true;
    }


    template <typename Key>
    bool Btr<Key>::leaf_page_search(GlobalAddress page_addr, const Key &k, SearchResult<Key> &result, int level, CoroContext *cxt,
                                          int coro_id) {
        assert(result.val.data()!= nullptr);
#ifdef PROCESSANALYSIS
        auto start = std::chrono::high_resolution_clock::now();
#endif
        auto rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        int counter = 0;
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
        Slice page_id((char*)&page_addr, sizeof(GlobalAddress));
        Cache::Handle* handle = nullptr;
        void* page_buffer;
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<Key>,global_lock);
        Header_Index<Key> * header;
        LeafPage<Key>* page;

//        ibv_mr* mr = nullptr;
        ddms_->SELCC_Shared_Lock(page_buffer, page_addr, handle);
#ifdef PROCESSANALYSIS
        if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
            auto stop = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//#ifndef NDEBUG
            printf("cache look up for level %d is (%ld) ns, \n", level, duration.count());
//          TimePrintCounter = 0;
        }
//#endif
#endif
        header = (Header_Index<Key> *) ((char*)page_buffer + (STRUCT_OFFSET(InternalPage<Key>, hdr)));
        page = (LeafPage<Key> *)page_buffer;
        result.Reset();

        //
        assert(page->hdr.this_page_g_ptr = page_addr);

        result.is_leaf = header->level == 0;
        result.level = header->level;
        level = result.level;
        path_stack[coro_id][result.level] = page_addr;
        assert(result.is_leaf );
        assert(result.level == 0 );
        assert(page->hdr.level < 100);
        //TODO: acquire the remote read lock. and keep trying until success.
//            page->check_invalidation_and_refetch_outside_lock(page_addr, rdma_mg, mr);
        assert(result.level == 0);
        if(secondary_ && page->hdr.highest == page->hdr.lowest && k == page->hdr.highest){
            //do nothing, this node is the correct node.
        }else{
            if (k >= page->hdr.highest) { // should turn right, the highest is not included
                // erase the upper level from the cache
                int last_level = 1;
                if (path_stack[coro_id][last_level] == GlobalAddress::Null()){
                    // If this node do not have upper level, then the root node must be invalidated
                    std::unique_lock<RWSpinLock> l(root_mtx);
                    if (page_addr == g_root_ptr.load()){
                        g_root_ptr.store(GlobalAddress::Null());
                    }
                }
                // In case that there is a long distance(num. of sibiling pointers) between current node and the target node
                if (nested_retry_counter <= 4){
                    nested_retry_counter++;
                    result.slibing = page->hdr.sibling_ptr;
                    goto returntrue;
                }else{
                    nested_retry_counter = 0;
                    DEBUG_PRINT_CONDITION("retry place 3\n");
                    goto returnfalse;
                }

            }
        }

        nested_retry_counter = 0;
        if ((k < page->hdr.lowest)) { // cache is stale
            assert(false);
            // erase the upper node from the cache and refetch the upper node to continue.
            int last_level = 1;
            if (path_stack[coro_id][last_level] != GlobalAddress::Null()){
            }else{
                std::unique_lock<RWSpinLock> l(root_mtx);
                if (page_addr == g_root_ptr.load()){
                    g_root_ptr.store(GlobalAddress::Null());
                }
            }
            DEBUG_PRINT_CONDITION("retry place 4\n");
            goto returnfalse;
        }

        page->leaf_page_search(k, result, page_addr, scheme_ptr);
#ifndef NDEBUG
        if(k == page->hdr.highest){
            assert(page->hdr.highest == page->hdr.lowest);
        }
#endif
        assert(result.val.data()!= nullptr);
    returntrue:
        assert(handle);
        ddms_->SELCC_Shared_UnLock(page_addr, handle);
        return true;
    returnfalse:
        assert(handle);
        ddms_->SELCC_Shared_UnLock(page_addr, handle);
        return false;



    }
    template <typename Key>
    bool Btr<Key>::leaf_page_delete(GlobalAddress page_addr, const Key &k, SearchResult<Key> &result, int level) {
#ifdef PROCESSANALYSIS
        auto start = std::chrono::high_resolution_clock::now();
#endif
        auto rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        int counter = 0;
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
        Slice page_id((char*)&page_addr, sizeof(GlobalAddress));
        Cache::Handle* handle = nullptr;
        void* page_buffer;
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<Key>,global_lock);
        Header_Index<Key> * header;
        LeafPage<Key>* page;
        bool need_merge = false;
        int cnt = 0;
//        ibv_mr* mr = nullptr;
        ddms_->SELCC_Shared_Lock(page_buffer, page_addr, handle);
#ifdef PROCESSANALYSIS
        if (TimePrintCounter[RDMA_Manager::thread_id]>=TIMEPRINTGAP){
            auto stop = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//#ifndef NDEBUG
            printf("cache look up for level %d is (%ld) ns, \n", level, duration.count());
//          TimePrintCounter = 0;
        }
//#endif
#endif
        header = (Header_Index<Key> *) ((char*)page_buffer + (STRUCT_OFFSET(InternalPage<Key>, hdr)));
        page = (LeafPage<Key> *)page_buffer;
        result.Reset();

        //
        assert(page->hdr.this_page_g_ptr = page_addr);

        result.is_leaf = header->level == 0;
        result.level = header->level;
        level = result.level;
        path_stack[0][result.level] = page_addr;
        assert(result.is_leaf );
        assert(result.level == 0 );
        assert(page->hdr.level < 100);
        //TODO: acquire the remote read lock. and keep trying until success.
//            page->check_invalidation_and_refetch_outside_lock(page_addr, rdma_mg, mr);
        assert(result.level == 0);
        if(secondary_ && page->hdr.highest == page->hdr.lowest && k == page->hdr.highest){
            //do nothing, this node is the correct node. THis has been deprecated.
        }else{
            if (k >= page->hdr.highest) { // should turn right, the highest is not included
                // erase the upper level from the cache
                int last_level = 1;
                if (path_stack[0][last_level] == GlobalAddress::Null()){
                    // If this node do not have upper level, then the root node must be invalidated
                    std::unique_lock<RWSpinLock> l(root_mtx);
                    if (page_addr == g_root_ptr.load()){
                        g_root_ptr.store(GlobalAddress::Null());
                    }
                }
                // In case that there is a long distance(num. of sibiling pointers) between current node and the target node
                if (nested_retry_counter <= 4){
                    nested_retry_counter++;
                    result.slibing = page->hdr.sibling_ptr;
                    goto returntrue;
                }else{
                    nested_retry_counter = 0;
                    DEBUG_PRINT_CONDITION("retry place 3\n");
                    goto returnfalse;
                }

            }
        }

        nested_retry_counter = 0;
        if ((k < page->hdr.lowest)) { // cache is stale
            assert(false);
            // erase the upper node from the cache and refetch the upper node to continue.
            int last_level = 1;
            if (path_stack[0][last_level] != GlobalAddress::Null()){
            }else{
                std::unique_lock<RWSpinLock> l(root_mtx);
                if (page_addr == g_root_ptr.load()){
                    g_root_ptr.store(GlobalAddress::Null());
                }
            }
            DEBUG_PRINT_CONDITION("retry place 4\n");
            goto returnfalse;
        }

        need_merge = page->leaf_page_delete(k, cnt, result, scheme_ptr);
    returntrue:
        assert(handle);
        ddms_->SELCC_Shared_UnLock(page_addr, handle);
        return true;
    returnfalse:
        assert(handle);
        ddms_->SELCC_Shared_UnLock(page_addr, handle);
        return false;



    }

    template <typename Key>
    bool Btr<Key>::leaf_page_find(GlobalAddress page_addr, const Key &k, SearchResult<Key> &result,
                                        Btr<Key>::iterator &iter, int level) {
        assert(result.val.data()!= nullptr);
        auto rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        int counter = 0;
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
        Slice page_id((char*)&page_addr, sizeof(GlobalAddress));
        Cache::Handle* handle = nullptr;
        void* page_buffer;
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<Key>,global_lock);
        Header_Index<Key> * header;
        LeafPage<Key>* page;
        int position;
//        ibv_mr* mr = nullptr;
        ddms_->SELCC_Shared_Lock(page_buffer, page_addr, handle);
        header = (Header_Index<Key> *) ((char*)page_buffer + (STRUCT_OFFSET(InternalPage<Key>, hdr)));
        page = (LeafPage<Key> *)page_buffer;
        result.Reset();
        assert(page->hdr.this_page_g_ptr = page_addr);
        result.is_leaf = header->level == 0;
        result.level = header->level;
        level = result.level;
        path_stack[0][result.level] = page_addr;
        assert(result.is_leaf );
        assert(result.level == 0 );
        assert(page->hdr.level < 100);
        //TODO: acquire the remote read lock. and keep trying until success.
//            page->check_invalidation_and_refetch_outside_lock(page_addr, rdma_mg, mr);
        assert(result.level == 0);

        if(secondary_ && page->hdr.highest == page->hdr.lowest && k == page->hdr.highest){
            //do nothing, this node is the correct node.
        }else {
            if (k >= page->hdr.highest) { // should turn right, the highest is not included
                // erase the upper level from the cache
                int last_level = 1;
                if (path_stack[0][last_level] == GlobalAddress::Null()) {
                    // If this node do not have upper level, then the root node must be invalidated
                    std::unique_lock<RWSpinLock> l(root_mtx);
                    if (page_addr == g_root_ptr.load()) {
                        g_root_ptr.store(GlobalAddress::Null());
                    }
                }
                // In case that there is a long distance(num. of sibiling pointers) between current node and the target node
                if (nested_retry_counter <= 4) {
                    nested_retry_counter++;
                    result.slibing = page->hdr.sibling_ptr;
                    goto returntrue;
                } else {
                    nested_retry_counter = 0;
                    DEBUG_PRINT_CONDITION("retry place 3\n");
                    goto returnfalse;
                }
            }
        }
        nested_retry_counter = 0;
        if ((k < page->hdr.lowest)) { // cache is stale
            // erase the upper node from the cache and refetch the upper node to continue.
            int last_level = 1;
            if (path_stack[0][last_level] != GlobalAddress::Null()){
            }else{
                std::unique_lock<RWSpinLock> l(root_mtx);
                if (page_addr == g_root_ptr.load()){
                    g_root_ptr.store(GlobalAddress::Null());
                }
            }
            DEBUG_PRINT_CONDITION("retry place 4\n");
            goto returnfalse;
        }
        // the position_idx is the index of the tuple in the GCL. real_offset = positon*Tuple_size.
        position = page->leaf_page_pos_lb(k, page_addr, scheme_ptr);
        if (position >= 0){
            iter.initialize(page, handle, position, scheme_ptr,ddms_);
        }else{
            // the iter shall point to the next leaf node.
            GlobalAddress sib_ptr = page->hdr.sibling_ptr;
            if(sib_ptr == GlobalAddress::Null()){
                iter.SetValid(false);
                goto returntrue;
            }
            ddms_->SELCC_Shared_UnLock(page_addr, handle);
            ddms_->SELCC_Shared_Lock(page_buffer, sib_ptr, handle);
            page = (LeafPage<Key> *)page_buffer;
            iter.initialize(page, handle, 0, scheme_ptr,ddms_);
        }
        assert(result.val.data() != nullptr);
    returntrue:
//        assert(handle);
//        ddms_->SELCC_Shared_UnLock(page_addr, handle);
        return true;
    returnfalse:
//        assert(handle);
//        ddms_->SELCC_Shared_UnLock(page_addr, handle);
        return false;



    }

// This function will return true unless it found that the key is smaller than the lower bound of a searched node.
// When this function return false the upper layer should backoff in the tree.

    template <typename Key>
    bool Btr<Key>::internal_page_store(GlobalAddress page_addr, Key &k, GlobalAddress &v, int level, CoroContext *cxt, int coro_id) {
        assert(page_addr != GlobalAddress::Null());
        assert(v != GlobalAddress::Null());
        uint64_t lock_index =
                CityHash64((char *)&page_addr, sizeof(page_addr)) % define::kNumOfLock;
        bool need_split;
        bool insert_success;
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(InternalPage<Key>,global_lock);
//        Slice page_id((char*)&page_addr, sizeof(GlobalAddress));
        ibv_mr* page_mr;
        void * page_buffer;
        InternalPage<Key>* page;
        bool skip_cache = false;
        Cache::Handle* handle = nullptr;
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
        if(!skip_cache) {
            ddms_->SELCC_Exclusive_Lock(page_buffer, page_addr, handle);
            assert(handle != nullptr);
#if ACCESS_MODE == 1
            assert(((ibv_mr *) handle->value)->addr == page_buffer);
#elif ACCESS_MODE == 0
            assert((ibv_mr *) handle->value== page_buffer);
#endif
            page = (InternalPage<Key> *) page_buffer;
            page_mr = (ibv_mr *) page_cache->Value(handle);

        }
        assert(((char*)&page->global_lock - (char*)page) == RDMA_OFFSET);
        assert(page->hdr.level == level);
        assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());
        path_stack[coro_id][page->hdr.level] = page_addr;
        // This is the result that we do not lock the btree when search for the key.
        // Not sure whether this will still work if we have node merge
        // Why this node can not be the right most node
        if(secondary_ && page->hdr.highest == page->hdr.lowest && k == page->hdr.highest){
            //do nothing, this node is the correct node.
        }else {
            if (k >= page->hdr.highest) {
                GlobalAddress sib_ptr = page->hdr.sibling_ptr;
                if (!skip_cache) {
                    ddms_->SELCC_Exclusive_UnLock(page_addr, handle);
                } else {
                    assert(false);
                }
                // TODO: No need for node invalidation when inserting things because the tree tranversing is enough for invalidation (Erase)
                if (UNLIKELY(level == tree_height.load()) || path_stack[coro_id][level + 1] == GlobalAddress::Null()) {
                    std::unique_lock<RWSpinLock> l(root_mtx);
                    if (page_addr == g_root_ptr.load()) {
                        g_root_ptr.store(GlobalAddress::Null());
                    }
                }
                if (nested_retry_counter <= 4) {
                    nested_retry_counter++;
                    insert_success = this->internal_page_store(sib_ptr, k, v, level, cxt, coro_id);
                } else {
                    nested_retry_counter = 0;
                    insert_success = false;
                    DEBUG_PRINT_CONDITION("retry place 5\n");
                }
                return insert_success;
            }
        }
        nested_retry_counter = 0;
        if (k < page->hdr.lowest ) {
            if (!skip_cache){
                ddms_->SELCC_Exclusive_UnLock(page_addr, handle);
            }else{
                assert(false);
            }
            // if key is smaller than the lower bound, the insert has to be restart from the
            // upper level. because the sibling pointer only points to larger one.
            if(UNLIKELY(level == tree_height.load()) || path_stack[coro_id][level+1]== GlobalAddress::Null()){
                std::unique_lock<RWSpinLock> l(root_mtx);
                if (page_addr == g_root_ptr.load()){
                    g_root_ptr.store(GlobalAddress::Null());
                }
            }
            insert_success = false;
            DEBUG_PRINT_CONDITION("retry place 6\n");

            return insert_success;// result in fall back search on the higher level.
        }
        Key split_key;
        GlobalAddress sibling_addr = GlobalAddress::Null();
//  assert(k >= page->hdr.lowest);
        need_split = page->internal_page_store(page_addr, k, v, level, cxt, coro_id);
        auto cnt = page->hdr.last_index + 1;
        InternalPage<Key>* sibling = nullptr;
        if (need_split) { // need split
            assert(cnt == InternalPage<Key>::kInternalCardinality);
            sibling_addr = rdma_mg->Allocate_Remote_RDMA_Slot(Regular_Page, 2 * round_robin_cur + 1);
            if(++round_robin_cur == rdma_mg->memory_nodes.size()){
                round_robin_cur = 0;
            }
//            printf("Node split, this g page addr is node %lu, offset %lu, sibling g page addr is node %lu, offset %lu\n", page_addr.nodeID, page_addr.offset, sibling_addr.nodeID, sibling_addr.offset);
            ibv_mr* sibling_mr = new ibv_mr{};
//          printf("Allocate slot for page 3 %p\n", sibling_addr);

            rdma_mg->Allocate_Local_RDMA_Slot(*sibling_mr, Regular_Page);
//            memset(sibling_mr->addr, 0, rdma_mg->name_to_chunksize.at(Regular_Page));

            assert(page->hdr.level >0);
            sibling = new(sibling_mr->addr) InternalPage<Key>(sibling_addr, secondary_, page->hdr.level);
            //clear the global lock state. The page initialization will not reset the global lock byte.
            sibling->global_lock = 0;
            int m = cnt / 2;
            // For key split we need to avoid duplicate keys who start in the middile of a leaf page and end in the middle of another leaf page,
            // this could result in the iterator locate at middle of the key range
            // [..... x,x,x,x,x] [x,x,x,x,x,x,x,x,x] [x,x,x,x,....]
            //                  /\: the iterator will start at here which is not the beginning of the duplicate key range.
            //                  ||
            if(!secondary_){
                // If this is primary index, then we simply make the middle key as the splited key.
                m = cnt / 2;
            }else{
                // If this is seconday index, then we need to split at the first duplicated key unless the whole node only contain one single key.
                int m_t = cnt / 2;
                if(page->hdr.lowest != split_key && page->hdr.lowest != page->hdr.highest ){
                    // make sure the split key is the first duplicated key.
                    Key cur_key = page->records[m_t].key;
                    Key prev_key = page->records[m_t-1].key;
                    while(cur_key == prev_key){
                        m_t--;
                        assert(m_t > 0);
                        cur_key = page->records[m_t].key;
                        prev_key = page->records[m_t-1].key;
                    }
                }
                m = m_t;
            }
            assert(m >0);

            split_key = page->records[m].key;
            // TODO: similar to the leaf node, the split key should be the first duplicated key, unless the lowest key equals to this key / the whole node only contains one key.
            // besides, we need to make sure the insertion should be inserted at the first duplicated key in the leaf node
            assert(split_key > page->hdr.lowest);
            assert(split_key < page->hdr.highest);
            page->hdr.last_index -= (cnt - m); // this is correct. because we extract the split key to upper layer
            assert(page->hdr.last_index == m-1);
//            sibling->hdr.last_index += (cnt - m - 1);
            // cnt - m pointer (cnt-m - 1) keys, so last index : (cnt -m -1 - 1)
            sibling->hdr.last_index = cnt - m - 1 - 1;
            assert(sibling->hdr.last_index == cnt - m - 1 - 1);
            for (int i = m + 1; i < cnt; ++i) { // move
                //Is this correct?
                sibling->records[i - m - 1].key = page->records[i].key;
                sibling->records[i - m - 1].ptr = page->records[i].ptr;
            }
            sibling->hdr.leftmost_ptr = page->records[m].ptr;
            sibling->hdr.lowest = page->records[m].key;
            sibling->hdr.highest = page->hdr.highest;
            page->hdr.highest = page->records[m].key;

            // link
            sibling->hdr.sibling_ptr = page->hdr.sibling_ptr;
            page->hdr.sibling_ptr = sibling_addr;
//    sibling->set_consistent();
            //the code below is just for debugging.
//    sibling_addr.mark = 2;

            rdma_mg->RDMA_Write(sibling_addr, sibling_mr, kInternalPageSize, IBV_SEND_SIGNALED, 1, Regular_Page);
            assert(sibling->records[sibling->hdr.last_index].ptr != GlobalAddress::Null());
            assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());
            k = split_key;
            v = sibling_addr;
//          printf("Create new node %p\n", v);
            // TODO (opt): we can directly add the sibling block into the cache here.


//      printf("page splitted last_index of page offset %lu is %hd, page level is %d\n", page_addr.offset,  page->hdr.last_index, page->hdr.level);

            //The code below is optional.
//            Slice sibling_page_id((char*)&sibling_addr, sizeof(GlobalAddress));
//            assert(page_mr!= nullptr);
//            auto sib_handle = page_cache->Insert(sibling_page_id, sibling_mr, kInternalPageSize, Deallocate_MR_WITH_CCP);
//            page_cache->Release(sib_handle);
          rdma_mg->Deallocate_Local_RDMA_Slot(sibling_mr->addr, Regular_Page);
          delete sibling_mr;
        } else {
//      k = Key ;
            // Only set the value as null is enough
            v = GlobalAddress::Null();
        }

        assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());

        if (!skip_cache){
            ddms_->SELCC_Exclusive_UnLock(page_addr, handle);
        }else{
            assert(false);
        }


        // We can also say if need_split
        if (sibling_addr != GlobalAddress::Null()){
            Cache::Handle* page_hint = nullptr;
            auto p = path_stack[coro_id][level+1];
            //check whether the node split is for a root node.
            if (UNLIKELY(p == GlobalAddress::Null() )){
                // First acquire local lock
                std::unique_lock<RWSpinLock> l(root_mtx);
                Cache::Handle* dummy_mr;
                p = get_root_ptr(dummy_mr);
                uint8_t height = tree_height.load();
                uint8_t old_height = tree_height.load();

                if (path_stack[coro_id][level] == p && (int)height == level){
                    //Acquire global lock for the root update.
                    GlobalAddress lock_addr = {};
                    // root node lock addr. but this could result in a deadlock for transaction cc.
                    lock_addr.nodeID = 1;
                    lock_addr.offset = 0;
                    auto cas_buffer = rdma_mg->Get_local_CAS_mr();
                    //aquire the global lock to avoid mulitple node creating the new  root node
                    acquire_global_lock:
                    *(uint64_t*)cas_buffer->addr = 0;
                    rdma_mg->RDMA_CAS(lock_addr, cas_buffer, 0, 1, IBV_SEND_SIGNALED,1, LockTable);
                    if ((*(uint64_t*) cas_buffer->addr) != 0){
                        printf("Two nodes are trying to modifying the same root for Btree \n");
                        goto acquire_global_lock;
                    }
                    refetch_rootnode();
                    p = g_root_ptr.load();
                    height = tree_height.load();
                    if (path_stack[coro_id][level] == p && (int)height == level) {
                        update_new_root(path_stack[coro_id][level], split_key, sibling_addr, level + 1,
                                        path_stack[coro_id][level], cxt, coro_id);
                        *(uint64_t *) cas_buffer->addr = 0;
                        //TODO: USE RDMA cas TO release lock
                        rdma_mg->RDMA_CAS(lock_addr, cas_buffer, 1, 0, IBV_SEND_SIGNALED,1, LockTable);
                        assert((*(uint64_t*) cas_buffer->addr) == 1);
//                        rdma_mg->RDMA_Write(lock_addr, cas_buffer, sizeof(uint64_t), IBV_SEND_SIGNALED, 1, LockTable);

                        return true;
                    }else{
//                        assert(false);
                        *(uint64_t *) cas_buffer->addr = 0;
                        rdma_mg->RDMA_CAS(lock_addr, cas_buffer, 1, 0, IBV_SEND_SIGNALED,1, LockTable);
                        assert((*(uint64_t*) cas_buffer->addr) == 1);
//                        rdma_mg->RDMA_Write(lock_addr, cas_buffer, sizeof(uint64_t), IBV_SEND_SIGNALED, 1, LockTable);

                        printf("There is another node updating the root node\n");
                    }
//                l.unlock();

                }else{
                    printf("There is another thread updating the root node\n");
                }
                l.unlock();

                {
                    //find the upper level
                    //TODO: shall I implement a function that search a ptr at particular level.
                    printf(" rare case the tranverse during the root update\n");
//                    assert(tree_height.load() != level && path_stack[coro_id][level] != p);
                    return insert_internal(split_key, sibling_addr,  cxt, coro_id, level+1);
                }


            }
            // if not a root split go ahead and insert in the upper level.
            level = level +1;
            //*****************Now it is not a root update, insert to the upper level******************
            SearchResult<Key> result{};
            memset(&result, 0, sizeof(SearchResult<Key>));
            int fall_back_level = 0;
            re_insert:
            if (UNLIKELY(!internal_page_store(p, split_key, sibling_addr, level, cxt, coro_id))){
                //this path should be a rare case.

                // fall back to upper level in the cache to search for the right node at this level
                fall_back_level = level + 1;

                p = path_stack[coro_id][fall_back_level];
                page_hint = nullptr;
                if ( p == GlobalAddress::Null()){
                    // insert it top-down. this function will keep searching until it is found
                    insert_internal(split_key,sibling_addr, cxt, coro_id, level);
                }else{
                    if(!internal_page_search(p, k, result, fall_back_level, false, page_hint, cxt, coro_id)){
                        // if the upper level is still a stale node, just insert the node by top down method.
                        insert_internal(split_key,sibling_addr, cxt, coro_id, level);
//                        level = level + 1; // move to upper level
//                        p = path_stack[coro_id][level];// move the pointer to upper level
                    }else{

                        if (result.next_level != GlobalAddress::Null()){
                            // the page was found successful by one step back, then we can set the p as new node.
                            // do not need to chanve level.
                            p = result.next_level;
                            page_hint = nullptr;
                            goto re_insert;
//                    level = result.level - 1;
                        }else{
                            assert(false);
                        }
                    }

                }

            }
        }
        return true;

    }
    template <class Key>
    bool Btr<Key>::leaf_page_store(GlobalAddress page_addr, const Key &k, const Slice &v, Key &split_key,
                                         GlobalAddress &sibling_addr, int level, CoroContext *cxt, int coro_id) {
#ifdef PROCESSANALYSIS
        auto start = std::chrono::high_resolution_clock::now();
#endif

        int counter = 0;
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;

        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(InternalPage<Key>,global_lock);
        // TODO: We need to implement the lock coupling. how to avoid unnecessary RDMA for lock coupling?
        //
        void* page_buffer;
        Cache::Handle* handle = nullptr;
        Slice page_id((char*)&page_addr, sizeof(GlobalAddress));
        Header_Index<Key> * header;
        LeafPage<Key>* page;
        ddms_->SELCC_Exclusive_Lock(page_buffer, page_addr,handle);
        assert(page_buffer != nullptr);
//        ibv_mr* local_mr;
        assert(level == 0);
//        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
//        handle = page_cache->LookupInsert(page_id, nullptr, kLeafPageSize, Deallocate_MR_WITH_CCP);
//        assert(handle!= nullptr);
//        handle->updater_pre_access(page_addr, kLeafPageSize, lock_addr, local_mr);

        // TODO: under some situation the lock is not released
//        page_buffer = local_mr->addr;
        page = (LeafPage<Key> *)page_buffer;
//        assert(page->hdr.last_index == level);
        //TODO: Create an assert to check the page is not an empty page, except root page.
//        assert(scheme_ptr->GetPrimary(page->data_));
        assert(page->hdr.level == level);
//        assert(page->check_consistent());
        path_stack[coro_id][page->hdr.level] = page_addr;
        // It is possible that the key is larger than the highest key
        // The range of a page is [lowest,largest).
        // TODO: find out why sometimes the node is far from the target node that it need multiple times of
        //  sibling access.
        //
        //  Note that it is normal to see that the local buffer are always the same accross the nested
        //  funciton call, because they are sharing the same local buffer.

        if(secondary_ && page->hdr.highest == page->hdr.lowest && k == page->hdr.highest){
            //do nothing, this node is the correct node.
        }else {
            if (k >= page->hdr.highest) {
                if (page->hdr.sibling_ptr != GlobalAddress::Null()) {
//                this->unlock_addr(lock_addr, cxt, coro_id, false);
                    if (path_stack[coro_id][level + 1] == GlobalAddress::Null()) {
                        std::unique_lock<RWSpinLock> lck(root_mtx);
                        if (page_addr == g_root_ptr.load()) {
                            g_root_ptr.store(GlobalAddress::Null());
                        }
                    }
                    if (nested_retry_counter <= 4) {
                        nested_retry_counter++;
                        auto sibling_ptr = page->hdr.sibling_ptr;
                        ddms_->SELCC_Exclusive_UnLock(page_addr, handle);
                        return this->leaf_page_store(sibling_ptr, k, v, split_key, sibling_addr, level, cxt, coro_id);
                    } else {
                        DEBUG_PRINT_CONDITION("retry place 7");
                        nested_retry_counter = 0;
                        ddms_->SELCC_Exclusive_UnLock(page_addr, handle);
                        return false;
                    }
                } else {
                    // impossible because the right most leaf node 's max is KeyMax
                    assert(false);
                }
            }
        }
        nested_retry_counter = 0;
        if (k < page->hdr.lowest ) {
            // if key is smaller than the lower bound, the insert has to be restart from the
            // upper level. because the sibling pointer only points to larger one.

            if (path_stack[coro_id][level+1] == GlobalAddress::Null()){
                std::unique_lock<RWSpinLock> lck(root_mtx);
                if (page_addr == g_root_ptr.load()){
                    g_root_ptr.store(GlobalAddress::Null());
                }
            }
            ddms_->SELCC_Exclusive_UnLock(page_addr,handle);
            DEBUG_PRINT_CONDITION_arg("retry place 8, this level is %d\n", level);
            return false;// result in fall back search on the higher level.
        }
        // Clear the retry counter, in case that there is a sibling call.
        assert(k >= page->hdr.lowest);
#ifndef NDEBUG
        if(!secondary_){
            assert(k < page->hdr.highest);
        }
#endif
        assert(page->hdr.highest !=0 || page->hdr.highest == page->hdr.lowest);
// TODO: Check whether the key is larger than the largest key of this node.
//  if yes, update the header.
        int cnt = 0;
//        int empty_index = -1;
//        char *update_addr = nullptr;
        int tuple_length = scheme_ptr->GetSchemaSize();

        bool need_split = page->leaf_page_store(k, v, cnt,  scheme_ptr);
        num_of_record++;
//        assert(page->hdr.last_index== 0 || page->data_[0]!=0);
        if (!need_split) {
                ddms_->SELCC_Exclusive_UnLock(page_addr,handle);

//            handle->updater_writer_post_access(page_addr, kLeafPageSize, lock_addr, local_mr);
//
//            page_cache->Release(handle);

            return true;
        }else {
            // need split
            sibling_addr = rdma_mg->Allocate_Remote_RDMA_Slot(Regular_Page, 2 * round_robin_cur + 1);
            if(++round_robin_cur == rdma_mg->memory_nodes.size()){
                round_robin_cur = 0;
            }
//            printf("Create new sibling nodeid %lu, offset %llu, on tree %llu\n", sibling_addr.nodeID, sibling_addr.offset, tree_id);
            //TODO: use a thread local sibling memory region to reduce the allocator contention.
            ibv_mr* sibling_mr = new ibv_mr{};
//      printf("Allocate slot for page 3 %p\n", sibling_addr);
            rdma_mg->Allocate_Local_RDMA_Slot(*sibling_mr, Regular_Page);
//      memset(sibling_mr->addr, 0, kLeafPageSize);
            auto sibling = new(sibling_mr->addr) LeafPage<Key>(sibling_addr, leaf_cardinality_, scheme_ptr->GetSchemaSize(),
                                                                      secondary_, page->hdr.level);
            sibling->global_lock = 0;
            assert(sibling->global_lock == 0);
            //TODO: add the sibling to the local cache.
//            sibling->front_version ++;
            int m ;
            char* tuple_start;
            // For key split we need to avoid duplicate keys who start in the middile of a leaf page and end in the middle of another leaf page,
            // this could result in the iterator locate at middle of the key range
            // [..... x,x,x,x,x] [x,x,x,x,x,x,x,x,x] [x,x,x,x,....]
            //                  /\: the iterator will start at here which is not the beginning of the duplicate key range.
            //                  ||
            if(!secondary_){
                // If this is primary index, then we simply make the middle key as the splited key.
                m = cnt / 2;
                tuple_start = page->data_ + m*tuple_length;

                Record split_record = Record(scheme_ptr,tuple_start);
                split_record.GetPrimaryKey(&split_key);
            }else{
                // If this is seconday index, then we need to split at the first duplicated key unless the whole node only contain one single key.
                int m_t = cnt / 2;
                tuple_start = page->data_ + m_t*tuple_length;

                Record split_record = Record(scheme_ptr,tuple_start);
                split_record.GetPrimaryKey(&split_key);

                if(page->hdr.lowest != split_key && page->hdr.lowest != page->hdr.highest ){
                    // make sure the split key is the first duplicated key.
                    int previous = m_t-1;
                    char* previous_add = page->data_ + previous*tuple_length;
                    Key prev_key;
                    Record previous_r = Record(scheme_ptr,previous_add);
                    previous_r.GetPrimaryKey(&prev_key);
                    while(prev_key == split_key){
                        m_t--;
                        assert(m_t > 0);
                        previous = m_t-1;
                        previous_add = page->data_ + previous*tuple_length;
                        Record prev_r = Record(scheme_ptr,previous_add);
                        prev_r.GetPrimaryKey(&prev_key);
                    }
                }
                m = m_t;
            }

//            if ((k & ((1ull << 40) -1)) == 0) {
//                printf("leaf node split, split key is %p, tree id is %lu, this node id is %lu\n", split_key, tree_id,
//                       rdma_mg->node_id);
//                fflush(stdout);
//            }
            //TODO： check why the split_record point to an empty record. when I print the page content, it is weird.
            // It turns out the page is an empty page
#ifndef NDEBUG
            if (!secondary_){
                assert(split_key > page->hdr.lowest);
                assert(split_key < page->hdr.highest);
            }
#endif
            for (int i = m; i < cnt; ++i) { // move
                char* to_be_moved_start = page->data_ + m*tuple_length;

                memcpy(sibling->data_, to_be_moved_start,  (page->hdr.last_index - m + 1)*tuple_length);
            }
            //We don't care about the last index in the leaf nodes actually,
            // because we iterate all the slots to find an entry.
            page->hdr.last_index -= (cnt - m);
            //TODO: double check the code below if there is a bug
            sibling->hdr.last_index = (cnt - m - 1);
            assert(sibling->hdr.last_index + 1 + page->hdr.last_index + 1 == cnt);
            sibling->hdr.lowest = split_key;
            sibling->hdr.highest = page->hdr.highest;
            page->hdr.highest = split_key;
            // link
            sibling->hdr.sibling_ptr = page->hdr.sibling_ptr;
            page->hdr.sibling_ptr = sibling_addr;
            // TODO: directly back the page with read lock and insert the page into the cache with shared state.
            rdma_mg->RDMA_Write(sibling_addr, sibling_mr, kLeafPageSize, IBV_SEND_SIGNALED, 1, Regular_Page);
            rdma_mg->Deallocate_Local_RDMA_Slot(sibling_mr->addr, Regular_Page);
            delete sibling_mr;
#ifdef DIRTY_ONLY_FLUSH
            // After split, the whole page is dirty.
//            page->hdr.reset_dirty_bounds();
            page->hdr.merge_dirty_bounds(STRUCT_OFFSET(LeafPage<Key>, hdr), kLeafPageSize);
#endif
        }
        ddms_->SELCC_Exclusive_UnLock(page_addr,handle);
//        handle->updater_writer_post_access(page_addr, kLeafPageSize, lock_addr, local_mr);
//
//        page_cache->Release(handle);

        if (sibling_addr != GlobalAddress::Null()){
            int upper_level = level + 1;
            auto p = path_stack[coro_id][upper_level];
            ibv_mr* page_hint = nullptr;
            //check whether the node split is for a root node.
            if (UNLIKELY(p == GlobalAddress::Null() )){
                // First acquire local lock
                std::unique_lock<RWSpinLock> l(root_mtx);
//                refetch_rootnode();
                // If you find the current root node does not have higher stack, and it is not a outdated root node,
                // the reason behind is that the inserted key is very small and the leaf node keep sibling shift to the right.
                // IN this case, the code will call "insert_internal"
                Cache::Handle* dummy_mr;
                p = get_root_ptr(dummy_mr);
                uint8_t height = tree_height;
                // Note path_stack is a global variable, be careful when debugging

                //If current store node is still the leaf (NO other thread create new root.), then we need to create a new page.
                if (path_stack[coro_id][level] == p && height == level){
                    //aquire the global lock to avoid mulitple node creating the new  root node
                    GlobalAddress lock_addr = {};
                    // root node lock addr. but this could result in a deadlock for transaction cc.
                    lock_addr.nodeID = 1;
                    lock_addr.offset = 0;
                    auto cas_buffer = rdma_mg->Get_local_CAS_mr();
                    //aquire the global lock
                acquire_global_lock:
                    // TODO: Modify the code below based on SELCC rather than a seperate lock table. we need to create a catalog
                    // page and then whenever there is a root node change, we need to update the catalog page.
                    *(uint64_t*)cas_buffer->addr = 0;
                    rdma_mg->RDMA_CAS(lock_addr, cas_buffer, 0, 1, IBV_SEND_SIGNALED,1, LockTable);
                    if ((*(uint64_t*) cas_buffer->addr) != 0){
                        goto acquire_global_lock;
                    }
                    refetch_rootnode();
                    p = g_root_ptr.load();
                    height = tree_height;
                    if (path_stack[coro_id][level] == p && height == level) {
                        update_new_root(path_stack[coro_id][level], split_key, sibling_addr, level + 1,
                                        path_stack[coro_id][level], cxt, coro_id);
                        *(uint64_t *) cas_buffer->addr = 0;
//                        rdma_mg->RDMA_Write(lock_addr, cas_buffer, sizeof(uint64_t), IBV_SEND_SIGNALED, 1, LockTable);
                        rdma_mg->RDMA_CAS(lock_addr, cas_buffer, 1, 0, IBV_SEND_SIGNALED,1, LockTable);
                        assert((*(uint64_t*) cas_buffer->addr) == 1);
//                        page_cache->Release(handle);
                        return true;
                    }else{
                        assert(height != level);
                        *(uint64_t *) cas_buffer->addr = 0;

//                        rdma_mg->RDMA_Write(lock_addr, cas_buffer, sizeof(uint64_t), IBV_SEND_SIGNALED, 1, LockTable);
                        rdma_mg->RDMA_CAS(lock_addr, cas_buffer, 1, 0, IBV_SEND_SIGNALED,1, LockTable);
                        assert((*(uint64_t*) cas_buffer->addr) == 1);
//                        assert(false);
                    }
//                l.unlock();

                }
                l.unlock();

                {
                    //find the upper level
                    //TODO: shall I implement a function that search a ptr at particular level.
                    printf(" rare case the tranverse during the root update\n");
                    // It is impossinle to insert the internal in level 0.
                    return insert_internal(split_key, sibling_addr,  cxt, coro_id, level+1);
                }


            }
            assert(p != GlobalAddress::Null());
            // if not a root split go ahead and insert in the upper level.
            level = level +1;
            //*****************Now it is not a root update, insert to the upper level******************
            SearchResult<Key> result;
            memset(&result, 0, sizeof(SearchResult<Key>));

            int fall_back_level = 0;
            re_insert:

            if (UNLIKELY(!internal_page_store(p, split_key, sibling_addr, level, cxt, coro_id))){
                //this path should be a rare case.

                // fall back to upper level in the cache to search for the right node at this level
                fall_back_level = level + 1;

                p = path_stack[coro_id][fall_back_level];
                if ( p == GlobalAddress::Null()){
                    // insert it top-down. this function will keep searching until it is found
                    insert_internal(split_key,sibling_addr, cxt, coro_id, level);
                }else{
                    //fall back one step.
                    if(!internal_page_search(p, k, result, fall_back_level, false, nullptr, cxt, coro_id)){
                        // if the upper level is still a stale node, just insert the node by top down method.
                        insert_internal(split_key,sibling_addr, cxt, coro_id, level);
//                        level = level + 1; // move to upper level
//                        p = path_stack[coro_id][level];// move the pointer to upper level
                    }else{

                        if (result.next_level != GlobalAddress::Null()){
                            // the page was found successful by one step back, then we can set the p as new node.
                            // do not need to chanve level.
                            p = result.next_level;
                            goto re_insert;
//                    level = result.level - 1;
                        }else{
                            assert(false);
                        }
                    }

                }

            }
        }


        return true;
    }



// Need BIG FIX

//    template <typename Key, typename Value>
//    bool Btr<Key,Value>::leaf_page_del(GlobalAddress page_addr, const Key &k, int level,
//                                       CoroContext *cxt, int coro_id) {
//        uint64_t lock_index =
//                CityHash64((char *)&page_addr, sizeof(page_addr)) % define::kNumOfLock;
//
//        GlobalAddress lock_addr;
//        lock_addr.nodeID = page_addr.nodeID;
//        lock_addr.offset = lock_index * sizeof(uint64_t);
//
//        auto rbuf = rdma_mg->Get_local_read_mr();
//        ibv_mr* cas_mr = rdma_mg->Get_local_CAS_mr();
//        auto page_buffer = rbuf->addr;
//        //        auto cas_buffer
//        bool insert_success;
//
//        auto tag = 1;
//        try_lock_addr(lock_addr, tag, cas_mr, cxt, coro_id);
//
//        //  auto page_buffer = rdma_mg->get_rbuf(coro_id).get_page_buffer();
//        rdma_mg->RDMA_Read(page_addr, rbuf, kLeafPageSize, IBV_SEND_SIGNALED, 1, Internal_and_Leaf);
//        auto page = (LeafPage<Key,Value> *)page_buffer;
//
//        assert(page->hdr.level == level);
////    assert(page->check_consistent());
//        path_stack[coro_id][page->hdr.level] = page_addr;
//        if (k >= page->hdr.highest) {
//            this->unlock_addr(lock_addr, cxt, coro_id, false);
//
//            assert(page->hdr.sibling_ptr != GlobalAddress::Null());
//
//            return this->leaf_page_del(page->hdr.sibling_ptr, k, level, cxt, coro_id);
//        }
//        if (k < page->hdr.lowest) {
//            this->unlock_addr(lock_addr, cxt, coro_id, false);
//
//            assert(page->hdr.sibling_ptr != GlobalAddress::Null());
//            DEBUG_PRINT_CONDITION("retry place 9\n");
//            return false;
//        }
//        page->front_version++;
//        auto cnt = page->hdr.last_index + 1;
//
//        int del_index = -1;
//        for (int i = 0; i < cnt; ++i) {
//            if (page->records[i].key == k) { // find and update
//                del_index = i;
//                break;
//            }
//        }
//
//        if (del_index != -1) { // remove and shift
//            for (int i = del_index + 1; i < cnt; ++i) {
//                page->records[i - 1].key = page->records[i].key;
//                page->records[i - 1].value = page->records[i].value;
//            }
//
//            page->hdr.last_index--;
////            page->rear_version = page->front_version;
////        page->set_consistent();
//            rdma_mg->RDMA_Write(page_addr, rbuf, kLeafPageSize, IBV_SEND_SIGNALED, 1, Internal_and_Leaf);
//        }
//        this->unlock_addr(lock_addr, cxt, coro_id, false);
//        return true;
//        // TODO: Merge page after the node is too small.
//    }

// Local Locks

//    = __atomic_load_n((uint64_t*)&page->local_lock_meta, (int)std::memory_order_seq_cst);
    template <typename Key>
    inline bool Btr<Key>::try_lock(Local_Meta *local_lock_meta) {
        auto currently_locked = __atomic_load_n(&local_lock_meta->local_lock_byte, __ATOMIC_SEQ_CST);
//        uint8_t currently_locked = 0;
        return !currently_locked &&
               __atomic_compare_exchange_n(&local_lock_meta->local_lock_byte, &currently_locked, 1, true, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
    }
    template <typename Key >
    inline void Btr<Key>::unlock_lock(Local_Meta *local_lock_meta) {
        __atomic_store_n(&local_lock_meta->local_lock_byte, 0, mem_cst_seq);
    }

    template <class Key>
    bool Btr<Key>::acquire_local_optimistic_lock(Local_Meta *local_lock_meta, CoroContext *cxt, int coro_id) {
        assert((uint64_t)local_lock_meta % 8 == 0);
        //TODO: local lock implementation over InternalPage::Local_Meta.


        __atomic_fetch_add(&local_lock_meta->issued_ticket, 1, mem_cst_seq);

//    assert(local_lock_meta->issued_ticket - local_lock_meta->current_ticket <= 16 );
        //TOTHINK(potential bug): what if the ticket out of buffer.
        uint8_t expected = 0;
#ifndef NDEBUG
        uint64_t spin_counter = 0;
        uint64_t global_static_var;

#endif
//    uint8_t lock_status =
        size_t tries = 0;
        while(1){

            if(try_lock(local_lock_meta)){
                break;
            }
            assert(local_lock_meta);
            port::AsmVolatilePause();
            if (tries++ > 100) {
                //        printf("I tried so many time I got yield\n");
                std::this_thread::yield();
            }
        }
#ifndef NDEBUG
        global_static_var = __atomic_load_n((uint64_t*)local_lock_meta, (int)std::memory_order_seq_cst);
        if(((Local_Meta*)&global_static_var)->issued_ticket - ((Local_Meta*)&global_static_var)->current_ticket == 2){
//        printf("mark here");
        }
//    printf("Acquire lock for %p, the current ticks is %d, issued ticket is%d, spin %lu times, thread %d\n", local_lock_meta,
//           ((Local_Meta*)&global_static_var)->current_ticket, ((Local_Meta*)&global_static_var)->issued_ticket, spin_counter, rdma_mg->thread_id);
#endif
        //    uint32_t ticket = lock_val << 32 >> 32;//clear the former 32 bit
//    uint8_t current = __atomic_load_n(&local_lock_addr->current_ticket, mem_cst_seq);// current is the former 32 bit in ticket lock
//    uint8_t current = local_lock_meta->current_ticket;
//        assert(ticket - current <=4);
//    printf("lock offest %lu's ticket %x current %x, thread %u\n", lock_addr.offset, ticket, current, thread_id);
//   printf("", );
//    if (current%define::kMaxHandOverTime == 0){
//        return false;
//    }else{
//        return true;
//    }
//    if (local_lock_meta->handover_times < define::kMaxHandOverTime &&)
        assert(local_lock_meta->local_lock_byte == 1);
        return local_lock_meta->hand_over;

    }

    //Call before release the global lock
    template <class Key>
    inline bool Btr<Key>::can_hand_over(Local_Meta * local_lock_meta) {

        uint8_t issued_ticket = __atomic_load_n(&local_lock_meta->current_ticket, mem_cst_seq);
        uint8_t current_ticket = local_lock_meta->current_ticket;// current is the former 32 bit in ticket lock

// if the handover in node is true, then the other thread can get the lock without any RDMAcas
// if the handover in node is false, then the other thread will acquire the lock from by RDMA cas AGAIN
        if (issued_ticket <= current_ticket + 1) { // no pending locks
            local_lock_meta->hand_over = false;// if no pending thread, then it will release the remote lock and next aquir need RDMA CAS again
        } else {
            local_lock_meta->hand_over = local_lock_meta->hand_time < define::kMaxHandOverTime; // check the limit
        }
        if (!local_lock_meta->hand_over) {
            local_lock_meta->hand_time = 0;// clear the handtime.
        } else {
            local_lock_meta->hand_time++;

//    handover_count[rdma_mg->getMyThreadID()][0]++;
        }

        return local_lock_meta->hand_over;
    }
    template <class Key>
    inline void Btr<Key>::releases_local_optimistic_lock(Local_Meta * local_lock_meta) {

//        auto &node = local_locks[(lock_addr.nodeID-1)/2][lock_addr.offset / 8];

        local_lock_meta->current_ticket++;
        assert(local_lock_meta->local_lock_byte == 1);
//    assert((uint64_t)&local_lock_meta->local_lock_byte % 8 == 0);
        unlock_lock(local_lock_meta);
//        node.ticket_lock.fetch_add((1ull << 32));
    }
//    template <class Key, class Value>
//    void Btr<Key,Value>::make_page_invalidated(InternalPage<Key> *upper_page) {
//        //TODO invalidate page with version, may be reuse the current and issue version?
//        uint8_t expected = 0;
//        if(try_lock(&upper_page->local_lock_meta)){
//            // if the local CAS succeed, then we set the invalidation, if not we just ignore that because,
//            // either another thread is writing (so a new read is coming) or other thread has detect the invalidation and
//            // already set it.
//            assert(expected == 0);
//            __atomic_fetch_add(&upper_page->local_lock_meta.issued_ticket,1, mem_cst_seq);
//            if (upper_page->hdr.valid_page){
//                upper_page->hdr.valid_page = false;
////            printf("Page invalidation %p\n", upper_page);
//            }
//
//            // keep the operation on the version.
//            upper_page->local_lock_meta.current_ticket++;
//            unlock_lock(&upper_page->local_lock_meta);
//        }
//    }
    template <class Key>
    void Btr<Key>::Initialize_page_invalidation(InternalPage<Key> *upper_page) {
        // TODO: cache invalidation RPC.

    }
    template <class Key>
    void Btr<Key>::clear_statistics() {
        for (int i = 0; i < MAX_APP_THREAD; ++i) {
            cache_hit_valid[i][0] = 0;
            cache_miss[i][0] = 0;
        }
    }

}

