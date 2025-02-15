//
// Created by ruihong on 10/1/23.
//

#ifndef SELCC_DDSM_H
#define SELCC_DDSM_H
#include "DSMEngine/cache.h"
#include "storage/page.h"
#include <libmemcached/memcached.h>
namespace DSMEngine {
    static void Deallocate_MR_WITH_CCP(Cache::Handle *handle) {
        // TOFIX: The code below is not protected by the lock shared mutex. It is Okay because,
        // there is definitely no other thread accessing it if a page is destroyed (refs == 0)
        assert(handle->refs.load() == 0);
        auto rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        //TODO: we need to check whether the cache handle have buffered inv message, if so we need to process it.
        // However, this shall rarely happen.
        auto mr = (ibv_mr*) handle->value;
        if (handle->buffer_inv_message.next_holder_id!= Invalid_Node_ID){
            handle->process_buffered_inv_message(handle->gptr, kLeafPageSize, handle->gptr, (ibv_mr*)handle->value, false);
        }
//        if (handle->strategy == 1){
            GlobalAddress lock_gptr = handle->gptr;
            //TODO: Figure out Leafpage or internal page?
            lock_gptr.offset = lock_gptr.offset + STRUCT_OFFSET(LeafPage<uint64_t>, global_lock);
            assert(STRUCT_OFFSET(LeafPage<uint64_t>, global_lock) == STRUCT_OFFSET(InternalPage<uint64_t>, global_lock));
            if (handle->remote_lock_status == 1){

                // RDMA read unlock
//            printf("release the read lock during the handle destroy\n ");
                rdma_mg->global_RUnlock(lock_gptr, rdma_mg->Get_local_CAS_mr());
//                handle->last_modifier_thread_id = 256;
                handle->remote_lock_status.store(0);

            }else if(handle->remote_lock_status == 2){

                // TODO: shall we not consider the global lock word when flushing back the page?
                assert(mr->addr!= nullptr );

//                TODO: recover the assert below if we are testing the blind write operation.
                LeafPage<uint64_t>* page = ((LeafPage<uint64_t>*)mr->addr);
#ifndef NDEBUG
                assert(STRUCT_OFFSET(InternalPage<uint64_t >, global_lock) == STRUCT_OFFSET(LeafPage<uint64_t>, global_lock));
                if (page->hdr.p_type == P_Internal_P){
                    printf("Internal page is being destroyed %p\n", handle->gptr);
                }
#endif
                assert(page->hdr.this_page_g_ptr == GlobalAddress::Null() || page->hdr.this_page_g_ptr == handle->gptr);
                assert(page->global_lock);
//                assert(handle->gptr == ((LeafPage<uint64_t,uint64_t>*)mr->addr)->hdr.this_page_g_ptr);

                //TODO: make handle state change and page dirty content flush back a atomic function in Cache_handle class.

                // RDMA write unlock and write back the data. THis shall be a sync write back, because the buffer will
                // be handover to other cache entry after this function. It is possible that the page content is changed when the
                // RDMA write back has not been finished. The write unlock for page invalidation can be a sync write back.
                rdma_mg->global_write_page_and_Wunlock(mr, handle->gptr, kLeafPageSize, lock_gptr, handle);
                handle->remote_lock_status.store(0);
            }else{
                //An invalidated page, do nothing
            }

        handle->clear_pending_inv_states();
#ifdef WRITER_STARV_SPIN_BASE
        handle->reader_spin_time.store(0);
#endif
#ifndef PAGE_FREE_LIST
        if (!handle->keep_the_mr){
            rdma_mg->Deallocate_Local_RDMA_Slot(mr->addr, Regular_Page);
            delete mr;
        }
#endif
        assert(handle->refs.load() == 0);
    }


    class DDSM {
    public:
        Cache *page_cache;
        RDMA_Manager *rdma_mg = nullptr;
        //TODO: implement a thread local cache line hold memo.
        memcached_st *memc;
        std::mutex memc_mutex;
        DDSM(Cache *page_cache, RDMA_Manager *rdma_mg = nullptr) : page_cache(page_cache), rdma_mg(rdma_mg) {
            if (!connectMemcached()) {
                printf("Failed to connect to memcached\n");
                return;
            }
            char temp[100] = "Try me ahahahahaha! kkk";
            memSet(reinterpret_cast<const char *>(&temp), 100, reinterpret_cast<const char *>(&temp), 100);
        };
        ~DDSM(){
            disconnectMemcached();
        }
        static DDSM *Get_Instance();
        //todo: do we need destroy instance?
        void SELCC_Shared_Lock(void*& page_buffer, GlobalAddress page_addr, Cache::Handle*& handle);
        bool TrySELCC_Shared_Lock(void*& page_buffer, GlobalAddress page_addr, Cache::Handle*& handle);
        void SELCC_Shared_UnLock(GlobalAddress page_addr, Cache::Handle *&handle);
        void SELCC_Exclusive_Lock_noread(void*& page_buffer, GlobalAddress page_addr, Cache::Handle *&handle);
//        bool TrySELCC_Exclusive_Lock_noread(void*& page_buffer, GlobalAddress page_addr, Cache::Handle *&handle);

        void SELCC_Exclusive_UnLock_noread(GlobalAddress page_addr, Cache::Handle *&handle);
        void SELCC_Exclusive_Lock(void*& page_buffer, GlobalAddress page_addr, Cache::Handle *&handle);
        bool TrySELCC_Exclusive_Lock(void*& page_buffer, GlobalAddress page_addr, Cache::Handle *&handle);

        // Handle should be not nullptr
        //TODO: make the hierachical lock upgrade atomaticlly, currently we release and then acquire the lock.
        bool SELCC_Lock_Upgrade(void*& page_buffer, GlobalAddress page_addr, Cache::Handle* handle);

        void SELCC_Exclusive_UnLock(GlobalAddress page_addr, Cache::Handle *handle);
        bool connectMemcached();
        bool disconnectMemcached();
        void memSet(const char *key, uint32_t klen, const char *val, uint32_t vlen);
        //blocking function.
        char *memGet(const char *key, uint32_t klen, size_t *v_size = nullptr);
        uint64_t ClusterSum(const std::string &sum_key, uint64_t value);
        uint64_t memFetchAndAdd(const char *key, uint32_t klen);
        GlobalAddress Allocate_Remote(Chunk_type pool_name); // allocate
        void Deallocate_Remote(Chunk_type pool_name, GlobalAddress gaddr){}; // free
        uint16_t GetID(){
            return rdma_mg->node_id;
        }
        static uint64_t GetNextIndexID(){
            static std::atomic<uint64_t> index_id = {0};
            return index_id.fetch_add(1);
        }
    private:
        std::atomic<uint64_t > target_node_counter = {0};
    };
    class SELCC_Guard{
    public:
        enum LockType{
            Shared,
            Exclusive
        };
        GlobalAddress page_addr_;
        Cache::Handle* handle_;
        LockType lock_type_;
//        SELCC_Guard(void* &page_buffer, GlobalAddress page_addr, Cache::Handle* & handle){} = default;
        // TODO: make the destructor a virtual function
        virtual ~SELCC_Guard(){
            if(lock_type_ == Shared){
                DDSM::Get_Instance()->SELCC_Shared_UnLock(page_addr_, handle_);
            }else {
                DDSM::Get_Instance()->SELCC_Exclusive_UnLock(page_addr_, handle_);
            }
        }
    };
    class Exclusive_Guard : public SELCC_Guard{
    public:
//        GlobalAddress page_addr_;
//        Cache::Handle* handle_;
        Exclusive_Guard(void* &page_buffer, GlobalAddress page_addr, Cache::Handle* & handle){
            DDSM::Get_Instance()->SELCC_Exclusive_Lock(page_buffer, page_addr, handle);
            handle_ = handle;
            page_addr_ = page_addr;

        }
        ~Exclusive_Guard() override {
            DDSM::Get_Instance()->SELCC_Exclusive_UnLock(page_addr_, handle_);

        }
    };
    class Shared_Guard: public SELCC_Guard{
    public:
//        GlobalAddress page_addr_;
//        Cache::Handle* handle_;
        Shared_Guard(void* &page_buffer, GlobalAddress page_addr, Cache::Handle* & handle){
            DDSM::Get_Instance()->SELCC_Shared_Lock(page_buffer, page_addr, handle);
            handle_ = handle;
            page_addr_ = page_addr;

        }
        ~Shared_Guard() override{
            DDSM::Get_Instance()->SELCC_Shared_UnLock(page_addr_, handle_);

        }
    };

}


#endif //SELCC_DDSM_H
