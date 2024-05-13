//
// Created by ruihong on 10/1/23.
//

#include <fstream>
#include "DDSM.h"
namespace DSMEngine {
    std::string trim(const std::string &s) {
        std::string res = s;
        if (!res.empty()) {
            res.erase(0, res.find_first_not_of(" "));
            res.erase(res.find_last_not_of(" ") + 1);
        }
        return res;
    }
#if ACCESS_MODE == 1 || ACCESS_MODE == 2
    void DDSM::PrePage_Read(void *& page_buffer, GlobalAddress page_addr, Cache::Handle *& handle) {
        assert((page_addr.offset % 1ULL*1024ULL*1024ULL*1024ULL)% kLeafPageSize == 0);
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;

        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        ibv_mr *mr = nullptr;
        Slice page_id((char *) &page_addr, sizeof(GlobalAddress));

        handle = page_cache->LookupInsert(page_id, nullptr, kLeafPageSize, Deallocate_MR_WITH_CCP);
        assert(handle != nullptr);
        handle->reader_pre_access(page_addr, kLeafPageSize, lock_addr, mr);
        page_buffer = mr->addr;
        assert(STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, hdr.this_page_g_ptr) == STRUCT_OFFSET(DataPage, hdr.this_page_g_ptr));
        assert(((LeafPage<uint64_t, uint64_t>*)page_buffer)->global_lock);
        assert(handle->gptr == page_addr);
        assert(((LeafPage<uint64_t, uint64_t>*)page_buffer)->hdr.this_page_g_ptr == GlobalAddress::Null()||((LeafPage<uint64_t, uint64_t>*)page_buffer)->hdr.this_page_g_ptr == page_addr);

    }

    bool DDSM::TryPrePage_Read(void *& page_buffer, GlobalAddress page_addr, Cache::Handle *& handle) {
        assert((page_addr.offset % 1ULL*1024ULL*1024ULL*1024ULL)% kLeafPageSize == 0);
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        ibv_mr *mr = nullptr;
        Slice page_id((char *) &page_addr, sizeof(GlobalAddress));
        handle = page_cache->LookupInsert(page_id, nullptr, kLeafPageSize, Deallocate_MR_WITH_CCP);
        assert(handle != nullptr);
        if(!handle->try_reader_pre_access(page_addr, kLeafPageSize, lock_addr, mr)){
            return false;
        }
        page_buffer = mr->addr;
        assert(STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, hdr.this_page_g_ptr) == STRUCT_OFFSET(DataPage, hdr.this_page_g_ptr));
        assert(((LeafPage<uint64_t, uint64_t>*)page_buffer)->global_lock);
        assert(handle->gptr == page_addr);
        assert(((LeafPage<uint64_t, uint64_t>*)page_buffer)->hdr.this_page_g_ptr == GlobalAddress::Null()||((LeafPage<uint64_t, uint64_t>*)page_buffer)->hdr.this_page_g_ptr == page_addr);
        return true;
    }

    void DDSM::PostPage_Read(GlobalAddress page_addr, Cache::Handle *&handle) {
        assert((page_addr.offset % 1ULL*1024ULL*1024ULL*1024ULL)% kLeafPageSize == 0);
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        ibv_mr *local_mr = (ibv_mr *) handle->value;
        handle->reader_post_access(page_addr, kLeafPageSize, lock_addr, local_mr);
        page_cache->Release(handle);
        handle = nullptr;

    }

    void DDSM::PrePage_Write(void *&page_buffer, GlobalAddress page_addr, Cache::Handle *& handle) {
//        printf("PRE Write page node %d, offset %lu, THIS NODE IS %u\n", page_addr.nodeID, page_addr.offset,RDMA_Manager::node_id);
        assert(TOPAGE(page_addr) == page_addr);
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;

        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        ibv_mr *mr = nullptr;
        Slice page_id((char *) &page_addr, sizeof(GlobalAddress));

        handle = page_cache->LookupInsert(page_id, nullptr, kLeafPageSize, Deallocate_MR_WITH_CCP);
        assert(handle != nullptr);
        handle->writer_pre_access(page_addr, kLeafPageSize, lock_addr, mr);
        page_buffer = mr->addr;
        // reset the local buffer beyond the lock word.
        memset((char*)page_buffer + 16,0, kLeafPageSize - 16);
//        assert(STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, hdr.this_page_g_ptr) == STRUCT_OFFSET(DataPage, hdr.this_page_g_ptr));
//        assert(((LeafPage<uint64_t, uint64_t>*)page_buffer)->global_lock);
//        assert(handle->gptr == page_addr);
//        assert(((LeafPage<uint64_t, uint64_t>*)page_buffer)->hdr.this_page_g_ptr == page_addr);
    }
//    bool DDSM::TryPrePage_Write(void *&page_buffer, GlobalAddress page_addr, Cache::Handle *& handle) {
////        printf("PRE Write page node %d, offset %lu, THIS NODE IS %u\n", page_addr.nodeID, page_addr.offset,RDMA_Manager::node_id);
//        assert(TOPAGE(page_addr) == page_addr);
//        GlobalAddress lock_addr;
//        lock_addr.nodeID = page_addr.nodeID;
//
//        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
//        ibv_mr *mr = nullptr;
//        Slice page_id((char *) &page_addr, sizeof(GlobalAddress));
//
//        handle = page_cache->LookupInsert(page_id, nullptr, kLeafPageSize, Deallocate_MR_WITH_CCP);
//        assert(handle != nullptr);
//        bool success = handle->try_writer_pre_access(page_addr, kLeafPageSize, lock_addr, mr);
//        if (success){
//            page_buffer = mr->addr;
//            // reset the local buffer beyond the lock word.
//            memset((char*)page_buffer + 16,0, kLeafPageSize - 16);
//        }
//        return success;
////        assert(STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, hdr.this_page_g_ptr) == STRUCT_OFFSET(DataPage, hdr.this_page_g_ptr));
////        assert(((LeafPage<uint64_t, uint64_t>*)page_buffer)->global_lock);
////        assert(handle->gptr == page_addr);
////        assert(((LeafPage<uint64_t, uint64_t>*)page_buffer)->hdr.this_page_g_ptr == page_addr);
//    }

    void DDSM::PostPage_Write(GlobalAddress page_addr, Cache::Handle *&handle) {
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        ibv_mr *local_mr = (ibv_mr *) handle->value;
        handle->writer_post_access(page_addr, kLeafPageSize, lock_addr, local_mr);
        page_cache->Release(handle);
        handle = nullptr;
    }

    void DDSM::PrePage_Update(void *&page_buffer, GlobalAddress page_addr, Cache::Handle *&handle) {
//        printf("PRE Update page node %d, offset %lu, THIS NODE IS %u\n", page_addr.nodeID, page_addr.offset,RDMA_Manager::node_id);
        assert(TOPAGE(page_addr) == page_addr);
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;

        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        ibv_mr *mr = nullptr;
        Slice page_id((char *) &page_addr, sizeof(GlobalAddress));

        handle = page_cache->LookupInsert(page_id, nullptr, kLeafPageSize, Deallocate_MR_WITH_CCP);
        assert(handle != nullptr);
        //TODO: unwarp the updater_pre_access.
        handle->updater_pre_access(page_addr, kLeafPageSize, lock_addr, mr);
        page_buffer = mr->addr;
//        assert(STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, hdr.this_page_g_ptr) == STRUCT_OFFSET(DataPage, hdr.this_page_g_ptr));
//        assert(((LeafPage<uint64_t, uint64_t>*)page_buffer)->global_lock);
//        assert(handle->gptr == page_addr);
//        assert(((LeafPage<uint64_t, uint64_t>*)page_buffer)->hdr.this_page_g_ptr == page_addr);

    }
    bool DDSM::TryPrePage_Update(void *&page_buffer, GlobalAddress page_addr, Cache::Handle *&handle) {
//        printf("PRE Update page node %d, offset %lu, THIS NODE IS %u\n", page_addr.nodeID, page_addr.offset,RDMA_Manager::node_id);
        assert(TOPAGE(page_addr) == page_addr);
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;

        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        ibv_mr *mr = nullptr;
        Slice page_id((char *) &page_addr, sizeof(GlobalAddress));

        handle = page_cache->LookupInsert(page_id, nullptr, kLeafPageSize, Deallocate_MR_WITH_CCP);
        assert(handle != nullptr);
        //TODO: unwarp the updater_pre_access.
        if(!handle->try_updater_pre_access(page_addr, kLeafPageSize, lock_addr, mr)){
            return false;
        }
        page_buffer = mr->addr;
        return true;
        //        assert(STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, hdr.this_page_g_ptr) == STRUCT_OFFSET(DataPage, hdr.this_page_g_ptr));
//        assert(((LeafPage<uint64_t, uint64_t>*)page_buffer)->global_lock);
//        assert(handle->gptr == page_addr);
//        assert(((LeafPage<uint64_t, uint64_t>*)page_buffer)->hdr.this_page_g_ptr == page_addr);

    }

    bool DDSM::PrePage_Upgrade(void *&page_buffer, GlobalAddress page_addr, Cache::Handle *handle) {
        assert(handle != nullptr);
        assert(handle->remote_lock_status == 1);
        assert(handle->rw_mtx.issharelocked());
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;

        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        ibv_mr *mr = nullptr;
        Slice page_id((char *) &page_addr, sizeof(GlobalAddress));

//        handle = page_cache->LookupInsert(page_id, nullptr, kLeafPageSize, Deallocate_MR_WITH_CCP);
        assert(handle->value != nullptr);
        //TODO: unwarp the updater_pre_access.
//        if (!handle->rw_mtx.try_upgrade()){
//            return false;
//        }
        if (!handle->try_upgrade_pre_access(page_addr, kLeafPageSize, lock_addr, mr)){
            return false;
        }
        page_buffer = mr->addr;
        assert(STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, hdr.this_page_g_ptr) == STRUCT_OFFSET(DataPage, hdr.this_page_g_ptr));
        assert(((LeafPage<uint64_t, uint64_t>*)page_buffer)->global_lock);
        assert(handle->gptr == page_addr);
        assert(((LeafPage<uint64_t, uint64_t>*)page_buffer)->hdr.this_page_g_ptr == page_addr);
        return true;
    }

    void DDSM::PostPage_UpdateOrWrite(GlobalAddress page_addr, Cache::Handle *&handle) {
//        printf("POST Update or Write page node %d, offset %lu, THIS NODE IS %u\n", page_addr.nodeID, page_addr.offset,RDMA_Manager::node_id);
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        //The assetion below is not always true.
        assert(handle->refs >1);
        ibv_mr *local_mr = (ibv_mr *) handle->value;
        handle->updater_writer_post_access(page_addr, kLeafPageSize, lock_addr, local_mr);
        page_cache->Release(handle);
        //TODO: delete the assert.
//        assert(!handle->rw_mtx.islocked());
        handle = nullptr;
        assert(STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, hdr.this_page_g_ptr) == STRUCT_OFFSET(DataPage, hdr.this_page_g_ptr));
        auto page_buffer = local_mr->addr;
        assert(((DataPage*)page_buffer)->global_lock);
//        assert(((DataPage*)page_buffer)->hdr.this_page_g_ptr == page_addr);
    }
#elif ACCESS_MODE == 0
    bool DDSM::TryPrePage_Read(void *&page_buffer, GlobalAddress page_addr, Cache::Handle *&handle) {
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        if (rdma_mg == nullptr){
            rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        }
        ibv_mr *mr = rdma_mg->Get_local_read_mr();

        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();

        if(!rdma_mg->global_Rlock_and_read_page_without_INVALID(mr, page_addr, kLeafPageSize, lock_addr, cas_mr, 5)){
            page_buffer = nullptr;
            return false;
        }

        //TODO: maybe we need to copy it to a new buffer to avoid overwrite. or we let the implementation outside this function to handle it
        page_buffer = mr->addr;
        return true;
    }
    bool DDSM::TryPrePage_Update(void *&page_buffer, GlobalAddress page_addr, Cache::Handle *&handle) {
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;

        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        if (rdma_mg == nullptr){
            rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        }
        ibv_mr *mr = rdma_mg->Get_local_read_mr();
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
        if(!rdma_mg->global_Wlock_and_read_page_without_INVALID(mr, page_addr, kLeafPageSize, lock_addr, cas_mr, 5)){
            return false;
        }
        page_buffer = mr->addr;
        return true;
    }
    bool DDSM::PrePage_Upgrade(void *&page_buffer, GlobalAddress page_addr, Cache::Handle *handle) {
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;

        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        ibv_mr *mr = rdma_mg->Get_local_read_mr();
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
        if (!rdma_mg->global_Rlock_update(mr,  lock_addr, cas_mr)){
            return false;
        }
        return true;
    }
    void DDSM::PrePage_Read(void *&page_buffer, GlobalAddress page_addr, Cache::Handle *&handle) {
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        if (rdma_mg == nullptr){
            rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        }
        ibv_mr *mr = rdma_mg->Get_local_read_mr();

        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();

        rdma_mg->global_Rlock_and_read_page_without_INVALID(mr, page_addr, kLeafPageSize, lock_addr, cas_mr);


        page_buffer = mr->addr;
    }

    void DDSM::PostPage_Read(GlobalAddress page_addr, Cache::Handle *&handle) {
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();

        rdma_mg->global_RUnlock(lock_addr, cas_mr);

    }

    void DDSM::PrePage_Write(void *&page_buffer, GlobalAddress page_addr, Cache::Handle *&handle) {
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        if (rdma_mg == nullptr){
            rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        }
        ibv_mr *mr = rdma_mg->Get_local_read_mr();
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();

        rdma_mg->global_Wlock_without_INVALID(mr, page_addr, kLeafPageSize, lock_addr, cas_mr);

        page_buffer = mr->addr;
    }

    void DDSM::PostPage_Write(GlobalAddress page_addr, Cache::Handle *&handle) {
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        ibv_mr *local_mr = rdma_mg->Get_local_read_mr();
        rdma_mg->global_write_page_and_Wunlock(local_mr, page_addr, kLeafPageSize, lock_addr);


    }

    void DDSM::PrePage_Update(void *&page_buffer, GlobalAddress page_addr, Cache::Handle *&handle) {
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        if (rdma_mg == nullptr){
            rdma_mg = RDMA_Manager::Get_Instance(nullptr);
        }
        ibv_mr *mr = rdma_mg->Get_local_read_mr();
        ibv_mr * cas_mr = rdma_mg->Get_local_CAS_mr();
        rdma_mg->global_Wlock_and_read_page_without_INVALID(mr, page_addr, kLeafPageSize, lock_addr, cas_mr);
        page_buffer = mr->addr;
    }

    void DDSM::PostPage_UpdateOrWrite(GlobalAddress page_addr, Cache::Handle *&handle){
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        ibv_mr *local_mr = rdma_mg->Get_local_read_mr();
        rdma_mg->global_write_page_and_Wunlock(local_mr, page_addr, kLeafPageSize, lock_addr);
    }
#endif

    bool DDSM::connectMemcached() {
        memcached_server_st *servers = NULL;
        memcached_return rc;

        std::ifstream conf("../memcached_ip.conf");

        if (!conf) {
            fprintf(stderr, "can't open memcached_ip.conf\n");
            return false;
        }

        std::string addr, port;
        std::getline(conf, addr);
        std::getline(conf, port);

        memc = memcached_create(NULL);
        servers = memcached_server_list_append(servers, trim(addr).c_str(),
                                               std::stoi(trim(port)), &rc);
        rc = memcached_server_push(memc, servers);

        if (rc != MEMCACHED_SUCCESS) {
            fprintf(stderr, "Counld't add server:%s\n", memcached_strerror(memc, rc));
            sleep(1);
            return false;
        }

        memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, 1);
        return true;
    }

    bool DDSM::disconnectMemcached() {
        if (memc) {
            memcached_quit(memc);
            memcached_free(memc);
            memc = NULL;
        }
        return true;
    }
    void DDSM::memSet(const char *key, uint32_t klen, const char *val,
                        uint32_t vlen) {

        memcached_return rc;
        while (true) {
            memc_mutex.lock();

            rc = memcached_set(memc, key, klen, val, vlen, (time_t)0, (uint32_t)0);
            if (rc == MEMCACHED_SUCCESS) {
                memc_mutex.unlock();
                break;
            }else{
                memc_mutex.unlock();

            }

            usleep(400);
        }
    }

    char *DDSM::memGet(const char *key, uint32_t klen, size_t *v_size) {

        size_t l;
        char *res;
        uint32_t flags;
        memcached_return rc;

        while (true) {
            memc_mutex.lock();
            res = memcached_get(memc, key, klen, &l, &flags, &rc);
            if (rc == MEMCACHED_SUCCESS) {
                memc_mutex.unlock();
                break;
            }else{
                memc_mutex.unlock();

            }
            usleep(400 * rdma_mg->node_id);
        }

        if (v_size != nullptr) {
            *v_size = l;
        }

        return res;
    }

    uint64_t DDSM::memFetchAndAdd(const char *key, uint32_t klen) {
        uint64_t res;
        while (true) {
            memcached_return rc = memcached_increment(memc, key, klen, 1, &res);
            if (rc == MEMCACHED_SUCCESS) {
                return res;
            }
            usleep(10000);
        }
    }

    GlobalAddress DDSM::Allocate_Remote(Chunk_type pool_name) {
        if (rdma_mg) {
            uint8_t target_node = target_node_counter.fetch_add(1) % rdma_mg->memory_nodes.size();
            auto ret = rdma_mg->Allocate_Remote_RDMA_Slot(pool_name, 2*target_node+1);
            return ret;
        } else {
            assert(false);
            DEBUG_PRINT("RDMA manager is not initialized\n");
            return GlobalAddress::Null();
        }
    }

    uint64_t DDSM::ClusterSum(const std::string &sum_key, uint64_t value) {
        std::string key_prefix = std::string("sum-") + sum_key;

        std::string key = key_prefix + std::to_string(this->GetID());
        memSet(key.c_str(), key.size(), (char *)&value, sizeof(value));

        uint64_t ret = 0;
        for (int i = 0; i < this->rdma_mg->GetComputeNodeNum(); ++i) {
            key = key_prefix + std::to_string(i);
            ret += *(uint64_t *)memGet(key.c_str(), key.size());
        }

        return ret;
    }




}
