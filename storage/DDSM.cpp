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

    void DDSM::PrePage_Read(void *&page_buffer, GlobalAddress page_addr, Cache::Handle *handle) {
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;

        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        ibv_mr *mr = nullptr;
        Slice page_id((char *) &page_addr, sizeof(GlobalAddress));

        handle = page_cache->LookupInsert(page_id, nullptr, kLeafPageSize, Deallocate_MR_WITH_CCP);
        assert(handle != nullptr);
        handle->reader_pre_access(page_addr, kLeafPageSize, lock_addr, mr);
        page_buffer = mr->addr;
    }

    void DDSM::PostPage_Read(GlobalAddress page_addr, DSMEngine::Cache::Handle *handle) {
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        handle->reader_post_access(lock_addr);
        page_cache->Release(handle);
    }

    void DDSM::PrePage_Write(void *&page_buffer, GlobalAddress page_addr, DSMEngine::Cache::Handle *handle) {
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;

        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        ibv_mr *mr = nullptr;
        Slice page_id((char *) &page_addr, sizeof(GlobalAddress));

        handle = page_cache->LookupInsert(page_id, nullptr, kLeafPageSize, Deallocate_MR_WITH_CCP);
        assert(handle != nullptr);
        handle->writer_pre_access(page_addr, kLeafPageSize, lock_addr, mr);
        page_buffer = mr->addr;
    }

    void DDSM::PostPage_Write(GlobalAddress page_addr, DSMEngine::Cache::Handle *handle) {
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        ibv_mr *local_mr = (ibv_mr *) handle->value;
        handle->writer_post_access(page_addr, kLeafPageSize, lock_addr, local_mr);
        page_cache->Release(handle);
    }

    void DDSM::PrePage_Update(void *&page_buffer, GlobalAddress page_addr, DSMEngine::Cache::Handle *handle) {
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
    }

    void DDSM::PostPage_Update(GlobalAddress page_addr, DSMEngine::Cache::Handle *handle) {
        GlobalAddress lock_addr;
        lock_addr.nodeID = page_addr.nodeID;
        lock_addr.offset = page_addr.offset + STRUCT_OFFSET(LeafPage<uint64_t COMMA uint64_t>, global_lock);
        ibv_mr *local_mr = (ibv_mr *) handle->value;
        handle->updater_post_access(page_addr, kLeafPageSize, lock_addr, local_mr);
        page_cache->Release(handle);
    }

    bool DDSM::connectMemcached() {
        memcached_server_st *servers = NULL;
        memcached_return rc;

        std::ifstream conf("../memcached.conf");

        if (!conf) {
            fprintf(stderr, "can't open memcached.conf\n");
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
            rc = memcached_set(memc, key, klen, val, vlen, (time_t)0, (uint32_t)0);
            if (rc == MEMCACHED_SUCCESS) {
                break;
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

            res = memcached_get(memc, key, klen, &l, &flags, &rc);
            if (rc == MEMCACHED_SUCCESS) {
                break;
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
            return rdma_mg->Allocate_Remote_RDMA_Slot(pool_name, 2*target_node+1);
        } else {
            assert(false);
            DEBUG_PRINT("RDMA manager is not initialized\n");
            return GlobalAddress::Null();
        }
    }
}
