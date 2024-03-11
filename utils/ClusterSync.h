#ifndef __DATABASE_UTILS_CLUSTER_SYNCHRONIZER_H__
#define __DATABASE_UTILS_CLUSTER_SYNCHRONIZER_H__

#include "DDSM.h"
#include "ClusterConfig.h"

namespace DSMEngine {
class ClusterSync{
public:
  ClusterSync(ClusterConfig *config) : config_(config) {
      sync_key_xcompute_ = 0;
      sync_key_xall_ = SYNC_XALL_OFFSET;
  }

    // sync use RDMA node_id as the key
    void Fence_XALLNodes() {
        uint16_t node_id = default_gallocator->GetID();
        int* id;
        uint64_t temp_sync_key = sync_key_xall_ + node_id;
        default_gallocator->memSet((char*)&temp_sync_key, sizeof(uint64_t), (char*)&node_id, sizeof(node_id));
        uint64_t no_node = config_->GetPartitionNum() + config_->GetMemoryNum();
        for (int i = 0; i < no_node; i++) {
            temp_sync_key = sync_key_xall_ + i;
            size_t get_size = 0;
            id = (int*)default_gallocator->memGet((char*)&temp_sync_key, sizeof(uint64_t), &get_size);
            assert(get_size == sizeof(uint16_t));
            assert(*id == i);
        }
        sync_key_xall_ += no_node;
    }
    //Sync use partition_id as the key
  void FenceXComputes() {
    size_t partition_id = config_->GetMyPartitionId();
    size_t partition_num = config_->GetPartitionNum();
    bool *flags = new bool[partition_num];
    memset(flags, 0, sizeof(bool)*partition_num);
    this->MasterCollect<bool>(flags + partition_id, flags);
    this->MasterBroadcast<bool>(flags + partition_id);
    delete[] flags;
    flags = nullptr;
  }

  template<class T>
  void MasterCollect(T *send, T *receive) {
    char* data;
    size_t partition_id = config_->GetMyPartitionId();
    size_t partition_num = config_->GetPartitionNum();
    uint64_t temp_sync_key = sync_key_xcompute_;
    if (config_->IsMaster()) {
      for (size_t i = 0; i < partition_num; ++i) {
        if (i != partition_id) {
            temp_sync_key = sync_key_xcompute_ + i;
            size_t get_size = 0;
            data = default_gallocator->memGet(
                  (char*)&temp_sync_key, sizeof(uint64_t), &get_size);
            assert(get_size == sizeof(T));
          memcpy(receive + i, data, sizeof(T));
        }
        else {
          memcpy(receive + i, send, sizeof(T));
        }
      }
    }
    else {
        temp_sync_key = sync_key_xcompute_ + partition_id;
      default_gallocator->memSet((char*)&temp_sync_key, sizeof(uint64_t), (char*)send, sizeof(T));
    }
      sync_key_xcompute_ += partition_num;
  }

  template<class T>
  void MasterBroadcast(T *send) {
    size_t partition_id = config_->GetMyPartitionId();
    size_t partition_num = config_->GetPartitionNum();
    uint64_t temp_sync_key = sync_key_xcompute_;
    if (config_->IsMaster()) {
      assert(partition_id == 0);
      temp_sync_key = sync_key_xcompute_ + partition_id;
      default_gallocator->memSet((char*)&temp_sync_key, sizeof(uint64_t), (char*)send, sizeof(T));
    }
    else {
      const size_t master_partition_id = 0;
        temp_sync_key = sync_key_xcompute_ + master_partition_id;
        size_t get_size = 0;
        void* ret = default_gallocator->memGet((char*)&temp_sync_key, sizeof(uint64_t), &get_size);
        assert(get_size == sizeof(T));
        memcpy(send, ret, sizeof(T));
    }
      sync_key_xcompute_ += partition_num;
  }

    void MasterBroadcast(char* key, size_t key_size, char* buff, size_t size) {
        size_t partition_id = config_->GetMyPartitionId();
        size_t partition_num = config_->GetPartitionNum();

        if (config_->IsMaster()) {
            assert(partition_id == 0);
            default_gallocator->memSet(key, key_size, (char*)buff, size);
        }
        else {
            size_t get_size = 0;
            void* ret = default_gallocator->memGet(key, key_size, &get_size);
            assert(get_size == size);
            memcpy(buff, ret, size);
        }
        sync_key_xcompute_ += partition_num;
    }

private:
  ClusterConfig *config_;
  uint64_t sync_key_xcompute_;
  uint64_t sync_key_xall_;
};
}

#endif
