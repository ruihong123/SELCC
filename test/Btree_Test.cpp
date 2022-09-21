//
// Created by wang4996 on 8/1/22.
//
#include "Btr.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
namespace DSMEngine {
    class Btree_Test : public testing::Test {
    public:
    protected:
        void SetUp() override {
            uint32_t tcp_port = 19843;
            uint32_t size = 8*1024;
            struct DSMEngine::config_t config = {
                    NULL,  /* dev_name */
                    NULL,  /* server_name */
                    tcp_port, /* tcp_port */
                    1,	 /* ib_port */
                    1, /* gid_idx */
                    0};
            rdma_mg = RDMA_Manager::Get_Instance(config);
            rdma_mg->Mempool_initialize(DataChunk, INDEX_BLOCK, 0);
            rdma_mg->node_id = 0;
            tree = new Btr(rdma_mg, nullptr, 0);
        }



        DSMEngine::RDMA_Manager* rdma_mg;
        Btr* tree;
        std::map<Key, Value> in_memory_records;

    };

    TEST_F(Btree_Test, loading) {
        for (int i = 0; i < 1000000; ++i) {
            Key k = i;
            Value v = i;
            tree->insert(k,v);
            in_memory_records.insert({k,v});

        }

        ASSERT_EQ(rdma_mg->name_to_mem_pool.at(DataChunk).size(), 1);
    }
    TEST_F(Btree_Test, retrieval) {
        for (int i = 0; i < 1000000; ++i) {
            Key k = i;
            Value v;
            tree->search(i, v);
            ASSERT_EQ(in_memory_records.at(k),v);
        }
    }
}
int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

