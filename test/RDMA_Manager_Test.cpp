//
// Created by wang4996 on 8/1/22.
//
#include "storage/rdma.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
namespace DSMEngine {
    class RDMA_Manager_Test : public testing::Test {
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
                    0,
                    0};
            rdma_mg = RDMA_Manager::Get_Instance(config);
            rdma_mg->Mempool_initialize(DataChunk, INDEX_BLOCK, 0);
        }



        DSMEngine::RDMA_Manager* rdma_mg;
    };

    TEST_F(RDMA_Manager_Test, LocalAllocation) {
        ibv_mr mr{};
        rdma_mg->Allocate_Local_RDMA_Slot(mr, DataChunk);
        ASSERT_EQ(rdma_mg->name_to_mem_pool.at(DataChunk).size(), 1);
        GlobalAddress gptr = rdma_mg->Allocate_Remote_RDMA_Slot(Regular_Page, 1);
        assert(gptr != GlobalAddress::Null());
    }

    TEST_F(RDMA_Manager_Test, RemoteAllocation) {
        GlobalAddress gptr = rdma_mg->Allocate_Remote_RDMA_Slot(Internal_and_Leaf, 1);
        assert(gptr != GlobalAddress::Null());
    }
}
int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

