#include <iostream>
#include <memory_node/memory_node_keeper.h>

#include "util/rdma.h"

//namespace DSMEngine{
int main(int argc,char* argv[])
{
  DSMEngine::Memory_Node_Keeper* mn_keeper;

  if (argc == 4){
    uint32_t tcp_port;
    int pr_size;
    int Memory_server_id;
    char* value = argv[1];
    std::stringstream strValue1;
    strValue1 << value;
    strValue1 >> tcp_port;
    value = argv[2];
    std::stringstream strValue2;
    //  strValue.str("");
    strValue2 << value;
    strValue2 >> pr_size;
    value = argv[3];
    std::stringstream strValue3;
    //  strValue.str("");
    strValue3 << value;
    strValue3 >> Memory_server_id;
      printf("TCP_PORT is %d, preregistered memory size is %d, node id is %d", tcp_port, pr_size, 2* Memory_server_id + 1);
     mn_keeper = new DSMEngine::Memory_Node_Keeper(true, tcp_port, pr_size);
     DSMEngine::RDMA_Manager::node_id = 2* Memory_server_id + 1;
  }else{
      printf("Please input  ./memory_server [TCP_PORT] [preregistered memory size] [node id]\n ");
    mn_keeper = new DSMEngine::Memory_Node_Keeper(true, 19843, 88);
    DSMEngine::RDMA_Manager::node_id = 1;
  }

  mn_keeper->SetBackgroundThreads(12, DSMEngine::ThreadPoolType::CompactionThreadPool);
  mn_keeper->Server_to_Client_Communication();
  delete mn_keeper;

  return 0;
}