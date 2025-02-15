#ifndef RDMA_H
#define RDMA_H
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <inttypes.h>
#include <endian.h>
#include <byteswap.h>
#include <cassert>
#include <algorithm>
#include <condition_variable>
#include <thread>
#include <chrono>
#include <memory>
#include <sstream>
#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <queue>
#include "utils/thread_local.h"
#include "Common.h"
#include "port/port_posix.h"
#include "utils/mutexlock.h"
#include "utils/ThreadPool.h"
#include <atomic>
#include <chrono>
#include <iostream>
#include <map>
#include <unordered_map>
#include <shared_mutex>
#include <vector>
#include <list>
#include <cstdint>
#include "utils/TimeMeasurer.h"
#define REMOTE_DEALLOC_BUFF_SIZE (128 + 128) * sizeof(uint64_t)


#if __BYTE_ORDER == __LITTLE_ENDIAN
#elif __BYTE_ORDER == __BIG_ENDIAN
  static inline uint64_t htonll(uint64_t x) { return x; }
  static inline uint64_t ntohll(uint64_t x) { return x; }
#else
#error __BYTE_ORDER is neither __LITTLE_ENDIAN nor __BIG_ENDIAN
#endif
#define RDMA_WRITE_BLOCK  (8*1024*1024)
#define INDEX_BLOCK  (8*1024*1024)
#define FILTER_BLOCK  (2*1024*1024)
namespace DSMEngine {
class Cache;
class Env;
enum Chunk_type {Regular_Page, LockTable, Message, BigPage, IndexChunk, FilterChunk, FlushBuffer, DataChunk};
static const char * EnumStrings[] = { "Internal_and_Leaf", "LockTable", "Message", "Version_edit", "IndexChunk", "FilterChunk", "FlushBuffer", "DataChunk"};

static char config_file_name[100] = "../connection.conf";

struct config_t {
  const char* dev_name;    /* IB device name */
  const char* server_name; /* server host name */
  u_int32_t tcp_port;      /* server TCP port */
  int ib_port; /* local IB port to work with, or physically port number */
  int gid_idx; /* gid index to use */
  int init_local_buffer_size; /*initial local SST buffer size*/
  uint16_t node_id;
//  Cache* cache_prt;
};

/* structure to exchange data which is needed to connect the QPs */
struct Registered_qp_config {
  uint32_t qp_num; /* QP number */
  uint16_t lid;    /* LID of the IB port */
  uint8_t gid[16]; /* gid */
  uint16_t node_id;
} __attribute__((packed));



struct Registered_qp_config_xcompute {
    uint32_t qp_num[NUM_QP_ACCROSS_COMPUTE]; /* QP numbers */
    uint16_t lid;    /* LID of the IB port */
    uint8_t gid[16]; /* gid */
    uint32_t node_id_pairs; // this node (16 bytes) & target nodeid <<16 (16 bytes)
} __attribute__((packed));

using QP_Map = std::map<uint16_t, ibv_qp*>;
using QP_Info_Map = std::map<uint16_t, Registered_qp_config*>;
using CQ_Map = std::map<uint16_t, ibv_cq*>;
struct install_versionedit {
  bool trival;
  size_t buffer_size;
  size_t version_id;
  uint8_t check_byte;
  int level;
  uint64_t file_number;
  uint16_t node_id;
} __attribute__((packed));
struct sst_unpin {
  uint16_t id;
  size_t buffer_size;
} __attribute__((packed));
struct sst_compaction {
  size_t buffer_size;

} __attribute__((packed));

struct New_Root {
    GlobalAddress new_ptr;
    int level;

} __attribute__((packed));
struct Tuple_info {
    bool log_enabled;
    uint64_t primary_key;
    uint32_t tuple_size;
    size_t table_id;
    uint16_t thread_id;

    size_t access_type;
//    size_t buffer_size;
} __attribute__((packed));
struct Prepare {
    bool log_enabled;
    uint16_t thread_id;
//    size_t buffer_size;
} __attribute__((packed));
struct Commit{
    bool log_enabled;
    uint16_t thread_id;
//    size_t buffer_size;
} __attribute__((packed));
struct Abort{
    bool log_enabled;
    uint16_t thread_id;
//    size_t buffer_size;
} __attribute__((packed));
enum RDMA_Command_Type {
  invalid_command_ = 0,
  create_qp_,
  create_mr_128MB_,
  create_mr_any_,
  near_data_compaction,
  install_version_edit,
  version_unpin_,
  sync_option,
  qp_reset_,
  broadcast_root,
  page_invalidation,
  put_qp_info,
  get_qp_info,
  writer_invalidate_modified,
  reader_invalidate_modified,
  writer_invalidate_shared,
  tuple_read_2pc,
  prepare_2pc,
  commit_2pc,
  abort_2pc,
  heart_beat
};
enum file_type { log_type, others };
struct fs_sync_command {
  int data_size;
  file_type type;
};
struct sst_gc {
  size_t buffer_size;
//  file_type type;
};
struct Invalid_Message{
    GlobalAddress page_addr;
    bool pending_reminder; // 0 for not pending, 1 for pending. if 1, the message can only update the priority of old request or forcing the invalidaiton message process the forgotten inv message.
    uint8_t starvation_level;
};
//struct WUnlock_message{
//    GlobalAddress page_addr;
//};
//TODO (ruihong): add the reply message address to avoid request&response conflict for the same queue pair.
// In other word, the threads will not need to figure out whether this message is a reply or response,
// when receive a message from the main queue pair.
union RDMA_Request_Content {
  size_t mem_size;
  Registered_qp_config qp_config;
  Registered_qp_config_xcompute qp_config_xcompute;
  fs_sync_command fs_sync_cmd;
  install_versionedit ive;
  sst_gc gc;
  sst_compaction sstCompact;
  sst_unpin psu;
  size_t unpinned_version_id;
  New_Root root_broadcast;
  uint32_t target_id_pair;
  Invalid_Message inv_message;
  Tuple_info tuple_info;
  Prepare prepare;
  Commit commit;
  Abort abort;
};
union RDMA_Reply_Content {
  ibv_mr mr;
  Registered_qp_config qp_config;
  Registered_qp_config_xcompute qp_config_xcompute;
  install_versionedit ive;
};
struct RDMA_Request {
  RDMA_Command_Type command;
  RDMA_Request_Content content;
  void* buffer;
  uint32_t rkey;
  void* buffer_large;
  uint32_t rkey_large;
  uint32_t imm_num; // 0 for Compaction threads signal, 1 for Flushing threads signal.
//  Options opt;
} __attribute__((packed));
//todo: split dropped into dropped and dropped_without_reply
enum Page_Forward_Reply_Type: uint8_t  {waiting = 0, processed = 1, dropped = 2, pending = 3, ignored = 4};
struct RDMA_ReplyXCompute {
    Page_Forward_Reply_Type inv_reply_type; // 0 not received, 1 message processed at the scene, 2 the target handle is not found or found invalidated, 3 message was pushed in the handle.
    uint8_t toPC_reply_type; // 0 not received, 1 commit, 2 abort.
    uint16_t to_fill_blank2;
} __attribute__((packed));

struct RDMA_Reply {
//  RDMA_Command_Type command;
  RDMA_Reply_Content content;
  void* buffer;
  uint32_t rkey;
  void* buffer_large;
  uint32_t rkey_large;
  volatile bool received;
} __attribute__((packed));
// Structure for the file handle in RDMA file system. it could be a link list
// for large files
struct SST_Metadata {
  std::shared_mutex file_lock;
  std::string fname;
  ibv_mr* mr;
  ibv_mr* map_pointer;
  SST_Metadata* last_ptr = nullptr;
  SST_Metadata* next_ptr = nullptr;
  unsigned int file_size = 0;
};
//TODO: The client ip and shard_target_node_id can just keep one.
struct Arg_for_handler{
  RDMA_Request* request;
  std::string client_ip;
  uint16_t target_node_id;
};
template <typename T>
struct atomwrapper {
  std::atomic<T> _a;

  atomwrapper() : _a() {}

  atomwrapper(const std::atomic<T>& a) : _a(a.load()) {}

  atomwrapper(const atomwrapper& other) : _a(other._a.load()) {}

  atomwrapper& operator=(const atomwrapper& other) {
    _a.store(other._a.load());
  }
};
#define ALLOCATOR_SHARD_NUM 16
class In_Use_Array {
 public:
  In_Use_Array(size_t size, size_t chunk_size, ibv_mr* mr_ori)
      : element_size_(size), chunk_size_(chunk_size), mr_ori_(mr_ori) {
    for (size_t i = 0; i < element_size_; ++i) {
      free_list.push_back(i);
    }
  }
  In_Use_Array(size_t size, size_t chunk_size, ibv_mr* mr_ori,
               std::atomic<bool>* in_use)
      : element_size_(size),
        chunk_size_(chunk_size),
//        in_use_(in_use),
        mr_ori_(mr_ori) {}
  int allocate_memory_slot() {
      // Below is a shortcut for memory allocation.
      if (Array_used_up.load()){
          return -1;
      }
    //maybe the conflict comes from here
    std::unique_lock<SpinMutex> lck(mtx);
    if (free_list.empty()){
        Array_used_up.store(true);
        return -1;  // Not find the empty memory chunk.
    }else{
      int result = free_list.back();
      free_list.pop_back();
      return result;
    }
  }
  bool deallocate_memory_slot(size_t index) {
    std::unique_lock<SpinMutex> lck(mtx);
    free_list.push_back(index);
    if (index < element_size_){
        if (Array_used_up == true){
            Array_used_up.store(false);
        }
      return true;
    }else{
      assert(false);
      return false;
    }
  }
  size_t get_chunk_size() { return chunk_size_; }
  ibv_mr* get_mr_ori() { return mr_ori_; }
  size_t get_element_size() { return element_size_; }
//  std::atomic<bool>* get_inuse_table() { return in_use_; }
  //  void deserialization(char*& temp, int& size){
  //
  //
  //  }
 private:

  size_t element_size_;
  size_t chunk_size_;
//  uint32_t shard_number = 16;
//  std::list<int> free_list[ALLOCATOR_SHARD_NUM];
  std::list<int> free_list;
  SpinMutex mtx;
  ibv_mr* mr_ori_;
  std::atomic<bool> Array_used_up = false;
};
/* structure of system resources */
struct resources {
  union ibv_gid my_gid;
  struct ibv_device_attr device_attr;
  /* Device attributes */
  struct ibv_sge* sge = nullptr;
  struct ibv_recv_wr* rr = nullptr;
  struct ibv_port_attr port_attr; /* IB port attributes */
  //  std::vector<registered_qp_config> remote_mem_regions; /* memory buffers for RDMA */
  struct ibv_context* ib_ctx = nullptr;  /* device handle */
  struct ibv_pd* pd = nullptr;           /* PD handle */
 // TODO: we can have mulitple cq_map and qp_maps to broaden the RPC bandwidth.
  std::map<uint16_t, std::pair<ibv_cq*, ibv_cq*>> cq_map; /* CQ Map */
  std::map<uint16_t, ibv_qp*> qp_map; /* QP Map */
  std::map<uint16_t, Registered_qp_config*> qp_main_connection_info;
  struct ibv_mr* mr_receive = nullptr;   /* MR handle for receive_buf */
  struct ibv_mr* mr_send = nullptr;      /* MR handle for send_buf */
  //  struct ibv_mr* mr_SST = nullptr;                        /* MR handle for SST_buf */ struct ibv_mr* mr_remote;                     /* remote MR handle for computing node */
  char* SST_buf = nullptr;     /* SSTable buffer pools pointer, it could contain
                                  multiple SSTbuffers */
  char* send_buf = nullptr;    /* SEND buffer pools pointer, it could contain
                                  multiple SEND buffers */
  char* receive_buf = nullptr; /* receive buffer pool pointer,  it could contain
                                  multiple acturall receive buffers */

  //TODO: change it
  std::map<uint16_t, int> sock_map; /* TCP socket file descriptor */
  std::map<std::string, ibv_mr*> mr_receive_map;
  std::map<std::string, ibv_mr*> mr_send_map;
};
struct IBV_Deleter {
  // Called by unique_ptr to destroy/free the Resource
  void operator()(ibv_mr* r) {
    if (r) {
      void* pointer = r->addr;
      ibv_dereg_mr(r);
      free(pointer);
    }
  }
};
static void spin_wait_ns(int64_t time){
    TimeMeasurer timer;
    timer.StartTimer();
    timer.EndTimer();
    while(timer.GetElapsedNanoSeconds() < time){
        timer.EndTimer();

        asm volatile("pause\n": : :"memory");
    }
}
static void spin_wait_us(int64_t time){
    TimeMeasurer* timer = new TimeMeasurer();
    timer->StartTimer();
    timer->EndTimer();
    while(timer->GetElapsedMicroSeconds() < time){
        timer->EndTimer();

        asm volatile("pause\n": : :"memory");
    }
    delete timer;
}
class Cache;
class Cache_Handle;
class Memory_Node_Keeper;
class RDMA_Manager {

 public:
    class Async_Tasks {
        public:
        void* handles[ATOMIC_OUTSTANDING_SIZE] = {nullptr};
        ibv_mr* mrs[ATOMIC_OUTSTANDING_SIZE] = {nullptr};
#if ASYNC_PLAN == 1
        uint32_t counter = 0;
#else
        uint32_t head = 0;
        uint32_t tail = 0;
        size_t max_size = ATOMIC_OUTSTANDING_SIZE;
#ifndef NDEBUG
        uint32_t issued_counter = 0;
        uint32_t signalled_counter = 0;
        std::vector<uint16_t > polled_number;
#endif
        ibv_mr* try_enqueue() {
            if (is_full()) {
                assert(size() == max_size - 1);
                return nullptr;
            }
            head = (head + 1) % max_size;
            return mrs[head];
        }



        void dequeue(size_t num) {
            assert(size() >= num);
            auto old_size = size();
            if (num > 0 && is_empty()) {
                throw std::runtime_error("Buffer is empty");
            }

            tail = (tail + num) % max_size;
            auto new_size = size();
            assert(new_size == old_size - num);
//            return item;
        }

        bool is_empty() {
            return head == tail;
        }
        // current logic can at most have ATOMIC_OUTSTANDING_SIZE - 1 elements in the queue.
        bool is_full() {
            return (head + 1) % max_size == tail;
        }

        size_t size() {
            if (tail <= head) {
                return head - tail;
            }
            return max_size - tail + head;
        }
        ibv_mr* enqueue(RDMA_Manager* rdma_mg, GlobalAddress lock_addr) {
            auto temp_mr = try_enqueue();
            while (!temp_mr) {
                ibv_wc wc[ATOMIC_OUTSTANDING_SIZE] = {};
                std::string qptype = "write_local_flush";
                int result = rdma_mg->try_poll_completions(wc, ATOMIC_OUTSTANDING_SIZE, qptype, true, lock_addr.nodeID);

                assert(result < ATOMIC_OUTSTANDING_SIZE);
                signalled_counter= signalled_counter+result;
                if (result>0){
                    //TODO: ADD thread id into print. need to understand what thread have issued wr more than 1000
                    printf("Node %u thread %d poll %d from completion queue, head is %u tail is %u, signal counter is %u, issued counter is %u \n", rdma_mg->node_id, rdma_mg->get_thread_id(), result, head, tail, signalled_counter, issued_counter);
                    fflush(stdout);
                    polled_number.push_back(result);
                }
                dequeue(result);
                temp_mr = try_enqueue();
            }
            issued_counter++;
//            printf("enqueue %p\n", temp_mr);
//            fflush(stdout);
            return temp_mr;
        }
#endif

        Async_Tasks(){
            auto rdma_mg = RDMA_Manager::Get_Instance();
            for (int i = 0; i < ATOMIC_OUTSTANDING_SIZE; ++i) {
                auto mr = new ibv_mr{};
                rdma_mg->Allocate_Local_RDMA_Slot(*mr, Regular_Page);
                mrs[i] = mr;
            }
        }
        ~Async_Tasks(){
            auto rdma_mg = RDMA_Manager::Get_Instance();
            for (int i = 0; i < ATOMIC_OUTSTANDING_SIZE; ++i) {
                rdma_mg->Deallocate_Local_RDMA_Slot(mrs[i]->addr, Regular_Page);
            }
        }

    };

    class Async_Xcompute_Tasks {
        public:
        uint32_t counter = 0;
//        void* handles[SEND_OUTSTANDING_SIZE_XCOMPUTE] = {nullptr};
        ibv_mr* mrs[SEND_OUTSTANDING_SIZE_XCOMPUTE - 1] = {nullptr};
        Async_Xcompute_Tasks(){
            auto rdma_mg = RDMA_Manager::Get_Instance();
            for (int i = 0; i < SEND_OUTSTANDING_SIZE_XCOMPUTE - 1; ++i) {
                ibv_mr* mr = new ibv_mr{};
                rdma_mg->Allocate_Local_RDMA_Slot(*mr, Regular_Page);
                mrs[i] = mr;
            }
        }
        ~Async_Xcompute_Tasks(){
            auto rdma_mg = RDMA_Manager::Get_Instance();
            for (int i = 0; i < SEND_OUTSTANDING_SIZE_XCOMPUTE - 1; ++i) {
                rdma_mg->Deallocate_Local_RDMA_Slot(mrs[i]->addr, Regular_Page);
            }
        }

    };
  friend class Memory_Node_Keeper;
  friend class DBImpl;
  RDMA_Manager(config_t config, size_t remote_block_size);
  //  RDMA_Manager(config_t config) : rdma_config(config){
  //    res = new resources();
  //    res->sock = -1;
  //  }
  //  RDMA_Manager()=delete;
  ~RDMA_Manager();
  static RDMA_Manager *Get_Instance(config_t* config = nullptr);
  void set_page_cache(Cache* cache){
      page_cache_ = cache;
  }
  /**
   *
   */
  size_t GetMemoryNodeNum();
    size_t GetComputeNodeNum();
    uint64_t GetNextTimestamp();
  /**
   * RDMA set up create all the resources, and create one query pair for RDMA send & Receive.
   */
  void Client_Set_Up_Resources();
//  void Socket_listening()
  void Initialize_threadlocal_map();
  // Set up the socket connection to remote shared memory.
  bool Get_Remote_qp_Info_Then_Connect(uint16_t target_node_id);
  void Cross_Computes_RPC_Threads_Creator(uint16_t target_node_id);
  void cross_compute_message_handling_worker(uint16_t target_node_id, int qp_num, ibv_mr *recv_mr);

    //FUnction for invalidation message handling
    void Writer_Inv_Shared_handler(RDMA_Request *receive_msg_buf, uint8_t target_node_id);
    void Reader_Inv_Modified_handler(RDMA_Request *receive_msg_buf, uint8_t target_node_id);
    void Writer_Inv_Modified_handler(RDMA_Request *receive_msg_buf, uint8_t target_node_id);
    static void Write_Invalidation_Message_Handler(void* thread_args);
    static void Read_Invalidation_Message_Handler(void* thread_args);
    void Tuple_read_2pc_handler(RDMA_Request *receive_msg_buf, uint8_t target_node_id);
    void Prepare_2pc_handler(RDMA_Request *receive_msg_buf, uint8_t target_node_id);
    void Commit_2pc_handler(RDMA_Request *receive_msg_buf, uint8_t target_node_id);
    void Abort_2pc_handler(RDMA_Request *receive_msg_buf, uint8_t target_node_id);
    Page_Forward_Reply_Type
    Writer_Invalidate_Modified_RPC(GlobalAddress global_ptr, ibv_mr *page_buffer, uint16_t target_node_id,
                                   uint8_t &starv_level, uint64_t &retry_cnt);
    Page_Forward_Reply_Type
    Reader_Invalidate_Modified_RPC(GlobalAddress global_ptr, ibv_mr *page_mr, uint16_t target_node_id,
                                   uint8_t &starv_level, uint64_t &retry_cnt);
    bool Writer_Invalidate_Shared_RPC(GlobalAddress g_ptr, uint16_t target_node_id, uint8_t starv_level,
                                      uint8_t pos);
    void Writer_Invalidate_Shared_RPC_Reply(int num_of_poll);
    bool Tuple_Read_2PC_RPC(uint16_t target_node_id, uint64_t primary_key, size_t table_id, size_t tuple_size,
                            char *&tuple_buffer, size_t access_type, bool log_enabled);
    bool Prepare_2PC_RPC(uint16_t target_node_id, bool log_enabled);
    bool Commit_2PC_RPC(uint16_t target_node_id, bool log_enabled);
    bool Abort_2PC_RPC(uint16_t target_node_id, bool log_enabled);

    void Put_qp_info_into_RemoteM(uint16_t target_compute_node_id,
                                  std::array<ibv_cq *, NUM_QP_ACCROSS_COMPUTE * 2> *cq_arr,
                                  std::array<ibv_qp *, NUM_QP_ACCROSS_COMPUTE> *qp_arr);
    Registered_qp_config_xcompute Get_qp_info_from_RemoteM(uint16_t target_compute_node_id);
    //Computes node sync compute sides (block function)
    void sync_with_computes_Cside();
    void sync_with_computes_Mside();
    // For the non-cached page only.
    ibv_mr* Get_local_read_mr();
    ibv_mr* Get_local_send_message_mr();
    ibv_mr* Get_local_receive_message_mr();
    ibv_mr* Get_local_CAS_mr();
  //Computes node sync memory sides (block function)

  void broadcast_to_computes_through_socket();
  ibv_mr* create_index_table();
  ibv_mr* create_lock_table();
    ibv_mr* create_timestamp_oracle();
  // client function to retrieve serialized data.
  //  bool client_retrieve_serialized_data(const std::string& db_name, char*& buff,
  //                                       size_t& buff_size, ibv_mr*& local_data_mr,
  //                                       file_type type);
  //  // client function to save serialized data.
  //  bool client_save_serialized_data(const std::string& db_name, char* buff,
  //                                   size_t buff_size, file_type type,
  //                                   ibv_mr* local_data_mr);
//  void client_message_polling_thread();
  void compute_message_handling_thread(std::string q_id, uint16_t shard_target_node_id);
  void ConnectQPThroughSocket(std::string qp_type, int socket_fd,
                              uint16_t& target_node_id);
  // Local memory register will register RDMA memory in local machine,
  // Both Computing node and share memory will call this function.
  // it also push the new block bit map to the Remote_Leaf_Node_Bitmap

  // Set the type of the memory pool. the mempool can be access by the pool name
  bool Mempool_initialize(Chunk_type pool_name, size_t size,
                          size_t allocated_size);
  //TODO: seperate the local memory registration by different shards. However,
  // now we can only seperate the registration by different compute node.
  //Allocate memory as "size", then slice the whole region into small chunks according to the pool name
  bool Local_Memory_Register(
      char** p2buffpointer, ibv_mr** p2mrpointer, size_t size,
      Chunk_type pool_name);  // register the memory on the local side
  // bulk deallocation preparation.

  // The RPC to bulk deallocation.
//  void Memory_Deallocation_RPC(uint16_t target_node_id);
  //TODO: Make it register not per 1GB, allocate and register the memory all at once.
  ibv_mr * Preregister_Memory(size_t gb_number); //Pre register the memroy do not allocate bit map
  // Remote Memory registering will call RDMA send and receive to the remote memory it also push the new SST bit map to the Remote_Leaf_Node_Bitmap
  bool Remote_Memory_Register(size_t size, uint16_t target_node_id, Chunk_type pool_name);


    bool Send_heart_beat();
    bool Send_heart_beat_xcompute(uint16_t target_memory_node_id);
  int Remote_Memory_Deregister();
  // new query pair creation and connection to remote Memory by RDMA send and receive
  bool Remote_Query_Pair_Connection(std::string& qp_type,
                                    uint16_t target_node_id);  // Only called by client.
    int RDMA_Read(GlobalAddress remote_ptr, ibv_mr *local_mr, size_t msg_size, size_t send_flag, int poll_num,
                  Chunk_type pool_name, std::string qp_type = "default");
  int RDMA_Read(ibv_mr *remote_mr, ibv_mr *local_mr, size_t msg_size, size_t send_flag, int poll_num,
                uint16_t target_node_id, std::string qp_type = "default");
    // TODO: implement this kind of RDMA operation for every primitive.
    int RDMA_Write(GlobalAddress remote_ptr, ibv_mr *local_mr, size_t msg_size, size_t send_flag, int poll_num,
                   Chunk_type pool_name, std::string qp_type = "default");
  int RDMA_Write(ibv_mr *remote_mr, ibv_mr *local_mr, size_t msg_size, size_t send_flag, int poll_num,
                 uint16_t target_node_id, std::string qp_type = "default");
  int RDMA_Write(void* addr, uint32_t rkey, ibv_mr* local_mr, size_t msg_size,
                 std::string qp_type, size_t send_flag, int poll_num,
                 uint16_t target_node_id);
  int RDMA_Write_xcompute(ibv_mr *local_mr, void *addr, uint32_t rkey, size_t msg_size, uint16_t target_node_id,
                          int num_of_qp, bool is_inline, bool async = true);
    int RDMA_Write_Batch(void* addr, uint32_t rkey, ibv_mr* local_mr, size_t msg_size,
                   std::string qp_type, size_t send_flag, int poll_num,
                   uint16_t target_node_id);
  int RDMA_Write_Imme(void* addr, uint32_t rkey, ibv_mr* local_mr,
                      size_t msg_size, std::string qp_type, size_t send_flag,
                      int poll_num, unsigned int imme, uint16_t target_node_id);
  // Return 0 mean success
  int RDMA_CAS(GlobalAddress remote_ptr, ibv_mr *local_mr, uint64_t compare,
               uint64_t swap, size_t send_flag, int poll_num,
           Chunk_type pool_name, std::string qp_type = "default");
  int RDMA_FAA(GlobalAddress remote_ptr, ibv_mr *local_mr, uint64_t add, size_t send_flag, int poll_num,
                 Chunk_type pool_name, std::string qp_type = "default");
  int RDMA_CAS(ibv_mr *remote_mr, ibv_mr *local_mr, uint64_t compare, uint64_t swap, size_t send_flag, int poll_num,
               uint16_t target_node_id, std::string qp_type = "default");
    int RDMA_FAA(ibv_mr *remote_mr, ibv_mr *local_mr, uint64_t add, uint16_t target_node_id, size_t send_flag,
                 int poll_num, std::string qp_type = "default");

    /**
       * | write lock holder id 1 byte | read holder number 1byte| reader holder bitmap 47 bit| starving preventer|
       * @param received_state
       * @return
       */
    uint64_t renew_swap_by_received_state_readlock(uint64_t& received_state);
    uint64_t renew_swap_by_received_state_readunlock(uint64_t& received_state);
    uint64_t renew_swap_by_received_state_readupgrade(uint64_t& received_state);


    bool global_Rlock_and_read_page_with_INVALID(ibv_mr *page_buffer, GlobalAddress page_addr, int page_size, GlobalAddress lock_addr,
                                                 ibv_mr *cas_buffer, int r_times = -1, CoroContext *cxt= nullptr, int coro_id = 0);

#if ACCESS_MODE == 0
    bool global_Rlock_and_read_page_without_INVALID(ibv_mr *page_buffer, GlobalAddress page_addr, int page_size, GlobalAddress lock_addr,
                                                    ibv_mr *cas_buffer, int r_time = 0, CoroContext *cxt= nullptr, int coro_id = 0);
#endif
    bool
    global_RUnlock(GlobalAddress lock_addr, ibv_mr *cas_buffer, bool async = true, Cache_Handle *handle = nullptr);
    //TODO: there is a potential lock upgrade deadlock, how to solve it?
    // potential solution: If not upgrade the lock after sending the message, the node should
    // unlock the read lock and then acquire the write lock by seperated RDMAs.
    // to avoid starvation, the updade RPC should let the reader release the lock with starvation preventer on.

    //Currently, we first invalidate other 's read lock and use CAS to upgrate the lock
    // Then here is the question, what if two node try to upgrade the lock at the same time.
    bool global_Rlock_update(ibv_mr *local_mr, GlobalAddress lock_addr, ibv_mr *cas_buffer);


    static void clear_page_forward_flag(char* page_addr){
        Page_Forward_Reply_Type* p = reinterpret_cast<Page_Forward_Reply_Type*>((char*)page_addr +kLeafPageSize -
                                                                                                  sizeof(Page_Forward_Reply_Type));
        *p = waiting;
    }
    bool global_Wlock_and_read_page_with_INVALID(ibv_mr *page_buffer, GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr,
                                                 ibv_mr *cas_buffer, int r_times = -1, uint8_t *starv_level= nullptr, int coro_id = 0);
    void global_Wlock_with_INVALID(ibv_mr *page_buffer, GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr,
                                                 ibv_mr *cas_buffer, uint64_t tag = 0, CoroContext *cxt= nullptr, int coro_id = 0);
#if ACCESS_MODE == 0
    bool global_Wlock_without_INVALID(ibv_mr *page_buffer, GlobalAddress page_addr, size_t page_size, GlobalAddress lock_addr,
                                      ibv_mr *cas_buffer, int r_time = -1, CoroContext *cxt= nullptr, int coro_id = 0);
    bool global_Wlock_and_read_page_without_INVALID(ibv_mr *page_buffer, GlobalAddress page_addr, int page_size, GlobalAddress lock_addr,
                                                    ibv_mr *cas_buffer, int r_time = -1, CoroContext *cxt= nullptr, int coro_id = 0);
#endif
    // THis function acctually does not flush global lock words, otherwise the RDMA write will interfere with RDMA FAA making the CAS failed always
    bool global_write_page_and_Wunlock(ibv_mr *page_buffer, GlobalAddress page_addr, size_t page_size,
                                       GlobalAddress remote_lock_addr, Cache_Handle *handle = nullptr,
                                       bool async = true);
    bool global_write_page_and_WHandover(ibv_mr *page_buffer, GlobalAddress page_addr, size_t page_size, uint8_t next_holder_id,
                                         GlobalAddress remote_lock_addr, bool async = false, Cache_Handle* handle = nullptr);
    bool global_WHandover(ibv_mr *page_buffer, GlobalAddress page_addr, size_t page_size, uint8_t next_holder_id,
                                         GlobalAddress remote_lock_addr, bool async = true, Cache_Handle* handle = nullptr);
    bool global_write_page_and_WdowntoR(ibv_mr *page_buffer, GlobalAddress page_addr, size_t page_size,
                                        GlobalAddress remote_lock_addr, uint8_t next_holder_id, bool async = true,
                                        Cache_Handle *handle = nullptr);
    void global_write_tuple_and_Wunlock(ibv_mr *page_buffer, GlobalAddress page_addr, int page_size,
                                       GlobalAddress remote_lock_addr, CoroContext *cxt = nullptr, int coro_id = 0, bool async = false);
    void global_unlock_addr(GlobalAddress remote_lock_add, CoroContext *cxt= nullptr, int coro_id = 0, bool async = false);

    void Prepare_WR_CAS(ibv_send_wr &sr, ibv_sge &sge, GlobalAddress remote_ptr, ibv_mr *local_mr, uint64_t compare,
                      uint64_t swap, size_t send_flag, Chunk_type pool_name);
    void Prepare_WR_FAA(ibv_send_wr &sr, ibv_sge &sge, GlobalAddress remote_ptr, ibv_mr *local_mr, uint64_t add,
                        size_t send_flag, Chunk_type pool_name);
  void Prepare_WR_Read(ibv_send_wr &sr, ibv_sge &sge, GlobalAddress remote_ptr, ibv_mr *local_mr, size_t msg_size,
                       size_t send_flag, Chunk_type pool_name);
  void Prepare_WR_Write(ibv_send_wr &sr, ibv_sge &sge, GlobalAddress remote_ptr, ibv_mr *local_mr, size_t msg_size,
                        size_t send_flag, Chunk_type pool_name);
  int Batch_Submit_WRs(ibv_send_wr *sr, int poll_num, uint16_t target_node_id, std::string qp_type = "default");
  // the coder need to figure out whether the queue pair has two seperated queue,
  // if not, only send_cq==true is a valid option.
  // For a thread-local queue pair, the send_cq does not matter.
  int poll_completion(ibv_wc* wc_p, int num_entries, std::string qp_type,
                      bool send_cq, uint16_t target_node_id);
  int poll_completion_xcompute(ibv_wc *wc_p, int num_entries, std::string qp_type, bool send_cq, uint16_t target_node_id,
                             int num_of_cp);
  void BatchGarbageCollection(uint64_t* ptr, size_t size);
  bool Deallocate_Local_RDMA_Slot(ibv_mr* mr, ibv_mr* map_pointer,
                                  Chunk_type buffer_type);
  bool Deallocate_Local_RDMA_Slot(void* p, Chunk_type buff_type);
  //  bool Deallocate_Remote_RDMA_Slot(SST_Metadata* sst_meta);
  bool Deallocate_Remote_RDMA_Slot(void* p, uint16_t target_node_id);
  //TOFIX: There will be memory leak for the remote_mr and mr_input for local/remote memory
  // allocation.
  //TODO: make all the memory allocator thread local and make the thread pinned and never get destroy.
  GlobalAddress Allocate_Remote_RDMA_Slot(Chunk_type pool_name, uint16_t target_node_id);
  void Allocate_Remote_RDMA_Slot(ibv_mr &remote_mr, Chunk_type pool_name, uint16_t target_node_id);
  void Allocate_Local_RDMA_Slot(ibv_mr& mr_input, Chunk_type pool_name);
  size_t Calculate_size_of_pool(Chunk_type pool_name);
  // this function will determine whether the pointer is with in the registered memory
  bool CheckInsideLocalBuff(
      void* p,
      std::_Rb_tree_iterator<std::pair<void* const, In_Use_Array>>& mr_iter,
      std::map<void*, In_Use_Array>* Bitmap);
  bool CheckInsideRemoteBuff(void* p, uint16_t target_node_id);
  void mr_serialization(char*& temp, size_t& size, ibv_mr* mr);
  void mr_deserialization(char*& temp, size_t& size, ibv_mr*& mr);
  int try_poll_completions(ibv_wc* wc_p, int num_entries,
                                       std::string& qp_type, bool send_cq,
                                       uint16_t target_node_id);
    int try_poll_completions_xcompute(ibv_wc *wc_p, int num_entries, bool send_cq, uint16_t target_node_id,
                                      int num_of_cp);
  // Deserialization for linked file is problematic because different file may link to the same SSTdata
  void fs_deserilization(
      char*& buff, size_t& size, std::string& db_name,
      std::unordered_map<std::string, SST_Metadata*>& file_to_sst_meta,
      std::map<void*, In_Use_Array*>& remote_mem_bitmap, ibv_mr* local_mr);
  //  void mem_pool_serialization
  bool poll_reply_buffer(RDMA_Reply* rdma_reply);
  static Page_Forward_Reply_Type poll_reply_buffer(volatile Page_Forward_Reply_Type* reply_buffer);
  static bool poll_reply_buffer(RDMA_ReplyXCompute * rdma_reply);
  void Set_message_handling_func(std::function<void(uint32_t)> &&func);
  void register_message_handling_thread(uint32_t handler_id);
    void join_all_handling_thread();
    int get_thread_id(){
        if (thread_id == 0){
            int thread_num = thread_num_generater.fetch_add(1);
            thread_id = thread_num;
        }
        return thread_id;
    }
  // TODO: Make all the variable more smart pointers.
//#ifndef NDEBUG
    static thread_local int thread_id;
    std::atomic<int > thread_num_generater = 0;
    static thread_local int qp_inc_ticket;
//#endif
  resources* res = nullptr;
    std::map<uint16_t,std::vector<ibv_mr*>*> remote_mem_pool; /* a vector for all the remote memory regions*/
 // TODO: seperate the pool for different shards
  std::vector<ibv_mr*> local_mem_regions; /* a vector for all the local memory regions.*/
  ibv_mr* preregistered_region;
  std::list<ibv_mr*> pre_allocated_pool;
  bool pre_allocated_flag = false;
//  std::map<void*, In_Use_Array*>* Remote_Leaf_Node_Bitmap;
//TODO: seperate the remote registered memory as different chunk types. similar to name_to_mem_pool
  std::map<uint16_t, std::map<void*, In_Use_Array*>*> Remote_Leaf_Node_Bitmap;
    std::map<uint16_t, std::map<void*, In_Use_Array*>*> Remote_Inner_Node_Bitmap;
    std::map<uint16_t, ibv_mr*> mr_map_data;
    std::map<uint16_t, uint32_t> rkey_map_data;
    std::map<uint16_t, uint64_t> base_addr_map_data;

    std::map<uint16_t, ibv_mr*> mr_map_lock;
    std::map<uint16_t, uint32_t> rkey_map_lock;
    std::map<uint16_t, uint64_t> base_addr_map_lock;
//    std::map<uint16_t, uint32_t> rkey_map_lock_area_size;
  size_t total_registered_size;

  uint64_t CachelineSize;
  std::shared_mutex remote_mem_mutex;

  std::shared_mutex rw_mutex;
//  std::shared_mutex main_qp_mutex;
  std::shared_mutex qp_cq_map_mutex;
  //  ThreadLocalPtr* t_local_1;
  //TODO: clean up the thread local queue pair,the btree may not need so much thread local queue pair

  std::map<uint16_t, ThreadLocalPtr*> local_write_flush_qp_info;
  std::map<uint16_t, ThreadLocalPtr*> qp_local_write_compact;
  std::map<uint16_t, ThreadLocalPtr*> cq_local_write_compact;
  std::map<uint16_t, ThreadLocalPtr*> local_write_compact_qp_info;
    std::map<uint16_t, std::array<ibv_qp*, NUM_QP_ACCROSS_COMPUTE>*> qp_xcompute;
    std::map<uint16_t, std::array<std::atomic<uint16_t>, NUM_QP_ACCROSS_COMPUTE*2>*> qp_xcompute_os_c;
    std::map<uint16_t,std::array<Async_Xcompute_Tasks, NUM_QP_ACCROSS_COMPUTE>*> qp_xcompute_asyncT;
    std::map<uint16_t, std::array<SpinMutex, NUM_QP_ACCROSS_COMPUTE>*> qp_xcompute_mtx;
    std::map<uint16_t, std::array<ibv_cq*, NUM_QP_ACCROSS_COMPUTE*2>*> cq_xcompute;
//    std::map<uint16_t, Registered_qp_config_xcompute*> qp_xcompute_info;
  std::map<uint16_t, ThreadLocalPtr*> qp_data_default;
  std::map<uint16_t, ThreadLocalPtr*> cq_data_default;
    std::map<uint16_t, ThreadLocalPtr*> qp_local_write_flush;
    std::map<uint16_t, ThreadLocalPtr*> cq_local_write_flush;
  std::map<uint16_t, ThreadLocalPtr*> local_read_qp_info;
    std::map<uint16_t, ThreadLocalPtr*> async_tasks;
    ThreadLocalPtr* read_buffer;
  ThreadLocalPtr* send_message_buffer;
  ThreadLocalPtr* receive_message_buffer;
  ThreadLocalPtr* CAS_buffer;
//  ThreadPool Invalidation_bg_threads;
    std::vector<std::thread> Invalidation_bg_threads;
    std::mutex invalidate_channel_mtx;
    std::atomic<int> sync_invalidation_qp_info_put = 0;
//    std::atomic<int> invalidation_threads_start_sync = 0;
  // TODO: replace the std::map<void*, In_Use_Array*> as a thread local vector of In_Use_Array*, so that
  // the conflict can be minimized.
  std::unordered_map<Chunk_type, std::map<void*, In_Use_Array*>>
      name_to_mem_pool;
  std::unordered_map<Chunk_type, size_t> name_to_chunksize;
  std::unordered_map<Chunk_type, size_t> name_to_allocated_size;
  std::shared_mutex local_mem_mutex;
  //Compute node is even, memory node is odd.
  static uint16_t node_id;
  //TODO: remove the page cache from RDMA manager. make the communicaiton thread exectute a function variable
  // which contains a pointer to the page_cache
  DSMEngine::Cache* page_cache_;
  std::unordered_map<uint16_t, ibv_mr*> comm_thread_recv_mrs;
  std::unordered_map<uint16_t , int> comm_thread_buffer;
//  std::map<uint16_t, uint64_t*> deallocation_buffers;
//  std::map<uint16_t, std::mutex*> dealloc_mtx;
//  std::map<uint16_t, std::condition_variable*> dealloc_cv;

  std::atomic<uint64_t> main_comm_thread_ready_num = 0;
//  uint64_t deallocation_buffers[REMOTE_DEALLOC_BUFF_SIZE / sizeof(uint64_t)];
//  std::map<uint16_t, ibv_mr*>  dealloc_mr;
  std::map<uint16_t, size_t>  top;

  // The variables for immutable notification RPC.
  std::map<uint16_t, std::mutex*> mtx_imme_map;
  std::map<uint16_t, std::atomic<uint32_t>*> imm_gen_map;
  std::map<uint16_t, uint32_t*> imme_data_map;
  std::map<uint16_t, uint32_t*> byte_len_map;
  std::map<uint16_t, std::condition_variable* > cv_imme_map;

  std::map<uint16_t, std::string> compute_nodes{};
  std::map<uint16_t, std::string> memory_nodes{};
  std::atomic<uint64_t> memory_connection_counter = 0;// Reuse by both compute nodes and memory nodes
    std::atomic<uint64_t> compute_connection_counter = 0;
  std::map<std::string, std::pair<ibv_cq*, ibv_cq*>> cq_map_Mside; /* CQ Map */
  std::map<std::string, ibv_qp*> qp_map_Mside; /* QP Map */
  std::map<std::string, Registered_qp_config*> qp_main_connection_info_Mside;
  // This global index table is in the node 0;
  std::mutex global_resources_mtx;
  ibv_mr* global_index_table = nullptr;
  ibv_mr* global_lock_table = nullptr;
  ibv_mr* timestamp_oracle = nullptr;
  Env* env_;
  std::function<void(uint32_t)> message_handling_func;
    std::shared_mutex user_df_map_mutex;
    std::atomic<bool> handler_is_finish = false;
    //TODO: clear those allocated resources when RDMA manager is being destroyed.
  std::vector<std::thread> user_defined_functions_handler;
  // TODO: Make the communicaiton buffer a queue rather than a single buffer.
//  std::map<uint32_t, RDMA_Request*> communication_queues;
  std::map<uint32_t, std::queue<RDMA_Request>> communication_queues;
  std::map<uint32_t, std::mutex*> communication_mtxs;
  std::map<uint32_t, std::condition_variable*> communication_cvs;
#ifdef PROCESSANALYSIS
  static std::atomic<uint64_t> RDMAReadTimeElapseSum;
  static std::atomic<uint64_t> ReadCount;

#endif
//#ifdef GETANALYSIS
//  static std::atomic<uint64_t> RDMAFindmrElapseSum;
//  static std::atomic<uint64_t> RDMAMemoryAllocElapseSum;
//  static std::atomic<uint64_t> ReadCount1;
//#endif
  //  std::unordered_map<std::string, ibv_mr*> fs_image;
  //  std::unordered_map<std::string, ibv_mr*> log_image;
  //  std::unique_ptr<ibv_mr, IBV_Deleter> log_image_mr;
  //  std::shared_mutex log_image_mutex;
  //  std::shared_mutex fs_image_mutex;
  // use thread local qp and cq instead of map, this could be lock free.
  //  static __thread std::string thread_id;
  template <typename T>
  int post_send(ibv_mr* mr, uint16_t target_node_id, std::string qp_type = "main") {
    struct ibv_send_wr sr;
    struct ibv_sge sge;
    struct ibv_send_wr* bad_wr = NULL;
    int rc;
    //  if (!rdma_config.server_name) {
    // server side.
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)mr->addr;
    sge.length = sizeof(T);
    sge.lkey = mr->lkey;
    //  }
    //  else {
    //    //client side
    //    /* prepare the scatter/gather entry */
    //    memset(&sge, 0, sizeof(sge));
    //    sge.addr = (uintptr_t)res->send_buf;
    //    sge.length = sizeof(T);
    //    sge.lkey = res->mr_send->lkey;
    //  }

    /* prepare the send work request */
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = 0;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.opcode = static_cast<ibv_wr_opcode>(IBV_WR_SEND);
    sr.send_flags = IBV_SEND_SIGNALED;

    /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
    //*(start) = std::chrono::steady_clock::now();
    // start = std::chrono::steady_clock::now();
//
//    if (rdma_config.server_name)
//      rc = ibv_post_send(res->qp_map["main"], &sr, &bad_wr);
//    else
//      rc = ibv_post_send(res->qp_map[qp_id], &sr, &bad_wr);
    ibv_qp* qp;
    if (qp_type == "default"){
      //    assert(false);// Never comes to here
      qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
      if (qp == NULL) {
        Remote_Query_Pair_Connection(qp_type,target_node_id);
        qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
      }
      rc = ibv_post_send(qp, &sr, &bad_wr);
    }else if (qp_type == "write_local_flush"){
      qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
      if (qp == NULL) {
        Remote_Query_Pair_Connection(qp_type,target_node_id);
        qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
      }
      rc = ibv_post_send(qp, &sr, &bad_wr);

    }else if (qp_type == "write_local_compact"){
      qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
      if (qp == NULL) {
        Remote_Query_Pair_Connection(qp_type,target_node_id);
        qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
      }
      rc = ibv_post_send(qp, &sr, &bad_wr);
    } else {
      std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
      rc = ibv_post_send(res->qp_map.at(target_node_id), &sr, &bad_wr);
      l.unlock();
    }
//    if (rc)
//      fprintf(stderr, "failed to post SR\n");
//    else {
//      fprintf(stdout, "Send Request was posted\n");
//    }
    return rc;
  }

  // three variables below are from rdma file system.
  //  std::string* db_name_;
  //  std::unordered_map<std::string, SST_Metadata*>* file_to_sst_meta_;
  //  std::shared_mutex* fs_mutex_;


 private:
  config_t rdma_config;
  int client_sock_connect(const char* servername, int port);

  int sock_sync_data(int sock, int xfer_size, char* local_data,
                     char* remote_data);

  int post_send(ibv_mr* mr, std::string qp_type, size_t size, uint16_t target_node_id);
  //  int post_receives(int len);

  int post_receive(ibv_mr* mr, std::string qp_type, size_t size,
                   uint16_t target_node_id);

  int resources_create();
  int modify_qp_to_reset(ibv_qp* qp);
  //todo: make the qp configuration configurable, different qp (x COMPUTE, to memory) should have differnt config
  int modify_qp_to_init(struct ibv_qp* qp);
  int modify_qp_to_rtr(struct ibv_qp* qp, uint32_t remote_qpn, uint16_t dlid,
                       uint8_t *dgid);
  int modify_qp_to_rts(struct ibv_qp* qp);
  ibv_qp *create_qp(uint16_t target_node_id, bool seperated_cq, std::string &qp_type, uint32_t send_outstanding_num,
                    uint32_t recv_outstanding_num);
  void create_qp_xcompute(uint16_t target_node_id, std::array<ibv_cq *, NUM_QP_ACCROSS_COMPUTE * 2> *cq_arr,
                          std::array<ibv_qp *, NUM_QP_ACCROSS_COMPUTE> *qp_arr);
  ibv_qp* create_qp_Mside(bool seperated_cq, std::string& qp_id);
  //q_id is for the remote qp informantion fetching
  int connect_qp(ibv_qp* qp, std::string& qp_type, uint16_t target_node_id);
  int connect_qp_Mside(ibv_qp* qp, std::string& q_id);
  int connect_qp(ibv_qp* qp, Registered_qp_config* remote_con_data);
  int connect_qp_xcompute(std::array<ibv_qp *, NUM_QP_ACCROSS_COMPUTE> *qp_arr, Registered_qp_config_xcompute* remote_con_data);
  int resources_destroy();
  void print_config(void);
  void usage(const char* argv0);

  int post_receive(ibv_mr** mr_list, size_t sge_size, std::string qp_type,
                   uint16_t target_node_id);
    int post_receive_xcompute(ibv_mr *mr, uint16_t target_node_id, int num_of_qp);
    int post_send_xcompute(ibv_mr *mr, uint16_t target_node_id, int num_of_qp, size_t msg_size);

        int post_send(ibv_mr** mr_list, size_t sge_size, std::string qp_type,
                uint16_t target_node_id);
  template <typename T>
  int post_receive(ibv_mr* mr, uint16_t target_node_id,
                   std::string qp_type = "main") {
    struct ibv_recv_wr rr;
    struct ibv_sge sge;
    struct ibv_recv_wr* bad_wr;
    int rc;
    //  if (!rdma_config.server_name) {
    //    /* prepare the scatter/gather entry */

    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)mr->addr;
    sge.length = sizeof(T);
    sge.lkey = mr->lkey;

    //  }
    //  else {
    //    /* prepare the scatter/gather entry */
    //    memset(&sge, 0, sizeof(sge));
    //    sge.addr = (uintptr_t)res->receive_buf;
    //    sge.length = sizeof(T);
    //    sge.lkey = res->mr_receive->lkey;
    //  }

    /* prepare the receive work request */
    memset(&rr, 0, sizeof(rr));
    rr.next = NULL;
    rr.wr_id = 0;
    rr.sg_list = &sge;
    rr.num_sge = 1;
    /* post the Receive Request to the RQ */
//    if (rdma_config.server_name)
//      rc = ibv_post_recv(res->qp_map["main"], &rr, &bad_wr);
//    else
//      rc = ibv_post_recv(res->qp_map[qp_id], &rr, &bad_wr);
    ibv_qp* qp;
    if (qp_type == "read_local"){
      //    assert(false);// Never comes to here
      qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
      if (qp == NULL) {
        Remote_Query_Pair_Connection(qp_type,target_node_id);
        qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
      }
      rc = ibv_post_recv(qp, &rr, &bad_wr);
    }else if (qp_type == "Xcompute"){
      qp = static_cast<ibv_qp*>((*qp_xcompute.at(target_node_id))[0]);
      if (qp == NULL) {
        Remote_Query_Pair_Connection(qp_type,target_node_id);
        qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
      }
      rc = ibv_post_recv(qp, &rr, &bad_wr);

    }else if (qp_type == "write_local_compact"){
      qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
      if (qp == NULL) {
        Remote_Query_Pair_Connection(qp_type,target_node_id);
        qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
      }
      rc = ibv_post_recv(qp, &rr, &bad_wr);
    } else {
      std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
      rc = ibv_post_recv(res->qp_map.at(target_node_id), &rr, &bad_wr);
      l.unlock();
    }
//    if (rc)
//#ifndef NDEBUG
//      fprintf(stderr, "failed to post RR\n");
//#endif
//    else
//#ifndef NDEBUG
//      fprintf(stdout, "Receive Request was posted\n");
//#endif
    return rc;
  }  // For a non-thread-local queue pair, send_cq==true poll the cq of send queue, send_cq==false poll the cq of receive queue
};



}
#endif