# SELCC: Coherent Caching over Compute-Limited Disaggregated Memory
This repo is the implementation of SELCC; a Shared-Exclusive Latch Cache Coherence protocol that maintains cache coherence without imposing any computational burden on the remote memory side. SELCC builds on a one-sided shared-exclusive latch protocol by introducing lazy latch release and invalidation messages among the compute nodes so that it can guarantee both data access atomicity and cache coherence. SELCC minimizes communication round-trips by embedding the current cache copy holder IDs into RDMA latch words and prioritizes local concurrency control over global concurrency control. We instantiate the SELCC protocol onto compute-sided cache, forming an abstraction layer over disaggregated memory. This abstraction layer provides main-memory-like APIs to upper-level applications,  and thus enabling existing data structures and algorithms to function over disaggregated memory with minimal code change. To demonstrate the usability of SELCC, we implement a B-tree and three transaction concurrency control algorithms over SELCC's APIs. Micro-benchmark results show that the SELCC protocol achieves better performance compared to RPC-based cache-coherence protocols. Additionally, YCSB and TPC-C benchmarks indicate that applications over SELCC can achieve comparable or superior performance against competitors over disaggregated memory.
## Usage:
* Establish the RDMA connection by calling **RDMA_Manager::Get_Instance(config_t* config)**
* Create Disaggregated memory abstraction object via class DDSM.
* Allocate global cache line by **DDSM::Allocate_Remote**
* Conduct read and write under the **protection of DDSM::SELCC_Shared_Lock** and **DDSM::SELCC_Exclusive_Lock**

### Build for POSIX
This project supports CMake out of the box.
```bash
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release .. && make all -j
```

### How to run
First, you should config the connection.conf under the main directory. The first line of that file represents the compute nodes' IP addresses, and the second line represents the memory nodes' IP addresses.
* Memory node side: 
```bash
./memory_server PortNum RemoteMemorySize NodeID
```
* Compute node side:
* To utilize SELCC in your code, you need refer to public interface in **include/DSMEngine/\*.h** .
* compile your code with static library libDSMEngine.a
```bash
YourCodeOverSELCC
```
## Performance

### Setup
### Microbench
### Index over SELCC

### Tranasction engine over SELCC

# Repository content:

