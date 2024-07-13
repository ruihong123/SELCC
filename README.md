# SELCC: Coherent Caching over Compute-Limited Disaggregated Memory
This repo is the implementation of SELCC; a Shared-Exclusive Latch Cache Coherence protocol that maintains cache coherence without imposing any computational burden on the remote memory side. SELCC builds on a one-sided shared-exclusive latch protocol by introducing lazy latch release and invalidation messages among the compute nodes so that it can guarantee both data access atomicity and cache coherence. SELCC minimizes communication round-trips by embedding the current cache copy holder IDs into RDMA latch words and prioritizes local concurrency control over global concurrency control. We instantiate the SELCC protocol onto compute-sided cache, forming an abstraction layer over disaggregated memory. This abstraction layer provides main-memory-like APIs to upper-level applications,  and thus enabling existing data structures and algorithms to function over disaggregated memory with minimal code change. To demonstrate the usability of SELCC, we implement a B-tree and three transaction concurrency control algorithms over SELCC's APIs. Micro-benchmark results show that the SELCC protocol achieves better performance compared to RPC-based cache-coherence protocols. Additionally, YCSB and TPC-C benchmarks indicate that applications over SELCC can achieve comparable or superior performance against competitors over disaggregated memory.
## Usage:

## Getting the source:

## Building:

### Build for POSIX
### How to run

## Performance

### Setup
### Microbench
### Index over SELCC

### Tranasction engine over SELCC

# Repository content:

