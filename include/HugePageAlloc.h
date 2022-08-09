#ifndef __HUGEPAGEALLOC_H__
#define __HUGEPAGEALLOC_H__


#include <cstdint>
#include "iostream"
#include <sys/mman.h>
#include <memory.h>

bool is_mmap_work = true;

inline void *hugePageAlloc(size_t size) {
    /**
     * mmap will actually go ahead and reserve the pages from the kernel's internal hugetlbfs mount, whose status can be
     * seen under /sys/kernel/mm/hugepages. The pages in question need to be available by the time mmap is invoked
     * (see HugePages_Free in /proc/meminfo), or mmap will fail. (https://stackoverflow.com/questions/30470972/using-mmap-and-madvise-for-huge-pages)
     */
    void *res = nullptr;
    if (is_mmap_work){
        res = mmap(NULL, size, PROT_READ | PROT_WRITE,
                         MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    }

//    void *res = malloc(size);
    if (res == MAP_FAILED) {
        printf("%s mmap failed!\n");
        is_mmap_work = false;
    }

    return res;
}

inline void hugePageDealloc(void* ptr, size_t size) {
    /**
     * mmap will actually go ahead and reserve the pages from the kernel's internal hugetlbfs mount, whose status can be
     * seen under /sys/kernel/mm/hugepages. The pages in question need to be available by the time mmap is invoked
     * (see HugePages_Free in /proc/meminfo), or mmap will fail. (https://stackoverflow.com/questions/30470972/using-mmap-and-madvise-for-huge-pages)
     */
    if (is_mmap_work){
        int ret = munmap(ptr, size);
    }else{
        printf("mmap is not enabled from the beginning\n");
    }

}

#endif /* __HUGEPAGEALLOC_H__ */
