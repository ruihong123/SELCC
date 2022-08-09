//
// Created by wang4996 on 22-8-7.
//

#ifndef MEMORYENGINE_LOCKS_H
#define MEMORYENGINE_LOCKS_H

#include <mutex>
#include <atomic>
#include <shared_mutex>

struct LocalLockNode {
    std::atomic<uint64_t> ticket_lock;
    bool hand_over;
    uint8_t hand_time;
    uint8_t hand_over_number;
    //Acquiring the shared lock does not require this node to hold the global lock.
    std::shared_mutex mtx;
};

class locks {

};


#endif //MEMORYENGINE_LOCKS_H
