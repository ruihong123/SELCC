//
// Created by wang4996 on 1/4/25.
//

#ifndef SELCC_DELTARECORD_H
#define SELCC_DELTARECORD_H
#include <infiniband/verbs.h>
#include "Common.h"
namespace DSMEngine {
    class DeltaRecord {
    public:
        uint64_t Wts_;
        GlobalAddress prev_delta_;
        uint32_t prev_record_size_;
        uint32_t current_record_size_;
//    size_t* column_ids_;
//    char** payloads_;
        char data_[1];

        DeltaRecord() {
            Wts_ = 0;
            prev_delta_ = GlobalAddress::Null();
            current_record_size_ = 0;
        }

        DeltaRecord(uint64_t wts, GlobalAddress prev_delta, uint32_t prev_record_size, uint32_t d_record_size) {
            Wts_ = wts;
            prev_delta_ = prev_delta;
            current_record_size_ = d_record_size;
            prev_record_size_ = prev_record_size;
        }

    };
}


#endif //SELCC_DELTARECORD_H
