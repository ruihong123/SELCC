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
        uint32_t current_record_size_;
        GlobalAddress prev_delta_gaddr;
        uint64_t prev_delta_wts_;
        uint32_t prev_record_size_;
//    size_t* column_ids_;
//    char** payloads_;
        char data_[1];

        DeltaRecord() {
            Wts_ = 0;
            prev_delta_gaddr = GlobalAddress::Null();
            current_record_size_ = 0;
        }

        DeltaRecord(uint64_t wts, GlobalAddress prev_delta, uint32_t prev_delta_wts, uint32_t prev_record_size, uint32_t d_record_size) {
            Wts_ = wts;
            prev_delta_gaddr = prev_delta;
            prev_delta_wts_ = prev_delta_wts;
            current_record_size_ = d_record_size;
            prev_record_size_ = prev_record_size;
        }
        void initialize(uint64_t wts, GlobalAddress prev_delta, uint32_t prev_delta_wts, uint32_t prev_record_size, uint32_t d_record_size) {
            Wts_ = wts;
            prev_delta_gaddr = prev_delta;
            prev_delta_wts_ = prev_delta_wts;
            current_record_size_ = d_record_size;
            prev_record_size_ = prev_record_size;
        }

    };
}


#endif //SELCC_DELTARECORD_H
