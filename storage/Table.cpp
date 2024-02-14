//
// Created by ruihong on 2/10/24.
//
#include "Table.h"
namespace DSMEngine {
    thread_local GlobalAddress Table::opened_block_ = GlobalAddress::Null();
}
