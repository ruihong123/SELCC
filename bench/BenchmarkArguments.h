// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_BENCHMARK_ARGUMENTS_H__
#define __DATABASE_BENCHMARK_ARGUMENTS_H__

#include <iostream>
#include <cassert>
#include "Meta.h"
#include "rdma.h"
#include "TpccSource.h"

namespace DSMEngine {
    static int app_type = -1;
    static double scale_factors[2] = { -1, -1 };
    static int factor_count = 0;
    static int dist_ratio = 1;
    static int num_txn = -1;
    static int num_core = -1;  // number of cores utilized in a single numa node.

    static size_t cache_size = 8 * 1024LLU * 1024LLU* 1024LLU;
    static std::string my_host_name;
    static unsigned int conn_port = -1;
    static std::string config_filename = "../connection.conf";
// To modify tpcc workload
    static size_t gReadRatio = 0;
    static size_t gTimeLocality = 0;
    static bool gForceRandomAccess = false; // fixed
    static bool gStandard = true;  // true if follow standard specification

    static int FREQUENCY_DELIVERY = 20;  //0 0
    static int FREQUENCY_PAYMENT = 20; // 43
    static int FREQUENCY_NEW_ORDER = 20; // 45
    static int FREQUENCY_ORDER_STATUS = 20;  //0
    static int FREQUENCY_STOCK_LEVEL = 20;  //0
    static void PrintUsage() {
        std::cout << "==========[USAGE]==========" << std::endl;
        std::cout << "\t-pINT: PORT(required)" << std::endl;
        std::cout << "\t-cINT: CORE_COUNT(required)" << std::endl;
        std::cout << "\t-sfINT: SCALE_FACTOR(required)" << std::endl;
        std::cout << "\t-sfDOUBLE: SCALE_FACTOR(required)" << std::endl;
        std::cout << "\t-tINT: TXN_COUNT(required)" << std::endl;
        std::cout << "\t-dINT: DIST_TXN_RATIO(optional,default=1)" << std::endl;
        //std::cout << "\t-zINT: BATCH_SIZE(optional)" << std::endl;
        std::cout << "\t-fSTRING: CONFIG_FILENAME(optional,default=config.txt)" << std::endl;
        std::cout << "\t-rINT: READ_RATIO(optional, [0,100])" << std::endl;
        std::cout << "\t-lINT: TIME_LOCALITY(optional, [0,100])" << std::endl;
        std::cout << "===========================" << std::endl;
        std::cout << "==========[EXAMPLES]==========" << std::endl;
        std::cout << "Benchmark -p11111 -c4 -sf10 -sf100 -t100000" << std::endl;
        std::cout << "==============================" << std::endl;
    }

    static void ArgumentsChecker() {
        if (conn_port == -1) {
            std::cout << "PORT (-p) should be set" << std::endl;
            exit(0);
        }
        if (factor_count == 0) {
            std::cout << "SCALE_FACTOR (-sf) should be set." << std::endl;
            exit(0);
        }
        if (num_core == -1) {
            std::cout << "CORE_COUNT (-c) should be set." << std::endl;
            exit(0);
        }
        if (num_txn == -1) {
            std::cout << "TXN_COUNT (-t) should be set." << std::endl;
            exit(0);
        }
        if (!(dist_ratio >= 0 && dist_ratio <= 100)) {
            std::cout << "DIST_TXN_RATIO should be [0,100]." << std::endl;
            exit(0);
        }
        if (!(gReadRatio >= 0 && gReadRatio <= 100)) {
            std::cout << "READ_RATIO should be [0,100]." << std::endl;
            exit(0);
        }
        if (!(gTimeLocality >= 0 && gTimeLocality <= 100)) {
            std::cout << "TIME_LOCALITY should be [0,100]." << std::endl;
            exit(0);
        }
    }

    static void ArgumentsParser(int argc, char *argv[]) {
        if (argc <= 4) {
            PrintUsage();
            exit(0);
        }
        void* temp_pointer;
        for (int i = 1; i < argc; ++i) {
            if (argv[i][0] != '-') {
                PrintUsage();
                exit(0);
            }
            if (argv[i][1] == 'p') {
                conn_port = atoi(&argv[i][2]);
            } else if (argv[i][1] == 's' && argv[i][2] == 'n') {
                my_host_name.assign(&argv[i][3]);
            } else if (argv[i][1] == 's' && argv[i][2] == 'f') {
                scale_factors[factor_count] = atof(&argv[i][3]);
                ++factor_count;
            } else if (argv[i][1] == 't') {
                num_txn = atoi(&argv[i][2]);
            } else if (argv[i][1] == 'd') {
                dist_ratio = atoi(&argv[i][2]);
            } else if (argv[i][1] == 'c') {
                num_core = atoi(&argv[i][2]);
                gThreadCount = num_core;
            } else if (argv[i][1] == 'f') {
                config_filename = std::string(&argv[i][2]);
            } else if (argv[i][1] == 'z') {
                gParamBatchSize = atoi(&argv[i][2]);
            } else if (argv[i][1] == 'r' && argv[i][2] == 'p' && argv[i][3] == 'a') {
                FREQUENCY_PAYMENT = atoi(&argv[i][4]);
            } else if (argv[i][1] == 'r' && argv[i][2] == 'n' && argv[i][3] == 'e') {
                FREQUENCY_NEW_ORDER = atoi(&argv[i][4]);
            } else if (argv[i][1] == 'r' && argv[i][2] == 'o' && argv[i][3] == 'r') {
                FREQUENCY_ORDER_STATUS = atoi(&argv[i][4]);
            } else if (argv[i][1] == 'r' && argv[i][2] == 's' && argv[i][3] == 't') {
                FREQUENCY_STOCK_LEVEL = atoi(&argv[i][4]);
            } else if (argv[i][1] == 'r' && argv[i][2] == 'd' && argv[i][3] == 'e') {
                FREQUENCY_DELIVERY = atoi(&argv[i][4]);
            } else if (argv[i][1] == 'r') {
                gReadRatio = atoi(&argv[i][2]);
                gStandard = false;
            } else if (argv[i][1] == 'l') {
                gTimeLocality = atoi(&argv[i][2]);
                gStandard = false;
            } else if (argv[i][1] == 'n' && argv[i][2] == 'i' && argv[i][3] == 'd') {
                RDMA_Manager::node_id = atoi(&argv[i][4]);
            } else if (argv[i][1] == 'h') {
                PrintUsage();
                exit(0);

            } else {
                PrintUsage();
                exit(0);
            }
        }
//        assert(FREQUENCY_DELIVERY!=20);
        printf("Frequencies: %d %d %d %d %d\n", FREQUENCY_DELIVERY, FREQUENCY_PAYMENT, FREQUENCY_NEW_ORDER, FREQUENCY_ORDER_STATUS, FREQUENCY_STOCK_LEVEL);
        ArgumentsChecker();
    }
}

#endif
