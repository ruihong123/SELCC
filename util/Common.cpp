#include "Common.h"
#include "Btr.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <net/if.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <thread>

void bindCore(uint16_t core) {

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);
    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
        printf("can't bind core!");
    }
#ifndef NDEBUG
    std::stringstream ss;
    ss << std::this_thread::get_id();
    DSMEngine::Btr::thread_id = std::stoull(ss.str());
#endif
}

char *getIP() {
    struct ifreq ifr;
    int fd = socket(AF_INET, SOCK_DGRAM, 0);

    ifr.ifr_addr.sa_family = AF_INET;
    strncpy(ifr.ifr_name, "ib0", IFNAMSIZ - 1);

    ioctl(fd, SIOCGIFADDR, &ifr);
    close(fd);

    return inet_ntoa(((struct sockaddr_in*)&ifr.ifr_addr)->sin_addr);
}

char *getMac() {
    static struct ifreq ifr;
    int fd = socket(AF_INET, SOCK_DGRAM, 0);

    ifr.ifr_addr.sa_family = AF_INET;
    strncpy(ifr.ifr_name, "ens2", IFNAMSIZ - 1);

    ioctl(fd, SIOCGIFHWADDR, &ifr);
    close(fd);

    return (char *)ifr.ifr_hwaddr.sa_data;
}

