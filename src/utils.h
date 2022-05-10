#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>
#include <numeric>
#include <grpc++/grpc++.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>
#include <random>
#include <arpa/inet.h>
#include <fcntl.h>
#include <ctime>
#include <memory>
#include <queue>
#include <sstream>
#include <signal.h>
#include <unordered_set>
#include <unordered_map>

#include "constants.h"

using namespace std;

int msleep(long msec) {
    struct timespec ts;
    int res;
    if (msec < 0) {
        errno = EINVAL;
        return -1;
    }

    ts.tv_sec = msec / 1000;
    ts.tv_nsec = (msec % 1000) * 1000000;

    do {
        res = nanosleep(&ts, &ts);
    } while (res && errno == EINTR);

    return res;
}

string getConsistencyString(Consistency consistency){
    switch(consistency) {
        case Consistency::strong            : return "strong";
        case Consistency::eventual          : return "eventual";
        case Consistency::fast_acknowledge  : return "fast_acknowledge";
        case Consistency::baseline          : return "baseline";
        default                             : return "strong";
    }
}

bool isRoleValid(const string &role) {
    return role == "primary" || role == "backup";
}

string parseArgument(const string & argumentString, const string & option) {
    string value;

    size_t pos = argumentString.find(option);
    if (pos != string::npos) {
        pos += option.length();
        size_t endPos = argumentString.find(' ', pos);
        value = argumentString.substr(pos, endPos - pos);
    }

    cout << __func__ << " : Parsed = " << value << endl;

    return value;
}


bool isIPValid(const string & address) {
    size_t ipEndPos = address.find(":");
    string ipAddress = address.substr(0, ipEndPos);
    struct sockaddr_in sa;
    int result = (ipAddress == "localhost") ? 1 : inet_pton(AF_INET, ipAddress.c_str(), &(sa.sin_addr));
    if (result == 0) {
        return false;
    }
    string portAddress = address.substr(ipEndPos + 1);
    int port = stoi(portAddress);
    return (port > 0 && port <= 65535);
}

string generateClientIdentifier(){
    int identifierLength = 32;

    const char alphanum[] =
                                "0123456789"
                                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                "abcdefghijklmnopqrstuvwxyz";
    string clientIdentifierString;
    clientIdentifierString.reserve(identifierLength);

    std::random_device dev;
    std::mt19937 rng(dev());
    std::uniform_int_distribution<std::mt19937::result_type> dist6(0,identifierLength - 1); // distribution in range [1, 6]

    for (int i = 0; i < identifierLength; ++i) {
        clientIdentifierString += alphanum[dist6(rng)];
    }
    return clientIdentifierString;
}

inline void get_time(struct timespec* ts) {
    clock_gettime(CLOCK_MONOTONIC, ts);
}

inline double get_time_diff(struct timespec* before, struct timespec* after) {
    double delta_s = after->tv_sec - before->tv_sec;
    double delta_ns = after->tv_nsec - before->tv_nsec;

    return (delta_s + (delta_ns * 1e-9)) * ((double)1e3);
}

struct timespec* max_time(struct timespec *t1, struct timespec *t2) {
    int diff = get_time_diff(t1, t2);
    if (diff >= 0) {
        return t1;
    }
    return t2;
}


string getCurrentWorkingDir() {
    char arg1[20];
    char exepath[PATH_MAX + 1] = {0};

    sprintf(arg1, "/proc/%d/exe", getpid());
    
    int res = readlink(arg1, exepath, 1024);
    std::string s_path(exepath);
    std::size_t lastPos = s_path.find_last_of("/");
    return s_path.substr(0, lastPos);
}


int makeFolder(const string & folderPath) {
    struct stat buffer;

    if (stat(folderPath.c_str(), &buffer) == 0) {
        printf("%s : Folder %s exists.\n", __func__, folderPath.c_str());
    } 
    else {
        int res = mkdir(folderPath.c_str(), 0777);
        if (res == 0) {
            printf("%s : Folder %s created successfully!\n", __func__,
                   folderPath.c_str());
        } 
        else {
            printf("%s : Failed to create folder %s!\n", __func__,
                   folderPath.c_str());
            return -1;
        }
    }

    return 0;
}