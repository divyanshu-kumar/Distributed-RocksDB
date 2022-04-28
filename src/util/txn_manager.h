#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <vector>
#include <fstream>
#include <iterator>
#include <dirent.h>
#include <cstring>
#include <chrono>
#include <atomic>
#include <shared_mutex>

using namespace std;

const std::string LOG_PREFIX = std::string("self.log.");

string computeLogPath(string storage_path, int logIndex) {
    return storage_path + "/" + LOG_PREFIX + to_string(logIndex);
}

int extractLogIndex(char* _fileName) {
    string fileName(_fileName);
    int index = stoi(fileName.substr(LOG_PREFIX.size()));

    return index;
}

int computeLastLogIndex(string storage_path) {
    DIR *dir = opendir(storage_path.c_str());
    struct dirent *dirEnt;
    int last = -1;

    if (dir) {
        while ((dirEnt = readdir(dir)) != NULL) {
            if (strcmp(dirEnt->d_name, ".") == 0 || strcmp(dirEnt->d_name, "..") == 0) {
                continue;
            }

            int index = extractLogIndex(dirEnt->d_name);
            if (index > last) {
                last = index;
            }
        }
        closedir(dir);
    }

    return last;
}

class TxnManager {
    private:
        // for locking put
        mutex outer_mutex;
        mutex put_lock;
        std::shared_mutex flush_mutex;
        unordered_map<string, shared_mutex> mutices;

        atomic<uint32_t> active_txn_count = {0};

        unordered_map<string,string> txns;
        string storage_path;
        int lastLogIndex;

        /**
         * @brief extract log index from the given file name in logs directory
         * 
         * @param _fileName 
         * @return int 
         */
        int extractLogIndex(char* _fileName);

    public:
        /**
         * 
         * @param _storage_path - where logs should be stored. Trailing separator is not allowed
         * 
         */
        TxnManager(string _storage_path);

        /**
         * @brief 
         * 
         * @param key
         * @param value 
         */
        void put(string key, string value);

        /**
         * @brief returns empty string if the key is not found in the txns
         * upto the caller to check by doing .empty() to distinguish between valid/invalid values
         * 
         * @param key 
         * @return string 
         */
        string get(string key);

        /**
         * @brief flushes the current txns into the log file
         * 
         */
        void flush();

        /**
         * @brief Get the Last Index of the last log that was flused
         * @return int 
         */
        int getLastLogIndex();

        /**
         * @brief Get the Txn Keys for the given log file indicated by \param logIndex
         * 
         * @param logIndex 
         * @return vector<string> 
         */
        vector<string> getTxnKeys(int logIndex);

        /**
         * @brief Get the Curr Txn Keys object
         * 
         * @return vector<string> 
         */
        vector<string> getCurrTxnKeys();

        /**
         * @brief Get the Put Lock object
         * 
         * @param key 
         * @return unique_lock<shared_mutex> 
         */
        unique_lock<shared_mutex> getPutLock(string key);

        /**
         * @brief releases the put lock
         * 
         * @param key_mutex 
         */
        void releasePutLock(unique_lock<shared_mutex>& key_mutex);

        /**
         * @brief acquires the flush lock for writes 
         */
        void acquireFlushLockForWrite() { flush_mutex.lock_shared(); }

        /**
         * @brief releases the flush lock for writes 
         */
        void releaseFlushLockForWrite() { flush_mutex.unlock_shared(); }

        /**
         * @brief acquires the flush lock for flush 
         */
        void acquireFlushLockForFlush() { flush_mutex.lock(); }

        /**
         * @brief releases the flush lock for flush 
         */
        void releaseFlushLockForFlush() { flush_mutex.unlock(); }

        void incActiveTxnCount();
        void decActiveTxnCount();
        uint32_t getActiveTxnCount();
        uint32_t getInMemoryTxnCount();
};