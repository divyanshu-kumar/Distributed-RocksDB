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

using namespace std;

class TxnManager {
    private:
        // for locking put
        mutex outer_mutex;
        unordered_map<string, shared_mutex> mutices;

        atomic<uint32_t> active_txn_count = {0};

        unordered_map<string,string> txns;
        string storage_path;
        int lastLogIndex;
        const string LOG_PREFIX = string("self.log.");

        /**
         * @brief extract log index from the given file name in logs directory
         * 
         * @param _fileName 
         * @return int 
         */
        int extractLogIndex(char* _fileName);

        /**
         * @brief should be used during the instantiation of this class
         * figures out the last stored log index
         * 
         * @return int 
         */
        int computeLastLogIndex();

        /**
         * @brief computes log path to flush txns
         * 
         * @param logIndex 
         * @return string 
         */
        string computeLogPath(int logIndex);


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


        void incActiveTxnCount();
        void decActiveTxnCount();
        uint32_t getTxnCount();
};