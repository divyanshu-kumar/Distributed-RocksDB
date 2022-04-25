#include "txn_manager.h"
#include <future>
#include <chrono>

int TxnManager::extractLogIndex(char* _fileName) {
    string fileName(_fileName);
    int index = stoi(fileName.substr(LOG_PREFIX.size()));

    return index;
}

int TxnManager::computeLastLogIndex() {
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

string TxnManager::computeLogPath(int logIndex) {
    return storage_path + "/" + LOG_PREFIX + to_string(logIndex);
}

unique_lock<shared_mutex> TxnManager::getPutLock(string key) {
    outer_mutex.lock();
    shared_mutex &key_mutex = mutices[key];
    outer_mutex.unlock();

    return unique_lock(key_mutex);
}

void TxnManager::releasePutLock(unique_lock<shared_mutex>& key_mutex)
{
    key_mutex.unlock();
}

TxnManager::TxnManager(string _storage_path) {
    storage_path = _storage_path;
    txns.clear();
    lastLogIndex = computeLastLogIndex();
}

void TxnManager::put(string key, string value) {
    txns.insert(make_pair(key, value));
}

string TxnManager::get(string key) {
    auto lookupRes = txns.find(key);

    if (lookupRes == txns.end()) {
        return string();
    }

    return lookupRes->second;
}

void TxnManager::flush() {
    string logPath = computeLogPath(lastLogIndex + 1);

    vector<std::string> keys;

    for(auto txn: txns) {
        keys.push_back(txn.first);
    }

    ofstream logFile(logPath);
    ostream_iterator<string> output_iterator(logFile, "\n");
    copy(keys.begin(), keys.end(), output_iterator);

    // advance system to next log index
    lastLogIndex++;
    txns.clear();
}

int TxnManager::getLastLogIndex() {
    return lastLogIndex;
}

void TxnManager::incActiveTxnCount() {
    active_txn_count++;
}

void TxnManager::decActiveTxnCount() {
    active_txn_count--;
}

uint32_t TxnManager::getActiveTxnCount() {
    return active_txn_count.load();
}

uint32_t TxnManager::getInMemoryTxnCount() {
    return txns.size();
}

vector<string> TxnManager::getCurrTxnKeys() {
    return getTxnKeys(lastLogIndex + 1);
}

vector<string> TxnManager::getTxnKeys(int logIndex) {
    vector<string> keys;
    keys.clear();

    // caller is asking for in memory keys
    if (logIndex == lastLogIndex + 1) {
        for(auto it: txns) {
            keys.push_back(it.first);
        }

        return keys;
    }

    string logPath = computeLogPath(logIndex);

    ifstream logStream(logPath);
    string key;

    while(logStream >> key) {
        keys.push_back(key);
    }

    cout << keys.size() << endl;

    return keys;
}

void exec(string key, TxnManager *tm, int tid, chrono::time_point<chrono::high_resolution_clock> begin) {
    auto lock = tm->getPutLock(key);

    auto first = chrono::high_resolution_clock::now();
    uint64_t started = chrono::duration_cast<chrono::nanoseconds> (first - begin).count();
    cout << "TID: " << tid << ", started:" << started << endl;

    tm->put(key, key);
    tm->releasePutLock(lock);

    auto second = chrono::high_resolution_clock::now();
    uint64_t ended = chrono::duration_cast<chrono::nanoseconds> (second - begin).count();
    cout << "TID: " << tid << ", ended:" << ended << endl;
}

void testMultiThreadDiff(TxnManager *tm) {
    int N = 3;
    future<void> workers[N];

    auto begin = chrono::high_resolution_clock::now();

    for (int i = 0; i < N; i++) {
        workers[i] = async(exec, to_string(i), tm, i, begin); 
    }

    for (int i = 0; i < N; i++) {
        workers[i].get();
    }
}

void testMultiThreadSame(TxnManager *tm) {
    int N = 3;
    future<void> workers[N];

    auto begin = chrono::high_resolution_clock::now();

    for (int i = 0; i < N; i++) {
        workers[i] = async(exec, to_string(10), tm, i, begin); 
    }

    for (int i = 0; i < N; i++) {
        workers[i].get();
    }
}

void increment(TxnManager *tm) {
    tm->incActiveTxnCount();
}

void decrement(TxnManager *tm, int id) {
    tm->decActiveTxnCount();
}

void testTxnCount(TxnManager *tm) {
    int N = 100;
    future<void> workersInc[N];
    future<void> workersDec[N];

    for(int i = 0; i < N; i++) {
        workersInc[i] = async(increment, tm);
    }

    for(int i = 0; i < N; i++) {
        workersInc[i].get();
    }

    cout << "After inc Count:" << tm->getActiveTxnCount() << endl;

    for(int i = 0; i < N; i++) {
        workersDec[i] = async(decrement, tm, i);
    }

    for(int i = 0; i < N; i++) {
        workersDec[i].get();
    }

    cout << "After Dec Count:" << tm->getActiveTxnCount() << endl;

}

int main() {
    TxnManager *tm = new TxnManager("/users/dkumar27/Distributed-RocksDB/logs");

    auto begin = chrono::high_resolution_clock::now();
    testMultiThreadDiff(tm);
    auto end = chrono::high_resolution_clock::now();
    uint64_t time_taken = chrono::duration_cast<chrono::nanoseconds> (end - begin).count();
    cout << "time_taken[DIff]:" << time_taken << endl;

    begin = chrono::high_resolution_clock::now();
    testMultiThreadSame(tm);
    end = chrono::high_resolution_clock::now();
    time_taken = chrono::duration_cast<chrono::nanoseconds> (end - begin).count();
    cout << "time_taken[Same]:" << time_taken << endl;

    testTxnCount(tm);


    // tm.put("hello1", "world1");

    // cout << tm.get("hello1") << endl;

    // string noVal = tm.get("h");

    // if (noVal.empty()) {
    //     cout << "noVal" << endl;
    // }

    // tm.flush();
    // tm.getTxnKeys(0);


    return 0;
}