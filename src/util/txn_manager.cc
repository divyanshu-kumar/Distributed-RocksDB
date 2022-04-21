#include "txn_manager.h"

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

vector<string> TxnManager::getTxnKeys(int logIndex) {
    vector<string> keys;

    string logPath = computeLogPath(logIndex);

    ifstream logStream(logPath);
    string key;

    while(logStream >> key) {
        keys.push_back(key);
    }

    cout << keys.size() << endl;

    return keys;
}

int main() {
    TxnManager tm("/users/dkumar27/Distributed-RocksDB/logs");

    tm.put("hello1", "world1");

    cout << tm.get("hello1") << endl;

    string noVal = tm.get("h");

    if (noVal.empty()) {
        cout << "noVal" << endl;
    }

    tm.flush();
    tm.getTxnKeys(0);

    return 0;
}