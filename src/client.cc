#include <fstream>
#include <iostream>

#include "client.h"

bool crashTestingEnabled(false);

int run_application(bool isReadOnlyMode);
void printStats();
void getRandomText(string &str, int size);

vector<vector<pair<double, int>>> allReadTimes, allWriteTimes;
void saveData(const vector<pair<double, int>> & v, const string & filename);

int Client::client_read(const uint32_t key, string &value, Consistency consistency) {
    if (debugMode <= DebugLevel::LevelInfo) {
        cout << __func__ << "\t : Key = " << key
             << ", ReadFromBackup = " << readFromBackup << endl;
    }

    ServerInfo* serverToContact = getServerToContact(consistency, false);

    if (debugMode <= DebugLevel::LevelInfo) {
        cout << __func__ << "\t : Contacting server "
             << serverToContact->address << endl;
    }

    int res = (serverToContact->connection)
                  ->rpc_read(key, value, isCachingEnabled, 
                  clientIdentifier, serverToContact->address, consistency);

    if (res == SERVER_OFFLINE_ERROR_CODE) {
        if (debugMode <= DebugLevel::LevelError) {
            cout << __func__
                << "\t : Read request timed-out, trying to contact other "
                    "server now."
                << endl;
        }
        res = (serverToContact->connection)->rpc_read(key, value, isCachingEnabled, 
                clientIdentifier, serverToContact->address, consistency);
        if (res < 0) {
            if (debugMode <= DebugLevel::LevelError) {
                cout << __func__ << "\t : Both servers are offline!" << endl;
            }
            return -1;
        }
    }

    return res;
}

int Client::client_write(const uint32_t key, const string &value, Consistency consistency) {
    if (debugMode <= DebugLevel::LevelInfo) {
        cout << __func__ << "\t : Key = " << key << endl;
    }

    ServerInfo* serverToContact = getServerToContact(consistency, true);

    int res = (serverToContact->connection)->rpc_write(key, value, clientIdentifier, serverToContact->address, consistency);

    if (res == SERVER_OFFLINE_ERROR_CODE) {
        if (debugMode <= DebugLevel::LevelError) {
            cout << __func__
                << "\t : Write request timed-out, trying to contact other "
                    "server now."
                << endl;
        }
        // TODO: in case of error, update the state from backup
        res = (serverToContact->connection)->rpc_write(key, value, clientIdentifier, serverToContact->address, consistency);

        if (res < 0) {
            if (debugMode <= DebugLevel::LevelError) {
                cout << __func__ << "\t : Both servers are offline!" << endl;
            }
            return -1;
        }
    }

    return res;
}

void cacheInvalidationListener(
    ServerInfo* serverToContact,
    bool isCachingEnabled, string clientIdentifier, unordered_map<int, CacheInfo> & cacheMap) {

    cout << __func__ << "\t : Listening for notifications.." << endl;
    Status status = grpc::Status::OK;
    do {
        
        status = (serverToContact->connection)->rpc_subscribeForNotifications(isCachingEnabled, clientIdentifier, cacheMap);

        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Error code = " << status.error_code()
                 << " and message = " << status.error_message() << endl;
        }

        if (status.error_code() == grpc::StatusCode::UNAVAILABLE) {

            for (auto &cachedEntry : cacheMap) {
                cachedEntry.second.invalidateCache();
            }
            if (debugMode <= DebugLevel::LevelNone) {
                cout << __func__ << "\t : Invalidated all cached entries as changing server!" << endl;
            }
            // TODO: Check if there needs to be any change in serverInfo
            if (debugMode <= DebugLevel::LevelError) {
                cout << __func__ << "\t : Should change server  " << endl;
            }
        }
    } while (grpc::StatusCode::UNAVAILABLE == status.error_code());

    cout << __func__ << "\t : Stopped listening for notifications now." << endl;
}


int main(int argc, char *argv[]) {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);
    cout.tie(nullptr);

    srand(time(NULL));

    if (debugMode <= DebugLevel::LevelInfo) {
        printf("%s \t: %s\n", __func__, argv[0]);
    }

    bool isServerArgPassed = false;
    bool isCrashSiteArgPassed = false;
    int crashSite = 0;
    string argumentString;
    bool isReadFromBackup = false;
    int numClients = 1;
    string coordinatorAddress;

    if (argc > 1) {
        for (int arg = 1; arg < argc; arg++) {
            argumentString.append(argv[arg]);
            argumentString.push_back(' ');
        }


        coordinatorAddress = parseArgument(argumentString, "--coordinator_address=");

        // only for dev purpose, take default address of coordinator to be 0.0.0.0:50051
        if (coordinatorAddress.empty()) {
            coordinatorAddress = "0.0.0.0:50051";
        }

        if (!isIPValid(coordinatorAddress)) {
            cout << "Enter a valid IP address, entered value is " << coordinatorAddress << endl;
            exit(1);
        }

        string clientArg = parseArgument(argumentString, "--numClients=");
        if (!clientArg.empty()) {
            int numClientsPassed = stoi(clientArg);
            if (numClientsPassed > 0 && numClientsPassed < 1000) {
                numClients = numClientsPassed;
            }
        }

        crashTestingEnabled = parseArgument(argumentString, "--crash=") == "true" ? true : false;
    }

    const bool isCachingEnabled = false;

    cout << "Num Clients = " << numClients << endl;

    vector<Client*> ourClients;
    for (int i = 0; i < numClients; i++) {
        allReadTimes.push_back({});
        allWriteTimes.push_back({});
        ourClients.push_back(new Client(coordinatorAddress, isCachingEnabled, i, isReadFromBackup));
    }
    vector<thread> threads;
    for (int i = 0; i < numClients; i++) {
        if (crashTestingEnabled) {
            //threads.push_back(thread(&Client::run_application_crashTesting, ourClients[i], 10));
            threads.push_back(thread(&Client::run_application, ourClients[i], 10));
        }
        else {
            threads.push_back(thread(&Client::run_application, ourClients[i], 10));
        }
    }
    for (int i = 0; i < numClients; i++) {
        threads[i].join();
        delete ourClients[i];
    }

    printStats();
    cout << "Finished with the threads!" << endl;
    return 0;
}

int Client::run_application(int NUM_RUNS = 50) {
    vector<pair<double, int>> &readTimes = allReadTimes[clientThreadId],
                              &writeTimes = allWriteTimes[clientThreadId];

    string write_data;

    std::random_device dev;
    std::mt19937 rng(dev());
    std::uniform_int_distribution<std::mt19937::result_type> dist6(0, 100);
    std::uniform_int_distribution<std::mt19937::result_type> dist7(0, (int)1e6);

    for (int i = 0; i < NUM_RUNS; i++) {
        getRandomText(write_data, 10);
    
        string value;
        uint32_t key = (int)dist7(rng);
        
        struct timespec write_start, write_end;
        get_time(&write_start);

        int result = client_write(key, write_data, Consistency::strong);

        get_time(&write_end);
        writeTimes.push_back(
            make_pair(get_time_diff(&write_start, &write_end), key));

        if ((result < 0) &&
            (debugMode <= DebugLevel::LevelError)) {
            printf("Failed to set the key = %d\n", key);
        }

        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << " \t : Written Key = " << key << ", value = " << write_data << endl;
        }

        msleep((int)dist6(rng));

        struct timespec read_start, read_end;
        get_time(&read_start);

        result = client_read(key, value, Consistency::eventual);

        get_time(&read_end);
        readTimes.push_back(
            make_pair(get_time_diff(&read_start, &read_end), key));

        if ((result < 0) && (debugMode <= DebugLevel::LevelError)) {
            printf(
                "Failed to get the key = %d!\n", key);
        }

        if (result == 0 && debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << " \t : Read Key = " << key << ", value = " << value << endl;
        }


        msleep((int)dist6(rng));
    }

    return 0;
}

void printPercentileTimes(const vector<pair<double, int>> &readTimes, const vector<pair<double, int>> &writeTimes){
    cout<<"Percentile,Read(ms),Write(ms)" << endl;
    vector<int> percentiles = {10,20,30,40,50,60,70,80,90,95,96,97,98,99,100};
    for(int percentile : percentiles) {
        int readItr = ((readTimes.size()-1) * percentile)/100, writeItr = ((writeTimes.size()-1) * percentile)/100;
        double readTime = !readTimes.empty() ? readTimes[readItr].first : 0, 
               writeTime = !writeTimes.empty() ? writeTimes[writeItr].first : 0;
        printf("%d,%f,%f\n", percentile, readTime, writeTime);
    }
}

void printStats() {
    vector<pair<double, int>> readTimes, writeTimes;
    for (auto readTime : allReadTimes) {
        for (auto p : readTime) {
            readTimes.push_back(p);
        }
    }
    
    for (auto writeTime : allWriteTimes) {
        for (auto p : writeTime) {
            writeTimes.push_back(p);
        }
    }
    
    double meanReadTime = 0;
    for (auto &readTime : readTimes) {
        meanReadTime += readTime.first;
    }
    if (!readTimes.empty())
        meanReadTime /= readTimes.size();

    double meanWriteTime = 0;
    for (auto &writeTime : writeTimes) {
        meanWriteTime += writeTime.first;
    }
    if (!writeTimes.empty())
        meanWriteTime /= writeTimes.size();

    auto originalReadTimes = readTimes;
    auto originalWriteTimes = writeTimes;

    sort(readTimes.begin(), readTimes.end());
    sort(writeTimes.begin(), writeTimes.end());

    double medianReadTime = !readTimes.empty() ? readTimes[readTimes.size() / 2].first : 0;
    double medianWriteTime = !writeTimes.empty() ? writeTimes[writeTimes.size() / 2].first : 0;

    // printf(
    //     "%s : *****STATS (milliseconds) *****\n"
    //     "meanRead   = %f \t meanWrite   = %f \n"
    //     "medianRead = %f \t medianWrite = %f\n"
    //     "minRead    = %f \t minWrite    = %f\n"
    //     "minAddress = %d \t minAddress  = %d\n"
    //     "maxRead    = %f \t maxWrite    = %f\n"
    //     "maxAddress = %d \t maxAddress  = %d\n",
    //     __func__, meanReadTime, meanWriteTime, medianReadTime, medianWriteTime,
    //     readTimes.front().first, writeTimes.front().first,
    //     readTimes.front().second, writeTimes.front().second,
    //     readTimes.back().first, writeTimes.back().first,
    //     readTimes.back().second, writeTimes.back().second);

    printf(
        "meanRead,medianRead,minRead,maxRead\n"
        "%f,%f,%f,%f\n"
        "meanWrite,medianWrite,minWrite,maxWrite\n"
        "%f,%f,%f,%f\n",
        meanReadTime, medianReadTime, !readTimes.empty() ? readTimes.front().first : 0, !readTimes.empty() ? readTimes.back().first : 0,
        meanWriteTime, medianWriteTime, !writeTimes.empty() ? writeTimes.front().first : 0, !writeTimes.empty() ? writeTimes.back().first : 0);

    printPercentileTimes(readTimes, writeTimes);

    if (!readTimes.empty()) 
        saveData(originalReadTimes, "readTimes.txt");

    if (!writeTimes.empty())
        saveData(originalWriteTimes, "writeTimes.txt");
}

void saveData(const vector<pair<double, int>> & v, const string & filename) {
    ofstream outfile;
    outfile.open(filename, ios::trunc);

    for (auto p : v) {
        outfile << p.first << endl;
    }
    // std::copy(v.begin(), v.end(), std::ostream_iterator<std::string>(std::ofstream, "\n"));

    outfile.close();
}

void getRandomText(string &str, int size = 4096) {
    str.clear();
    int num_bytes_written = 0;
    for (int i = 0; i < size; i++)
        str.push_back((rand() % 26) + 'a');
}
