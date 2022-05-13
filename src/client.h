#include "utils.h"
#include "distributedRocksDB.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;
using namespace DistributedRocksDB;
using namespace std;

const int MAX_NUM_RETRIES = 3;
const int INITIAL_BACKOFF_MS = 2000;
const int MULTIPLIER = 2;

struct CacheInfo {
    bool isCached;
    string data;
    struct timespec lastRefreshTs;
    CacheInfo() : isCached(false) { get_time(&lastRefreshTs); }
    bool isStale();
    void cacheData(const string &data);
    void invalidateCache() { isCached = false; }
};

class DistributedRocksDBClient {
   public:
    DistributedRocksDBClient(std::shared_ptr<Channel> channel)
        : stub_(DistributedRocksDBService::NewStub(channel)) {}

    int rpc_read(uint32_t key, string &value, bool isCachingEnabled, 
                 string & clientIdentifier, const string &serverAddress, 
                 Consistency consistency, const int timeout, 
                 struct timespec &start_time, struct timespec &end_time) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << " for server address: " << serverAddress 
                 << " Key: " << key << " Consistency: " 
                 << getConsistencyString(consistency) << endl;
        }

        ReadResult rres;

        int error_code = 0;

        ClientContext clientContext;
        ReadRequest rr;
        rr.set_key(key);
        rr.set_requirecache(isCachingEnabled);
        rr.set_consistency(getConsistencyString(consistency));
        rr.set_clientidentifier(clientIdentifier);

        // Set timeout for API
        std::chrono::system_clock::time_point deadline =
            std::chrono::system_clock::now() +
            std::chrono::milliseconds(timeout);

        clientContext.set_wait_for_ready(true);
        clientContext.set_deadline(deadline);
        
        get_time(&start_time);
        Status status = stub_->rpc_read(&clientContext, rr, &rres);
        get_time(&end_time);
        error_code = status.error_code();

        // case where server is not responding/offline
        if (error_code != grpc::StatusCode::OK) {
            if (debugMode <= DebugLevel::LevelError) {
                cout << __func__ << "\t : Failed because of timeout!" << endl;
            }
            return SERVER_OFFLINE_ERROR_CODE;
        }

        if (rres.err() == 0) {
            value = rres.value();
        } else {
            if (debugMode <= DebugLevel::LevelError) {
                cout << __func__ << "\t : Failed with read error returned as "
                     << rres.err() << endl;
            }
        }

        return rres.err();
    }

    int rpc_write(uint32_t key, const string &value,
         string & clientIdentifier, const string &serverAddress, 
         Consistency consistency, const int timeout, 
          struct timespec &start_time, struct timespec &end_time) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << " for server address " << serverAddress 
                 << " Key: " << key << " Consistency: " 
                 << getConsistencyString(consistency) << endl;
        }

        WriteResult wres;

        int error_code = 0;

        ClientContext ctx;
        WriteRequest wreq;
        wreq.set_key(key);
        wreq.set_value(value);
        wreq.set_consistency(getConsistencyString(consistency));
        wreq.set_clientidentifier(clientIdentifier);

        std::chrono::system_clock::time_point deadline =
            std::chrono::system_clock::now() +
            std::chrono::milliseconds(timeout);

        ctx.set_wait_for_ready(true);
        ctx.set_deadline(deadline);

        get_time(&start_time);
        Status status = stub_->rpc_write(&ctx, wreq, &wres);
        get_time(&end_time);
        error_code = status.error_code();

        if (error_code != grpc::StatusCode::OK) {
            if (debugMode <= DebugLevel::LevelError) {
                cout << __func__ << "\t : Failed because of timeout!" << endl;
            }
            return SERVER_OFFLINE_ERROR_CODE;
        }

        if (wres.err() != 0) {
            if (debugMode <= DebugLevel::LevelError) {
                cout << __func__ << "\t : Failed with write error returned as "
                     << wres.err() << endl;
            }
        }

        return wres.err();
    }

    Status rpc_subscribeForNotifications(bool isCachingEnabled, string & clientIdentifier,
        unordered_map<int, CacheInfo> &cacheMap) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << endl;
        }
        ClientContext context;
        ClientCacheNotify notifyMessage;
        SubscribeForNotifications subReq;

        subReq.set_identifier(clientIdentifier);

        std::unique_ptr<ClientReader<ClientCacheNotify> > reader(
            stub_->rpc_subscribeForNotifications(&context, subReq));

        while (reader->Read(&notifyMessage)) {
            int key = notifyMessage.key();
            if (debugMode <= DebugLevel::LevelInfo) {
                cout << __func__
                     << "\t : Invalidate cache with key: " << key
                     << endl;
            }
            if (isCachingEnabled) {
                cacheMap[key].invalidateCache();
            }
        }

        Status status = reader->Finish();
        if (!status.ok()) {
            if (debugMode <= DebugLevel::LevelError) {
                cout << __func__ << "\t : Status returned as "
                     << status.error_message() << endl;
            }
        }
        return status;
    }

    void rpc_unSubscribeForNotifications(string & clientIdentifier) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << endl;
        }
        ClientContext context;
        SubscribeForNotifications unSubReq, unSubRes;

        unSubReq.set_identifier(clientIdentifier);

        Status status = stub_->rpc_unSubscribeForNotifications(
            &context, unSubReq, &unSubRes);

        if (status.ok() == false) {
            if (debugMode <= DebugLevel::LevelError) {
                cout << __func__ << "\t : Status returned as "
                     << status.error_message() << endl;
            }
        }
    }

   private:
    std::unique_ptr<DistributedRocksDBService::Stub> stub_;
};

struct ServerInfo {
    string address;
    DistributedRocksDBClient *connection;

    ServerInfo(string addr) : 
        address(addr), 
        connection(new DistributedRocksDBClient(grpc::CreateChannel(
                        address.c_str(), grpc::InsecureChannelCredentials()))) { 
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Initialize connection from client to "
                 << address << endl;
        }
    }

    ~ServerInfo() {
        delete connection;
    }
};

void cacheInvalidationListener(ServerInfo* serverToContact,
    bool isCachingEnabled, string clientIdentifier, unordered_map<int, CacheInfo> & cacheMap);

bool CacheInfo::isStale() {
    struct timespec curTime;
    get_time(&curTime);
    double timeDiff = get_time_diff(&lastRefreshTs, &curTime);
    isCached = isCached && (timeDiff < stalenessLimit);
    return !isCached;
}

void CacheInfo::cacheData(const string &data) {
    this->data = data;
    isCached = true;
    get_time(&lastRefreshTs);
}

class ClusterState {
    public:
    mutex systemStateLock;
    string primaryAddress;
    unordered_set<string> backupAddresses;

    ClusterState() {}

    ClusterState& operator=(const ClusterState& state) {
        primaryAddress = state.primaryAddress;
        backupAddresses = state.backupAddresses;
        return *this;
    }
};

class Client {

public:
    int throughput, goodput;
    bool readFromBackup, isCachingEnabled;
    int clientThreadId;
    map<int, ClusterState> clusterInfo;
    mutex clusterInfoLock;
    string clientIdentifier;
    unordered_map<int, CacheInfo> cacheMap;
    unordered_map<string, ServerInfo*> serverInfos;
    unordered_map<int, std::thread> notificationThread;
    unique_ptr<DistributedRocksDBService::Stub> coordinator_stub_;

    Client(string coordinatorAddress, bool cachingEnabled, int threadId, 
                bool isReadFromBackup = false) 
        : clientThreadId(threadId), throughput(0), goodput(0),
          coordinator_stub_(DistributedRocksDBService::NewStub(grpc::CreateChannel(
                coordinatorAddress.c_str(), grpc::InsecureChannelCredentials()))) {
        isCachingEnabled = cachingEnabled;
        readFromBackup = isReadFromBackup;
        coordinatorAddress = coordinatorAddress;
        clientIdentifier = generateClientIdentifier();

        getSystemState();

        cout << __func__ << "\t : Client tid = " << clientThreadId 
             << " created with id = " <<  clientIdentifier << endl;
    }

    int run_application(int NUM_RUNS);
    int run_application_data_consistency(int NUM_RUNS);
    int client_read(uint32_t key, string &value, Consistency consistency,
                        struct timespec &start_time, struct timespec &end_time);
    pair<double, bool> read_wrapper(const uint32_t &key, string &value, const Consistency &consistency);

    int client_write(uint32_t key, const string &value, Consistency consistency
                    , struct timespec &start_time, struct timespec &end_time);
    pair<double, bool> write_wrapper(const uint32_t &key, string &value, const Consistency &consistency);

    int getClusterIdForKey(int key) {
        lock_guard<mutex> lock(clusterInfoLock);
        int hash = key % NUM_MAX_CLUSTERS;
        auto it = clusterInfo.upper_bound(hash);
        if (it == clusterInfo.end()) {
            return clusterInfo.begin()->first;
        }
        return it->first;
    }
    string selectBackupServerForRead(int clusterId) {
        lock_guard<mutex> guard(clusterInfo[clusterId].systemStateLock);
        int size = clusterInfo[clusterId].backupAddresses.size();
        std::random_device dev;
        std::mt19937 rng(dev());
        std::uniform_int_distribution<std::mt19937::result_type> dist6(0, size-1);
        return *(std::next(std::begin(clusterInfo[clusterId].backupAddresses), (int)dist6(rng)));
    }

    ServerInfo* getServerToContact(int key, Consistency consistency, bool isWriteRequest){
        int clusterId = getClusterIdForKey(key);
        if(!isWriteRequest && consistency == Consistency::eventual && !clusterInfo[clusterId].backupAddresses.empty()){
            string backupAddress = selectBackupServerForRead(clusterId);
            if (debugMode <= DebugLevel::LevelInfo) {
                cout << __func__ << "\t : selecting server "<< backupAddress << endl;
            }
            
            if(serverInfos.find(backupAddress) == serverInfos.end()){
                cout << __func__ << "\t :  could not find " << backupAddress <<" gRPC connection" << endl;
                std::quick_exit( EXIT_SUCCESS );
            }
            return serverInfos[backupAddress];          
        }

        clusterInfo[clusterId].systemStateLock.lock();
        const string primary(clusterInfo[clusterId].primaryAddress);
        clusterInfo[clusterId].systemStateLock.unlock();

        if(serverInfos.find(primary) == serverInfos.end()){
                cout << __func__ << "\t : ERR: No server info for primary address " << primary << endl;
                std::quick_exit( EXIT_SUCCESS );
        }
        if (debugMode <= DebugLevel::LevelInfo) {
                cout << __func__ << "\t : selecting server "<< primary << endl;
        }
        return serverInfos[primary];
    }

    void initServerInfo(const string &primary,
                        const vector<string> &newAddresses, 
                        const int keyRange, 
                        const bool &needNotificationThread) {

        for (const string &address : newAddresses) {
            if (debugMode <= DebugLevel::LevelInfo) {
                cout << __func__ << "\t : create gRPC Connection for " << address << endl; 
            }
            if(serverInfos.find(address) != serverInfos.end()){
                cout << __func__ << " serverInfos already has the connection" << endl;
            } else
                serverInfos[address] = new ServerInfo(address);
        }
        if(needNotificationThread){
            notificationThread[keyRange] = (std::thread(cacheInvalidationListener, (serverInfos[primary]),
                                              isCachingEnabled, clientIdentifier, std::ref(cacheMap)));
            notificationThread[keyRange].detach();
            msleep(1);
        }
    }


    void updateSystemState(const SystemStateResult &systemStateMsg){
        lock_guard<mutex> guard(clusterInfoLock);
        std::map<int, ClusterState> newClusterInfo;

        for (auto &systemState : systemStateMsg.systemstate()) {
            ClusterState state;
            state.primaryAddress = systemState.primary();
            for (auto &backup : systemState.backups()) {
                state.backupAddresses.insert(backup);
            }
            newClusterInfo[(int)systemState.keyrange()] = state;
        }

        for (auto &state : newClusterInfo) {
            const int keyRange = state.first;
            const string &primary = state.second.primaryAddress;
            const auto &backups = state.second.backupAddresses;

            if(primary.empty()) {
                cout << __func__ << "\t : No primary server received from Coordinator for keyRange " 
                    << keyRange << ". Exiting.... " << endl;
                std::quick_exit( EXIT_SUCCESS );
            }
            
            // newAddresses stores the addresses whose gRPC connection is to be created
            vector<string> newAddresses;  
            unordered_set<string> receivedBackupAddresses;

            // Add only the addresses seen for first time in backups
            for (auto &address : backups) {
                if(clusterInfo[keyRange].backupAddresses.find(address) == clusterInfo[keyRange].backupAddresses.end()){
                    if (debugMode <= DebugLevel::LevelInfo) {
                        cout << __func__ << "\t : adding new Backup Address " << address << endl;
                    }
                    clusterInfo[keyRange].backupAddresses.insert(address);
                    newAddresses.push_back(address);
                }
                receivedBackupAddresses.insert(address);
            }
            unordered_set<string> tempSet(clusterInfo[keyRange].backupAddresses);
            // remove the addresses & connection that are not sent by coordinator
            for(auto address : tempSet){
                if(receivedBackupAddresses.find(address) == receivedBackupAddresses.end()){
                    
                    if(!clusterInfo[keyRange].primaryAddress.empty() && address == primary){
                        if (debugMode <= DebugLevel::LevelInfo) {
                            cout << __func__ << "\t : removing Address in backups " << address << endl;
                        }
                        clusterInfo[keyRange].backupAddresses.erase(address);
                    } else {
                        if (debugMode <= DebugLevel::LevelInfo) {
                            cout << __func__ << "\t : removing from backups & removing gRPC connection address: " << address << endl;
                        }
                        clusterInfo[keyRange].backupAddresses.erase(address);
                        serverInfos.erase(address);
                    }
                }
            }

            bool needNotificationThread = true;
            if(clusterInfo[keyRange].primaryAddress == primary){
                needNotificationThread = false;
            }

            clusterInfo[keyRange].primaryAddress = primary;

            if(serverInfos.find(primary) == serverInfos.end()){
                newAddresses.push_back(primary);
            }

            if (debugMode <= DebugLevel::LevelInfo) {
                cout << __func__ << "\t : Updated Primary Address as " << primary << " for range " << keyRange << endl;
                for(auto &address : backups){
                    cout << __func__ << "\t : Key = " << keyRange << ", Backup contains " << address << endl;
                }
            }
            
            initServerInfo(primary, newAddresses, keyRange, needNotificationThread);
        }
    }

    int getSystemState(){
        if (debugMode <= DebugLevel::LevelInfo) {
                cout << __func__ << "\t : Contacting Coordinator: " << endl;
        }
        bool isDone = false;
        int numRetriesLeft = MAX_NUM_RETRIES;
        unsigned int currentBackoff = INITIAL_BACKOFF_MS;
        int error_code = 0;
        ClientContext clientContext;
        SystemStateRequest systemStateRequest;
        SystemStateResult systemStateReply;

        while (!isDone) {
            
            systemStateRequest.set_request(clientIdentifier);
            
            // Set timeout for API
            std::chrono::system_clock::time_point deadline =
                std::chrono::system_clock::now() +
                std::chrono::milliseconds(currentBackoff);

            clientContext.set_wait_for_ready(true);
            clientContext.set_deadline(deadline);

            Status status = coordinator_stub_->rpc_getSystemState(&clientContext, systemStateRequest, &systemStateReply);
            // TODO add error code in SystemStateReply for coordinator
            error_code = status.error_code();
            currentBackoff *= MULTIPLIER;

            if (status.error_code() == grpc::StatusCode::OK ||
                numRetriesLeft-- == 0) {
                isDone = true;
            } else {
                if (debugMode <= DebugLevel::LevelInfo) {
                    printf("%s \t : Timed out to contact coordinator server.\n", __func__);
                    cout << __func__
                         << "\t : Error code = " << status.error_message()
                         << endl;
                }
                if (debugMode <= DebugLevel::LevelError) {
                    cout << __func__ << "\t : Retrying to coordinator"
                         << currentBackoff << " MULTIPLIER = " << MULTIPLIER << endl;
                }
            }
        }

        // case where server is not responding/offline
        if (error_code != grpc::StatusCode::OK) {
            if (debugMode <= DebugLevel::LevelError) {
                cout << __func__ << "\t : Failed because of timeout!" << endl;
                cout << __func__ << "Coordinator server seems offline/not responding" << endl;
            }
            return SERVER_OFFLINE_ERROR_CODE;
        }

        updateSystemState(systemStateReply);
        return 0;
    }

    ~Client() {
        for (auto &state : clusterInfo) {
            if(serverInfos.find(state.second.primaryAddress) != serverInfos.end())
                (serverInfos[state.second.primaryAddress]->connection)->rpc_unSubscribeForNotifications(clientIdentifier);
            // notificationThread[state.first].join();
        }
        for (auto iter = serverInfos.begin(); iter != serverInfos.end(); ++iter) {
            delete iter->second;
        }
    }
};

