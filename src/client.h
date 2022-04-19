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

const int MAX_NUM_RETRIES = 5;
const int INITIAL_BACKOFF_MS = 200;
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
                 string & clientIdentifier, int currentServerIdx) {
        if (debugMode <= DebugLevel::LevelInfo) {
            printf("%s : Key = %d\n", __func__, key);
        }

        ReadResult rres;

        bool isDone = false;
        int numRetriesLeft = MAX_NUM_RETRIES;
        unsigned int currentBackoff = INITIAL_BACKOFF_MS;
        int error_code = 0;

        while (!isDone) {
            ClientContext clientContext;
            ReadRequest rr;
            rr.set_key(key);
            rr.set_requirecache(isCachingEnabled);
            rr.set_clientidentifier(clientIdentifier);

            // Set timeout for API
            std::chrono::system_clock::time_point deadline =
                std::chrono::system_clock::now() +
                std::chrono::milliseconds(currentBackoff);

            clientContext.set_wait_for_ready(true);
            clientContext.set_deadline(deadline);

            Status status = stub_->rpc_read(&clientContext, rr, &rres);
            error_code = status.error_code();
            currentBackoff *= MULTIPLIER;

            if (status.error_code() == grpc::StatusCode::OK ||
                numRetriesLeft-- == 0) {
                isDone = true;
            } else {
                if (debugMode <= DebugLevel::LevelInfo) {
                    printf("%s \t : Timed out to contact server.\n", __func__);
                    cout << __func__
                         << "\t : Error code = " << status.error_message()
                         << endl;
                }
                if (debugMode <= DebugLevel::LevelError) {
                    cout << __func__ << "\t : Retrying to " 
                         << currentServerIdx << " with timeout (ms) of "
                         << currentBackoff << " MULTIPLIER = " << MULTIPLIER << endl;
                }
            }
        }

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
         string & clientIdentifier, int currentServerIdx) {
        if (debugMode <= DebugLevel::LevelInfo) {
            printf("%s : Key = %d\n", __func__, key);
        }

        WriteResult wres;

        bool isDone = false;
        int numRetriesLeft = MAX_NUM_RETRIES;
        unsigned int currentBackoff = INITIAL_BACKOFF_MS;
        int error_code = 0;

        while (!isDone) {
            ClientContext ctx;
            WriteRequest wreq;
            wreq.set_key(key);
            wreq.set_value(value);
            wreq.set_clientidentifier(clientIdentifier);

            std::chrono::system_clock::time_point deadline =
                std::chrono::system_clock::now() +
                std::chrono::milliseconds(currentBackoff);

            ctx.set_wait_for_ready(true);
            ctx.set_deadline(deadline);

            Status status = stub_->rpc_write(&ctx, wreq, &wres);
            error_code = status.error_code();
            currentBackoff *= MULTIPLIER;

            if (status.error_code() == grpc::StatusCode::OK ||
                numRetriesLeft-- == 0) {
                isDone = true;
            } else {
                if (debugMode <= DebugLevel::LevelInfo) {
                    printf("%s \t : Timed out to contact server.\n", __func__);
                    cout << __func__
                         << "\t : Error code = " << status.error_message()
                         << endl;
                }
                if (debugMode <= DebugLevel::LevelError) {
                    cout << __func__ << "\t : Retrying to " 
                         << currentServerIdx << " with timeout (ms) of "
                         << currentBackoff << endl;
                }
            }
        }

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

void cacheInvalidationListener(vector<ServerInfo*> & serverInfos, int & currentServerIdx,
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

class Client {

public:
    string clientIdentifier;
    std::thread notificationThread;
    int currentServerIdx, clientThreadId;
    vector<ServerInfo*> serverInfos;
    unordered_map<int, CacheInfo> cacheMap;
    bool readFromBackup, isCachingEnabled;

    Client(vector<string> &serverAddresses, bool cachingEnabled, 
            int threadId, bool isReadFromBackup = false) 
        : clientThreadId(threadId) {
        isCachingEnabled = cachingEnabled;
        readFromBackup = isReadFromBackup;
        currentServerIdx = 0;
        clientIdentifier = generateClientIdentifier();
        initServerInfo(serverAddresses);
        if (debugMode <= DebugLevel::LevelInfo) {
            printf("%s \t: Connecting to server at %s...\n", __func__,
            serverInfos[currentServerIdx]->address.c_str());
        }
        cout << __func__ << "\t : Client tid = " << clientThreadId 
             << " created with id = " <<  clientIdentifier << endl;
    }

    int run_application(int NUM_RUNS);
    int client_read(uint32_t key, string &value);
    int client_write(uint32_t key, const string &value);

    void initServerInfo(vector<string> addresses) {
        for (string address : addresses) {
            serverInfos.push_back(new ServerInfo(address));
        }
        notificationThread = (std::thread(cacheInvalidationListener, std::ref(serverInfos),
            std::ref(currentServerIdx), isCachingEnabled, clientIdentifier, std::ref(cacheMap)));
        msleep(1);
    }

    ~Client() {
        (serverInfos[currentServerIdx]->connection)->rpc_unSubscribeForNotifications(clientIdentifier);
        notificationThread.join();
        for (int i = 0; i < serverInfos.size(); i++) {
            delete serverInfos[i];
        }
    }
};

