#include "utils.h"
#include "fs_utils.h"
#include "distributedRocksDB.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using grpc::StatusCode;
using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

using namespace DistributedRocksDB;
using namespace std;

DB* db;

const int MAX_NUM_RETRIES = 5;
const int INITIAL_BACKOFF_MS = 100;
const int MULTIPLIER = 2;

string currentWorkDir, dataDirPath, writeTxLogsDirPath;

static string role, other_address, my_address;

static unordered_map<int, std::mutex> blockLock;
unordered_map<int, struct timespec> backupLastWriteTime;

thread heartbeatThread;
bool heartbeatShouldRun;
bool isBackupAvailable;
bool crashTestingEnabled(false);

void    rollbackUncommittedWrites();
int     logWriteTransaction(int address);
int     unLogWriteTransaction(int address);

struct timespec* max_time(struct timespec *t1, struct timespec *t2);

struct BackupOutOfSync {
    mutex m_lock;
    unordered_set<int> outOfSyncBlocks;

    BackupOutOfSync() {}

    void logOutOfSync(const int address);

    int sync();

    bool isOutOfSync() {
        lock_guard<mutex> guard(m_lock);
        return !outOfSyncBlocks.empty();
    }
} backupSyncState;

struct NotificationInfo {
    unordered_map<string, bool> subscriberShouldRun;
    unordered_map<int, unordered_set<string>> subscribedClients;
    unordered_map<string, ServerWriter<ClientCacheNotify>*> clientWriters;

    void Subscribe(int address, const string& id) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : for address " << address << " id " << id
                 << " with role " << role << endl;
        }
        subscribedClients[address].insert(id);
    }

    void UnSubscribe(int address, const string& id) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : for address " << address << " id " << id
                 << " with role " << role << endl;
        }
        subscribedClients[address].erase(id);
    }

    void AddClient(const string& clientId,
                   ServerWriter<ClientCacheNotify>* clientWriter) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Client " << clientId << " with role "
                 << role << endl;
        }
        clientWriters[clientId] = clientWriter;
        subscriberShouldRun[clientId] = true;
    }

    void RemoveClient(const string& clientId) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Client " << clientId << " with role "
                 << role << endl;
        }
        clientWriters.erase(clientId);
        subscriberShouldRun[clientId] = false;
    }

    bool ShouldKeepAlive(const string& clientId) {
        return subscriberShouldRun[clientId];
    }

    void Notify(int address, const string& writerClientId) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Address " << address << " with role "
                 << role << endl;
        }
        auto clientIds = subscribedClients[address];
        if (clientIds.empty()) {
            return;
        }
        for (auto clientId : clientIds) {
            if (clientWriters.find(clientId) == clientWriters.end()) {
                continue;
            }
            if (writerClientId != clientId) {
                NotifySingleClient(clientId, address);
            }
            UnSubscribe(address, clientId);
        }
    }

    void NotifySingleClient(const string& id, const int& address) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Client " << id << ", address " << address
                 << endl;
        }
        try {
            ServerWriter<ClientCacheNotify>* writer = clientWriters[id];
            ClientCacheNotify notifyReply;
            notifyReply.set_key(address);
            writer->Write(notifyReply);
        } catch (const std::exception& ex) {
            std::ostringstream sts;
            if (debugMode <= DebugLevel::LevelError) {
                sts << __func__ << "\t : Error contacting client " << id
                    << endl;
            }
            std::cerr << sts.str() << endl;
        }
    }
};

static NotificationInfo notificationManager;

class ServerReplication final : public DistributedRocksDBService::Service {
   public:

    ReadCache readCache;

    ServerReplication() { }

    ServerReplication(std::shared_ptr<Channel> channel)
        : stub_(DistributedRocksDBService::NewStub(channel)) { }

    int rpc_write(uint32_t key, const string& value) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Key " << key << ", role " << role << endl;
        }

        WriteResult wres;
        bool isDone = false;
        int numRetriesLeft = MAX_NUM_RETRIES;
        unsigned int currentBackoff = INITIAL_BACKOFF_MS;

        while (!isDone) {
            ClientContext ctx;
            WriteRequest wreq;
            wreq.set_key(key);
            wreq.set_value(value);

            std::chrono::system_clock::time_point deadline =
                std::chrono::system_clock::now() +
                std::chrono::milliseconds(currentBackoff);

            ctx.set_wait_for_ready(true);
            ctx.set_deadline(deadline);

            Status status = stub_->rpc_write(&ctx, wreq, &wres);
            currentBackoff *= MULTIPLIER;
            if (status.error_code() != grpc::StatusCode::DEADLINE_EXCEEDED ||
                numRetriesLeft-- == 0) {
                if (numRetriesLeft <= 0) {
                    if (debugMode <= DebugLevel::LevelError) {
                        printf("%s \t : Backup server seems offline\n", __func__);
                    }
                    return -1;
                }
                isDone = true;
            } else {
                // printf("%s \t : Timed out to contact server. Retrying...\n",
                // __func__);
            }
        }

        return wres.err();
    }

    Status rpc_read(ServerContext* context, const ReadRequest* rr,
                    ReadResult* reply) override {
        const string currentRole = role;
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Key " << rr->key() << ", role " << currentRole << endl;
        }

        if (true || currentRole == "primary") { // TODO - Decide if want to lock only for primary 
            blockLock[rr->key()].lock();
        }

        bool isCachingRequested =
            rr->requirecache() && (currentRole == "primary");

        if (isCachingRequested) {
            string clientId = rr->clientidentifier();
            notificationManager.Subscribe(rr->key(), clientId);
        }

        string buf;
        int res = 0;

        ROCKSDB_NAMESPACE::Status status = db->Get(ReadOptions(), to_string(rr->key()), &buf);
        assert(status.ok() || status.IsNotFound());

        reply->set_value(buf);
        reply->set_err(0);

        if (true || currentRole == "primary") { // TODO - Decide if want to lock only for primary 
            blockLock[rr->key()].unlock();
        }

        return Status::OK;
    }

    Status rpc_write(ServerContext* context, const WriteRequest* wr,
                     WriteResult* reply) override {
        const string currentRole = role;
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Key " << wr->key() << ", role " << currentRole 
                 << ", isBackupAvailable = " << isBackupAvailable << endl;
        }

        lock_guard<mutex> guard(blockLock[wr->key()]);

        if (false && currentRole == "primary") { // TODO
            if (crashTestingEnabled) {
                if (wr->key() == 5) {
                    raise(SIGSEGV);
                }
            }
            notificationManager.Notify(wr->key(), wr->clientidentifier());
        }

        int res = logWriteTransaction(wr->key());

        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : About to write Key " << wr->key() << endl;
        }

        ROCKSDB_NAMESPACE::Status status = db->Put(WriteOptions(), to_string(wr->key()), wr->value());
        assert(status.ok());

        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Wrote Key " << wr->key() << endl;
        }

        reply->set_err(0);
        
        if (currentRole == "primary") {
            if (backupSyncState.isOutOfSync()) {
                backupSyncState.logOutOfSync(wr->key());
                if (isBackupAvailable && backupSyncState.isOutOfSync()) {
                    backupSyncState.sync();
                }
            } else {
                int result = isBackupAvailable ? 
                    this->rpc_write(wr->key(), wr->value()) : 0;
                if (result != 0) {
                    backupSyncState.logOutOfSync(wr->key());
                }
            }
        }

        res = unLogWriteTransaction(wr->key());
        
        if (res == -1) {
            printf("%s \t : Error : Failed to unlog the write the transaction.",
                   __func__);
        }

        return Status::OK;
    }

    Status rpc_subscribeForNotifications(
        ServerContext* context,
        const SubscribeForNotifications* subscribeMessage,
        ServerWriter<ClientCacheNotify>* writer) override {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Client " << subscribeMessage->identifier()
                 << ", role " << role << endl;
        }

        const string& clientId = subscribeMessage->identifier();
        notificationManager.AddClient(clientId, writer);

        while (notificationManager.ShouldKeepAlive(clientId)) {
            msleep(500);
        }

        return Status::OK;
    }

    Status rpc_unSubscribeForNotifications(
        ServerContext* context, const SubscribeForNotifications* unSubReq,
        SubscribeForNotifications* reply) override {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Client " << unSubReq->identifier()
                 << ", role " << role << endl;
        }
        const string& clientId = unSubReq->identifier();

        notificationManager.RemoveClient(clientId);

        reply->set_identifier("Unsubscribed!");

        return Status::OK;
    }

    void rpc_heartbeatSender() {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << endl;
        }

        ClientContext context;
        Heartbeat heartbeatReq, heartbeatRes;

        heartbeatReq.set_msg("");

        std::unique_ptr<ClientReader<Heartbeat>> reader(
            stub_->rpc_heartbeatListener(&context, heartbeatReq));

        while (reader->Read(&heartbeatRes)) {
            auto message = heartbeatRes.msg();
            if (!message.empty() && message == "OK") {
                cout << __func__ << "\t : Starting to sync with backup server!" << endl;
                backupSyncState.sync();
                isBackupAvailable = true;
            }
            else {
                cout << __func__ << "\t : Heartbeat connection broken." << endl;
            }
        }

        Status status = reader->Finish();
        if (!status.ok()) {
            // if (debugMode <= DebugLevel::LevelInfo) {
            //     cout << __func__ << "\t : Status returned as "
            //          << status.error_message() << endl;
            // }
        }
    }

    Status rpc_heartbeatListener(ServerContext* context,
                                 const Heartbeat* heartbeatMessage,
                                 ServerWriter<Heartbeat>* writer) override {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << endl;
        }
        Heartbeat heartbeatResult;
        heartbeatResult.set_msg("OK");
        bool isFirstMsgSent(false);

        while (true) {
            if (!isFirstMsgSent) {
                writer->Write(heartbeatResult);
                isFirstMsgSent = true;
            }
            msleep(500);
        }

        return Status::OK;
    }

   private:
    std::unique_ptr<DistributedRocksDBService::Stub> stub_;
};

static ServerReplication* serverReplication;

void BackupOutOfSync::logOutOfSync(const int address) {
    lock_guard<mutex> guard(m_lock);
    if (debugMode <= DebugLevel::LevelInfo) {
        cout << __func__ << "\t : Out of sync address = " << address << endl;
    }
    outOfSyncBlocks.insert(address);
}

int BackupOutOfSync::sync() {
    lock_guard<mutex> guard(m_lock);
    if (debugMode <= DebugLevel::LevelInfo) {
        cout << __func__ << endl;
    }
    int res = 0;

    for (auto key : outOfSyncBlocks) {
        string buf;
        ROCKSDB_NAMESPACE::Status status = db->Get(ReadOptions(), to_string(key), &buf);
        cout << __func__ << "\t : Writing key " << key << " to backup server" << endl;
        res = serverReplication->rpc_write(key, buf);
        if (res < 0) {
            if (debugMode <= DebugLevel::LevelInfo) {
                cout << __func__
                     << "\t : Failed to sync keys to backup server." << endl;
            }
            return -1;
        }
    }

    outOfSyncBlocks.clear();

    if (debugMode <= DebugLevel::LevelError) {
        cout << __func__ << "\t : Successfully sync'd changed files!" << endl;
    }

    return 0;
}

int logWriteTransaction(int address) {
    if (debugMode <= DebugLevel::LevelInfo) {
        cout << __func__ << "\t : Address = " << address << endl;
    }
    return 0;
    // string destPath = writeTxLogsDirPath + "/" + to_string(address);
    // string sourcePath = dataDirPath + "/" + to_string(address);

    // int res = copyFile(destPath, sourcePath);
    // if (res == -1) {
    //     if (debugMode <= DebugLevel::LevelError) {
    //         printf("%s\t : Error: Dest Path = %s, Source Path = %s\n", __func__,
    //                destPath.c_str(), sourcePath.c_str());
    //     }
    //     perror(strerror(errno));
    // }

    // return res;
}

int unLogWriteTransaction(int address) {
    if (debugMode <= DebugLevel::LevelInfo) {
        cout << __func__ << "\t : Address = " << address << endl;
    }
    return 0;
    // string filePath = writeTxLogsDirPath + "/" + to_string(address);

    // int res = unlink(filePath.c_str());
    // if (res == -1) {
    //     if (debugMode <= DebugLevel::LevelError) {
    //         printf("%s\t : Error: File Path = %s\n", __func__,
    //                filePath.c_str());
    //     }
    //     perror(strerror(errno));
    // }

    // return res;
}

void rollbackUncommittedWrites() {
    DIR* dir = opendir(writeTxLogsDirPath.c_str());
    if (dir == NULL) {
        return;
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL) {
        string fileName(entry->d_name);
        if (fileName == "." || fileName == "..") continue;
        string sourcePath = writeTxLogsDirPath + "/" + fileName;
        string destPath = dataDirPath + "/" + fileName;
        string command = "mv " + sourcePath + " " + destPath;
        int res = system(command.c_str());
        if (res != 0) {
            if (debugMode <= DebugLevel::LevelError) {
                printf("%s : Error - failed to rename the file %s \n", __func__,
                       sourcePath.c_str());
            }
            perror(strerror(errno));
        }
    }

    closedir(dir);
}


void runHeartbeat() {
    if (debugMode <= DebugLevel::LevelError) {
        cout << __func__ << "\t : Starting heartbeat service!" << endl;
    }
    while (heartbeatShouldRun) {
        serverReplication->rpc_heartbeatSender();
        isBackupAvailable = false;
        if (role == "backup") {
            if (debugMode <= DebugLevel::LevelError) {
                cout << __func__
                     << "\t : Backup is switching to Primary mode now." << endl;
            }
            role = "primary";
            backupLastWriteTime.clear();
        }
        msleep(500);
    }
}