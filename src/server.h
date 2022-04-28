#include "utils.h"
#include "fs_utils.h"
#include "distributedRocksDB.grpc.pb.h"
#include "util/txn_manager.h"
#include <future>
#include <semaphore.h>

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
using grpc::ClientReaderWriter;
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

// Constants
const int MAX_NUM_RETRIES = 5;
const int INITIAL_BACKOFF_MS = 100;
const int MULTIPLIER = 2;
const int NUM_WORKER_THREADS = 100;
const int TXN_FLUSH_THRESHOLD = 10;

string currentWorkDir, dataDirPath, writeTxLogsDirPath;

static string role, my_address, coordinator_address;
string storage_path;

static unordered_map<int, std::mutex> blockLock;
unordered_map<int, struct timespec> backupLastWriteTime;

bool crashTestingEnabled(false);
bool writeThreadPoolEnabled(false);

sem_t sem_recovery, sem_register;

struct timespec* max_time(struct timespec* t1, struct timespec* t2);

struct WriteInfo {
    int key;
    string value;
    string address;
    std::atomic<int>& countSent;

    WriteInfo(int k, string val, string addr, std::atomic<int>& count)
        : key(k), value(val), address(addr), countSent(count) {}
};
struct NotificationInfo {
    unordered_map<string, bool> subscriberShouldRun;
    unordered_map<int, unordered_set<string>> subscribedClients;
    unordered_map<string, ServerWriter<ClientCacheNotify>*> clientWriters;
    unordered_map<string, std::mutex> clientNotifyLocks;

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
            lock_guard<std::mutex> lock(clientNotifyLocks[id]);
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

    ServerReplication() : threadPool(NUM_WORKER_THREADS) {
        // sometimes old values are cached, faced in P3, so needed
        stubs.clear();
    }

    void set_tm(TxnManager *_tm) {
        tm = _tm;
    }

    int rpc_write(uint32_t key, const string& value, const unordered_set<string> &currentBackups) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Key " << key << endl;
        }

        vector<thread> threads;
        for (auto backupAddress : currentBackups) {
            threads.push_back(thread(&ServerReplication::sendWritesToBackups,
                                     this, backupAddress, key, value));
        }

        for (int i = 0; i < threads.size(); i++) {
            threads[i].join();
        }

        return 0;
    }

    void sendWritesToBackups(string backupAddress, uint32_t key,
                             const string& value) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << " Key: " << key << " Value: " << value
                 << " Backup Address: " << backupAddress << endl;
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

            Status status = stubs[backupAddress]->rpc_write(&ctx, wreq, &wres);
            currentBackoff *= MULTIPLIER;
            if (status.error_code() != grpc::StatusCode::DEADLINE_EXCEEDED ||
                numRetriesLeft-- == 0) {
                if (numRetriesLeft <= 0) {
                    if (debugMode <= DebugLevel::LevelError) {
                        printf("%s \t : Backup server seems offline\n",
                               __func__);
                    }
                }
                isDone = true;
            }
        }

        if (wres.err() != 0) {
            if (debugMode <= DebugLevel::LevelInfo) {
                printf("%s \t : Error code in result is not success\n",
                        __func__);
            }
        }
    }

    Status rpc_read(ServerContext* context, const ReadRequest* rr,
                    ReadResult* reply) override {
        systemStateLock.lock();
        const string currentRole = role;
        systemStateLock.unlock();

        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Key " << rr->key() << ", role "
                 << currentRole << endl;
        }

        bool isCachingRequested =
            rr->requirecache() && (currentRole == "primary");

        if (isCachingRequested) {
            string clientId = rr->clientidentifier();
            notificationManager.Subscribe(rr->key(), clientId);
        }

        string buf;
        int res = 0;

        ROCKSDB_NAMESPACE::Status status =
            db->Get(ReadOptions(), to_string(rr->key()), &buf);
        assert(status.ok() || status.IsNotFound());

        reply->set_value(buf);
        reply->set_err(0);

        return Status::OK;
    }

    void writeToDB(string key, string value) {
        // Common across Primary or Backup
        ROCKSDB_NAMESPACE::Status status =
            db->Put(WriteOptions(), key, value);
        assert(status.ok());

        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Wrote Key " << key << endl;
        }
    }

    void execAsPrimary(const WriteRequest* wr, const unordered_set<string> &currentBackups) {
        writeToDB(to_string(wr->key()), wr->value());

        if (wr->consistency() == getConsistencyString(Consistency::fast_acknowledge)) {
            std::thread asyncWriteThread(&ServerReplication::replicateToBackups, this, *wr, currentBackups);
            asyncWriteThread.detach();
        }
        else {
            replicateToBackups(*wr, currentBackups);
        }

    }

    void execAsReplica(const WriteRequest *wr) {
        writeToDB(to_string(wr->key()), wr->value());
        tm->put(to_string(wr->key()), wr->value());
    }

    Status rpc_write(ServerContext* context, const WriteRequest* wr,
                     WriteResult* reply) override {
        systemStateLock.lock();
        string currentRole(role);
        unordered_set<string> currentBackups(backups);
        systemStateLock.unlock();

        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Key " << wr->key() << ", role "
                 << currentRole << endl;
        }

        if (currentRole == "primary") {  // TODO
            if (crashTestingEnabled) {
                if (wr->key() == 5) {
                    raise(SIGSEGV);
                }
            }
            notificationManager.Notify(wr->key(), wr->clientidentifier());
        }

        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : About to write Key " << wr->key() << endl;
        }

        // In case of Primary, replicate the data
        if (currentRole == "primary") {
            // TXN management stuff - START
            tm->acquireFlushLockForWrite();
            if (debugMode <= DebugLevel::LevelInfo) {
                cout << __func__ << "\t : Acquired flush lock for write, key = " << wr->key() << endl;
            }
            unique_lock<shared_mutex> putLock = tm->getPutLock(to_string(wr->key()));
            tm->incActiveTxnCount();
            tm->put(to_string(wr->key()), wr->value());

            execAsPrimary(wr, currentBackups);
            
            // TXN management stuff - END
            tm->releasePutLock(putLock);
            tm->decActiveTxnCount();
            if (debugMode <= DebugLevel::LevelInfo) {
                cout << __func__ << "\t : Releasing flush lock for write, key = " << wr->key() << endl;
            }
            tm->releaseFlushLockForWrite();
        } else {
            // backup is dumb and does not do anything
            tm->incActiveTxnCount();
            execAsReplica(wr);
            tm->decActiveTxnCount();
        }

        reply->set_err(0);

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

    Status rpc_heartbeat(ServerContext* context,
                         ServerReader<SystemState>* reader,
                         Heartbeat* response) override {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << endl;
        }

        SystemState systemStateMsg;

        while (reader->Read(&systemStateMsg)) {
            updateSystemView(systemStateMsg);
        }

        response->set_msg("OK");

        return Status::OK;
    }

    Status rpc_flush(ServerContext* context, const TxnFlushRequest* request, TxnFlushReply* reply) override {
        // No need to coordinate. Primary coordinates everything
        tm->flush();

        return Status::OK;
    }

    vector<string> fetchKeys(int logIndex) {
        return tm->getTxnKeys(logIndex);
    }

    RecoverReply getLogTxns(int fromIndex, int toIndex) {
        unordered_set<string> keySet;

        // 1. Fetch all the keys from the log - let the txns be going on
        vector<future<vector<string>>> futures;

        for(int i = fromIndex; i <= toIndex; i++) {
            futures.push_back(async(launch::async, &ServerReplication::fetchKeys, this, i));
        }

        for(int i = 0; i < futures.size(); i++) {
            vector<string> keys = futures[i].get();
            std::copy(keys.begin(), keys.end(), std::inserter(keySet, keySet.end()));
        }

        // 2. Get corresponding values from DB and form a response
        RecoverReply reply;
        reply.set_logindex(toIndex);
        reply.set_error(0);
        reply.set_replytype(RecoveryReplyType::TXNS);

        for(string key: keySet) {
            string value;
            int res = 0;

            ROCKSDB_NAMESPACE::Status status =
                db->Get(ReadOptions(), key, &value);
            assert(status.ok() || status.IsNotFound());

            Txn* txn = reply.add_txns();
            txn->set_key(key);
            txn->set_value(value);
        }

        return reply;
    }

    Status rpc_recover(ServerContext* context, ServerReaderWriter<RecoverReply, RecoverRequest>* stream) override {
        RecoverRequest request;

        if(!stream->Read(&request)) {
            return Status::OK;
        }

        int fromLogIndex = request.lastlogindex();
        int toLogIndex = tm->getLastLogIndex();
        // 1. get all txns so far
        RecoverReply reply = getLogTxns(fromLogIndex + 1, toLogIndex);

        if(!stream->Write(reply)) {
            return Status::OK;
        }

        // 2. While txns are being written at server, stop writes
        lock_guard<mutex> guard(systemStateLock);
        // lock global writes
        tm->acquireFlushLockForFlush();

        while(tm->getActiveTxnCount() != 0) {
            msleep(1);
        }

        // 3. force flush
        if (tm->getInMemoryTxnCount() != 0) {
            flush();
        }
        

        // 4. If there were new logs written, then send them too
        int currLastLogIndex = tm->getLastLogIndex();
        if (currLastLogIndex > toLogIndex) {
            reply = getLogTxns(toLogIndex + 1, currLastLogIndex);
            if(!stream->Write(reply)) {
                return Status::OK;
            }
        }

        // 5. wait for it to send REG_DONE. We don't care if the connection fails at this point
        reply.set_replytype(RecoveryReplyType::TXNS_DONE);
        stream->Write(reply);

        stream->Read(&request);

        // unlock global writes
        tm->releaseFlushLockForFlush();

        return Status::OK;
    }

    void initiateThreadPool() {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Intiating " << threadPool.size()
                 << " threads" << endl;
        }

        for (int i = 0; i < threadPool.size(); i++) {
            threadPool[i] = new std::thread{[&] { this->replicatorThread(); }};
        }
    }

    unordered_map<string, std::unique_ptr<DistributedRocksDBService::Stub>>* getStubs() {
        return &stubs;
    }

    TxnManager* getTxnManager() {
        return tm;
    }

   private:
    TxnManager *tm;
    mutex systemStateLock;
    string primaryAddress;
    unordered_set<string> backups;
    unordered_map<string, std::unique_ptr<DistributedRocksDBService::Stub>> stubs;
    vector<std::thread*> threadPool;
    std::queue<WriteInfo> writeQueue;
    std::mutex writeQMutex;
    std::condition_variable work, workDone;


    grpc::Status execFlushInReplica(string ip, std::unique_ptr<DistributedRocksDBService::Stub> *stub) {
        cout << "[INFO]: Flush-START for ip:" << ip << endl;

        ClientContext context;
        TxnFlushRequest request;
        TxnFlushReply reply;

        // TODO: need to populate request? for not stalling the write requests
        grpc::Status status = (*stub)->rpc_flush(&context, request, &reply);

        if (status.ok()) {
            cout << "[INFO]: flush success at ip:" << ip << endl;
        } else {
            cout << "[INFO]: flush failed at ip:" << ip << endl;
        }

        cout << "[INFO]: Flush-END for ip:" << ip << endl;

        return status;
    }

    void flush() {
        // wait till active txns are finished
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Starting to flush." << endl;
        }

        while(tm->getActiveTxnCount() != 0) {
            msleep(1);
        }

        int REPLICA_COUNT = stubs.size();

        future<grpc::Status> flushes[REPLICA_COUNT];

        int i = 0;
        for (const auto& b: backups) {
            flushes[i] = async(launch::async, &ServerReplication::execFlushInReplica, this, b, &stubs[b]);
            i++;
        }

        for(int i = 0; i < REPLICA_COUNT; i++) {
            grpc::Status status = flushes[i].get();
        }

        tm->flush();
        cout << "[INFO]: flushes done" << endl;
    }

    void updateSystemView(SystemState systemStateMsg) {
        lock_guard<mutex> guard(systemStateLock);

        primaryAddress = systemStateMsg.primary();

        if (primaryAddress == my_address) {
            if (role != "primary") {
                if (debugMode <= DebugLevel::LevelError) {
                    cout << __func__ << "\t : Role changed to primary." << endl;
                }
                if (writeThreadPoolEnabled) {
                    initiateThreadPool();
                }
            }
            role = "primary";
        } else {
            role = "backup";
            return;
        }

        unordered_set<string> newBackups;

        for (auto backupAddress : systemStateMsg.backups()) {
            newBackups.insert(backupAddress);
            if (backups.find(backupAddress) == backups.end()) {
                if (debugMode <= DebugLevel::LevelInfo) {
                    cout << __func__ << "\t : Adding node " << backupAddress
                         << endl;
                }
                std::shared_ptr<Channel> channel = grpc::CreateChannel(
                    backupAddress.c_str(), grpc::InsecureChannelCredentials());
                stubs[backupAddress] =
                    DistributedRocksDBService::NewStub(channel);
            }
        }

        for (auto backup : backups) {
            if (newBackups.find(backup) == newBackups.end()) {
                if (debugMode <= DebugLevel::LevelInfo) {
                    cout << __func__ << "\t : Removing node " << backup << endl;
                }
                stubs.erase(backup);
            }
        }

        swap(newBackups, backups);

        if (role == "primary" && tm->getInMemoryTxnCount() > TXN_FLUSH_THRESHOLD) {
            tm->acquireFlushLockForFlush();

            if (debugMode <= DebugLevel::LevelInfo) {
                cout << __func__ << "\t : Successfully acquired flush lock" << endl;
            }

            flush();

            if (debugMode <= DebugLevel::LevelInfo) {
                cout << __func__ << "\t : Releasing flush lock" << endl;
            }

            tm->releaseFlushLockForFlush();
        }
        
    }

    void replicatorThread() {
        while (true) {
            int key;
            string value;
            string address;
            atomic<int>* countSent;

            {
                std::unique_lock<std::mutex> lock(writeQMutex);

                work.wait(lock, [&] { return !writeQueue.empty(); });

                WriteInfo writeInfo(writeQueue.front());
                writeQueue.pop();

                key = writeInfo.key;
                value = writeInfo.value;
                address = writeInfo.address;
                countSent = &(writeInfo.countSent);

                if (debugMode <= DebugLevel::LevelInfo) {
                    cout << __func__ << "\t : Key " << key
                         << " dequeued for backup " << address << " by thread "
                         << std::this_thread::get_id() << endl;
                }
            }

            sendWritesToBackups(address, key, value);

            ++(*countSent);
            workDone.notify_all();
        }
    }
    
    void replicateToBackups(const WriteRequest wr, const unordered_set<string> currentBackups) {
        if (writeThreadPoolEnabled) { // Parallel writes to thread pool if feature is enabled
            std::atomic<int> replicateCount = 0;
            std::unique_lock<std::mutex> lock(writeQMutex);

            for (auto& backupAddress : currentBackups) {
                writeQueue.push(WriteInfo(wr.key(), wr.value(),
                                            backupAddress, replicateCount));
                work.notify_one();
            }

            workDone.wait(lock, [&] {
                return replicateCount == currentBackups.size();
            });
        } else {  // Parallel writes to new threads created on demand
            int result = this->rpc_write(wr.key(), wr.value(), currentBackups);
            if (result != 0) {
                printf("%s \t : Error : Failed to write to backups",
                        __func__);
            }
        }

        return;
    }
};

static ServerReplication* serverReplication;

void registerServer() {
    // TODO: Hemal needed?
    msleep(100);

    std::unique_ptr<DistributedRocksDBService::Stub> coordinator_stub_(
        DistributedRocksDBService::NewStub(grpc::CreateChannel(
            coordinator_address.c_str(), grpc::InsecureChannelCredentials())));

    if (debugMode <= DebugLevel::LevelInfo) {
        printf("%s : Attempting to register with coordinator = %s\n", __func__,
               coordinator_address.c_str());
    }

    RegisterResult registerResult;

    bool isDone = false;
    int numRetriesLeft = MAX_NUM_RETRIES;
    unsigned int currentBackoff = INITIAL_BACKOFF_MS;
    int error_code = 0;

    cout << "[INFO-Register-Wait]: waiting for recovery" << endl;
    sem_wait(&sem_recovery);
    cout << "[INFO-Register]: recovery done" << endl;
    

    while (!isDone) {
        ClientContext ctx;
        RegisterRequest registerRequest;
        registerRequest.set_address(my_address);

        std::chrono::system_clock::time_point deadline =
            std::chrono::system_clock::now() +
            std::chrono::milliseconds(currentBackoff);

        ctx.set_wait_for_ready(true);
        ctx.set_deadline(deadline);

        Status status = coordinator_stub_->rpc_registerNewNode(
            &ctx, registerRequest, &registerResult);
        error_code = status.error_code();
        currentBackoff *= MULTIPLIER;

        if (status.error_code() == grpc::StatusCode::OK ||
            numRetriesLeft-- == 0) {
            isDone = true;
            cout << "[INFO-Register]: register done" << endl;
            // recovery is waiting for this signal
            sem_post(&sem_register);

            serverReplication->set_tm(new TxnManager(storage_path));
        } else {
            if (debugMode <= DebugLevel::LevelInfo) {
                printf("%s \t : Timed out to contact coordinator server.\n",
                       __func__);
                cout << __func__
                     << "\t : Error code = " << status.error_message() << endl;
            }
            if (debugMode <= DebugLevel::LevelError) {
                cout << __func__ << "\t : Retrying to " << coordinator_address
                     << " with timeout (ms) of " << currentBackoff << endl;
            }
        }
    }

    if (error_code != grpc::StatusCode::OK) {
        if (debugMode <= DebugLevel::LevelError) {
            cout << __func__ << "\t : Failed because of timeout!" << endl;
        }
        return;
    } else {
        string assignedResult = registerResult.result();
        if (assignedResult == "primary" || assignedResult == "backup") {
            role = assignedResult;
            if (role == "primary" && writeThreadPoolEnabled) {
                serverReplication->initiateThreadPool();
            }
            if (debugMode <= DebugLevel::LevelError) {
                cout << __func__ << "\t : Role assigned as " << role << endl;
            }
        } else {
            cout << __func__ << "\t : Was not able to get an assigned role!"
                 << endl;
            quick_exit(EXIT_SUCCESS);
        }
    }

    return;
}