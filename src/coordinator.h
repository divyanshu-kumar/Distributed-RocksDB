#include "utils.h"
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
using namespace std;

const int MAX_NUM_RETRIES = 5;
const int INITIAL_BACKOFF_MS = 100;
const int MULTIPLIER = 2;

class Coordinator final : public DistributedRocksDBService::Service {
    private:
    unordered_map<string, std::unique_ptr<DistributedRocksDBService::Stub>> stubs;
    string primary;
    unordered_set<string> backups;
    mutex systemStateLock;
    
    bool isPrimaryElected() {
        return !primary.empty();
    }

    void setPrimary(const string & address) {
        primary = address;
    }

    bool isPrimary(const string & address) {
        lock_guard<mutex> guard(systemStateLock);

        return primary == address;
    }

    bool addNode(const string & address) {
        // TODO - can the address be already present. if so, handle here
        lock_guard<mutex> guard(systemStateLock);

        stubs[address] = DistributedRocksDBService::NewStub(grpc::CreateChannel(
        address.c_str(), grpc::InsecureChannelCredentials()));

        if (isPrimaryElected()) {
            backups.insert(address);
        }
        else {
            setPrimary(address);
        }

        establishHeartbeat(address);
    }

    void establishHeartbeat(const string & address) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << endl;
        }

        ClientContext context;
        Heartbeat heartbeatReq, heartbeatRes;

        heartbeatReq.set_msg("");

        std::unique_ptr<ClientReader<Heartbeat>> reader(
            stubs[address]->rpc_heartbeatListener(&context, heartbeatReq));

        while (reader->Read(&heartbeatRes)) {
            auto message = heartbeatRes.msg();
            if (!message.empty() && message == "OK") {
                cout << __func__ << "\t : Starting heartbeat with server address " << address << endl;
            }
            else {
                cout << __func__ << "\t : Heartbeat connection broken." << endl;
                lock_guard<mutex> guard(systemStateLock);
                delete stubs[address];
                stubs.erase(address);
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
   public:

    Coordinator() { }

    Status rpc_registerNewNode(ServerContext* context, const RegisterRequest* rr,
                    RegisterResult* reply) override {
        
        const string & nodeAddress = rr->address();

        bool result = addNode(nodeAddress);

        reply->set_result((isPrimary(nodeAddress) ? "primary" : "backup"));
        if (result) {
            reply->set_err(0);
        }
        else {
            reply->set_err(-1);
        }

        return Status::OK;
    }

    Status rpc_getSystemState(ServerContext* context, const SystemStateRequest* wr,
                     SystemStateResult* reply) override {
        
        reply->set_primary(primary);

        for (auto backupAddress : backups) {
            reply.add_backups(backupAddress);
        }

        return Status::OK;
    }
};

static Coordinator* Coordinator;