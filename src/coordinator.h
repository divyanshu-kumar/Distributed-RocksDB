#include "distributedRocksDB.grpc.pb.h"
#include "utils.h"

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

using namespace DistributedRocksDB;
using namespace std;

const int MAX_NUM_RETRIES = 5;
const int INITIAL_BACKOFF_MS = 100;
const int MULTIPLIER = 2;
const int HEARTBEAT_FREQUENCY = 50;  // (ms)

static string my_address;
class Coordinator final : public DistributedRocksDBService::Service {
   private:
    unordered_map<string, std::unique_ptr<DistributedRocksDBService::Stub>>
        stubs;
    string primary;
    unordered_set<string> backups;
    mutex systemStateLock;

    bool isPrimaryElected() { return !primary.empty(); }

    void setPrimary(const string& address) { primary = address; }

    bool isPrimary(const string& address) {
        lock_guard<mutex> guard(systemStateLock);

        return primary == address;
    }

    bool addNode(const string& address) {
        if (address == primary || backups.find(address) != backups.end()) {
            cout << __func__ << "\t : Node already present in the system."
                " Use different address!" << endl;
            return false;
        }

        lock_guard<mutex> guard(systemStateLock);

        stubs[address] = DistributedRocksDBService::NewStub(grpc::CreateChannel(
            address.c_str(), grpc::InsecureChannelCredentials()));

        if (isPrimaryElected()) {
            backups.insert(address);
        } else {
            setPrimary(address);
        }

        std::thread heartbeatThread(&Coordinator::establishHeartbeat, this,
                                    address);
        heartbeatThread.detach();

        return true;
    }

    void establishHeartbeat(string address) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Address = " << address << endl;
        }

        ClientContext context;
        Heartbeat heartbeatRes;

        std::unique_ptr<ClientWriter<SystemState>> writer(
            stubs[address]->rpc_heartbeat(&context, &heartbeatRes));

        while (true) {
            SystemState systemStateMsg;

            getSystemState(systemStateMsg);

            if (!writer->Write(systemStateMsg)) {
                cout << __func__ << "\t : Heartbeat connection broken." << endl;
                stubs.erase(address);
                if (isPrimary(address)) {
                    primary.clear();
                    cout << __func__ << "\t : Starting primary election now."
                         << endl;
                    electPrimary();
                }
                return;
            }

            msleep(HEARTBEAT_FREQUENCY);
        }

        writer->WritesDone();

        Status status = writer->Finish();

        if (!status.ok()) {
            if (debugMode <= DebugLevel::LevelError) {
                cout << __func__ << "\t : Status returned as "
                     << status.error_message() << endl;
            }
        }
    }

    void electPrimary() {
        if (backups.empty()) {
            cout << __func__
                 << "\t : Failed to elect a primary since there are no nodes "
                    "left!"
                 << endl;
            return;
        }

        {
            lock_guard<mutex> guard(systemStateLock);
            auto it = backups.begin();
            primary = *it;
            backups.erase(it);
        }

        cout << __func__ << "\t : Elected new primary = " << primary
             << ", and Number of backup nodes left = " << backups.size()
             << endl;
    }

    void getSystemState(SystemState &systemStateMsg) {
            lock_guard<mutex> guard(systemStateLock);

            systemStateMsg.set_primary(primary);

            for (auto backupAddress : backups) {
                systemStateMsg.add_backups(backupAddress);
            }
    }

   public:
    Coordinator() {}

    Status rpc_registerNewNode(ServerContext* context,
                               const RegisterRequest* rr,
                               RegisterResult* reply) override {
        const string& nodeAddress = rr->address();

        bool result = addNode(nodeAddress);

        if (result) {
            reply->set_result((isPrimary(nodeAddress) ? "primary" : "backup"));
            cout << __func__ << "\t : New node " << nodeAddress
                 << " has joined the system with role as " << reply->result()
                 << endl;
        }

        return Status::OK;
    }

    Status rpc_getSystemState(ServerContext* context,
                              const SystemStateRequest* wr,
                              SystemState* reply) override {
        
        getSystemState(*reply);

        return Status::OK;
    }
};

static Coordinator* coordinator;