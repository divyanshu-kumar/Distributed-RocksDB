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

class ClusterCoordinator {
    public:
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
            cout << __func__ << "\t : Primary is elected, making it a backup node." << endl;
            backups.insert(address);
        } else {
            cout << __func__ << "\t : Primary is not elected, making it a primary node." << endl;
            setPrimary(address);
        }

        return true;
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
            if (!isPrimaryElected()) {
                return;
            }

            systemStateMsg.set_primary(primary);

            for (auto backupAddress : backups) {
                systemStateMsg.add_backups(backupAddress);
            }
    }
};

class MasterCoordinator final : public DistributedRocksDBService::Service {
    int numClusters;
    vector<int> keyRanges;
    vector<ClusterCoordinator*> clusterCoordinators;

    void establishHeartbeat(string address, int clusterId) {
        if (debugMode <= DebugLevel::LevelInfo) {
            cout << __func__ << "\t : Address = " << address << endl;
        }

        ClientContext context;
        Heartbeat heartbeatRes;

        std::unique_ptr<ClientWriter<SystemStateResult>> writer(
            clusterCoordinators[clusterId]->stubs[address]->rpc_heartbeat(&context, &heartbeatRes));

        while (true) {
            SystemStateResult clusterSystemState;

            for (int cluster = 0; cluster < numClusters; cluster++) {
                SystemState clusterReply;
                clusterCoordinators[clusterId]->getSystemState(clusterReply);

                SystemState *state = clusterSystemState.add_systemstate();
                state->set_primary(clusterReply.primary());
                state->set_keyrange(keyRanges[cluster]);
                
                for (auto &backup : clusterReply.backups()) {
                    state->add_backups(backup);
                }
            }

            if (!writer->Write(clusterSystemState)) {
                cout << __func__ << "\t : Heartbeat connection broken." << endl;
                clusterCoordinators[clusterId]->stubs.erase(address);
                if (clusterCoordinators[clusterId]->isPrimary(address)) {
                    clusterCoordinators[clusterId]->primary.clear();
                    cout << __func__ << "\t : Starting primary election now."
                         << endl;
                    clusterCoordinators[clusterId]->electPrimary();
                }
                else {
                    clusterCoordinators[clusterId]->backups.erase(address);
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

   public:
    MasterCoordinator(int num_clusters) :  
        numClusters(num_clusters) {
        int totalRange = NUM_MAX_CLUSTERS;
        int singleClusterRange = totalRange / numClusters;
        for (int i = 0; i < numClusters; i++) {
            clusterCoordinators.push_back(new ClusterCoordinator());
            keyRanges.push_back(singleClusterRange * (i + 1));
        }
    }

    ~MasterCoordinator() {
        for (int i = 0; i < numClusters; i++) {
            delete clusterCoordinators[i];
        }
    }

    Status rpc_registerNewNode(ServerContext* context,
                               const RegisterRequest* rr,
                               RegisterResult* reply) override {
        const string& nodeAddress = rr->address();
        const int clusterId = rr->clusterid();

        if (clusterId < 0 || clusterId >= numClusters) {
            cout << __func__ << "\t : INVALID_CLUSTER_ID " << endl;
            reply->set_result("INVALID_CLUSTER_ID");
            return Status::OK;
        }

        bool result = clusterCoordinators[clusterId]->addNode(nodeAddress);

        std::thread heartbeatThread(&MasterCoordinator::establishHeartbeat, this,
                                    nodeAddress, clusterId);
        heartbeatThread.detach();

        if (result) {
            reply->set_result(
                (clusterCoordinators[clusterId]->isPrimary(nodeAddress) ? "primary" : "backup")
                );
            cout << __func__ << "\t : New node " << nodeAddress
                 << " has joined the cluster " << clusterId << " with role as " << reply->result()
                 << endl;
        }

        return Status::OK;
    }

    Status rpc_getSystemState(ServerContext* context,
                              const SystemStateRequest* rr,
                              SystemStateResult* reply) override {
        
        for (int cluster = 0; cluster < numClusters; cluster++) {
            SystemState clusterReply;
            clusterCoordinators[cluster]->getSystemState(clusterReply);

            SystemState *state = reply->add_systemstate();
            state->set_primary(clusterReply.primary());
            state->set_keyrange(keyRanges[cluster]);

            for (auto &backup : clusterReply.backups()) {
                state->add_backups(backup);
            }
        }

        return Status::OK;
    }
};

static MasterCoordinator* masterCoordinator;

