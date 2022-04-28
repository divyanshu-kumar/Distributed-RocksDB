#include <cstdio>
#include <string>
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "server.h"

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_distributed";
#else
std::string kDBPath = "/tmp/rocksdb/";
#endif

string getPrimaryAddress() {
    std::unique_ptr<DistributedRocksDBService::Stub> coordinator_stub_(
        DistributedRocksDBService::NewStub(grpc::CreateChannel(
            coordinator_address.c_str(), grpc::InsecureChannelCredentials())));

    ClientContext clientContext;
    SystemStateRequest request;
    SystemStateResult reply;

    Status status = coordinator_stub_->rpc_getClusterSystemState(&clientContext, request, &reply);

    if (!status.ok()) {
        // Kill system
    }

    return (reply.systemstate())[serverReplication->getClusterId()].primary();
}

void flush_recovery(RecoverReply reply) {
    string logPath = computeLogPath(storage_path, reply.logindex());

    vector<std::string> keys;

    for(auto txn: reply.txns()) {
        keys.push_back(txn.key());
    }

    ofstream logFile(logPath);
    ostream_iterator<string> output_iterator(logFile, "\n");
    copy(keys.begin(), keys.end(), output_iterator);
}

void dumpToDB(RecoverReply reply) {
    for(auto txn: reply.txns()) {
        serverReplication->writeToDB(txn.key(), txn.value());
    }
}

void RunRecovery() {
    // 1. Get primary address from the coordinator
    string primary_addr = getPrimaryAddress();

    // if there is no primary then this is the first server
    if (primary_addr.empty()) {
        sem_post(&sem_recovery);
        cout << "[INFO:RunRecovery]: recovery done" << endl;

        // no need to wait for register as well
        return;
    }

    std::unique_ptr<DistributedRocksDBService::Stub> primary_stub_(
        DistributedRocksDBService::NewStub(grpc::CreateChannel(
            primary_addr.c_str(), grpc::InsecureChannelCredentials())));

    // 2. contact primary for logs
    ClientContext context;
    std::shared_ptr<ClientReaderWriter<RecoverRequest, RecoverReply> > stream(
            primary_stub_->rpc_recover(&context));
    
    RecoverRequest request;
    RecoverReply reply;
    int lastLogIndex = computeLastLogIndex(storage_path);

    request.set_reqtype(RecoveryRequestType::GET_TXNS);
    request.set_lastlogindex(lastLogIndex);

    stream->Write(request);

    while(1) {
        stream->Read(&reply);

        if (reply.replytype() == RecoveryReplyType::TXNS_DONE) {
            break;
        }

        // write to DB
        dumpToDB(reply);

         // flush custom
        flush_recovery(reply);
    }

    // 3. sem signal recovery done
    sem_post(&sem_recovery);
    cout << "[INFO:RunRecovery]: recovery done" << endl;

    // 4. wait for register
    cout << "[INFO-RunRecover]: waiting for register" << endl;
    sem_wait(&sem_register);
    cout << "[INFO-RunRecovery]: register done" << endl;

    // 5. tell primary that I am up
    request.set_reqtype(RecoveryRequestType::REG_DONE);
    stream->Write(request);

    stream->WritesDone();
    Status status = stream->Finish();
}

void RunServer(int clusterId) {
    ServerBuilder builder;

    builder.AddListeningPort(my_address, grpc::InsecureServerCredentials());

    builder.RegisterService(serverReplication);

    std::unique_ptr<Server> server(builder.BuildAndStart());

    std::cout << "Server listening on " << my_address << std::endl;

    std::thread registerThread(registerServer, clusterId);
    registerThread.detach();

    server->Wait();
}

int main(int argc, char** argv) {
    // ios::sync_with_stdio(true);
    // cin.tie(nullptr);
    // cout.tie(nullptr);
    
    srand(time(NULL));

    string argumentString;
    int clusterId;

    if (argc > 1) {
        for (int arg = 1; arg < argc; arg++) {
            argumentString.append(argv[arg]);
            argumentString.push_back(' ');
        }
        
        my_address = parseArgument(argumentString, "--my_address=");
        coordinator_address = parseArgument(argumentString, "--coordinator_address=");
        string clusterStr = parseArgument(argumentString, "--cluster_id=");

        if (clusterStr.empty()) {
            clusterId = 0;
        }
        else {
            clusterId = stoi(clusterStr);
        }

        // only for dev purpose, take default address of coordinator to be 0.0.0.0:50051
        if (coordinator_address.empty()) {
            coordinator_address = "0.0.0.0:50051";
        }

        writeThreadPoolEnabled = parseArgument(argumentString, "--writeThreadPool=") == "true" ? true : false;
        crashTestingEnabled = parseArgument(argumentString, "--crash=") == "true" ? true : false;

        if (!isIPValid(my_address) || !isIPValid(coordinator_address)) {
            cout << "\nMy Address = " << my_address << ", Coordinator Address = " << coordinator_address << endl;
            my_address = "";
        }
    }

    if (my_address.empty() || coordinator_address.empty()) {
        printf(
            "Enter arguments like below and try again - \n"
            "./server --my_address=[IP:PORT] --coordinator_address=[IP:PORT]\n");
        return 0;
    }

    cout << "\nMy Address = " << my_address << ", Coordinator Address = " << coordinator_address << endl;

    kDBPath = kDBPath + my_address;
    storage_path = kDBPath + "/logs";

    system(string("mkdir -p " + storage_path).c_str());

    cout << "DB path = " << kDBPath << endl;
    Options options;
    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    // create the DB if it's not already present
    options.create_if_missing = true;

    // open DB
    ROCKSDB_NAMESPACE::Status s = DB::Open(options, kDBPath, &db);
    assert(s.ok());

    // sem inits
    sem_init(&sem_register, 0, 0);
    sem_init(&sem_recovery, 0, 0);

    serverReplication = new ServerReplication(clusterId);

    // recovery thread
    std::thread recoveryThread(RunRecovery);
    recoveryThread.detach();

    backupLastWriteTime.clear();
    
    RunServer(clusterId);

    delete serverReplication;
    delete db;
    sem_destroy(&sem_register);
    sem_destroy(&sem_recovery);

    return 0;
}

/*
Example server commands (my_address should be unique to this server):
In src folder : ./server --my_address=0.0.0.0:50051 --coordinator_address=0.0.0.0:50053
*/