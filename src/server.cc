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

void RunServer() {
    ServerBuilder builder;

    builder.AddListeningPort(my_address, grpc::InsecureServerCredentials());

    builder.RegisterService(serverReplication);

    std::unique_ptr<Server> server(builder.BuildAndStart());

    std::cout << "Server listening on " << my_address << std::endl;

    server->Wait();
}

int main(int argc, char** argv) {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);
    cout.tie(nullptr);
    
    srand(time(NULL));

    string argumentString;

    if (argc > 1) {
        for (int arg = 1; arg < argc; arg++) {
            argumentString.append(argv[arg]);
            argumentString.push_back(' ');
        }
        role = parseArgument(argumentString, "--role=");
        my_address = parseArgument(argumentString, "--my_address=");
        other_address = parseArgument(argumentString, "--other_address=");
        crashTestingEnabled = parseArgument(argumentString, "--crash=") == "true" ? true : false;

        if (!isRoleValid(role) || !isIPValid(my_address) || !isIPValid(other_address)) {
            cout << "Role = " << role << "\nMy Address = " << my_address << 
            "\nOther Address = " << other_address << endl;
            role = "";
        }
    }

    if (role.empty() || my_address.empty()) {
        printf(
            "Enter arguments like below and try again - \n"
            "./server --role=[primary or backup] "
            "--my_address=[IP:PORT] --other_address=[IP:PORT]\n");
        return 0;
    }

    cout << "Role = " << role << "\nMy Address = " << my_address
         << "\nOther Address = " << other_address << endl;

    kDBPath = kDBPath + my_address;

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

    serverReplication = new ServerReplication(grpc::CreateChannel(
        other_address.c_str(), grpc::InsecureChannelCredentials()));

    rollbackUncommittedWrites();

    backupLastWriteTime.clear();

    isBackupAvailable = true;
    heartbeatShouldRun = true;
    heartbeatThread = thread(runHeartbeat);

    RunServer();

    heartbeatShouldRun = false;
    heartbeatThread.join();

    delete serverReplication;
    delete db;

    return 0;
}

/*
Example server commands:
For primary, in src folder : ./server --role=primary --my_address=0.0.0.0:50051 --other_address=0.0.0.0:50053
For backup, in folder BackupServer: ./server --role=backup --my_address=0.0.0.0:50053 --other_address=0.0.0.0:50051
*/