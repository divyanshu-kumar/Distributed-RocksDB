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

    std::thread registerThread(registerServer);
    registerThread.detach();

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
        
        my_address = parseArgument(argumentString, "--my_address=");
        coordinator_address = parseArgument(argumentString, "--coordinator_address=");
        
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

    serverReplication = new ServerReplication;

    backupLastWriteTime.clear();

    RunServer();

    delete serverReplication;
    delete db;

    return 0;
}

/*
Example server commands (my_address should be unique to this server):
In src folder : ./server --my_address=0.0.0.0:50051 --coordinator_address=0.0.0.0:50053
*/