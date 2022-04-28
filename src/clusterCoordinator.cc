#include <cstdio>
#include <iostream>
#include <string>

#include "clusterCoordinator.h"

void RunServer()
{
    ServerBuilder builder;

    builder.AddListeningPort(my_address, grpc::InsecureServerCredentials());

    builder.RegisterService(masterCoordinator);

    std::unique_ptr<Server> server(builder.BuildAndStart());

    std::cout << "Coordinator listening on " << my_address << std::endl;

    server->Wait();
}

int main(int argc, char** argv)
{
    ios::sync_with_stdio(false);
    cin.tie(nullptr);
    cout.tie(nullptr);

    string argumentString;
    string numClustersArg;

    int numClusters = 1;

    if (argc > 1) {
        for (int arg = 1; arg < argc; arg++) {
            argumentString.append(argv[arg]);
            argumentString.push_back(' ');
        }

        my_address = parseArgument(argumentString, "--my_address=");
        numClustersArg = parseArgument(argumentString, "--num_clusters=");

        if (!isIPValid(my_address)) {
            cout << "\nMy Address = " << my_address << endl;
            my_address = "";
        }

        if (!numClustersArg.empty()) {
            numClusters = stoi(numClustersArg);
            if (numClusters <= 0 || numClusters > NUM_MAX_CLUSTERS) {
                cout << __func__ << "\t : Clusters should be in the range of 1 to " 
                     << NUM_MAX_CLUSTERS << ". Making it 1." << endl;
                numClusters = 1;
            }
            else {
                cout << __func__ << "\t : Num Clusters = " << numClusters << endl;
            }
        }
    }

    if (my_address.empty()) {
        printf("Enter arguments like below and try again - \n"
               "./server --my_address=[IP:PORT]\n");
        return 0;
    }

    cout << "\nMy Address = " << my_address << endl;

    masterCoordinator = new MasterCoordinator(numClusters);

    RunServer();

    delete masterCoordinator;

    return 0;
}