#include <cstdio>
#include <iostream>
#include <string>

#include "coordinator.h"

void RunServer()
{
    ServerBuilder builder;

    builder.AddListeningPort(my_address, grpc::InsecureServerCredentials());

    builder.RegisterService(coordinator);

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

    if (argc > 1) {
        for (int arg = 1; arg < argc; arg++) {
            argumentString.append(argv[arg]);
            argumentString.push_back(' ');
        }

        my_address = parseArgument(argumentString, "--my_address=");

        if (!isIPValid(my_address)) {
            cout << "\nMy Address = " << my_address << endl;
            my_address = "";
        }
    }

    if (my_address.empty()) {
        printf("Enter arguments like below and try again - \n"
               "./server --my_address=[IP:PORT]\n");
        return 0;
    }

    cout << "\nMy Address = " << my_address << endl;

    coordinator = new Coordinator();

    RunServer();

    delete coordinator;

    return 0;
}