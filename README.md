# Distributed-RocksDB
A distributed version of the popular RocksDB

First time installation (in dir = DistributedRocksDB):
```
source installation_script.sh
```

To build (in dir = DistributedRocksDB/src):
```
source make.sh
```

Run coordinator:
```
./coordinator --my_address=0.0.0.0:50050
```

For running server (primary/backup):
```
./server --my_address=0.0.0.0:50051 --coordinator_address=0.0.0.0:50050
```
(my_address needs to be unique for each server).

Then run client:
```
./client --address1=0.0.0.0:50051
```
(for now pass the address of primary. Eventually this address will be foundout from the coordinator node.)

Using CMake:
```
./build.sh
```