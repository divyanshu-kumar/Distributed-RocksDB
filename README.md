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

First run server:
```
./server --role=primary --my_address=0.0.0.0:50051 --other_address=0.0.0.0:50053
```

Optionally run backup server:
```
./server --role=backup --my_address=0.0.0.0:50053 --other_address=0.0.0.0:50051
```

Then run client:
```
./client
```
