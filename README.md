# Go Redis Clone

A Redis-like server implementation in Go with replication support, featuring core Redis functionality and commands.

## Features

- RESP (Redis Serialization Protocol) support
- Master-Replica replication with **FULLSYNC** and **PSYNC**
- **Asynchronous** command propagation
- **ACK-based** replication tracking
- In-memory key-value store with **persistence**
- **RDB** file format support with streaming transfer
- Concurrent client handling
- Key expiration support
- TCP server implementation

### Build the application:

```bash
make build
```
### Running as Master
```bash
./build/redis-clone -role master -replication-port 6380
or
make run-master
```
### Running as Replica
```bash
./build/redis-clone -role replica -master localhost:6380 -port 5379 -dbfilename "replica_1.rdb"
./build/redis-clone -role replica -master localhost:6380 -port 5380 -dbfilename "replica_2.rdb" 
or
make run-replica
make run-replicas (multiple replicas)
```
## Supported Commands

| Command | Description |
|---------|-------------|
| PING | Test server connection |
| ECHO | Echo the given string |
| SET key value [EX seconds] | Set a key-value pair with optional expiration |
| GET key | Retrieve value of a key |
| INFO | Display replication information |
| CONFIG GET parameter | Get configuration parameters |
| SAVE | Create RDB snapshot |
| REPLCONF | Handle replication configuration |
| REPLCONF ACK | Acknowledge received offset |
| REPLCONF getack | Get current replication offset |
| PSYNC replid offset | Initiate partial synchronization |
| CONTINUE | Continue with partial synchronization |
| FULLSYNC | Complete dataset transfer with RDB streaming |


## Replication Features
- Full synchronization (PSYNC) support
- RDB file transfer for initial sync
- Asynchronous command propagation
- ACK-based command tracking
- Replica offset management
- Command buffering using channels
