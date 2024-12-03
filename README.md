# Go Redis Clone

A Redis-like server implementation in Go with replication support, featuring core Redis functionality and commands.

## Features

- RESP (Redis Serialization Protocol) support
- Master-Replica replication
- In-memory key-value store with persistence
- RDB file format support
- Concurrent client handling
- Key expiration support
- TCP server implementation

## Building and Running

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
./build/redis-clone -role replica -master localhost:6380
or
make run-replica
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
