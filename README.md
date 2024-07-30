# Go Redis Implementation

A Redis-like server implementation in Go, featuring basic Redis functionality and commands.

## Features

- TCP server implementation
- RESP (Redis Serialization Protocol) support
- In-memory key-value store
- Command handling for:
  - PING
  - ECHO
  - SET
  - GET
- Key expiration support
- Concurrent client handling

## Supported Commands

- `PING`: Test server connection
- `ECHO`: Echo the given string
- `SET key value [EX seconds]`: Set a key-value pair, optionally with expiration
- `GET key`: Retrieve the value of a key
