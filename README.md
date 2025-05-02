# Key-Value Store

## Stages
- [x] Store key-value pairs in memory and persist them to a json file on disk.
- [x] Utilize RocksDB as the underlying storage engine for the key-value store.
- [x] Watch mechanism (like etcd) to detect changes in the key-value store and notify other nodes.
- [x] Import Raft consensus algorithm and use it to ensure that the key-value store is consistent across all nodes.
- [ ] Implement a distributed key-value store using Raft consensus algorithm.
- [ ] Upgrade raft implementation from openraft 0.8 to 0.9
- [ ] Append watch mechanism to raft implementation


## Implementation 1 : Key-Value Store (RocksDB)

This is a simple key-value store implemented in Rust using [Tonic](https://github.com/hyperium/tonic) and [Tokio](https://github.com/tokio-rs/tokio).

The key-value store is implemented as a gRPC service that allows clients to put and get key-value pairs.

### Get Started
```bash
# start the server
cargo run --bin kv-store 
# start the client
cargo run --bin kv-cli
```

### Service
- `put <key> <value>`
- `get <key>`
- `list [<prefix>]`
- `delete <key>`
- `delete_all`

## Implementation 2 : Distributed Key-Value Store (Raft + RocksDB)