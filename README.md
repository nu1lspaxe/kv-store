# Key-Value Store

This is a simple key-value store implemented in Rust using [Tonic](https://github.com/hyperium/tonic) and [Tokio](https://github.com/tokio-rs/tokio).

The key-value store is implemented as a gRPC service that allows clients to put and get key-value pairs.

## Get Started
```bash
# start the server
cargo run --bin kv-store 
# start the client
cargo run --bin kv-cli
```

## Stages
- [x] Store key-value pairs in memory and persist them to a json file on disk.
- [x] Utilize RocksDB as the underlying storage engine for the key-value store.
- [x] Watch mechanism (like etcd) to detect changes in the key-value store and notify other nodes.
- [ ] Import Raft consensus algorithm and use it to ensure that the key-value store is consistent across all nodes.

## KvStore Service
- `put <key> <value>`
- `get <key>`
- `list [<prefix>]`
- `delete <key>`
- `delete_all`

## KvStore Client
```text
kv-cli/
├── src/
│   ├── main.rs         # entry point
│   ├── commands.rs     # put/get/list/exit/watch
│   ├── client.rs       # KvStoreClient implementation
│   └── utils.rs        # colored output, tab completion, etc.
├── Cargo.toml
```
