# Key-Value Store

This is a simple key-value store implemented in Rust using [Tonic](https://github.com/hyperium/tonic) and [Tokio](https://github.com/tokio-rs/tokio).

The key-value store is implemented as a gRPC service that allows clients to put and get key-value pairs.

## Phase
- [x] Store key-value pairs in memory and persist them to a json file on disk.
- [ ] Import Raftconsensus algorithm and use it to ensure that the key-value store is consistent across all nodes.
- [ ] Watch mechanism (like etcd) to detect changes in the key-value store and notify other nodes.
- [ ] Utilize RocksDB as the underlying storage engine for the key-value store.
