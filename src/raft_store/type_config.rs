use std::io::Cursor;
use openraft::{BasicNode, Entry, StorageError, TokioRuntime};

use super::rocks_client::{RocksRequest, RocksResponse};

pub type RocksNodeId = u64;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = RocksRequest,
        R = RocksResponse,
        NodeId = RocksNodeId,
        Node = BasicNode,
        Entry = Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = TokioRuntime
);

pub type StorageResult<T> = Result<T, StorageError<RocksNodeId>>;