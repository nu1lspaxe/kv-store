use std::sync::Arc;
use openraft::storage::RaftLogStorage;
use rocksdb::DB;
use async_trait::async_trait;

use super::type_config::{RocksNodeId, TypeConfig};


pub struct RocksStore {
    db: Arc<DB>,
}

