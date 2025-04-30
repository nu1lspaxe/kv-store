use std::{collections::BTreeMap, error::Error, sync::Arc};

use openraft::{BasicNode, LogId, StorageError, StorageIOError, StoredMembership};
use rocksdb::{ColumnFamily, DB};
use serde::{Deserialize, Serialize};

use super::type_config::{RocksNodeId, StorageResult};



#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct SerializableRocksStateMachine {
    pub last_applied_log: Option<LogId<RocksNodeId>>,
    
    pub last_membership : StoredMembership<RocksNodeId, BasicNode>,

    pub data: BTreeMap<String, String>,
}

impl From<&RocksStateMachine> for SerializableRocksStateMachine {
    fn from(state_machine: &RocksStateMachine) -> Self {
        let mut data = BTreeMap::new();

        let iter = state_machine.db.iterator_cf(
            state_machine.cf_sm_data(), rocksdb::IteratorMode::Start
        );

        for item in iter {
            let (key, value) = item.expect("invalid kv record");

            let key: &[u8] = &key;
            let value: &[u8] = &value;
            data.insert(
                String::from_utf8(key.to_vec()).expect("invalid key"),
                String::from_utf8(value.to_vec()).expect("invalid data"),
            );
        }

        Self {
            last_applied_log: state_machine.get_last_applied_log().expect("last_applied_log"),
            last_membership: state_machine.get_last_membership().expect("last_membership"),
            data,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RocksStateMachine {
    pub db: Arc<DB>,
}

fn sm_r_err<E: Error + 'static>(e: E) -> StorageError<RocksNodeId> {
    StorageIOError::read_state_machine(&e).into()
}

fn sm_w_err<E: Error + 'static>(e: E) -> StorageError<RocksNodeId> {
    StorageIOError::write_state_machine(&e).into()
}

impl RocksStateMachine {
    fn cf_sm_meta(&self) -> &ColumnFamily {
        self.db.cf_handle("sm_meta").unwrap()
    }

    fn cf_sm_data(&self) -> &ColumnFamily {
        self.db.cf_handle("sm_data").unwrap()
    }

    fn get_last_membership(&self) -> StorageResult<StoredMembership<RocksNodeId, BasicNode>> {
        self.db
            .get_cf(self.cf_sm_meta(),"last_membership".as_bytes())
            .map_err(sm_r_err)
            .and_then(|value| {
                value
                .map(|v| serde_json::from_slice(&v).map_err(sm_r_err))
                .unwrap_or_else(|| Ok(StoredMembership::default()))
            })
    }

    fn set_last_membership(&self, membership: StoredMembership<RocksNodeId, BasicNode>) -> StorageResult<()> {
        self.db
            .put_cf(
                self.cf_sm_meta(), 
                "last_membership".as_bytes(), 
                serde_json::to_vec(&membership).map_err(sm_w_err)?
            )
            .map_err(sm_w_err)
    }

    fn get_last_applied_log(&self) -> StorageResult<Option<LogId<RocksNodeId>>> {
        self.db
            .get_cf(self.cf_sm_meta(), "last_applied_log".as_bytes())
            .map_err(sm_r_err)
            .and_then(|value| {
                value
                    .map(|v| serde_json::from_slice(&v).map_err(sm_r_err))
                    .transpose()
            })
    }

    fn set_last_applied_log(&self, log_id: &LogId<RocksNodeId>) -> StorageResult<()> {
        self.db
            .put_cf(
                self.cf_sm_meta(), 
                "last_applied_log".as_bytes(), 
                serde_json::to_vec(log_id).map_err(sm_w_err)?
            )
            .map_err(sm_w_err)
    }

    fn from_serializable(
        sm: SerializableRocksStateMachine, 
        db: Arc<DB>
    ) -> StorageResult<Self> {
        let r = Self { db };

        for (key, value) in sm.data {
            r.db.put_cf(r.cf_sm_data(), key.as_bytes(), value.as_bytes()).map_err(sm_w_err)?;
        }

        if let Some(log_id) = sm.last_applied_log {
            r.set_last_applied_log(&log_id)?;
        } 

        r.set_last_membership(sm.last_membership)?;

        Ok(r)
    }

    fn new(db: Arc<DB>) -> RocksStateMachine {
        Self { db }
    }

    fn insert(&self, key: String, value: String) -> StorageResult<()> {
        self.db
            .put_cf(self.cf_sm_data(), key.as_bytes(), value.as_bytes())
            .map_err(|e|StorageIOError::write(&e).into())
    }

    pub fn get(&self, key: String) -> StorageResult<Option<String>> {
        let key = key.as_bytes();

        self.db
            .get_cf(self.cf_sm_data(), key)
            .map(|value| value.map(|v| String::from_utf8(v).expect("invalid value")))
            .map_err(|e| StorageIOError::read(&e).into())
    }

    pub fn put(&self, key: String, value: String) -> StorageResult<()> {
        self.insert(key, value)
    }

    pub fn delete(&self, key: String) -> StorageResult<()> {
        self.db
            .delete_cf(self.cf_sm_data(), key.as_bytes())
            .map_err(|e| StorageIOError::write(&e).into())
    }
}
