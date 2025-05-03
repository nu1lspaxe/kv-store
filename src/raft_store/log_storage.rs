use std::path::Path;
use std::{error::Error, io::Cursor, ops::RangeBounds, sync::Arc};
use async_std::sync::RwLock;
use openraft::{
    AnyError, BasicNode, Entry, EntryPayload, ErrorVerb, LogId, LogState, OptionalSend, RaftLogReader, RaftSnapshotBuilder, RaftStorage, RaftTypeConfig, SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote
};
use openraft::storage::Snapshot;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use std::fmt::Debug;
use serde::{Deserialize, Serialize};

use crate::raft_store::rocks_client::{ClientError, RocksRequest};

use super::rocks_client::RocksResponse;
use super::{
    state_machine::{RocksStateMachine, SerializableRocksStateMachine}, 
    type_config::{RocksNodeId, StorageResult, TypeConfig}
};

// Converts an id to a byte vector for storing in the database
// Using big endian encoding to ensure correct sorting of keys
fn id_to_bytes(id: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8);
    buf.write_u64::<BigEndian>(id).unwrap();
    buf
}

fn bytes_to_id(buf: &[u8]) -> u64 {
    (&buf[0..8]).read_u64::<BigEndian>().unwrap()
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RocksSnapshot {
    pub meta: SnapshotMeta<RocksNodeId, BasicNode>,

    // The data of the state machine at the time of this snapshot
    pub data: Vec<u8>,
}

mod meta {
    use openraft::ErrorSubject;
    use openraft::LogId;
    use serde::de::DeserializeOwned;
    use serde::Serialize;

    use crate::raft_store::type_config::RocksNodeId;
    use crate::raft_store::log_storage::RocksSnapshot;

    pub(crate) trait StoreMeta {
        /// The key used to store in rocksdb
        const KEY: &'static str;
        /// The type of the value to store
        type Value: Serialize + DeserializeOwned;
        /// The subject this meta belongs to, and will be embedding into the returned storage error
        fn subject(v: Option<&Self::Value>) -> ErrorSubject<RocksNodeId>;
    }

    pub(crate) struct LastPurged {}
    pub(crate) struct SnapshotIndex {}
    pub(crate) struct Vote {}
    pub(crate) struct Snapshot {}

    impl StoreMeta for LastPurged {
        const KEY: &'static str = "last_purged_log_id";
        type Value = LogId<u64>;

        fn subject(_v: Option<&Self::Value>) -> ErrorSubject<RocksNodeId> {
            ErrorSubject::Store
        }
    }

    impl StoreMeta for SnapshotIndex {
        const KEY: &'static str = "snapshot_index";
        type Value = u64;

        fn subject(_v: Option<&Self::Value>) -> ErrorSubject<RocksNodeId> {
            ErrorSubject::Store
        }
    }

    impl StoreMeta for Vote {
        const KEY: &'static str = "vote";
        type Value = openraft::Vote<RocksNodeId>;

        fn subject(_v: Option<&Self::Value>) -> ErrorSubject<RocksNodeId> {
            ErrorSubject::Vote
        }
    }

    impl StoreMeta for Snapshot {
        const KEY: &'static str = "snapshot";
        type Value = RocksSnapshot;

        fn subject(v: Option<&Self::Value>) -> ErrorSubject<RocksNodeId> {
            ErrorSubject::Snapshot(Some(v.unwrap().meta.signature()))
        }
    }
}

pub struct RocksStore {
    db: Arc<DB>,

    pub state_machine: RwLock<RocksStateMachine>,
}

impl RocksStore {
    fn cf_meta(&self) -> &rocksdb::ColumnFamily {
        self.db.cf_handle("meta").unwrap()
    }

    fn cf_logs(&self) -> &rocksdb::ColumnFamily {
        self.db.cf_handle("logs").unwrap()
    }

    fn get_meta<M: meta::StoreMeta>(&self) -> Result<Option<M::Value>, StorageError<RocksNodeId>> {
        let v = self
            .db
            .get_cf(self.cf_meta(), M::KEY)
            .map_err(|e| StorageIOError::new(M::subject(None), ErrorVerb::Read, AnyError::new(&e)))?;

        let t = match v {
            None => None,
            Some(bytes) => Some(
                serde_json::from_slice(&bytes)
                    .map_err(|e| StorageIOError::new(M::subject(None), ErrorVerb::Read, AnyError::new(&e)))?
            ),
        };
        Ok(t)
    }

    fn put_meta<M: meta::StoreMeta>(&self, value: &M::Value) -> Result<(), StorageError<RocksNodeId>> {
        let json_value = serde_json::to_vec(value)
            .map_err(|e| StorageIOError::new(M::subject(Some(value)), ErrorVerb::Write, AnyError::new(&e)))?;

        self.db
            .put_cf(self.cf_meta(), M::KEY, json_value)
            .map_err(|e| StorageIOError::new(M::subject(Some(value)), ErrorVerb::Write, AnyError::new(&e)))?;
        Ok(())
    }

    pub async fn new<P: AsRef<Path>>(db_path: P) -> Arc<RocksStore> {
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let meta = ColumnFamilyDescriptor::new("meta", Options::default());
        let sm_meta = ColumnFamilyDescriptor::new("sm_meta", Options::default());
        let sm_data = ColumnFamilyDescriptor::new("sm_data", Options::default());
        let logs = ColumnFamilyDescriptor::new("logs", Options::default());

        let db = DB::open_cf_descriptors(&db_opts, db_path, vec![meta, sm_meta, sm_data, logs]).unwrap();

        let db = Arc::new(db);
        let state_machine = RwLock::new(RocksStateMachine::new(db.clone()));
        Arc::new(RocksStore { db, state_machine })
    }
}

fn read_logs_err(e: impl Error + 'static) -> StorageError<RocksNodeId> {
    StorageError::IO {
        source: StorageIOError::read_logs(&e),
    }
}

impl RaftLogReader<TypeConfig> for Arc<RocksStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry<TypeConfig>>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(x) => id_to_bytes(*x),
            std::ops::Bound::Excluded(x) => id_to_bytes(*x + 1),
            std::ops::Bound::Unbounded => id_to_bytes(0),
        };

        let mut res = Vec::new();

        let it = self.db.iterator_cf(
            self.cf_logs(), 
            rocksdb::IteratorMode::From(&start, rocksdb::Direction::Forward)
        );
        for item in it {
            let (id, val) = item.map_err(read_logs_err)?;

            let id = bytes_to_id(&id);
            if !range.contains(&id) {
                break;
            }

            let entry: Entry<_> = serde_json::from_slice(&val).map_err(read_logs_err)?;

            assert_eq!(id, entry.log_id.index);
            res.push(entry);
        }

        Ok(res)
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<RocksStore> {
    #[tracing::instrument(level = "trace", skip(self))]
    async  fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<RocksNodeId>> {
        let data;
        let last_applied_log;
        let last_membership;

        {
            let state_machine = SerializableRocksStateMachine::from(&*self.state_machine.read().await);

            data = serde_json::to_vec(&state_machine).map_err(|e| StorageIOError::read_state_machine(&e))?;

            last_applied_log = state_machine.last_applied_log;
            last_membership = state_machine.last_membership;
        }

        let snapshot_idx: u64 = self.get_meta::<meta::SnapshotIndex>()?.unwrap_or_default() + 1;
        self.put_meta::<meta::SnapshotIndex>(&snapshot_idx)?;

        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta  {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id
        };

        let snapshot = RocksSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        self.put_meta::<meta::Snapshot>(&snapshot)?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data))
        })
    }
}

impl RaftStorage<TypeConfig> for Arc<RocksStore> {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async  fn get_log_state(&mut self) -> StorageResult<LogState<TypeConfig>> {
        let last = self.db.iterator_cf(self.cf_logs(), rocksdb::IteratorMode::End).next();

        let last_log_id = match last {
            None => None,
            Some(res) => {
                let (_, entry_bytes) = res.map_err(read_logs_err)?;
                let ent = serde_json::from_slice::<Entry<TypeConfig>>(&entry_bytes).map_err(read_logs_err)?;
                Some(ent.log_id)
            }
        };

        let last_purged_log_id = self.get_meta::<meta::LastPurged>()?;

        let last_log_id = match last_log_id {
            None => last_purged_log_id,
            Some(x) => Some(x),
        };

        Ok(LogState { 
            last_purged_log_id: last_purged_log_id, 
            last_log_id: last_log_id,
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn save_vote(&mut self, vote: &Vote<RocksNodeId>) -> Result<(), StorageError<RocksNodeId>> {
        self.put_meta::<meta::Vote>(vote)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<RocksNodeId>>, StorageError<RocksNodeId>> {
        self.get_meta::<meta::Vote>()
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn append_to_log<I>(&mut self, entries: I) -> StorageResult<()>
    where 
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend
    {
        for entry in entries {
            let id = id_to_bytes(entry.log_id.index);
            assert_eq!(bytes_to_id(&id), entry.log_id.index);
            self.db
                .put_cf(
                    self.cf_logs(), 
                    id, 
                serde_json::to_vec(&entry).map_err(|e| StorageIOError::write_logs(&e))?,
                )
                .map_err(|e| StorageIOError::write_logs(&e))?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_conflict_logs_since(&mut self, log_id: LogId<RocksNodeId>) -> StorageResult<()> {
        tracing::debug!("delete_log: [{:?}, +oo)", log_id);

        let from = id_to_bytes(log_id.index);
        let to = id_to_bytes(0xff_ff_ff_ff_ff_ff_ff_ff);
        self.db
            .delete_range_cf(self.cf_logs(), &from, &to)
            .map_err(|e| StorageIOError::write_logs(&e).into())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn purge_logs_upto(&mut self, log_id: LogId<RocksNodeId>) -> Result<(), StorageError<RocksNodeId>> {
        tracing::debug!("delete_log: [0, {:?}]", log_id);

        self.put_meta::<meta::LastPurged>(&log_id)?;

        let from = id_to_bytes(0);
        let to = id_to_bytes(log_id.index + 1);
        self.db
            .delete_range_cf(self.cf_logs(), &from, &to)
            .map_err(|e| StorageIOError::write_logs(&e).into())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<RocksNodeId>>, StoredMembership<RocksNodeId, BasicNode>), StorageError<RocksNodeId>> {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.get_last_applied_log()?,
            state_machine.get_last_membership()?,
        ))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<RocksResponse>, StorageError<RocksNodeId>> {
        let mut res = Vec::with_capacity(entries.len());

        let sm = self.state_machine.write().await;

        for entry in entries {
            tracing::debug!(%entry.log_id, "replicate to sm");

            sm.set_last_applied_log(&entry.log_id)?;

            match &entry.payload {
                EntryPayload::Blank => res.push(RocksResponse::Put(Ok(()))),
                EntryPayload::Normal(req) => match req {
                    RocksRequest::Put { key, value } => {
                        let result = sm.put(key.clone(), value.clone()).map_err(|e| {
                            ClientError::InternalError(e.to_string())
                        });
                        res.push(RocksResponse::Put(result));
                    }
                    RocksRequest::Delete { key } => {
                        let result = sm.delete(key.to_string()).map_err(|e| {
                            ClientError::InternalError(e.to_string())
                        });
                        res.push(RocksResponse::Delete(result));
                    }
                },
                EntryPayload::Membership(mem) => {
                    sm.set_last_membership(StoredMembership::new(Some(entry.log_id), mem.clone()))?;
                    res.push(RocksResponse::Put(Ok(())));
                }
            };
        }
        self.db.flush_wal(true).map_err(|e| StorageIOError::write_logs(&e))?;
        Ok(res)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<TypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<RocksNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<RocksNodeId, BasicNode>,
        snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<RocksNodeId>> {
        tracing::info!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );

        let new_snapshot = RocksSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        // Update the state machine
        {
            let updated_state_machine: SerializableRocksStateMachine = serde_json::from_slice(&new_snapshot.data)
                .map_err(|e| StorageIOError::read_snapshot(Some(new_snapshot.meta.signature()), &e))?;
            let mut state_machine = self.state_machine.write().await;
            *state_machine = RocksStateMachine::from_serializable(updated_state_machine, self.db.clone())?;
        }

        self.put_meta::<meta::Snapshot>(&new_snapshot)?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StorageError<RocksNodeId>> {
        let curr_snap = self.get_meta::<meta::Snapshot>()?;

        match curr_snap {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta,
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        }
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}
