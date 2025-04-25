use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use rocksdb::{DB, Options};

#[derive(Debug)]
pub struct KvStore {
    db: DB,
}

impl KvStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, path).expect("Failed to open RocksDB");
        KvStore { db }
    }

    pub async fn put(&self, key: String, value: String) -> Result<(), rocksdb::Error> {
        self.db.put(key, value)?;
        Ok(())
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        match self.db.get(key) {
            Ok(Some(value)) => String::from_utf8(value).ok(),
            _ => None,
        }
    }
}

pub type SharedKvStore = Arc<RwLock<KvStore>>;
