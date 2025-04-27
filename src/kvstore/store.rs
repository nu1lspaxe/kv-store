use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use rocksdb::{DB, Options};
use tokio::sync::broadcast;

#[derive(Debug)]
pub struct KvStore {
    db: DB,
    pub watcher: broadcast::Sender<(String, String, String)>, // (key, value, op)
}

impl KvStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, path).expect("Failed to open RocksDB");
        let (tx, _) = broadcast::channel(32);
        KvStore { db, watcher: tx }
    }

    pub async fn put(&self, key: String, value: String) -> Result<(), rocksdb::Error> {
        self.db.put(&key, &value)?;
        let _ = self.watcher.send((key, value, "PUT".into()));
        Ok(())
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        match self.db.get(key) {
            Ok(Some(value)) => String::from_utf8(value).ok(),
            _ => None,
        }
    }

    pub async fn prefix_scan(&self, prefix: &str) -> Vec<(String, String)> {
        let mut result = Vec::new();

        let iter = if prefix.is_empty() {
            self.db.iterator(rocksdb::IteratorMode::Start)
        } else {
            self.db.prefix_iterator(prefix)
        };

        for item in iter {
            if let Ok((k, v)) = item {
                if let (Ok(key), Ok(value)) = (
                    String::from_utf8(k.to_vec()),
                    String::from_utf8(v.to_vec())
                ) {
                    if prefix.is_empty() || key.starts_with(prefix) {
                        result.push((key, value));
                    }
                }
            }
        }
        result
    }

    pub async fn delete(&self, key: &str) -> Result<(), rocksdb::Error> {
        self.db.delete(&key)?;
        let _ = self.watcher.send((key.to_string(), "".to_string(), "DELETE".into()));
        Ok(())
    }

    pub async fn delete_all(&self) -> Result<(), rocksdb::Error> {
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        for item in iter {
            if let Ok((k, _)) = item {
                self.db.delete(&k)?;
                let _ = self.watcher.send((String::from_utf8(k.to_vec()).unwrap(), "".to_string(), "DELETE".into()));
            }
        }
        Ok(())
    }

    pub async fn close(&self) {
        self.db.flush().expect("Failed to flush RocksDB");
    }

}

pub type SharedKvStore = Arc<RwLock<KvStore>>;
