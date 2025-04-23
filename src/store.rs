use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Serialize, Deserialize, Debug)]
pub struct KvStore {
    data: HashMap<String, String>,
    #[serde(skip)]
    file_path: String,
}

impl KvStore {
    pub fn new(file_path: &str) -> Self {
        let data = match File::open(file_path) {
            Ok(file) => serde_json::from_reader(file).unwrap_or_default(),
            Err(_) => HashMap::new(),
        };
        KvStore {
            data, 
            file_path: file_path.to_string(),
        }
    }

    pub async fn put(&mut self, key: String, value: String) -> io::Result<()> {
        self.data.insert(key, value);
        self.persist()?;
        Ok(())
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        self.data.get(key).cloned()
    }

    fn persist(&self) -> io::Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.file_path)?;
        serde_json::to_writer(file, &self.data)?;
        Ok(())
    }
}

pub type SharedKvStore = Arc<RwLock<KvStore>>;
