use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RocksRequest {
    Put { key: String, value: String },
    Delete { key: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RocksResponse {
    Put(Result<(), ClientError>),
    Delete(Result<(), ClientError>),
}

#[derive(Error, Debug, Serialize, Deserialize, Clone)]
pub enum ClientError {
    #[error("Key not found")]
    KeyNotFound,
    #[error("Internal error: {0}")]
    InternalError(String),
}