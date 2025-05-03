use std::sync::Arc;
use openraft::Raft;
use raft_service::raft_service_server::{RaftService, RaftServiceServer};
use raft_service::{
    PutRequest, PutResponse,
    GetRequest, GetResponse,
    DeleteRequest, DeleteResponse,
};
use tonic::{Request, Response, Status};
use crate::raft::type_config::TypeConfig;
use crate::raft::log_storage::RocksStore;
use crate::raft::rocks_client::RocksRequest;

pub mod raft_service {
    tonic::include_proto!("raft_service");
}

#[derive(Clone)]
struct RaftServer {
    raft: Arc<Raft<TypeConfig>>,
    store: Arc<RocksStore>,
}

#[tonic::async_trait]
impl RaftService for RaftServer {
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        let raft_req = RocksRequest::Put {
            key: req.key,
            value: req.value,
        };
        match self.raft.client_write(raft_req).await {
            Ok(_) => Ok(Response::new(PutResponse {
                    success: true,
                    error: String::new(),
                })),
            Err(e) => Ok(Response::new(PutResponse {
                    success: false,
                    error: e.to_string(),
                })),
        }
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        let state_machine = self.store.state_machine.read().await;
        match state_machine.get(req.key) {
            Ok(Some(value)) => Ok(Response::new(GetResponse {
                value: value.to_string(),
                found: true,
                error: String::new(),
            })),
            Ok(None) => Ok(Response::new(GetResponse {
                value: String::new(),
                found: false,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(GetResponse {
                value: String::new(),
                found: false,
                error: e.to_string(),
            })),
        }
    }

    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        let raft_req = RocksRequest::Delete {
            key: req.key,
        };
        
        match self.raft.client_write(raft_req).await {
            Ok(_) => Ok(Response::new(DeleteResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(DeleteResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }
}