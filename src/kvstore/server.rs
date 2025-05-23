use tonic::{Request, Response, Status};
use kvstore::kv_store_server::{KvStore, KvStoreServer};
use kvstore::event::EventType;
use kvstore::{
    PutRequest, PutResponse, 
    GetRequest, GetResponse, 
    ListRequest, ListResponse, 
    DeleteRequest, DeleteResponse, 
    DeleteAllRequest, DeleteAllResponse, 
    WatchRequest, WatchResponse,
    KeyValue, Event,
};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use futures_util::stream::Stream; 
use std::pin::Pin;

use crate::kvstore::store::SharedKvStore;

pub mod kvstore {
    tonic::include_proto!("kvstore");
}

#[derive(Debug, Clone)]
pub struct KvStoreService {
    store: SharedKvStore,
}

impl KvStoreService {
    pub fn new(store: SharedKvStore) -> Self {
        KvStoreService { store }
    }
}

#[tonic::async_trait]
impl KvStore for KvStoreService {
    async fn put(
        &self,
        request: Request<PutRequest>,
    ) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        let store = self.store.write().await;
        store
            .put(req.key, req.value)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PutResponse { success: true}))
    }

    async fn get(
        &self,
        request: Request<GetRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        let store = self.store.read().await;
        let value = store
            .get(&req.key)
            .await
            .ok_or_else(|| Status::not_found("Key not found"))?;
        Ok(Response::new(GetResponse { value }))
    }

    async fn list(
        &self,
        request: Request<ListRequest>,
    ) -> Result<Response<ListResponse>, Status> {
        let prefix = request.into_inner().prefix;
        let store = self.store.read().await;
        let items = store.prefix_scan(&prefix).await;
        let items = items.into_iter()
            .map(|(k, v)| KeyValue{ key: k, value: v })
            .collect();

        Ok(Response::new(ListResponse { items }))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        let store = self.store.write().await;
        store
            .delete(&req.key)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(DeleteResponse { success: true}))
    }

    async fn delete_all(
        &self,
        _request: Request<DeleteAllRequest>,
    ) -> Result<Response<DeleteAllResponse>, Status> {
        let store = self.store.write().await;
        store
            .delete_all()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(DeleteAllResponse { success: true}))
    }

    type WatchStream = Pin<Box<dyn Stream<Item = Result<WatchResponse, Status>> + Send>>;

    async fn watch(
        &self,
        request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let key = request.into_inner().key;
        let receiver = self.store.read().await.watcher.subscribe();
        let watch_id = rand::random::<u64>();

        let output = BroadcastStream::new(receiver)
            .filter_map(move |event| {
                match event {
                    Ok((event_key, value, op)) if event_key == key => {
                        let event_type = match op.as_str() {
                            "PUT" => EventType::Put,
                            "DELETE" => EventType::Delete,
                            _ => return None,
                        };

                        Some(Ok(WatchResponse {
                            watch_id: watch_id, 
                            events: vec![Event {
                                r#type: event_type.into(),
                                kv: Some(KeyValue {
                                    key: event_key,
                                    value,
                                }),
                            }],
                        }))
                    }
                    _ => None,
                }
            });

        let stream = Box::pin(output) as Self::WatchStream;

        Ok(Response::new(stream))
}

}

pub fn create_server(store: SharedKvStore) -> KvStoreServer<KvStoreService> {
    KvStoreServer::new(KvStoreService::new(store))
}