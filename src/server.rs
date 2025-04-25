use tonic::{Request, Response, Status};
use crate::store::SharedKvStore;
use kvstore::kv_store_server::{KvStore, KvStoreServer};
use kvstore::{PutRequest, PutResponse, GetRequest, GetResponse, ListRequest, ListResponse, KeyValue};

pub mod kvstore {
    tonic::include_proto!("kvstore");
}

#[derive(Debug)]
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
}

pub fn create_server(store: SharedKvStore) -> KvStoreServer<KvStoreService> {
    KvStoreServer::new(KvStoreService::new(store))
}