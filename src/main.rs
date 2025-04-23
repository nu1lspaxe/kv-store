use log::info;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::Server;

mod server;
mod store;

use server::create_server;
use store::KvStore;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let addr = "[::1]:50051".parse()?;
    let store = Arc::new(RwLock::new(KvStore::new("kv_store.json")));
    let server = create_server(store);

    info!("Starting server on {}", addr);
    Server::builder()
        .add_service(server)
        .serve(addr)
        .await?;

    Ok(())
}