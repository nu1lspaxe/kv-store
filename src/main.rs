use log::info;
use std::sync::Arc;
use tokio::{signal, sync::RwLock};
use tonic::transport::Server;

mod kvstore;
use kvstore::server::create_server;
use kvstore::store::KvStore;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let addr = "[::1]:50051".parse()?;
    let store = Arc::new(RwLock::new(KvStore::new("kv-store.db")));
    let server = create_server(store.clone());

    info!("🚀 KvStoreServer listening on {}", addr);

    let shutdown = async {
        signal::ctrl_c()
            .await
            .expect("❌ Failed to shutdown with Ctrl+C");

        info!("🛑 Shutdown signal received. Flushing database..");
        store.write().await.close().await;
        info!("✅ Database flushed. Exiting.");
    };

    Server::builder()
        .add_service(server)
        .serve_with_shutdown(addr, shutdown)
        .await?;

    Ok(())
}