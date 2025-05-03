use clap::{Parser, Subcommand};

use crate::raft_store::client::client::raft_service::{raft_service_client::RaftServiceClient, DeleteRequest, GetRequest, PutRequest};


#[derive(Parser)]
#[command(name = "raft-cli")]
#[command(about = "CLI for Raft key-value store")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    addr: String,
}

#[derive(Subcommand)]
enum Commands {
    Put { key: String, value: String },
    Get { key: String },
    Delete { key: String },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let mut client = RaftServiceClient::connect(cli.addr).await?;

    match cli.command {
        Commands::Put { key, value } => {
            let request = PutRequest { key, value };
            match client.put(request).await {
                Ok(response) => println!("✅ Put: {:?}", response),
                Err(e) => eprintln!("❌ Error: {:?}", e),
            }
        }
        Commands::Get { key } => {
            let request = GetRequest { key };
            match client.get(request).await {
                Ok(response) => println!("✅ Get: {:?}", response),
                Err(e) => eprintln!("❌ Error: {:?}", e),
            }
        }
        Commands::Delete { key } => {
            let request = DeleteRequest { key };
            match client.delete(request).await {
                Ok(response) => println!("✅ Delete: {:?}", response),
                Err(e) => eprintln!("❌ Error: {:?}", e),
            }
        }
    }

    Ok(())
}