use kvstore::kv_store_client::KvStoreClient;
use kvstore::{PutRequest, GetRequest, ListRequest, DeleteRequest, DeleteAllRequest};
use std::io::{self, Write};

pub mod kvstore {
    tonic::include_proto!("kvstore");
}

fn help() {
    println!("Available commands:");
    println!("  put <key> <value>   - Insert a key-value pair");
    println!("  get <key>           - Retrieve the value for a key");
    println!("  list                - List all key-value pairs");
    println!("  delete <key>        - Delete a key-value pair");
    println!("  delete_all          - Delete all key-value pairs");
    println!("  exit                - Exit the client");
    println!("  help                - Show this help message");
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = KvStoreClient::connect("http://[::1]:50051").await?;

    println!("Connected to gRPC KvStore server. Type commands `help` to see available commands:");

    loop {
        print!("> ");
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let parts: Vec<&str> = input.trim().split_whitespace().collect();

        match parts.as_slice() {
            ["put", key, value] => {
                let request = PutRequest {
                    key: key.to_string(),
                    value: value.to_string(),
                };
                match client.put(request).await {
                    Ok(response) => println!("✅ Put: {:?}", response),
                    Err(e) => eprintln!("❌ Error: {:?}", e),
                }
            }

            ["get", key] => {
                let request = GetRequest {
                    key: key.to_string(),
                };
                match client.get(request).await {
                    Ok(response) => println!("✅ Get: {:?}", response),
                    Err(e) => eprintln!("❌ Error: {:?}", e),
                }
            }

            ["list", prefix] => {
                let request = ListRequest {
                    prefix: prefix.to_string(),
                };

                match client.list(request).await {
                    Ok(response) => {
                        println!("✅ List: {:#?}", response.into_inner().items);
                    }
                    Err(e) => eprintln!("❌ Error: {:?}", e),
                }
            }

            ["delete", key] => {
                let request = DeleteRequest {
                    key: key.to_string(),
                };
                match client.delete(request).await {
                    Ok(response) => println!("✅ Delete: {:?}", response),
                    Err(e) => eprintln!("❌ Error: {:?}", e),
                }
            }

            ["delete_all"] => {
                let request = DeleteAllRequest {};
                match client.delete_all(request).await {
                    Ok(response) => println!("✅ Delete All: {:?}", response),
                    Err(e) => eprintln!("❌ Error: {:?}", e),
                }
            }

            ["help"] => {
                help();
            }
                
            ["exit"] => {
                println!("🛑 Exiting...");
                break;
            }

            _ => {
                println!("❌ Unknown command. Use `help` to see available commands.");
            }
        }
    }

    Ok(())
}