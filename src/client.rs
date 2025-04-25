use kvstore::kv_store_client::KvStoreClient;
use kvstore::{PutRequest, GetRequest, ListRequest};
use std::io::{self, Write};

pub mod kvstore {
    tonic::include_proto!("kvstore");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = KvStoreClient::connect("http://[::1]:50051").await?;

    println!("Connected to gRPC KvStore server. Type commands (put/get/list/exit):");

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
                    Ok(response) => println!("Put: {:?}", response),
                    Err(e) => eprintln!("Error: {:?}", e),
                }
            }

            ["get", key] => {
                let request = GetRequest {
                    key: key.to_string(),
                };
                match client.get(request).await {
                    Ok(response) => println!("Get: {:?}", response),
                    Err(e) => eprintln!("Error: {:?}", e),
                }
            }

            ["list", prefix] => {
                let request = ListRequest {
                    prefix: prefix.to_string(),
                };

                match client.list(request).await {
                    Ok(response) => {
                        println!("List: {:#?}", response.into_inner().items);
                    }
                    Err(e) => eprintln!("Error: {:?}", e),
                }
            }
                
            ["exit"] => {
                println!("Exiting...");
                break;
            }

            _ => {
                println!("Unknown command. Use 'put <key> <value>', 'get <key>', 'list <prefix>', or 'exit'");
            }
        }
    }

    Ok(())
}