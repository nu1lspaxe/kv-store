use kvstore::kv_store_client::KvStoreClient;
use kvstore::{PutRequest, GetRequest};

pub mod kvstore {
    tonic::include_proto!("kvstore");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = KvStoreClient::connect("http://[::1]:50051").await?;

    let put_request = PutRequest {
        key: "foo".to_string(),
        value: "bar".to_string(),
    };

    let put_response = client.put(put_request).await?;
    println!("Put response: {:?}", put_response.into_inner());

    let get_request = GetRequest {
        key: "foo".to_string(),
    };
    let get_response = client.get(get_request).await?;
    println!("Get response: {:?}", get_response.into_inner());

    Ok(())
}