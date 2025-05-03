use raft_service::{PutRequest, GetRequest, DeleteRequest};
use raft_service::raft_service_client::RaftServiceClient;
use tonic::transport::Channel;

pub mod raft_service {
    tonic::include_proto!("raft_service");
}

pub struct RaftClient {
    client: RaftServiceClient<Channel>
}

impl RaftClient {
    pub async fn new(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let client = RaftServiceClient::connect(addr.to_string()).await?;
        Ok(RaftClient { client })
    }

    pub async fn put(&mut self, key: String, value: String) -> Result<(), Box<dyn std::error::Error>> {
        let request = PutRequest { key, value };
        let response = self.client.put(request).await?.into_inner();
        if response.success {
            Ok(())
        } else {
            Err(response.error.into())
        }
    }

    pub async fn get(&mut self, key: String) -> Result<String, Box<dyn std::error::Error>> {
        let request = GetRequest { key };
        let response = self.client.get(request).await?.into_inner();
        if response.found {
            Ok(response.value)
        } else if !response.found {
            Ok("Key not found".to_string())
        } else {
            Err(response.error.into())
        }
    }

    pub async fn delete(&mut self, key: String) -> Result<(), Box<dyn std::error::Error>> {
        let request = DeleteRequest { key };
        let response = self.client.delete(request).await?.into_inner();
        if response.success {
            Ok(())
        } else {
            Err(response.error.into())
        }
    }
}