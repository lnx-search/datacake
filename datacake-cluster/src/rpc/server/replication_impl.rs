use async_trait::async_trait;
use tonic::{Request, Response, Status};

use crate::rpc::datacake_api::{Empty, FetchDocs, FetchedDocs, GetState, KeyspaceOrSwotSet, KeyspaceTimestamps};
use crate::rpc::datacake_api::replication_api_server::ReplicationApi;


pub struct ReplicationService {

}

#[async_trait]
impl ReplicationApi for ReplicationService {
    async fn poll_keyspace(&self, request: Request<Empty>) -> Result<Response<KeyspaceTimestamps>, Status> {
        todo!()
    }

    async fn get_state(&self, request: Request<GetState>) -> Result<Response<KeyspaceOrSwotSet>, Status> {
        todo!()
    }

    async fn fetch_docs(&self, request: Request<FetchDocs>) -> Result<Response<FetchedDocs>, Status> {
        todo!()
    }
}

