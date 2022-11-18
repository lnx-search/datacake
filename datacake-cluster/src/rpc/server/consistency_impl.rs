use async_trait::async_trait;
use tonic::{Request, Response, Status};

use crate::rpc::datacake_api::consistency_api_server::ConsistencyApi;
use crate::rpc::datacake_api::{Empty, MultiPutPayload, MultiRemovePayload, PutPayload, RemovePayload};

pub struct ConsistencyService {

}

#[async_trait]
impl ConsistencyApi for ConsistencyService {
    async fn put(&self, request: Request<PutPayload>) -> Result<Response<Empty>, Status> {
        todo!()
    }

    async fn multi_put(&self, request: Request<MultiPutPayload>) -> Result<Response<Empty>, Status> {
        todo!()
    }

    async fn remove(&self, request: Request<RemovePayload>) -> Result<Response<Empty>, Status> {
        todo!()
    }

    async fn multi_remove(&self, request: Request<MultiRemovePayload>) -> Result<Response<Empty>, Status> {
        todo!()
    }
}

