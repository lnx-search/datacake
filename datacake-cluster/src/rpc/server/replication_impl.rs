use async_trait::async_trait;
use tonic::{Request, Response, Status};

use crate::keyspace::KeyspaceGroup;
use crate::rpc::datacake_api;
use crate::rpc::datacake_api::replication_api_server::ReplicationApi;
use crate::rpc::datacake_api::{
    Empty,
    FetchDocs,
    FetchedDocs,
    GetState,
    KeyspaceInfo,
    KeyspaceOrSwotSet,
};
use crate::storage::Storage;

pub struct ReplicationService<S: Storage> {
    group: KeyspaceGroup<S>,
}

impl<S: Storage> ReplicationService<S> {
    pub fn new(group: KeyspaceGroup<S>) -> Self {
        Self { group }
    }
}

#[async_trait]
impl<S: Storage + Send + Sync + 'static> ReplicationApi for ReplicationService<S> {
    async fn poll_keyspace(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<KeyspaceInfo>, Status> {
        let keyspace_timestamps = self
            .group
            .serialize_keyspace_counters()
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(KeyspaceInfo {
            keyspace_timestamps,
        }))
    }

    async fn get_state(
        &self,
        request: Request<GetState>,
    ) -> Result<Response<KeyspaceOrSwotSet>, Status> {
        let inner = request.into_inner();

        let keyspace = self.group.get_or_create_keyspace(&inner.keyspace).await;

        let last_updated = keyspace.last_updated();
        let set_data = keyspace
            .serialize()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(KeyspaceOrSwotSet {
            last_updated,
            set_data,
        }))
    }

    async fn fetch_docs(
        &self,
        request: Request<FetchDocs>,
    ) -> Result<Response<FetchedDocs>, Status> {
        let inner = request.into_inner();
        let storage = self.group.storage();

        if inner.doc_ids.len() == 1 {
            let documents = storage
                .get(&inner.keyspace, inner.doc_ids[0])
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .map(datacake_api::Document::from)
                .map(|doc| vec![doc])
                .unwrap_or_default();

            return Ok(Response::new(FetchedDocs { documents }));
        }

        let documents = self
            .group
            .storage()
            .multi_get(&inner.keyspace, inner.doc_ids.into_iter())
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map(datacake_api::Document::from)
            .collect();

        Ok(Response::new(FetchedDocs { documents }))
    }
}
