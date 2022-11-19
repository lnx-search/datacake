use async_trait::async_trait;
use tonic::{Request, Response, Status};
use datacake_crdt::HLCTimestamp;

use crate::core::Document;
use crate::keyspace::KeyspaceGroup;
use crate::rpc::datacake_api::consistency_api_server::ConsistencyApi;
use crate::rpc::datacake_api::{Empty, MultiPutPayload, MultiRemovePayload, PutPayload, RemovePayload};
use crate::storage::Storage;

pub struct ConsistencyService<S: Storage> {
    group: KeyspaceGroup<S>,
}

impl<S: Storage> ConsistencyService<S> {
    pub fn new(group: KeyspaceGroup<S>) -> Self {
        Self {
            group
        }
    }
}

#[async_trait]
impl<S: Storage + Send + Sync + 'static> ConsistencyApi for ConsistencyService<S> {
    async fn put(&self, request: Request<PutPayload>) -> Result<Response<Empty>, Status> {
        let inner = request.into_inner();
        let document = Document::from(inner.document.unwrap());
        let doc_id = document.id;
        let last_updated = document.last_updated;

        self.group
            .storage()
            .put(&inner.keyspace, document)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let keyspace = self.group
            .get_or_create_keyspace(&inner.keyspace)
            .await;

        keyspace
            .put(doc_id, last_updated)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(Empty{}))
    }

    async fn multi_put(&self, request: Request<MultiPutPayload>) -> Result<Response<Empty>, Status> {
        let inner = request.into_inner();

        let mut entries = Vec::new();
        let documents = inner.documents
            .into_iter()
            .map(|doc| {
                let doc =  Document::from(doc);
                entries.push((doc.id, doc.last_updated));
                doc
            });

        self.group
            .storage()
            .multi_put(&inner.keyspace, documents)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let keyspace = self.group
            .get_or_create_keyspace(&inner.keyspace)
            .await;

        keyspace
            .multi_put(entries)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(Empty{}))
    }

    async fn remove(&self, request: Request<RemovePayload>) -> Result<Response<Empty>, Status> {
        let inner = request.into_inner();
        let document = inner.document.unwrap();
        let doc_id = document.id;
        let last_updated = HLCTimestamp::from(document.last_updated.unwrap());

        self.group
            .storage()
            .del(&inner.keyspace, doc_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let keyspace = self.group
            .get_or_create_keyspace(&inner.keyspace)
            .await;

        keyspace
            .del(doc_id, last_updated)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(Empty{}))
    }

    async fn multi_remove(&self, request: Request<MultiRemovePayload>) -> Result<Response<Empty>, Status> {
        let inner = request.into_inner();

        let mut entries = Vec::new();
        let documents = inner.documents
            .into_iter()
            .map(|doc| {
                let ts = HLCTimestamp::from(doc.last_updated.unwrap());
                entries.push((doc.id, ts));
                doc.id
            });

        self.group
            .storage()
            .multi_del(&inner.keyspace, documents)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let keyspace = self.group
            .get_or_create_keyspace(&inner.keyspace)
            .await;

        keyspace
            .multi_del(entries)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(Empty{}))
    }
}
