use async_trait::async_trait;
use datacake_crdt::HLCTimestamp;
use tonic::{Request, Response, Status};

use crate::core::Document;
use crate::keyspace::KeyspaceGroup;
use crate::rpc::datacake_api::consistency_api_server::ConsistencyApi;
use crate::rpc::datacake_api::{
    Empty,
    MultiPutPayload,
    MultiRemovePayload,
    PutPayload,
    RemovePayload,
};
use crate::storage::Storage;

pub struct ConsistencyService<S: Storage> {
    group: KeyspaceGroup<S>,
}

impl<S: Storage> ConsistencyService<S> {
    pub fn new(group: KeyspaceGroup<S>) -> Self {
        Self { group }
    }
}

#[async_trait]
impl<S: Storage + Send + Sync + 'static> ConsistencyApi for ConsistencyService<S> {
    async fn put(
        &self,
        request: Request<PutPayload>,
    ) -> Result<Response<Empty>, Status> {
        let inner = request.into_inner();
        let document = Document::from(inner.document.unwrap());

        crate::core::put_data(&inner.keyspace, document, &self.group)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(Empty {}))
    }

    async fn multi_put(
        &self,
        request: Request<MultiPutPayload>,
    ) -> Result<Response<Empty>, Status> {
        let inner = request.into_inner();
        let documents = inner.documents.into_iter().map(Document::from);

        crate::core::put_many_data(&inner.keyspace, documents, &self.group)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(Empty {}))
    }

    async fn remove(
        &self,
        request: Request<RemovePayload>,
    ) -> Result<Response<Empty>, Status> {
        let inner = request.into_inner();
        let document = inner.document.unwrap();
        let doc_id = document.id;
        let last_updated = HLCTimestamp::from(document.last_updated.unwrap());

        crate::core::del_data(&inner.keyspace, doc_id, last_updated, &self.group)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(Empty {}))
    }

    async fn multi_remove(
        &self,
        request: Request<MultiRemovePayload>,
    ) -> Result<Response<Empty>, Status> {
        let inner = request.into_inner();

        let documents = inner.documents.into_iter().map(|doc| {
            let ts = HLCTimestamp::from(doc.last_updated.unwrap());
            (doc.id, ts)
        });

        crate::core::del_many_data(&inner.keyspace, documents, &self.group)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(Empty {}))
    }
}
