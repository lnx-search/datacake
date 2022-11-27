use async_trait::async_trait;
use datacake_crdt::HLCTimestamp;
use tonic::{Request, Response, Status};

use crate::core::Document;
use crate::keyspace::{ConsistencySource, KeyspaceGroup};
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

        self.group.clock().register_ts(document.last_updated).await;

        crate::core::put_data::<ConsistencySource, _>(
            &inner.keyspace,
            document,
            &self.group,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(Empty {}))
    }

    async fn multi_put(
        &self,
        request: Request<MultiPutPayload>,
    ) -> Result<Response<Empty>, Status> {
        let inner = request.into_inner();
        let mut newest_ts = HLCTimestamp::new(0, 0, 0);
        let documents = inner.documents.into_iter().map(Document::from).map(|doc| {
            if doc.last_updated > newest_ts {
                newest_ts = doc.last_updated;
            }
            doc
        });

        crate::core::put_many_data::<ConsistencySource, _>(
            &inner.keyspace,
            documents,
            &self.group,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        self.group.clock().register_ts(newest_ts).await;

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

        self.group.clock().register_ts(last_updated).await;

        crate::core::del_data::<ConsistencySource, _>(
            &inner.keyspace,
            doc_id,
            last_updated,
            &self.group,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(Empty {}))
    }

    async fn multi_remove(
        &self,
        request: Request<MultiRemovePayload>,
    ) -> Result<Response<Empty>, Status> {
        let inner = request.into_inner();

        let mut newest_ts = HLCTimestamp::new(0, 0, 0);
        let documents = inner
            .documents
            .into_iter()
            .map(|doc| {
                let ts = HLCTimestamp::from(doc.last_updated.unwrap());
                (doc.id, ts)
            })
            .map(|(id, ts)| {
                if ts > newest_ts {
                    newest_ts = ts;
                }
                (id, ts)
            });

        crate::core::del_many_data::<ConsistencySource, _>(
            &inner.keyspace,
            documents,
            &self.group,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        self.group.clock().register_ts(newest_ts).await;

        Ok(Response::new(Empty {}))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::rpc::datacake_api::DocumentMetadata;
    use crate::storage::mem_store::MemStore;

    #[tokio::test]
    async fn test_consistency_put() {
        static KEYSPACE: &str = "put-keyspace";
        let group = KeyspaceGroup::<MemStore>::new_for_test().await;
        let clock = group.clock();
        let storage = group.storage();
        let service = ConsistencyService::new(group.clone());

        let doc = Document::new(1, clock.get_time().await, b"Hello, world".to_vec());
        let put_req = Request::new(PutPayload {
            keyspace: KEYSPACE.to_string(),
            document: Some(doc.clone().into()),
        });

        service
            .put(put_req)
            .await
            .expect("Put request should succeed.");

        let saved_doc = storage
            .get(KEYSPACE, doc.id)
            .await
            .expect("Get new doc.")
            .expect("Doc should not be None");
        assert_eq!(saved_doc, doc, "Documents stored should match.");

        let metadata = storage
            .iter_metadata(KEYSPACE)
            .await
            .expect("Iter metadata")
            .collect::<Vec<_>>();
        assert_eq!(metadata, vec![(doc.id, doc.last_updated, false)]);
    }

    #[tokio::test]
    async fn test_consistency_put_many() {
        static KEYSPACE: &str = "put-many-keyspace";
        let group = KeyspaceGroup::<MemStore>::new_for_test().await;
        let clock = group.clock();
        let storage = group.storage();
        let service = ConsistencyService::new(group.clone());

        let doc_1 = Document::new(1, clock.get_time().await, b"Hello, world 1".to_vec());
        let doc_2 = Document::new(2, clock.get_time().await, b"Hello, world 2".to_vec());
        let doc_3 = Document::new(3, clock.get_time().await, b"Hello, world 3".to_vec());
        let put_req = Request::new(MultiPutPayload {
            keyspace: KEYSPACE.to_string(),
            documents: vec![
                doc_1.clone().into(),
                doc_2.clone().into(),
                doc_3.clone().into(),
            ],
        });

        service
            .multi_put(put_req)
            .await
            .expect("Put request should succeed.");

        let saved_docs = storage
            .multi_get(KEYSPACE, vec![doc_1.id, doc_2.id, doc_3.id].into_iter())
            .await
            .expect("Get new doc.")
            .collect::<Vec<_>>();
        assert_eq!(
            saved_docs,
            vec![doc_1.clone(), doc_2.clone(), doc_3.clone(),],
            "Documents stored should match."
        );

        let metadata = storage
            .iter_metadata(KEYSPACE)
            .await
            .expect("Iter metadata")
            .collect::<HashSet<_>>();
        assert_eq!(
            metadata,
            HashSet::from_iter([
                (doc_1.id, doc_1.last_updated, false),
                (doc_2.id, doc_2.last_updated, false),
                (doc_3.id, doc_3.last_updated, false),
            ])
        );
    }

    #[tokio::test]
    async fn test_consistency_remove() {
        static KEYSPACE: &str = "remove-keyspace";
        let group = KeyspaceGroup::<MemStore>::new_for_test().await;
        let clock = group.clock();
        let storage = group.storage();
        let service = ConsistencyService::new(group.clone());

        let mut doc =
            Document::new(1, clock.get_time().await, b"Hello, world 1".to_vec());
        add_docs(KEYSPACE, vec![doc.clone()], &service).await;

        let saved_doc = storage
            .get(KEYSPACE, doc.id)
            .await
            .expect("Get new doc.")
            .expect("Doc should not be None");
        assert_eq!(saved_doc, doc, "Documents stored should match.");

        doc.last_updated = clock.get_time().await;
        let remove_req = Request::new(RemovePayload {
            keyspace: KEYSPACE.to_string(),
            document: Some(DocumentMetadata {
                id: doc.id,
                last_updated: Some(doc.last_updated.into()),
            }),
        });

        service.remove(remove_req).await.expect("Remove document.");

        let saved_doc = storage.get(KEYSPACE, doc.id).await.expect("Get new doc.");
        assert!(saved_doc.is_none(), "Documents should no longer exist.");

        let metadata = storage
            .iter_metadata(KEYSPACE)
            .await
            .expect("Iter metadata")
            .collect::<Vec<_>>();
        assert_eq!(metadata, vec![(doc.id, doc.last_updated, true),]);
    }

    #[tokio::test]
    async fn test_consistency_remove_many() {
        static KEYSPACE: &str = "remove-many-keyspace";
        let group = KeyspaceGroup::<MemStore>::new_for_test().await;
        let clock = group.clock();
        let storage = group.storage();
        let service = ConsistencyService::new(group.clone());

        let mut doc_1 =
            Document::new(1, clock.get_time().await, b"Hello, world 1".to_vec());
        let mut doc_2 =
            Document::new(2, clock.get_time().await, b"Hello, world 2".to_vec());
        let doc_3 = Document::new(3, clock.get_time().await, b"Hello, world 3".to_vec());
        add_docs(
            KEYSPACE,
            vec![doc_1.clone(), doc_2.clone(), doc_3.clone()],
            &service,
        )
        .await;

        let saved_docs = storage
            .multi_get(KEYSPACE, vec![doc_1.id, doc_2.id, doc_3.id].into_iter())
            .await
            .expect("Get new doc.")
            .collect::<Vec<_>>();
        assert_eq!(
            saved_docs,
            vec![doc_1.clone(), doc_2.clone(), doc_3.clone(),],
            "Documents stored should match."
        );

        doc_1.last_updated = clock.get_time().await;
        doc_2.last_updated = clock.get_time().await;
        let remove_req = Request::new(MultiRemovePayload {
            keyspace: KEYSPACE.to_string(),
            documents: vec![
                DocumentMetadata {
                    id: doc_1.id,
                    last_updated: Some(doc_1.last_updated.into()),
                },
                DocumentMetadata {
                    id: doc_2.id,
                    last_updated: Some(doc_2.last_updated.into()),
                },
            ],
        });

        service
            .multi_remove(remove_req)
            .await
            .expect("Remove documents.");

        let saved_docs = storage
            .multi_get(KEYSPACE, vec![doc_1.id, doc_2.id, doc_3.id].into_iter())
            .await
            .expect("Get new doc.")
            .collect::<Vec<_>>();
        assert_eq!(
            saved_docs,
            vec![doc_3.clone()],
            "Documents stored should match."
        );

        let metadata = storage
            .iter_metadata(KEYSPACE)
            .await
            .expect("Iter metadata")
            .collect::<HashSet<_>>();
        assert_eq!(
            metadata,
            HashSet::from_iter([
                (doc_1.id, doc_1.last_updated, true),
                (doc_2.id, doc_2.last_updated, true),
                (doc_3.id, doc_3.last_updated, false),
            ])
        );
    }

    async fn add_docs(
        keyspace: &str,
        docs: Vec<Document>,
        service: &ConsistencyService<MemStore>,
    ) {
        let put_req = Request::new(MultiPutPayload {
            keyspace: keyspace.to_string(),
            documents: docs.into_iter().map(|d| d.into()).collect(),
        });

        service
            .multi_put(put_req)
            .await
            .expect("Put request should succeed.");
    }
}
