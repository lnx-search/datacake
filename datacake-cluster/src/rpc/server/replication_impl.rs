use async_trait::async_trait;
use datacake_crdt::HLCTimestamp;
use tonic::{Request, Response, Status};

use crate::keyspace::{KeyspaceGroup, LastUpdated, Serialize};
use crate::rpc::datacake_api;
use crate::rpc::datacake_api::replication_api_server::ReplicationApi;
use crate::rpc::datacake_api::{
    FetchDocs,
    FetchedDocs,
    GetState,
    KeyspaceInfo,
    KeyspaceOrSwotSet,
    PollPayload,
};
use crate::storage::Storage;

pub struct ReplicationService<S>
where
    S: Storage + Send + Sync + 'static
{
    group: KeyspaceGroup<S>,
}

impl<S> ReplicationService<S>
where
    S: Storage + Send + Sync + 'static
{
    pub fn new(group: KeyspaceGroup<S>) -> Self {
        Self { group }
    }
}

#[async_trait]
impl<S: Storage + Send + Sync + 'static> ReplicationApi for ReplicationService<S> {
    async fn poll_keyspace(
        &self,
        request: Request<PollPayload>,
    ) -> Result<Response<KeyspaceInfo>, Status> {
        let inner = request.into_inner();
        let clock = self.group.clock();

        let ts = HLCTimestamp::from(inner.timestamp.unwrap());
        clock.register_ts(ts).await;

        let payload = self
            .group
            .get_keyspace_info()
            .await;

        Ok(Response::new(payload))
    }

    async fn get_state(
        &self,
        request: Request<GetState>,
    ) -> Result<Response<KeyspaceOrSwotSet>, Status> {
        let inner = request.into_inner();
        let clock = self.group.clock();

        let ts = HLCTimestamp::from(inner.timestamp.unwrap());
        clock.register_ts(ts).await;

        let keyspace = self.group.get_or_create_keyspace(&inner.keyspace).await;

        let last_updated = keyspace.send(LastUpdated).await;
        let set_data = keyspace
            .send(Serialize)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let ts = self.group.clock().get_time().await;
        Ok(Response::new(KeyspaceOrSwotSet {
            timestamp: Some(ts.into()),
            last_updated: Some(last_updated.into()),
            set_data,
        }))
    }

    async fn fetch_docs(
        &self,
        request: Request<FetchDocs>,
    ) -> Result<Response<FetchedDocs>, Status> {
        let inner = request.into_inner();
        let clock = self.group.clock();

        let ts = HLCTimestamp::from(inner.timestamp.unwrap());
        clock.register_ts(ts).await;

        let storage = self.group.storage();

        if inner.doc_ids.len() == 1 {
            let documents = storage
                .get(&inner.keyspace, inner.doc_ids[0])
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .map(datacake_api::Document::from)
                .map(|doc| vec![doc])
                .unwrap_or_default();

            let ts = clock.get_time().await;
            return Ok(Response::new(FetchedDocs {
                timestamp: Some(ts.into()),
                documents,
            }));
        }

        let documents = self
            .group
            .storage()
            .multi_get(&inner.keyspace, inner.doc_ids.into_iter())
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map(datacake_api::Document::from)
            .collect();

        let ts = self.group.clock().get_time().await;
        Ok(Response::new(FetchedDocs {
            timestamp: Some(ts.into()),
            documents,
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::marker::PhantomData;

    use super::*;
    use crate::keyspace::{KeyspaceTimestamps, READ_REPAIR_SOURCE_ID, Set};
    use crate::test_utils::MemStore;
    use crate::Document;

    #[tokio::test]
    async fn test_poll_keyspace() {
        static KEYSPACE: &str = "poll-keyspace";
        let group = KeyspaceGroup::<MemStore>::new_for_test().await;
        let clock = group.clock();
        let service = ReplicationService::new(group.clone());

        let ts = clock.get_time().await;
        let poll_req = Request::new(PollPayload {
            timestamp: Some(ts.into()),
        });
        let resp = service
            .poll_keyspace(poll_req)
            .await
            .expect("Get keyspace info")
            .into_inner();

        assert!(resp.keyspace_timestamps.is_empty());

        // Add a new keyspace which is effectively changed.
        let _keyspace = group.get_or_create_keyspace(KEYSPACE).await;

        let ts = clock.get_time().await;
        let poll_req = Request::new(PollPayload {
            timestamp: Some(ts.into()),
        });
        let resp = service
            .poll_keyspace(poll_req)
            .await
            .expect("Get keyspace info")
            .into_inner();

        let blank_timestamps = KeyspaceTimestamps::default();
        let diff = blank_timestamps.diff(&resp.into()).collect::<Vec<_>>();
        assert_eq!(
            diff,
            vec![Cow::Borrowed(KEYSPACE)],
            "No keyspace should exist initially."
        );
    }

    #[tokio::test]
    async fn test_get_state() {
        static KEYSPACE: &str = "get-keyspace";
        let group = KeyspaceGroup::<MemStore>::new_for_test().await;
        let clock = group.clock();
        let service = ReplicationService::new(group.clone());

        let keyspace = group.get_or_create_keyspace(KEYSPACE).await;
        keyspace
            .send(Set {
                source: READ_REPAIR_SOURCE_ID,
                doc: Document::new(1, clock.get_time().await, Vec::new()),
                ctx: None,
                _marker: PhantomData::<MemStore>::default()
            })
            .await
            .expect("Set value in store.");

        let last_updated = keyspace.send(LastUpdated).await;
        let set_data = keyspace
            .send(Serialize)
            .await
            .map_err(|e| Status::internal(e.to_string()))
            .expect("Get serialized state.");

        let ts = clock.get_time().await;
        let state_req = Request::new(GetState {
            timestamp: Some(ts.into()),
            keyspace: KEYSPACE.to_string(),
        });

        let resp = service
            .get_state(state_req)
            .await
            .expect("Get keyspace state.")
            .into_inner();

        assert_eq!(
            HLCTimestamp::from(resp.last_updated.unwrap()), last_updated,
            "Last updated timestamps should match."
        );
        assert_eq!(resp.set_data, set_data, "State data should match.");
    }

    #[tokio::test]
    async fn test_fetch_docs() {
        static KEYSPACE: &str = "fetch-keyspace";
        let group = KeyspaceGroup::<MemStore>::new_for_test().await;
        let clock = group.clock();
        let service = ReplicationService::new(group.clone());

        let keyspace = group.get_or_create_keyspace(KEYSPACE).await;

        let doc = Document::new(1, clock.get_time().await, b"Hello, world".to_vec());
        keyspace
            .send(Set {
                source: READ_REPAIR_SOURCE_ID,
                doc: doc.clone(),
                ctx: None,
                _marker: PhantomData::<MemStore>::default()
            })
            .await
            .expect("Set value in store.");

        let ts = clock.get_time().await;
        let fetch_docs_req = Request::new(FetchDocs {
            timestamp: Some(ts.into()),
            keyspace: KEYSPACE.to_string(),
            doc_ids: vec![1],
        });

        let docs = service
            .fetch_docs(fetch_docs_req)
            .await
            .expect("Fetch docs.")
            .into_inner()
            .documents
            .into_iter()
            .map(Document::from)
            .collect::<Vec<_>>();

        assert_eq!(docs, vec![doc], "Documents should match.");
    }
}
