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

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::*;
    use crate::keyspace::{KeyspaceTimestamps, ReplicationSource};
    use crate::storage::mem_store::MemStore;
    use crate::Document;

    #[tokio::test]
    async fn test_poll_keyspace() {
        static KEYSPACE: &str = "poll-keyspace";
        let group = KeyspaceGroup::<MemStore>::new_for_test().await;
        let clock = group.clock();
        let service = ReplicationService::new(group.clone());

        let poll_req = Request::new(Empty {});
        let resp = service
            .poll_keyspace(poll_req)
            .await
            .expect("Get keyspace info")
            .into_inner();

        let counters: KeyspaceTimestamps =
            rkyv::from_bytes(&resp.keyspace_timestamps).expect("Deserialize timestamps");
        assert!(counters.is_empty(), "No keyspace should exist initially.");

        // Add a new keyspace which is effectively changed.
        let keyspace = group.get_or_create_keyspace(KEYSPACE).await;
        keyspace
            .put::<ReplicationSource>(1, clock.get_time().await)
            .await
            .expect("Put key.");

        let poll_req = Request::new(Empty {});
        let resp = service
            .poll_keyspace(poll_req)
            .await
            .expect("Get keyspace info")
            .into_inner();

        let blank_timestamps = KeyspaceTimestamps::default();
        let counters: KeyspaceTimestamps =
            rkyv::from_bytes(&resp.keyspace_timestamps).expect("Deserialize timestamps");
        let diff = blank_timestamps.diff(&counters).collect::<Vec<_>>();
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
            .put::<ReplicationSource>(1, clock.get_time().await)
            .await
            .expect("Put key.");
        let last_updated = keyspace.last_updated();
        let state = keyspace
            .serialize()
            .await
            .expect("Get serialized version of state");

        let state_req = Request::new(GetState {
            keyspace: KEYSPACE.to_string(),
        });

        let resp = service
            .get_state(state_req)
            .await
            .expect("Get keyspace state.")
            .into_inner();

        assert_eq!(
            resp.last_updated, last_updated,
            "Last updated timestamps should match."
        );
        assert_eq!(resp.set_data, state, "State data should match.");
    }

    #[tokio::test]
    async fn test_fetch_docs() {
        static KEYSPACE: &str = "fetch-keyspace";
        let group = KeyspaceGroup::<MemStore>::new_for_test().await;
        let clock = group.clock();
        let storage = group.storage();
        let service = ReplicationService::new(group.clone());

        let keyspace = group.get_or_create_keyspace(KEYSPACE).await;

        let doc = Document::new(1, clock.get_time().await, b"Hello, world".to_vec());
        storage
            .put(KEYSPACE, doc.clone())
            .await
            .expect("Store entry");
        keyspace
            .put::<ReplicationSource>(doc.id, doc.last_updated)
            .await
            .expect("Set state value.");

        let fetch_docs_req = Request::new(FetchDocs {
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
