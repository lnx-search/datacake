use datacake_crdt::{HLCTimestamp, Key};
use datacake_rpc::{Handler, Request, RpcService, ServiceRegistry, Status};
use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};

use crate::keyspace::KeyspaceInfo;
use crate::Document;
use crate::keyspace::{KeyspaceGroup, LastUpdated};
use crate::storage::Storage;

pub struct ReplicationService<S>
where
    S: Storage + Send + Sync + 'static,
{
    group: KeyspaceGroup<S>,
}

impl<S> ReplicationService<S>
where
    S: Storage + Send + Sync + 'static,
{
    pub fn new(group: KeyspaceGroup<S>) -> Self {
        Self { group }
    }
}

impl<S> RpcService for ReplicationService<S>
where
    S: Storage + Send + Sync + 'static,
{
    fn register_handlers(registry: &mut ServiceRegistry<Self>) {
        registry.add_handler::<PollKeyspace>();
        registry.add_handler::<GetState>();
        registry.add_handler::<FetchDocs>();
    }
}

#[datacake_rpc::async_trait]
impl<S> Handler<PollKeyspace> for ReplicationService<S>
where
    S: Storage + Send + Sync + 'static,
{
    type Reply = KeyspaceInfo;

    async fn on_message(&self, msg: Request<PollKeyspace>) -> Result<Self::Reply, Status> {
        let msg = msg.to_owned()
            .map_err(Status::internal)?;
        self.group.clock().register_ts(msg.0).await;

        let payload = self.group.get_keyspace_info().await;
        Ok(payload)
    }
}

#[datacake_rpc::async_trait]
impl<S> Handler<GetState> for ReplicationService<S>
where
    S: Storage + Send + Sync + 'static,
{
    type Reply = KeyspaceOrSwotSet;

    async fn on_message(&self, msg: Request<GetState>) -> Result<Self::Reply, Status> {
        let msg = msg.to_owned()
            .map_err(Status::internal)?;
        self.group.clock().register_ts(msg.timestamp).await;

        let keyspace = self.group.get_or_create_keyspace(&msg.keyspace).await;

        let last_updated = keyspace.send(LastUpdated).await;
        let set = keyspace
            .send(crate::keyspace::Serialize)
            .await
            .map_err(Status::internal)?;

        let timestamp = self.group.clock().get_time().await;
        Ok(KeyspaceOrSwotSet {
            timestamp,
            last_updated,
            set,
        })
    }
}

#[datacake_rpc::async_trait]
impl<S> Handler<FetchDocs> for ReplicationService<S>
where
    S: Storage + Send + Sync + 'static,
{
    type Reply = FetchedDocs;

    async fn on_message(&self, msg: Request<FetchDocs>) -> Result<Self::Reply, Status> {
        let msg = msg.to_owned()
            .map_err(Status::internal)?;
        let clock = self.group.clock();
        clock.register_ts(msg.timestamp).await;

        let storage = self.group.storage();

        if msg.doc_ids.len() == 1 {
            let documents = storage
                .get(&msg.keyspace, msg.doc_ids[0])
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .map(|doc| vec![doc])
                .unwrap_or_default();

            let timestamp = clock.get_time().await;
            return Ok(FetchedDocs {
                timestamp,
                documents,
            });
        }

        let documents = self
            .group
            .storage()
            .multi_get(&msg.keyspace, msg.doc_ids.into_iter())
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .collect();

        let timestamp = self.group.clock().get_time().await;
        Ok(FetchedDocs {
            timestamp,
            documents,
        })
    }
}

#[repr(C)]
#[derive(Serialize, Deserialize, Archive)]
#[archive_attr(derive(CheckBytes))]
pub struct PollKeyspace(pub HLCTimestamp);


#[repr(C)]
#[derive(Serialize, Deserialize, Archive)]
#[archive_attr(derive(CheckBytes))]
pub struct GetState {
    pub keyspace: String,
    pub timestamp: HLCTimestamp,
}

#[repr(C)]
#[derive(Serialize, Deserialize, Archive)]
#[archive_attr(derive(CheckBytes))]
pub struct KeyspaceOrSwotSet {
    pub timestamp: HLCTimestamp,
    pub last_updated: HLCTimestamp,
    #[with(rkyv::with::CopyOptimize)]
    pub set: Vec<u8>,
}

#[repr(C)]
#[derive(Serialize, Deserialize, Archive)]
#[archive_attr(derive(CheckBytes))]
pub struct FetchDocs {
    pub keyspace: String,
    #[with(rkyv::with::CopyOptimize)]
    pub doc_ids: Vec<Key>,
    pub timestamp: HLCTimestamp,
}

#[repr(C)]
#[derive(Serialize, Deserialize, Archive)]
#[archive_attr(derive(CheckBytes))]
pub struct FetchedDocs {
    pub documents: Vec<Document>,
    pub timestamp: HLCTimestamp,
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::marker::PhantomData;

    use super::*;
    use crate::keyspace::{KeyspaceTimestamps, Set, READ_REPAIR_SOURCE_ID, Serialize};
    use crate::test_utils::MemStore;
    use crate::Document;

    #[tokio::test]
    async fn test_poll_keyspace() {
        static KEYSPACE: &str = "poll-keyspace";
        let group = KeyspaceGroup::<MemStore>::new_for_test().await;
        let clock = group.clock();
        let service = ReplicationService::new(group.clone());

        let timestamp = clock.get_time().await;
        let poll_req = Request::using_owned(PollKeyspace(timestamp));
        let resp = service
            .on_message(poll_req)
            .await
            .expect("Get keyspace info");

        assert!(resp.keyspace_timestamps.is_empty());

        // Add a new keyspace which is effectively changed.
        let _keyspace = group.get_or_create_keyspace(KEYSPACE).await;

        let timestamp = clock.get_time().await;
        let poll_req = Request::using_owned(PollKeyspace(timestamp));
        let resp = service
            .on_message(poll_req)
            .await
            .expect("Get keyspace info");

        let blank_timestamps = KeyspaceTimestamps::default();
        let diff = blank_timestamps.diff(&resp.keyspace_timestamps).collect::<Vec<_>>();
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
                _marker: PhantomData::<MemStore>::default(),
            })
            .await
            .expect("Set value in store.");

        let last_updated = keyspace.send(LastUpdated).await;
        let set_data = keyspace
            .send(Serialize)
            .await
            .map_err(|e| Status::internal(e.to_string()))
            .expect("Get serialized state.");

        let timestamp = clock.get_time().await;
        let state_req = Request::using_owned(GetState {
            timestamp,
            keyspace: KEYSPACE.to_string(),
        });

        let resp = service
            .on_message(state_req)
            .await
            .expect("Get keyspace state.");

        assert_eq!(
            resp.last_updated,
            last_updated,
            "Last updated timestamps should match."
        );
        assert_eq!(resp.set, set_data, "State data should match.");
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
                _marker: PhantomData::<MemStore>::default(),
            })
            .await
            .expect("Set value in store.");

        let timestamp = clock.get_time().await;
        let fetch_docs_req = Request::using_owned(FetchDocs {
            timestamp,
            keyspace: KEYSPACE.to_string(),
            doc_ids: vec![1],
        });

        let resp = service
            .on_message(fetch_docs_req)
            .await
            .expect("Fetch docs.");

        assert_eq!(resp.documents, vec![doc], "Documents should match.");
    }
}
