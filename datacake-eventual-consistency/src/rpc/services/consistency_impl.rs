use std::marker::PhantomData;
use std::net::SocketAddr;

use datacake_crdt::HLCTimestamp;
use datacake_node::{NodeId, RpcNetwork};
use datacake_rpc::{Handler, Request, RpcService, ServiceRegistry, Status};
use rkyv::{Archive, Deserialize, Serialize};

use crate::core::{Document, DocumentMetadata};
use crate::keyspace::{KeyspaceGroup, CONSISTENCY_SOURCE_ID};
use crate::{DocVec, ProgressTracker, PutContext, Storage};

macro_rules! try_send {
    ($keyspace:expr, $msg:expr) => {{
        if let Err(e) = $keyspace.send($msg).await {
            error!(error = ?e, keyspace = $keyspace.name(), "Failed to handle storage request on consistency API.");
            Err(Status::internal(e.to_string()))
        } else {
            Ok(())
        }
    }};
}

pub struct ConsistencyService<S>
where
    S: Storage,
{
    group: KeyspaceGroup<S>,
    network: RpcNetwork,
}

impl<S> ConsistencyService<S>
where
    S: Storage,
{
    pub fn new(group: KeyspaceGroup<S>, network: RpcNetwork) -> Self {
        Self { group, network }
    }

    fn get_put_ctx(&self, ctx: Option<Context>) -> Result<Option<PutContext>, Status> {
        let ctx = if let Some(info) = ctx {
            let remote_rpc_channel = self.network.get_or_connect(info.node_addr);

            Some(PutContext {
                progress: ProgressTracker::default(),
                remote_node_id: info.node_id,
                remote_addr: info.node_addr,
                remote_rpc_channel,
            })
        } else {
            None
        };

        Ok(ctx)
    }
}

impl<S> RpcService for ConsistencyService<S>
where
    S: Storage,
{
    fn register_handlers(registry: &mut ServiceRegistry<Self>) {
        registry.add_handler::<PutPayload>();
        registry.add_handler::<MultiPutPayload>();
        registry.add_handler::<RemovePayload>();
        registry.add_handler::<MultiRemovePayload>();
        registry.add_handler::<BatchPayload>();
    }
}

#[datacake_rpc::async_trait]
impl<S> Handler<PutPayload> for ConsistencyService<S>
where
    S: Storage,
{
    type Reply = HLCTimestamp;

    async fn on_message(
        &self,
        msg: Request<PutPayload>,
    ) -> Result<HLCTimestamp, Status> {
        let payload = msg
            .into_inner()
            .deserialize_view()
            .map_err(Status::internal)?;

        let doc = payload.document;
        let ctx = self.get_put_ctx(payload.ctx)?;

        self.group.clock().register_ts(payload.timestamp).await;

        let msg = crate::keyspace::Set {
            source: CONSISTENCY_SOURCE_ID,
            doc,
            ctx,
            _marker: PhantomData::<S>::default(),
        };

        let keyspace = self.group.get_or_create_keyspace(&payload.keyspace).await;
        try_send!(keyspace, msg)?;
        Ok(self.group.clock().get_time().await)
    }
}

#[datacake_rpc::async_trait]
impl<S> Handler<MultiPutPayload> for ConsistencyService<S>
where
    S: Storage,
{
    type Reply = HLCTimestamp;

    async fn on_message(
        &self,
        msg: Request<MultiPutPayload>,
    ) -> Result<Self::Reply, Status> {
        let payload = msg
            .into_inner()
            .deserialize_view()
            .map_err(Status::internal)?;

        let ctx = self.get_put_ctx(payload.ctx)?;
        self.group.clock().register_ts(payload.timestamp).await;

        let msg = crate::keyspace::MultiSet {
            source: CONSISTENCY_SOURCE_ID,
            docs: payload.documents,
            ctx,
            _marker: PhantomData::<S>::default(),
        };

        let keyspace = self.group.get_or_create_keyspace(&payload.keyspace).await;
        try_send!(keyspace, msg)?;
        Ok(self.group.clock().get_time().await)
    }
}

#[datacake_rpc::async_trait]
impl<S> Handler<RemovePayload> for ConsistencyService<S>
where
    S: Storage,
{
    type Reply = HLCTimestamp;

    async fn on_message(
        &self,
        msg: Request<RemovePayload>,
    ) -> Result<Self::Reply, Status> {
        let payload = msg
            .into_inner()
            .deserialize_view()
            .map_err(Status::internal)?;

        self.group.clock().register_ts(payload.timestamp).await;

        let msg = crate::keyspace::Del {
            source: CONSISTENCY_SOURCE_ID,
            doc: payload.document,
            _marker: PhantomData::<S>::default(),
        };

        let keyspace = self.group.get_or_create_keyspace(&payload.keyspace).await;
        try_send!(keyspace, msg)?;
        Ok(self.group.clock().get_time().await)
    }
}

#[datacake_rpc::async_trait]
impl<S> Handler<MultiRemovePayload> for ConsistencyService<S>
where
    S: Storage,
{
    type Reply = HLCTimestamp;

    async fn on_message(
        &self,
        msg: Request<MultiRemovePayload>,
    ) -> Result<Self::Reply, Status> {
        let payload = msg
            .into_inner()
            .deserialize_view()
            .map_err(Status::internal)?;

        self.group.clock().register_ts(payload.timestamp).await;

        let msg = crate::keyspace::MultiDel {
            source: CONSISTENCY_SOURCE_ID,
            docs: payload.documents,
            _marker: PhantomData::<S>::default(),
        };

        let keyspace = self.group.get_or_create_keyspace(&payload.keyspace).await;
        try_send!(keyspace, msg)?;
        Ok(self.group.clock().get_time().await)
    }
}

#[datacake_rpc::async_trait]
impl<S> Handler<BatchPayload> for ConsistencyService<S>
where
    S: Storage,
{
    type Reply = HLCTimestamp;

    async fn on_message(
        &self,
        msg: Request<BatchPayload>,
    ) -> Result<Self::Reply, Status> {
        let msg = msg
            .into_inner()
            .deserialize_view()
            .map_err(Status::internal)?;

        self.group.clock().register_ts(msg.timestamp).await;

        for payload in msg.removed {
            let msg = crate::keyspace::MultiDel {
                source: CONSISTENCY_SOURCE_ID,
                docs: payload.documents,
                _marker: PhantomData::<S>::default(),
            };

            let keyspace = self.group.get_or_create_keyspace(&payload.keyspace).await;
            try_send!(keyspace, msg)?;
        }

        for payload in msg.modified {
            let ctx = self.get_put_ctx(payload.ctx)?;

            let msg = crate::keyspace::MultiSet {
                source: CONSISTENCY_SOURCE_ID,
                docs: payload.documents,
                ctx,
                _marker: PhantomData::<S>::default(),
            };

            let keyspace = self.group.get_or_create_keyspace(&payload.keyspace).await;
            try_send!(keyspace, msg)?;
        }
        Ok(self.group.clock().get_time().await)
    }
}

#[repr(C)]
#[derive(Serialize, Deserialize, Archive)]
#[archive(check_bytes)]
pub struct PutPayload {
    pub keyspace: String,
    pub ctx: Option<Context>,
    pub document: Document,
    pub timestamp: HLCTimestamp,
}

#[repr(C)]
#[derive(Serialize, Deserialize, Archive, Debug)]
#[archive(check_bytes)]
pub struct MultiPutPayload {
    pub keyspace: String,
    pub ctx: Option<Context>,
    pub documents: DocVec<Document>,
    pub timestamp: HLCTimestamp,
}

#[repr(C)]
#[derive(Serialize, Deserialize, Archive, Debug)]
#[archive(check_bytes)]
pub struct RemovePayload {
    pub keyspace: String,
    pub document: DocumentMetadata,
    pub timestamp: HLCTimestamp,
}

#[repr(C)]
#[derive(Serialize, Deserialize, Archive, Debug)]
#[archive(check_bytes)]
pub struct MultiRemovePayload {
    pub keyspace: String,
    pub documents: DocVec<DocumentMetadata>,
    pub timestamp: HLCTimestamp,
}

#[repr(C)]
#[derive(Serialize, Deserialize, Archive, Debug)]
#[archive(check_bytes)]
pub struct BatchPayload {
    pub timestamp: HLCTimestamp,
    pub modified: DocVec<MultiPutPayload>,
    pub removed: DocVec<MultiRemovePayload>,
}

#[repr(C)]
#[derive(Serialize, Deserialize, Archive, Debug)]
#[archive(check_bytes)]
pub struct Context {
    pub node_id: NodeId,
    pub node_addr: SocketAddr,
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use smallvec::smallvec;

    use super::*;
    use crate::test_utils::MemStore;

    #[tokio::test]
    async fn test_consistency_put() {
        static KEYSPACE: &str = "put-keyspace";
        let group = KeyspaceGroup::<MemStore>::new_for_test().await;
        let clock = group.clock();
        let storage = group.storage();
        let service = ConsistencyService::new(group.clone(), RpcNetwork::default());

        let doc = Document::new(1, clock.get_time().await, b"Hello, world".to_vec());
        let put_req = Request::using_owned(PutPayload {
            keyspace: KEYSPACE.to_string(),
            document: doc.clone(),
            ctx: None,
            timestamp: clock.get_time().await,
        })
        .await;

        service
            .on_message(put_req)
            .await
            .expect("Put request should succeed.");

        let saved_doc = storage
            .get(KEYSPACE, doc.id())
            .await
            .expect("Get new doc.")
            .expect("Doc should not be None");
        assert_eq!(saved_doc, doc, "Documents stored should match.");

        let metadata = storage
            .iter_metadata(KEYSPACE)
            .await
            .expect("Iter metadata")
            .collect::<Vec<_>>();
        assert_eq!(metadata, vec![(doc.id(), doc.last_updated(), false)]);
    }

    #[tokio::test]
    async fn test_consistency_put_many() {
        static KEYSPACE: &str = "put-many-keyspace";
        let group = KeyspaceGroup::<MemStore>::new_for_test().await;
        let clock = group.clock();
        let storage = group.storage();
        let service = ConsistencyService::new(group.clone(), RpcNetwork::default());

        let doc_1 = Document::new(1, clock.get_time().await, b"Hello, world 1".to_vec());
        let doc_2 = Document::new(2, clock.get_time().await, b"Hello, world 2".to_vec());
        let doc_3 = Document::new(3, clock.get_time().await, b"Hello, world 3".to_vec());
        let put_req = Request::using_owned(MultiPutPayload {
            keyspace: KEYSPACE.to_string(),
            ctx: None,
            documents: smallvec![doc_1.clone(), doc_2.clone(), doc_3.clone()],
            timestamp: clock.get_time().await,
        })
        .await;

        service
            .on_message(put_req)
            .await
            .expect("Put request should succeed.");

        let saved_docs = storage
            .multi_get(
                KEYSPACE,
                vec![doc_1.id(), doc_2.id(), doc_3.id()].into_iter(),
            )
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
                (doc_1.id(), doc_1.last_updated(), false),
                (doc_2.id(), doc_2.last_updated(), false),
                (doc_3.id(), doc_3.last_updated(), false),
            ])
        );
    }

    #[tokio::test]
    async fn test_consistency_remove() {
        static KEYSPACE: &str = "remove-keyspace";
        let group = KeyspaceGroup::<MemStore>::new_for_test().await;
        let clock = group.clock();
        let storage = group.storage();
        let service = ConsistencyService::new(group.clone(), RpcNetwork::default());

        let mut doc =
            Document::new(1, clock.get_time().await, b"Hello, world 1".to_vec());
        add_docs(
            KEYSPACE,
            smallvec![doc.clone()],
            clock.get_time().await,
            &service,
        )
        .await;

        let saved_doc = storage
            .get(KEYSPACE, doc.id())
            .await
            .expect("Get new doc.")
            .expect("Doc should not be None");
        assert_eq!(saved_doc, doc, "Documents stored should match.");

        doc.metadata.last_updated = clock.get_time().await;
        let remove_req = Request::using_owned(RemovePayload {
            keyspace: KEYSPACE.to_string(),
            document: doc.metadata,
            timestamp: clock.get_time().await,
        })
        .await;

        service
            .on_message(remove_req)
            .await
            .expect("Remove document.");

        let saved_doc = storage.get(KEYSPACE, doc.id()).await.expect("Get new doc.");
        assert!(saved_doc.is_none(), "Documents should no longer exist.");

        let metadata = storage
            .iter_metadata(KEYSPACE)
            .await
            .expect("Iter metadata")
            .collect::<Vec<_>>();
        assert_eq!(metadata, vec![(doc.id(), doc.last_updated(), true)]);
    }

    #[tokio::test]
    async fn test_consistency_remove_many() {
        static KEYSPACE: &str = "remove-many-keyspace";
        let group = KeyspaceGroup::<MemStore>::new_for_test().await;
        let clock = group.clock();
        let storage = group.storage();
        let service = ConsistencyService::new(group.clone(), RpcNetwork::default());

        let mut doc_1 =
            Document::new(1, clock.get_time().await, b"Hello, world 1".to_vec());
        let mut doc_2 =
            Document::new(2, clock.get_time().await, b"Hello, world 2".to_vec());
        let doc_3 = Document::new(3, clock.get_time().await, b"Hello, world 3".to_vec());
        add_docs(
            KEYSPACE,
            smallvec![doc_1.clone(), doc_2.clone(), doc_3.clone()],
            clock.get_time().await,
            &service,
        )
        .await;

        let saved_docs = storage
            .multi_get(
                KEYSPACE,
                vec![doc_1.id(), doc_2.id(), doc_3.id()].into_iter(),
            )
            .await
            .expect("Get new doc.")
            .collect::<Vec<_>>();
        assert_eq!(
            saved_docs,
            vec![doc_1.clone(), doc_2.clone(), doc_3.clone()],
            "Documents stored should match."
        );

        doc_1.metadata.last_updated = clock.get_time().await;
        doc_2.metadata.last_updated = clock.get_time().await;
        let remove_req = Request::using_owned(MultiRemovePayload {
            keyspace: KEYSPACE.to_string(),
            documents: smallvec![doc_1.metadata, doc_2.metadata],
            timestamp: clock.get_time().await,
        })
        .await;

        service
            .on_message(remove_req)
            .await
            .expect("Remove documents.");

        let saved_docs = storage
            .multi_get(
                KEYSPACE,
                vec![doc_1.id(), doc_2.id(), doc_3.id()].into_iter(),
            )
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
                (doc_1.id(), doc_1.last_updated(), true),
                (doc_2.id(), doc_2.last_updated(), true),
                (doc_3.id(), doc_3.last_updated(), false),
            ])
        );
    }

    async fn add_docs(
        keyspace: &str,
        documents: DocVec<Document>,
        timestamp: HLCTimestamp,
        service: &ConsistencyService<MemStore>,
    ) {
        let put_req = Request::using_owned(MultiPutPayload {
            keyspace: keyspace.to_string(),
            ctx: None,
            documents,
            timestamp,
        })
        .await;

        service
            .on_message(put_req)
            .await
            .expect("Put request should succeed.");
    }
}
