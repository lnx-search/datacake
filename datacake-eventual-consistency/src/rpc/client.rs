use std::collections::BTreeMap;
use std::net::SocketAddr;

use datacake_crdt::{HLCTimestamp, Key, OrSWotSet};
use datacake_node::{Clock, NodeId};
use datacake_rpc::{Channel, RpcClient, Status};
use rkyv::AlignedVec;

use crate::core::{Document, DocumentMetadata};
use crate::rpc::services::consistency_impl::{
    BatchPayload,
    ConsistencyService,
    Context,
    MultiPutPayload,
    MultiRemovePayload,
    PutPayload,
    RemovePayload,
};
use crate::rpc::services::replication_impl::{
    FetchDocs,
    GetState,
    PollKeyspace,
    ReplicationService,
};
use crate::Storage;

/// A high level wrapper around the consistency GRPC service.
pub struct ConsistencyClient<S>
where
    S: Storage,
{
    clock: Clock,
    inner: RpcClient<ConsistencyService<S>>,
}

impl<S> ConsistencyClient<S>
where
    S: Storage,
{
    pub fn new(clock: Clock, channel: Channel) -> Self {
        Self {
            clock,
            inner: RpcClient::new(channel),
        }
    }
}

impl<S> ConsistencyClient<S>
where
    S: Storage,
{
    /// Adds a document to the remote node's state.
    pub async fn put(
        &mut self,
        keyspace: impl Into<String>,
        document: Document,
        node_id: NodeId,
        node_addr: SocketAddr,
    ) -> Result<(), Status> {
        let timestamp = self.clock.get_time().await;
        let ts = self
            .inner
            .send(&PutPayload {
                keyspace: keyspace.into(),
                document,
                ctx: Some(Context { node_id, node_addr }),
                timestamp,
            })
            .await?
            .to_owned()
            .map_err(Status::internal)?;
        self.clock.register_ts(ts).await;
        Ok(())
    }

    /// Adds a set of documents to the remote node's state.
    pub async fn multi_put(
        &mut self,
        keyspace: impl Into<String>,
        documents: impl Iterator<Item = Document>,
        node_id: NodeId,
        node_addr: SocketAddr,
    ) -> Result<(), Status> {
        let timestamp = self.clock.get_time().await;
        let ts = self
            .inner
            .send(&MultiPutPayload {
                keyspace: keyspace.into(),
                documents: documents.collect(),
                ctx: Some(Context { node_id, node_addr }),
                timestamp,
            })
            .await?
            .to_owned()
            .map_err(Status::internal)?;
        self.clock.register_ts(ts).await;
        Ok(())
    }

    /// Removes a document from the remote node's state.
    pub async fn del(
        &mut self,
        keyspace: impl Into<String>,
        id: Key,
        ts: HLCTimestamp,
    ) -> Result<(), Status> {
        let timestamp = self.clock.get_time().await;
        let ts = self
            .inner
            .send(&RemovePayload {
                keyspace: keyspace.into(),
                document: DocumentMetadata::new(id, ts),
                timestamp,
            })
            .await?
            .to_owned()
            .map_err(Status::internal)?;
        self.clock.register_ts(ts).await;
        Ok(())
    }

    /// Removes a set of documents from the remote node's state.
    pub async fn multi_del(
        &mut self,
        keyspace: impl Into<String>,
        documents: Vec<DocumentMetadata>,
    ) -> Result<(), Status> {
        let timestamp = self.clock.get_time().await;
        let ts = self
            .inner
            .send(&MultiRemovePayload {
                keyspace: keyspace.into(),
                documents,
                timestamp,
            })
            .await?
            .to_owned()
            .map_err(Status::internal)?;
        self.clock.register_ts(ts).await;
        Ok(())
    }

    pub async fn apply_batch(&mut self, batch: &BatchPayload) -> Result<(), Status> {
        let ts = self
            .inner
            .send(batch)
            .await?
            .to_owned()
            .map_err(Status::internal)?;
        self.clock.register_ts(ts).await;
        Ok(())
    }
}

/// A high level wrapper around the replication GRPC service.
pub struct ReplicationClient<S>
where
    S: Storage,
{
    clock: Clock,
    inner: RpcClient<ReplicationService<S>>,
}

impl<S> ReplicationClient<S>
where
    S: Storage,
{
    pub fn new(clock: Clock, channel: Channel) -> Self {
        Self {
            clock,
            inner: RpcClient::new(channel),
        }
    }
}

impl<S> ReplicationClient<S>
where
    S: Storage,
{
    /// Fetches the newest version of the node's keyspace timestamps.
    pub async fn poll_keyspace(
        &mut self,
    ) -> Result<BTreeMap<String, HLCTimestamp>, Status> {
        let timestamp = self.clock.get_time().await;
        let inner = self
            .inner
            .send(&PollKeyspace(timestamp))
            .await?
            .to_owned()
            .map_err(Status::internal)?;

        self.clock.register_ts(inner.timestamp).await;
        Ok(inner.keyspace_timestamps)
    }

    /// Fetches the node's current state for a given keyspace and returns
    /// the last time the keyspace was modified.
    ///
    /// The returned timestamp must only be used when compared against timestamps produced
    /// by the remote node itself. This is mostly provided to reduce unnecessary IO if the state
    /// has changed between when the keyspace was polled, and when the state was requested.
    pub async fn get_state(
        &mut self,
        keyspace: impl Into<String>,
    ) -> Result<(HLCTimestamp, OrSWotSet<{ crate::keyspace::NUM_SOURCES }>), Status>
    {
        let timestamp = self.clock.get_time().await;
        let inner = self
            .inner
            .send(&GetState {
                timestamp,
                keyspace: keyspace.into(),
            })
            .await?
            .to_owned()
            .map_err(Status::internal)?;

        self.clock.register_ts(inner.timestamp).await;

        let mut aligned = AlignedVec::with_capacity(inner.set.len());
        aligned.extend_from_slice(&inner.set);

        let state = rkyv::from_bytes(&aligned).map_err(|_| Status::invalid())?;
        Ok((inner.last_updated, state))
    }

    /// Fetches a set of documents with the provided IDs belonging to the given keyspace.
    pub async fn fetch_docs(
        &mut self,
        keyspace: impl Into<String>,
        doc_ids: Vec<Key>,
    ) -> Result<Vec<Document>, Status> {
        let timestamp = self.clock.get_time().await;
        let inner = self
            .inner
            .send(&FetchDocs {
                timestamp,
                keyspace: keyspace.into(),
                doc_ids,
            })
            .await?;

        let payload = inner.to_owned().unwrap();

        self.clock.register_ts(payload.timestamp).await;
        Ok(payload.documents)
    }
}
