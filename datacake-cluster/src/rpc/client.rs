use std::borrow::Cow;
use std::net::SocketAddr;
use std::sync::Arc;

use crossbeam_utils::atomic::AtomicCell;
use datacake_crdt::{HLCTimestamp, Key, OrSWotSet};
use rkyv::AlignedVec;
use tonic::transport::Channel;
use tonic::Status;
use datacake_node::Clock;

use crate::core::Document;
use crate::keyspace::KeyspaceTimestamps;
use crate::rpc::datacake_api;
use crate::rpc::datacake_api::consistency_api_client::ConsistencyApiClient;
use crate::rpc::datacake_api::replication_api_client::ReplicationApiClient;
use crate::rpc::datacake_api::{
    Context,
    DocumentMetadata,
    FetchDocs,
    GetState,
    MultiPutPayload,
    MultiRemovePayload,
    PollPayload,
    PutPayload,
    RemovePayload,
};

/// A high level wrapper around the consistency GRPC service.
pub struct ConsistencyClient {
    clock: Clock,
    inner: ConsistencyApiClient<Channel>,
}

impl ConsistencyClient {
    pub fn new(clock: Clock, channel: Channel) -> Self {
        Self {
            clock,
            inner: ConsistencyApiClient::new(channel),
        }
    }
}

impl ConsistencyClient {
    /// Adds a document to the remote node's state.
    pub async fn put(
        &mut self,
        keyspace: impl Into<String>,
        doc: Document,
        node_id: &str,
        node_addr: SocketAddr,
    ) -> Result<(), Status> {
        let ts = self
            .inner
            .put(PutPayload {
                keyspace: keyspace.into(),
                document: Some(doc.into()),
                ctx: Some(Context {
                    node_id: node_id.to_string(),
                    node_addr: node_addr.to_string(),
                }),
            })
            .await?
            .into_inner()
            .into();
        self.clock.register_ts(ts).await;
        Ok(())
    }

    /// Adds a set of documents to the remote node's state.
    pub async fn multi_put(
        &mut self,
        keyspace: impl Into<String>,
        docs: impl Iterator<Item = Document>,
        node_id: &str,
        node_addr: SocketAddr,
    ) -> Result<(), Status> {
        let ts = self
            .inner
            .multi_put(MultiPutPayload {
                keyspace: keyspace.into(),
                documents: docs.map(|doc| doc.into()).collect(),
                ctx: Some(Context {
                    node_id: node_id.to_string(),
                    node_addr: node_addr.to_string(),
                }),
            })
            .await?
            .into_inner()
            .into();
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
        let ts = self
            .inner
            .remove(RemovePayload {
                keyspace: keyspace.into(),
                document: Some(DocumentMetadata {
                    id,
                    last_updated: Some(ts.into()),
                }),
            })
            .await?
            .into_inner()
            .into();
        self.clock.register_ts(ts).await;
        Ok(())
    }

    /// Removes a set of documents from the remote node's state.
    pub async fn multi_del(
        &mut self,
        keyspace: impl Into<String>,
        pairs: impl Iterator<Item = (Key, HLCTimestamp)>,
    ) -> Result<(), Status> {
        let ts = self
            .inner
            .multi_remove(MultiRemovePayload {
                keyspace: keyspace.into(),
                documents: pairs
                    .map(|(id, ts)| DocumentMetadata {
                        id,
                        last_updated: Some(ts.into()),
                    })
                    .collect(),
            })
            .await?
            .into_inner()
            .into();
        self.clock.register_ts(ts).await;
        Ok(())
    }

    pub async fn apply_batch(
        &mut self,
        batch: datacake_api::BatchPayload,
    ) -> Result<(), Status> {
        let ts = self.inner.apply_batch(batch).await?.into_inner().into();
        self.clock.register_ts(ts).await;
        Ok(())
    }
}

/// A high level wrapper around the replication GRPC service.
pub struct ReplicationClient {
    clock: Clock,
    inner: ReplicationApiClient<Channel>,
}

impl ReplicationClient {
    pub fn new(clock: Clock, channel: Channel) -> Self {
        Self {
            clock,
            inner: ReplicationApiClient::new(channel),
        }
    }
}

impl ReplicationClient {
    /// Fetches the newest version of the node's keyspace timestamps.
    pub async fn poll_keyspace(&mut self) -> Result<KeyspaceTimestamps, Status> {
        let ts = self.clock.get_time().await;
        let inner = self
            .inner
            .poll_keyspace(PollPayload {
                timestamp: Some(ts.into()),
            })
            .await?
            .into_inner();

        let ts = HLCTimestamp::from(inner.timestamp.unwrap());
        self.clock.register_ts(ts).await;

        let mut timestamps = KeyspaceTimestamps::default();
        for (keyspace, ts) in inner.keyspace_timestamps {
            let ts = HLCTimestamp::from(ts);
            timestamps.insert(Cow::Owned(keyspace), Arc::new(AtomicCell::new(ts)));
        }

        Ok(timestamps)
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
        let ts = self.clock.get_time().await;
        let inner = self
            .inner
            .get_state(GetState {
                timestamp: Some(ts.into()),
                keyspace: keyspace.into(),
            })
            .await?
            .into_inner();

        let ts = HLCTimestamp::from(inner.timestamp.unwrap());
        self.clock.register_ts(ts).await;

        let mut aligned = AlignedVec::with_capacity(inner.set_data.len());
        aligned.extend_from_slice(&inner.set_data);

        let state = rkyv::from_bytes(&aligned)
            .map_err(|_| Status::data_loss("Returned buffer is corrupted."))?;

        Ok((inner.last_updated.unwrap().into(), state))
    }

    /// Fetches a set of documents with the provided IDs belonging to the given keyspace.
    pub async fn fetch_docs(
        &mut self,
        keyspace: impl Into<String>,
        doc_ids: Vec<Key>,
    ) -> Result<impl Iterator<Item = Document>, Status> {
        let ts = self.clock.get_time().await;
        let inner = self
            .inner
            .fetch_docs(FetchDocs {
                timestamp: Some(ts.into()),
                keyspace: keyspace.into(),
                doc_ids,
            })
            .await?
            .into_inner();

        let ts = HLCTimestamp::from(inner.timestamp.unwrap());
        self.clock.register_ts(ts).await;

        let documents = inner.documents.into_iter().map(Document::from);

        Ok(documents)
    }
}
