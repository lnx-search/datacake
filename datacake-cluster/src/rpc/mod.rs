use bytecheck::CheckBytes;
use bytes::Bytes;
use datacake_crdt::{HLCTimestamp, Key};
use rkyv::{Archive, Deserialize, Serialize};
use tonic::{Code, Status};

pub mod client;
mod client_cluster;
mod cluster_rpc_models;
pub mod server;

pub use client_cluster::{Client, ClientCluster};

use crate::rpc::cluster_rpc_models::Timestamp;

#[derive(Debug, Clone, Serialize, Deserialize, Archive)]
#[archive_attr(derive(CheckBytes))]
/// A wrapper type that contains all of the necessary document information.
pub struct Document {
    /// The unique id of the document.
    pub id: Key,

    /// The timestamp of when the document was last modified.
    ///
    /// This should not be modified under any circumstances.
    pub last_modified: HLCTimestamp,

    /// The raw data of the document.
    pub data: Vec<u8>,
}

#[tonic::async_trait]
pub trait DataHandler: Send + Sync + 'static {
    /// Fetches a set of documents and returns the individual key-buffer pairs.
    async fn get_documents(
        &self,
        doc_ids: &[Key],
    ) -> Result<Vec<Document>, anyhow::Error>;

    /// Fetches a single document from the datastore.
    async fn get_document(
        &self,
        doc_id: Key,
    ) -> Result<Option<Document>, anyhow::Error> {
        Ok(self.get_documents(&[doc_id]).await?.pop())
    }

    /// Insert or update a set of documents.
    ///
    /// NOTE:
    ///  This should clear any associated tombstones on the document if applicable.
    async fn upsert_documents(
        &self,
        docs: Vec<(Key, HLCTimestamp, Vec<u8>)>,
    ) -> Result<(), anyhow::Error>;

    /// Insert or update a single document.
    ///
    /// NOTE:
    ///  This should clear any associated tombstones on the document if applicable.
    async fn upsert_document(&self, doc: Document) -> Result<(), anyhow::Error> {
        self.upsert_documents(vec![(doc.id, doc.last_modified, doc.data)])
            .await
    }

    /// Mark a set of documents as deleted.
    ///
    /// This should be a no-op if the document does not exist.
    ///
    /// NOTE:
    ///  The original data does not need to be maintained, but a marker signalling
    ///  that the key has been deleted does.
    async fn mark_tombstone_documents(
        &self,
        doc_ids: Vec<(Key, HLCTimestamp)>,
    ) -> Result<(), anyhow::Error>;

    /// Mark a documents as deleted.
    ///
    /// This should be a no-op if the document does not exist.
    ///
    /// NOTE:
    ///  The original data does not need to be maintained, but a marker signalling
    ///  that the key has been deleted does.
    async fn mark_tombstone_document(
        &self,
        doc_id: Key,
        ts: HLCTimestamp,
    ) -> Result<(), anyhow::Error> {
        self.mark_tombstone_documents(vec![(doc_id, ts)]).await
    }

    /// Clears a set of document's tombstones.
    ///
    /// This should be a no-op if the document isn't marked as a tombstone.
    async fn clear_tombstone_documents(
        &self,
        doc_ids: Vec<Key>,
    ) -> Result<(), anyhow::Error>;
}

/// A helper iterator that generates documents
/// from the primary buffer and set of offsets.
pub struct DocsBlock {
    pub(crate) doc_ids: Vec<Key>,
    pub(crate) offsets: Vec<u32>,
    pub(crate) timestamps: Vec<HLCTimestamp>,
    pub(crate) docs_buffer: Bytes,
}

impl Iterator for DocsBlock {
    type Item = (Key, HLCTimestamp, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.doc_ids.is_empty()
            || self.offsets.is_empty()
            || self.timestamps.is_empty()
        {
            return None;
        }

        let doc_id = self.doc_ids.remove(0);
        let timestamp = self.timestamps.remove(0);
        let len = self.offsets.remove(0) as usize;
        let buffer = self.docs_buffer.split_to(len);

        Some((doc_id, timestamp, buffer.to_vec()))
    }

    fn count(self) -> usize {
        self.doc_ids.len()
    }
}

pub(super) async fn build_docs_buffer<B: AsRef<[u8]>>(
    docs: impl Iterator<Item = (Key, HLCTimestamp, B)>,
) -> (Vec<Key>, Vec<u32>, Vec<Timestamp>, Bytes, u32) {
    let mut continuous_buffer = vec![];
    let mut doc_ids = vec![];
    let mut offsets = vec![];
    let mut timestamps = vec![];

    for (doc_id, ts, data) in docs {
        let buffer = data.as_ref();
        continuous_buffer.extend_from_slice(buffer);
        doc_ids.push(doc_id);
        offsets.push(buffer.len() as u32);
        timestamps.push(Timestamp {
            millis: ts.millis(),
            counter: ts.counter() as u32,
            node_id: ts.node(),
        });
    }

    let uncompressed_size = continuous_buffer.len() as u32;
    let docs = Bytes::from(crate::shared::compress_docs(continuous_buffer).await);

    (doc_ids, offsets, timestamps, docs, uncompressed_size)
}

#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    #[error("The rpc actor has encountered a fatal error and was unable to resume")]
    DeadActor,

    #[error("Failed to deserialize response body due to and error, normally because the response payload is malformed.")]
    DeserializeError,

    #[error("The remote node failed to respond to the request: {0} - {1}")]
    RemoteError(Code, String),

    #[error("The operation failed due to the request payload not matching the expected schema: {0}")]
    MalformedPayload(String),

    #[error("The operation failed due to a unknown error: {0} - {1}")]
    Unknown(Code, String),

    #[error("Connection has been disconnected.")]
    Disconnected,
}

impl From<Status> for RpcError {
    fn from(s: Status) -> Self {
        match s.code() {
            Code::Internal | Code::Aborted | Code::Cancelled => {
                Self::RemoteError(s.code(), s.message().to_string())
            },
            Code::Unavailable => Self::Disconnected,
            Code::DataLoss
            | Code::InvalidArgument
            | Code::FailedPrecondition
            | Code::OutOfRange => Self::MalformedPayload(s.message().to_string()),
            _ => Self::Unknown(s.code(), s.message().to_string()),
        }
    }
}
