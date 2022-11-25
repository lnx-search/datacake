use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};

use bytes::Bytes;
use datacake_crdt::{HLCTimestamp, Key};

use crate::rpc::datacake_api;
use crate::{error, KeyspaceGroup, Storage};

#[derive(Clone)]
pub struct Document {
    /// The unique id of the document.
    pub id: Key,

    /// The timestamp of when the document was last updated.
    pub last_updated: HLCTimestamp,

    /// The raw binary data of the document's value.
    pub data: Bytes,
}

impl Document {
    /// A convenience method for passing data values which can be sent as bytes.
    pub fn new(id: Key, last_updated: HLCTimestamp, data: impl Into<Bytes>) -> Self {
        Self {
            id,
            last_updated,
            data: data.into(),
        }
    }
}

impl Eq for Document {}

impl PartialEq for Document {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.last_updated == other.last_updated
            && self.data == other.data
    }
}

impl Hash for Document {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}

impl Debug for Document {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut f = f.debug_struct("Document");
        f.field("id", &self.id);
        f.field("last_updated", &self.last_updated);

        #[cfg(test)]
        {
            f.field("data", &bytes::Bytes::copy_from_slice(&self.data));
        }

        f.finish()
    }
}

impl From<datacake_api::Document> for Document {
    fn from(doc: datacake_api::Document) -> Self {
        let metadata = doc.metadata.unwrap();
        let last_updated = metadata.last_updated.unwrap();
        Self {
            id: metadata.id,
            last_updated: last_updated.into(),
            data: doc.data,
        }
    }
}

impl From<Document> for datacake_api::Document {
    fn from(doc: Document) -> Self {
        Self {
            metadata: Some(datacake_api::DocumentMetadata {
                id: doc.id,
                last_updated: Some(doc.last_updated.into()),
            }),
            data: doc.data,
        }
    }
}

impl From<datacake_api::Timestamp> for HLCTimestamp {
    fn from(ts: datacake_api::Timestamp) -> Self {
        Self::new(ts.millis, ts.counter as u16, ts.node_id)
    }
}

impl From<HLCTimestamp> for datacake_api::Timestamp {
    fn from(ts: HLCTimestamp) -> Self {
        Self {
            millis: ts.millis(),
            counter: ts.counter() as u32,
            node_id: ts.node(),
        }
    }
}

/// Inserts or updates the given keyspace locally and updates the given keyspace state.
pub(crate) async fn put_data<S>(
    keyspace: &str,
    document: Document,
    group: &KeyspaceGroup<S>,
) -> Result<(), error::DatacakeError<S::Error>>
where
    S: Storage + Send + Sync + 'static,
{
    let doc_id = document.id;
    let last_updated = document.last_updated;

    group.storage().put(keyspace, document).await?;

    let keyspace = group.get_or_create_keyspace(keyspace).await;

    keyspace.put(doc_id, last_updated).await?;

    Ok(())
}

/// Inserts or updates many entries in the given keyspace
/// locally and updates the given keyspace state.
pub(crate) async fn put_many_data<S>(
    keyspace: &str,
    documents: impl Iterator<Item = Document> + Send,
    group: &KeyspaceGroup<S>,
) -> Result<(), error::DatacakeError<S::Error>>
where
    S: Storage + Send + Sync + 'static,
{
    let mut entries = Vec::new();
    let documents = documents.map(|doc| {
        entries.push((doc.id, doc.last_updated));
        doc
    });

    group.storage().multi_put(keyspace, documents).await?;

    let keyspace = group.get_or_create_keyspace(keyspace).await;

    keyspace.multi_put(entries).await?;

    Ok(())
}

/// Removes a document from the local keyspace and updates the given keyspace state.
pub(crate) async fn del_data<S>(
    keyspace: &str,
    doc_id: Key,
    last_updated: HLCTimestamp,
    group: &KeyspaceGroup<S>,
) -> Result<(), error::DatacakeError<S::Error>>
where
    S: Storage + Send + Sync + 'static,
{
    group.storage().del(keyspace, doc_id).await?;

    let keyspace = group.get_or_create_keyspace(keyspace).await;

    keyspace.del(doc_id, last_updated).await?;

    Ok(())
}

/// Removes multiple documents from the local keyspace and updates the given keyspace state.
pub(crate) async fn del_many_data<S>(
    keyspace: &str,
    documents: impl Iterator<Item = (Key, HLCTimestamp)> + Send,
    group: &KeyspaceGroup<S>,
) -> Result<(), error::DatacakeError<S::Error>>
where
    S: Storage + Send + Sync + 'static,
{
    let mut entries = Vec::new();
    let documents = documents.map(|doc| {
        entries.push(doc);
        doc.0
    });

    group.storage().multi_del(keyspace, documents).await?;

    let keyspace = group.get_or_create_keyspace(keyspace).await;

    keyspace.multi_del(entries).await?;

    Ok(())
}
