use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};

use datacake_crdt::{HLCTimestamp, Key};

use crate::rpc::datacake_api;

#[derive(Clone, Eq, PartialEq)]
pub struct Document {
    /// The unique id of the document.
    pub id: Key,

    /// The timestamp of when the document was last updated.
    pub last_updated: HLCTimestamp,

    /// The raw binary data of the document's value.
    pub data: Vec<u8>,
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