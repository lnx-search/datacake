use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datacake_crdt::{HLCTimestamp, Key};
use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};
use rkyv::with::CopyOptimize;


#[repr(C)]
#[derive(Serialize, Deserialize, Archive, Copy, Clone, Debug, PartialEq)]
#[archive_attr(repr(C), derive(CheckBytes))]
pub struct DocumentMetadata {
    /// The unique id of the document.
    pub id: Key,

    /// The timestamp of when the document was last updated.
    pub last_updated: HLCTimestamp,
}

impl DocumentMetadata {
    pub fn new(id: Key, last_updated: HLCTimestamp) -> Self {
        Self {
            id,
            last_updated,
        }
    }
}

#[repr(C)]
#[derive(Serialize, Deserialize, Archive, Clone)]
#[archive_attr(repr(C), derive(CheckBytes))]
pub struct Document {
    /// The metadata associated with the document.
    pub metadata: DocumentMetadata,

    /// The raw binary data of the document's value.
    data: Arc<Bytes>,
}

impl Document {
    /// A convenience method for passing data values which can be sent as bytes.
    pub fn new(id: Key, last_updated: HLCTimestamp, data: impl Into<Vec<u8>>) -> Self {
        Self {
            metadata: DocumentMetadata {
                id,
                last_updated,
            },
            data: Arc::new(Bytes { buffer: data.into() }),
        }
    }

    #[inline]
    /// The binary data of the document.
    pub fn data(&self) -> &[u8] {
        &self.data.buffer
    }

    #[inline]
    /// The unique id of the document.
    pub fn id(&self) -> Key {
        self.metadata.id
    }

    #[inline]
    /// The timestamp of when the document was last updated.
    pub fn last_updated(&self) -> HLCTimestamp {
        self.metadata.last_updated
    }
}

impl Eq for Document {}

impl PartialEq for Document {
    fn eq(&self, other: &Self) -> bool {
        self.metadata.id == other.metadata.id
            && self.metadata.last_updated == other.metadata.last_updated
            && self.data() == other.data()
    }
}

impl Hash for Document {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.metadata.id.hash(state)
    }
}

impl Debug for Document {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut f = f.debug_struct("Document");
        f.field("id", &self.id());
        f.field("last_updated", &self.last_updated());

        #[cfg(any(test, feature = "test-utils"))]
        {
            f.field("data", &String::from_utf8_lossy(self.data()));
        }

        f.finish()
    }
}

#[repr(C)]
#[derive(Serialize, Deserialize, Archive, PartialEq, Clone)]
#[archive_attr(repr(C), derive(CheckBytes))]
/// A new type wrapper around a `Vec<u8>` to implement the
/// [CopyOptimize] optimisations from [rkyv].
pub struct Bytes {
    #[with(CopyOptimize)]
    buffer: Vec<u8>,
}