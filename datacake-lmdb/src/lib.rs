//! # Datacake LMDB
//!
//! A pre-built implementation of the datacake-eventual-consistency `Storage` trait, this allows you to set up
//! a persistent cluster immediately without any hassle of implementing a correct store.
//!
//! For more info see <https://github.com/lnx-search/datacake>
//!
//! ## Example
//!
//! ```rust
//! use std::env::temp_dir;
//! use anyhow::Result;
//! use uuid::Uuid;
//! use datacake_eventual_consistency::EventuallyConsistentStoreExtension;
//! use datacake_node::{
//!     ConnectionConfig,
//!     Consistency,
//!     DCAwareSelector,
//!     DatacakeNodeBuilder,
//! };
//! use datacake_lmdb::LmdbStorage;
//!
//! static KEYSPACE: &str = "lmdb-store";
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     tracing_subscriber::fmt::init();
//!
//!     let temp_dir = temp_dir().join(Uuid::new_v4().to_string());
//!     std::fs::create_dir_all(&temp_dir)?;
//!
//!     let store = LmdbStorage::open(temp_dir).await?;
//!
//!     let addr = test_helper::get_unused_addr();
//!     let connection_cfg = ConnectionConfig::new(addr, addr, Vec::<String>::new());
//!
//!     let node = DatacakeNodeBuilder::<DCAwareSelector>::new(1, connection_cfg)
//!         .connect()
//!         .await?;
//!     let store = node
//!         .add_extension(EventuallyConsistentStoreExtension::new(store))
//!         .await?;
//!
//!     let handle = store.handle();
//!
//!     handle.put(KEYSPACE, 1, b"Hello, world".to_vec(), Consistency::All).await?;
//!
//!     let doc = handle
//!         .get(KEYSPACE, 1)
//!         .await?
//!         .expect("Document should not be none");
//!     assert_eq!(doc.id(), 1);
//!     assert_eq!(doc.data(), b"Hello, world");
//!
//!     handle.del(KEYSPACE, 1, Consistency::All).await?;
//!     let doc = handle.get(KEYSPACE, 1).await?;
//!     assert!(doc.is_none(), "No document should not exist!");
//!
//!     handle.del(KEYSPACE, 2, Consistency::All).await?;
//!     let doc = handle.get(KEYSPACE, 2).await?;
//!     assert!(doc.is_none(), "No document should not exist!");
//!
//!     node.shutdown().await;
//!
//!     Ok(())
//! }
//! ```

mod db;

use std::path::Path;

use async_trait::async_trait;
use datacake_crdt::{HLCTimestamp, Key};
use datacake_eventual_consistency::{
    BulkMutationError,
    Document,
    DocumentMetadata,
    Storage,
};
pub use db::StorageHandle;
pub use heed;
pub use heed::Error;

pub struct LmdbStorage {
    db: StorageHandle,
}

impl LmdbStorage {
    /// Connects to the LMDB database.
    /// This spawns 1 background threads with actions being executed within that thread.
    /// This approach reduces the affect of writes blocking reads and vice-versa.
    pub async fn open(path: impl AsRef<Path>) -> heed::Result<Self> {
        let db = StorageHandle::open(path).await?;

        Ok(Self { db })
    }

    /// Access to the LMDB storage handle.
    ///
    /// This allows you to access the LMDB db directly
    /// including it's environment, but it does
    /// not provide any access to the KV databases used
    /// by the datacake storage layer.
    pub fn handle(&self) -> &StorageHandle {
        &self.db
    }
}

#[async_trait]
impl Storage for LmdbStorage {
    type Error = heed::Error;
    type DocsIter = Box<dyn Iterator<Item = Document>>;
    type MetadataIter = Box<dyn Iterator<Item = (Key, HLCTimestamp, bool)>>;

    async fn get_keyspace_list(&self) -> Result<Vec<String>, Self::Error> {
        self.handle().keyspace_list().await
    }

    async fn iter_metadata(
        &self,
        keyspace: &str,
    ) -> Result<Self::MetadataIter, Self::Error> {
        self.handle()
            .get_metadata(keyspace)
            .await
            .map(|v| Box::new(v.into_iter()) as Self::MetadataIter)
    }

    async fn remove_tombstones(
        &self,
        keyspace: &str,
        keys: impl Iterator<Item = Key> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        self.handle()
            .remove_tombstones(keyspace, keys)
            .await
            .map_err(BulkMutationError::empty_with_error)
    }

    async fn put(&self, keyspace: &str, document: Document) -> Result<(), Self::Error> {
        self.handle().put_kv(keyspace, document).await
    }

    async fn multi_put(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = Document> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        self.handle()
            .put_many_kv(keyspace, documents)
            .await
            .map_err(BulkMutationError::empty_with_error)
    }

    async fn mark_as_tombstone(
        &self,
        keyspace: &str,
        doc_id: Key,
        timestamp: HLCTimestamp,
    ) -> Result<(), Self::Error> {
        self.handle()
            .mark_tombstone(keyspace, doc_id, timestamp)
            .await
    }

    async fn mark_many_as_tombstone(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = DocumentMetadata> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        self.handle()
            .mark_many_as_tombstone(keyspace, documents)
            .await
            .map_err(BulkMutationError::empty_with_error)
    }

    async fn get(
        &self,
        keyspace: &str,
        doc_id: Key,
    ) -> Result<Option<Document>, Self::Error> {
        self.handle().get(keyspace, doc_id).await
    }

    async fn multi_get(
        &self,
        keyspace: &str,
        doc_ids: impl Iterator<Item = Key> + Send,
    ) -> Result<Self::DocsIter, Self::Error> {
        self.handle()
            .get_many(keyspace, doc_ids)
            .await
            .map(|v| Box::new(v.into_iter()) as Self::DocsIter)
    }
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;

    use datacake_eventual_consistency::test_suite;
    use uuid::Uuid;

    use crate::LmdbStorage;

    #[tokio::test]
    async fn test_storage_logic() {
        let path = temp_dir().join(Uuid::new_v4().to_string());
        std::fs::create_dir_all(&path).unwrap();

        let storage = LmdbStorage::open(path).await.expect("Open DB");
        test_suite::run_test_suite(storage).await;
    }
}
