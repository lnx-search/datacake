use std::fmt::{Debug, Display};
use datacake_crdt::{HLCTimestamp, Key, StateChanges};

use crate::rpc::Document;

#[tonic::async_trait]
/// The metadata store and controller.
///
/// This exists to persist and maintain the shard states.
///
/// It is upto the user how this store scales and manages the expected work load.
/// Typically this manages all the state shards data, which can amount to a lot of
/// keys and timestamps at a larger scale.
pub trait Metastore: Send + Sync + 'static {
    type Error: Display + Debug + Send + Sync;

    /// Fetch all primary state keys
    async fn get_keys(&self, shard_id: usize) -> Result<StateChanges, Self::Error>;

    /// Insert or update the given set of keys with their applicable timestamp into the primary state.
    async fn update_keys(
        &self,
        shard_id: usize,
        states: Vec<(Key, HLCTimestamp)>,
    ) -> Result<(), Self::Error>;

    /// Remove the following keys from the primary state.
    ///
    /// This should not remove the tombstone state.
    async fn remove_keys(
        &self,
        shard_id: usize,
        states: Vec<Key>,
    ) -> Result<(), Self::Error>;

    /// Get all tombstone state keys.
    async fn get_tombstone_keys(
        &self,
        shard_id: usize,
    ) -> Result<StateChanges, Self::Error>;

    /// Mark or update the existing keys as tombstones.
    async fn update_tombstone_keys(
        &self,
        shard_id: usize,
        states: Vec<(Key, HLCTimestamp)>,
    ) -> Result<(), Self::Error>;

    /// Remove the following keys from the tombstone states.
    async fn remove_tombstone_keys(
        &self,
        shard_id: usize,
        states: Vec<Key>,
    ) -> Result<(), Self::Error>;
}

#[tonic::async_trait]
/// The primary data store manager.
///
/// This manages the bulk of the data after it's been processed by Datacake.
pub trait Datastore: Metastore {
    /// Get a set of documents from the datastore.
    async fn get_documents(
        &self,
        doc_ids: &[Key],
    ) -> Result<Vec<Document>, Self::Error>;

    /// Get a single document from the datastore.
    async fn get_document(
        &self,
        doc_id: Key,
    ) -> Result<Option<Document>, Self::Error> {
        Ok(self.get_documents(&[doc_id]).await?.pop())
    }

    /// Insert or update a set of documents in the datastore.
    async fn upsert_documents(
        &self,
        documents: Vec<Document>,
    ) -> Result<(), Self::Error>;

    /// Insert or update a single document in the datastore.
    async fn upsert_document(&self, document: Document) -> Result<(), Self::Error> {
        self.upsert_documents(vec![document]).await
    }

    /// Delete a set of documents from the datastore.
    async fn delete_documents(&self, doc_ids: &[Key]) -> Result<(), Self::Error>;

    /// Insert or update a single document in the datastore.
    async fn delete_document(&self, doc_id: Key) -> Result<(), Self::Error> {
        self.delete_documents(&[doc_id]).await
    }
}
