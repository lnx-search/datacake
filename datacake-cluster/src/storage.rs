use std::fmt::{Debug, Display};

use async_trait::async_trait;
use datacake_crdt::{HLCTimestamp, Key};

use crate::core::Document;

#[async_trait]
pub trait Storage {
    type Error: Display + Debug;
    type DocsIter: Iterator<Item = Document>;

    /// Persists the state of a given key-timestamp pair.
    ///
    /// If the given `keyspace` does not exist, it should be created. A new keyspace name should
    /// not result in an error being returned by the storage trait.
    ///
    /// `is_tombstone` is passed when the key is being marked as a tombstone which can later
    /// be removed by a purge operation managed by datacake.
    async fn set_metadata(
        &self,
        keyspace: &str,
        key: Key,
        ts: HLCTimestamp,
        is_tombstone: bool,
    ) -> Result<(), Self::Error>;

    /// Persists several sets of key-timestamp pairs.
    ///
    /// If the given `keyspace` does not exist, it should be created. A new keyspace name should
    /// not result in an error being returned by the storage trait.
    ///
    /// `is_tombstone` is passed when the key is being marked as a tombstone which can later
    /// be removed by a purge operation managed by datacake.
    async fn set_many_metadata(
        &self,
        keyspace: &str,
        pairs: impl Iterator<Item = (Key, HLCTimestamp)>,
        is_tombstone: bool,
    ) -> Result<(), Self::Error>;

    /// Remove a set of keys from the metadata store.
    ///
    /// If the given `keyspace` does not exist, it should be created. A new keyspace name should
    /// not result in an error being returned by the storage trait.
    async fn remove_many_metadata(
        &self,
        keyspace: &str,
        keys: impl Iterator<Item = Key>,
    ) -> Result<(), Self::Error>;

    /// Inserts or updates a document in the persistent store.
    ///
    /// If the given `keyspace` does not exist, it should be created. A new keyspace name should
    /// not result in an error being returned by the storage trait.
    async fn put(&self, keyspace: &str, document: Document) -> Result<(), Self::Error>;

    /// Inserts or updates a set of documents in the persistent store.
    ///
    /// If the given `keyspace` does not exist, it should be created. A new keyspace name should
    /// not result in an error being returned by the storage trait.
    async fn multi_put(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = Document>,
    ) -> Result<(), Self::Error>;

    /// Removes a document with the given ID from the persistent store.
    ///
    /// If the document does not exist this should be a no-op.
    ///
    /// If the given `keyspace` does not exist, it should be created. A new keyspace name should
    /// not result in an error being returned by the storage trait.
    async fn del(&self, keyspace: &str, doc_id: Key) -> Result<(), Self::Error>;

    /// Removes a set of documents from the persistent store.
    ///
    /// If the document does not exist this should be a no-op.
    ///
    /// If the given `keyspace` does not exist, it should be created. A new keyspace name should
    /// not result in an error being returned by the storage trait.
    async fn multi_del(
        &self,
        keyspace: &str,
        doc_ids: impl Iterator<Item = Key>,
    ) -> Result<(), Self::Error>;

    /// Retrieves a single document belonging to a given keyspace from the store.
    async fn get(
        &self,
        keyspace: &str,
        doc_id: Key,
    ) -> Result<Option<Document>, Self::Error>;

    /// Retrieves a set of documents belonging to a given keyspace from the store.
    ///
    /// No error should be returned if a document id cannot be found, instead it should
    /// just be ignored.
    async fn multi_get(
        &self,
        keyspace: &str,
        doc_ids: impl Iterator<Item = Key>,
    ) -> Result<Self::DocsIter, Self::Error>;
}
