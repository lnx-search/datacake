mod from_row_impl;
mod db;


use std::path::Path;
use async_trait::async_trait;
use datacake_cluster::{BulkMutationError, Document, PutContext, Storage};
use datacake_crdt::{HLCTimestamp, Key};
pub use db::FromRow;

#[derive(Debug, thiserror::Error)]
pub enum SqliteStorageError {
    SqliteError(#[from] rusqlite::Error)
}


/// A [datacake_cluster::Storage] implementation based on an SQLite database.
pub struct SqliteStorage {
    inner: db::StorageHandle,
}

impl SqliteStorage {
    /// Opens a new SQLite database in the given path.
    ///
    /// If the database does not already exist it will be created.
    ///
    /// ```rust
    /// use datacake_sqlite::SqliteStorage;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let storage = SqliteStorage::open("./data.db").await.expect("Create database");
    /// # drop(storage);
    /// # }
    /// ```
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self, SqliteStorageError> {
        let inner = db::StorageHandle::open(path.as_ref()).await?;
        Ok(Self { inner })
    }

    /// Opens a new SQLite database in memory.
    ///
    /// ```rust
    /// use datacake_sqlite::SqliteStorage;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let storage = SqliteStorage::open_in_memory().await.expect("Create database");
    /// # drop(storage);
    /// # }
    /// ```
    pub async fn open_in_memory() -> Result<Self, SqliteStorageError> {
        let inner = db::StorageHandle::open_in_memory().await?;
        Ok(Self { inner })
    }
}

#[async_trait]
impl Storage for SqliteStorage {
    type Error = SqliteStorageError;
    type DocsIter = ();
    type MetadataIter = ();

    async fn get_keyspace_list(&self) -> Result<Vec<String>, Self::Error> {
        todo!()
    }

    async fn iter_metadata(&self, keyspace: &str) -> Result<Self::MetadataIter, Self::Error> {
        todo!()
    }

    async fn remove_tombstones(&self, keyspace: &str, keys: impl Iterator<Item=Key> + Send) -> Result<(), BulkMutationError<Self::Error>> {
        todo!()
    }

    async fn put_with_ctx(&self, keyspace: &str, document: Document, _ctx: Option<&PutContext>) -> Result<(), Self::Error> {
        todo!()
    }

    async fn put(&self, keyspace: &str, document: Document) -> Result<(), Self::Error> {
        todo!()
    }

    async fn multi_put_with_ctx(&self, keyspace: &str, documents: impl Iterator<Item=Document> + Send, _ctx: Option<&PutContext>) -> Result<(), BulkMutationError<Self::Error>> {
        todo!()
    }

    async fn multi_put(&self, keyspace: &str, documents: impl Iterator<Item=Document> + Send) -> Result<(), BulkMutationError<Self::Error>> {
        todo!()
    }

    async fn mark_as_tombstone(&self, keyspace: &str, doc_id: Key, timestamp: HLCTimestamp) -> Result<(), Self::Error> {
        todo!()
    }

    async fn mark_many_as_tombstone(&self, keyspace: &str, documents: impl Iterator<Item=(Key, HLCTimestamp)> + Send) -> Result<(), BulkMutationError<Self::Error>> {
        todo!()
    }

    async fn get(&self, keyspace: &str, doc_id: Key) -> Result<Option<Document>, Self::Error> {
        todo!()
    }

    async fn multi_get(&self, keyspace: &str, doc_ids: impl Iterator<Item=Key> + Send) -> Result<Self::DocsIter, Self::Error> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use datacake_cluster::test_suite;
    use crate::SqliteStorage;

    #[tokio::test]
    async fn test_storage() {
        let storage = SqliteStorage::open_in_memory().await.unwrap();
        test_suite::run_test_suite(storage).await;
    }
}