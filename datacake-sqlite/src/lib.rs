mod db;
mod from_row_impl;

use std::path::Path;

use async_trait::async_trait;
use datacake_cluster::{BulkMutationError, Document, Storage};
use datacake_crdt::{HLCTimestamp, Key};

pub use db::FromRow;
pub use crate::db::StorageHandle;

/// A [datacake_cluster::Storage] implementation based on an SQLite database.
pub struct SqliteStorage {
    inner: StorageHandle,
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
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self, rusqlite::Error> {
        let inner = StorageHandle::open(path.as_ref()).await?;
        setup_db(inner.clone()).await?;
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
    pub async fn open_in_memory() -> Result<Self, rusqlite::Error> {
        let inner = StorageHandle::open_in_memory().await?;
        setup_db(inner.clone()).await?;
        Ok(Self { inner })
    }

    /// Creates a new [SqliteStorage] instances from an existing storage handle.
    pub fn from_handle(handle: StorageHandle) -> Self {
        Self { inner: handle }
    }

    /// Creates a copy of the storage handle to be used in other sections of code
    /// which do not need to be distributed.
    ///
    /// WARNING:
    /// Any changes made to this will not be reflected in the cluster, it is primarily
    /// only provided for ease of reading.
    ///
    /// The table `state_entries` is already created and reserved.
    pub fn handle(&self) -> StorageHandle {
        self.inner.clone()
    }
}

#[async_trait]
impl Storage for SqliteStorage {
    type Error = rusqlite::Error;
    type DocsIter = Box<dyn Iterator<Item = Document>>;
    type MetadataIter = Box<dyn Iterator<Item = (Key, HLCTimestamp, bool)>>;

    async fn get_keyspace_list(&self) -> Result<Vec<String>, Self::Error> {
        let list = self
            .inner
            .fetch_all::<_, (String,)>(queries::SELECT_KEYSPACE_LIST, ())
            .await?
            .into_iter()
            .map(|row| row.0)
            .collect();
        Ok(list)
    }

    async fn iter_metadata(
        &self,
        keyspace: &str,
    ) -> Result<Self::MetadataIter, Self::Error> {
        let list = self
            .inner
            .fetch_all::<_, models::Metadata>(
                queries::SELECT_METADATA_LIST,
                (keyspace.to_string(),),
            )
            .await?
            .into_iter()
            .map(|metadata| (metadata.0, metadata.1, metadata.2));
        Ok(Box::new(list))
    }

    async fn remove_tombstones(
        &self,
        keyspace: &str,
        keys: impl Iterator<Item = Key> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        let params = keys
            .map(|doc_id| (keyspace.to_string(), doc_id as i64))
            .collect::<Vec<_>>();
        self.inner
            .execute_many(queries::DELETE_TOMBSTONE, params)
            .await // Safe as we're in a transaction.
            .map_err(BulkMutationError::empty_with_error)?;
        Ok(())
    }

    async fn put(&self, keyspace: &str, doc: Document) -> Result<(), Self::Error> {
        self.inner
            .execute(
                queries::INSERT,
                (
                    keyspace.to_string(),
                    doc.id as i64,
                    doc.last_updated.to_string(),
                    doc.data.to_vec(),
                ),
            )
            .await?;
        Ok(())
    }

    async fn multi_put(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = Document> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        let params = documents
            .map(|doc| {
                (
                    keyspace.to_string(),
                    doc.id as i64,
                    doc.last_updated.to_string(),
                    doc.data.to_vec(),
                )
            })
            .collect::<Vec<_>>();
        self.inner
            .execute_many(queries::INSERT, params)
            .await // Safe as we're in a transaction.
            .map_err(BulkMutationError::empty_with_error)?;
        Ok(())
    }

    async fn mark_as_tombstone(
        &self,
        keyspace: &str,
        doc_id: Key,
        timestamp: HLCTimestamp,
    ) -> Result<(), Self::Error> {
        self.inner
            .execute(
                queries::SET_TOMBSTONE,
                (keyspace.to_string(), doc_id as i64, timestamp.to_string()),
            )
            .await?;
        Ok(())
    }

    async fn mark_many_as_tombstone(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = (Key, HLCTimestamp)> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        let params = documents
            .map(|(doc_id, ts)| (keyspace.to_string(), doc_id as i64, ts.to_string()))
            .collect::<Vec<_>>();
        self.inner
            .execute_many(queries::SET_TOMBSTONE, params)
            .await // Safe as we're in a transaction.
            .map_err(BulkMutationError::empty_with_error)?;
        Ok(())
    }

    async fn get(
        &self,
        keyspace: &str,
        doc_id: Key,
    ) -> Result<Option<Document>, Self::Error> {
        let entry = self
            .inner
            .fetch_one::<_, models::Doc>(
                queries::SELECT_DOC,
                (keyspace.to_string(), doc_id as i64),
            )
            .await?;

        Ok(entry.map(|d| d.0))
    }

    async fn multi_get(
        &self,
        keyspace: &str,
        doc_ids: impl Iterator<Item = Key> + Send,
    ) -> Result<Self::DocsIter, Self::Error> {
        let doc_ids = doc_ids.map(|id| (keyspace.to_string(), id)).collect::<Vec<_>>();
        let docs = self.inner
            .fetch_many::<_, models::Doc>(queries::SELECT_DOC, doc_ids)
            .await?
            .into_iter()
            .map(|d| d.0);

        Ok(Box::new(docs))
    }
}

mod queries {
    pub static INSERT: &str = r#"
        INSERT INTO state_entries (keyspace, doc_id, ts, data) VALUES (?, ?, ?, ?)
            ON CONFLICT (keyspace, doc_id) DO UPDATE SET ts = excluded.ts, data = excluded.data;
        "#;
    pub static SELECT_DOC: &str = r#"
        SELECT doc_id, ts, data FROM state_entries WHERE keyspace = ? AND doc_id = ? AND data IS NOT NULL;
        "#;
    pub static SELECT_KEYSPACE_LIST: &str = r#"
        SELECT DISTINCT keyspace FROM state_entries GROUP BY keyspace;
        "#;
    pub static SELECT_METADATA_LIST: &str = r#"
        SELECT doc_id, ts, (data IS NULL) as tombstone FROM state_entries WHERE keyspace = ?;
        "#;
    pub static SET_TOMBSTONE: &str = r#"
        INSERT INTO state_entries (keyspace, doc_id, ts, data) VALUES (?, ?, ?, NULL)
            ON CONFLICT (keyspace, doc_id) DO UPDATE SET ts = excluded.ts, data = NULL;
        "#;
    pub static DELETE_TOMBSTONE: &str = r#"
        DELETE FROM state_entries WHERE keyspace = ? AND doc_id = ?;
        "#;
}

mod models {
    use std::str::FromStr;

    use datacake_cluster::Document;
    use datacake_crdt::{HLCTimestamp, Key};
    use rusqlite::Row;

    use crate::FromRow;

    pub struct Doc(pub Document);
    impl FromRow for Doc {
        fn from_row(row: &Row) -> rusqlite::Result<Self> {
            let id = row.get::<_, i64>(0)? as Key;
            let ts = row.get::<_, String>(1)?;
            let data = row.get::<_, Vec<u8>>(2)?;

            let ts = HLCTimestamp::from_str(&ts)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

            Ok(Self(Document::new(id, ts, data)))
        }
    }

    pub struct Metadata(pub Key, pub HLCTimestamp, pub bool);
    impl FromRow for Metadata {
        fn from_row(row: &Row) -> rusqlite::Result<Self> {
            let id = row.get::<_, i64>(0)? as Key;
            let ts = row.get::<_, String>(1)?;
            let is_tombstone = row.get::<_, bool>(2)?;

            let ts = HLCTimestamp::from_str(&ts)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

            Ok(Self(id, ts, is_tombstone))
        }
    }
}

async fn setup_db(handle: StorageHandle) -> rusqlite::Result<()> {
    let table = r#"
        CREATE TABLE IF NOT EXISTS state_entries (
            keyspace TEXT,
            doc_id BIGINT,
            ts TEXT,
            data BLOB,
            PRIMARY KEY (keyspace, doc_id)
        );
    "#;
    handle.execute(table, ()).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use datacake_cluster::test_suite;

    use crate::SqliteStorage;

    #[tokio::test]
    async fn test_storage_logic() {
        let storage = SqliteStorage::open_in_memory().await.unwrap();
        test_suite::run_test_suite(storage).await;
    }
}
