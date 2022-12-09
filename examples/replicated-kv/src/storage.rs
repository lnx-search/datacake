use std::cmp;
use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};
use std::path::Path;

use anyhow::Result;
use axum::async_trait;
use datacake::cluster::{BulkMutationError, Document, Storage};
use datacake::crdt::{HLCTimestamp, Key};
use datacake::sqlite::SqliteStorage;

/// A wrapper around several [datacake::sqlite::SqliteStorage] which
/// evenly distributes the workload across all of the stores.
pub struct ShardedStorage {
    shards: Vec<SqliteStorage>,
}

impl ShardedStorage {
    /// Opens a new store in a given directory.
    ///
    /// The directory will be created if it doesn't already exist.
    pub async fn open_in_dir(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        std::fs::create_dir_all(path)?;

        let num_shards = cmp::min(num_cpus::get(), 16);

        let mut shards = Vec::with_capacity(num_shards);
        for shard_id in 0..num_shards {
            let db_path = path.join(format!("shard-{}.db", shard_id));
            let db = SqliteStorage::open(db_path).await?;
            shards.push(db);
        }

        Ok(Self { shards })
    }

    fn get_shard_id(&self, key: Key) -> usize {
        // This probably shouldn't be crc based but it's just for a demo.
        let mut hasher = crc32fast::Hasher::new();
        key.hash(&mut hasher);
        let shard_id = hasher.finish() % self.shards.len() as u64;

        shard_id as usize
    }
}

#[async_trait]
/// Our implementation of the storage trait is simplified here,
/// in reality you may want to do some more complicated logic or make use of the `with_ctx`
/// methods for your production system. This is done for the sake of a demo.
///
/// The idea is we simply batch up and group our documents into blocks for each shard,
/// then we execute the request for each shard we have.
///
/// Although we execute these operations sequentially it still allows other operations to
/// continue concurrently rather than sequentially like having a single store is;
/// that being said, SQLite can be incredibly fast if used correctly so don't do this
/// unless you have a really high workload (or maybe just dont use SQLite in that case)
impl Storage for ShardedStorage {
    type Error = <SqliteStorage as Storage>::Error;
    type DocsIter = std::vec::IntoIter<Document>;
    type MetadataIter = std::vec::IntoIter<(Key, HLCTimestamp, bool)>;

    async fn get_keyspace_list(&self) -> std::result::Result<Vec<String>, Self::Error> {
        let mut keyspace_set = BTreeSet::new();

        for shard in self.shards.iter() {
            let keyspace_list = shard.get_keyspace_list().await?;
            keyspace_set.extend(keyspace_list);
        }

        Ok(keyspace_set.into_iter().collect())
    }

    async fn iter_metadata(
        &self,
        keyspace: &str,
    ) -> std::result::Result<Self::MetadataIter, Self::Error> {
        let mut metadata = Vec::new();
        for shard in self.shards.iter() {
            let keyspace_list = shard.iter_metadata(keyspace).await?;
            metadata.extend(keyspace_list);
        }
        Ok(metadata.into_iter())
    }

    async fn remove_tombstones(
        &self,
        keyspace: &str,
        keys: impl Iterator<Item = Key> + Send,
    ) -> std::result::Result<(), BulkMutationError<Self::Error>> {
        let mut shard_blocks = Vec::new();
        shard_blocks.resize_with(self.shards.len(), Vec::new);

        for key in keys {
            let shard_id = self.get_shard_id(key);
            shard_blocks[shard_id].push(key);
        }

        let mut error = None;
        let mut successful_ids = Vec::new();
        for (shard_id, doc_ids) in shard_blocks.into_iter().enumerate() {
            let res = self.shards[shard_id]
                .remove_tombstones(keyspace, doc_ids.iter().cloned())
                .await;

            if let Err(e) = res {
                successful_ids.extend(e.successful_doc_ids().iter().copied());
                error = Some(e.into_inner());
            } else {
                successful_ids.extend(doc_ids);
            }
        }

        if let Some(e) = error {
            Err(BulkMutationError::new(e, successful_ids))
        } else {
            Ok(())
        }
    }

    async fn put(
        &self,
        keyspace: &str,
        document: Document,
    ) -> std::result::Result<(), Self::Error> {
        let shard_id = self.get_shard_id(document.id);
        self.shards[shard_id].put(keyspace, document).await
    }

    async fn multi_put(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = Document> + Send,
    ) -> std::result::Result<(), BulkMutationError<Self::Error>> {
        let mut total_docs = 0;
        let mut shard_blocks = Vec::new();
        let mut doc_id_blocks = Vec::new();
        shard_blocks.resize_with(self.shards.len(), Vec::new);
        doc_id_blocks.resize_with(self.shards.len(), Vec::new);

        for doc in documents {
            total_docs += 1;
            let shard_id = self.get_shard_id(doc.id);
            doc_id_blocks[shard_id].push(doc.id);
            shard_blocks[shard_id].push(doc);
        }

        let mut error = None;
        let mut successful_ids = Vec::with_capacity(total_docs);

        let shard_iter = shard_blocks.into_iter().zip(doc_id_blocks).enumerate();
        for (shard_id, (documents, doc_ids)) in shard_iter {
            let res = self.shards[shard_id]
                .multi_put(keyspace, documents.into_iter())
                .await;

            if let Err(e) = res {
                successful_ids.extend(e.successful_doc_ids().iter().copied());
                error = Some(e.into_inner());
            } else {
                successful_ids.extend(doc_ids);
            }
        }

        if let Some(e) = error {
            Err(BulkMutationError::new(e, successful_ids))
        } else {
            Ok(())
        }
    }

    async fn mark_as_tombstone(
        &self,
        keyspace: &str,
        doc_id: Key,
        timestamp: HLCTimestamp,
    ) -> std::result::Result<(), Self::Error> {
        let shard_id = self.get_shard_id(doc_id);
        self.shards[shard_id]
            .mark_as_tombstone(keyspace, doc_id, timestamp)
            .await
    }

    async fn mark_many_as_tombstone(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = (Key, HLCTimestamp)> + Send,
    ) -> std::result::Result<(), BulkMutationError<Self::Error>> {
        let mut total_docs = 0;
        let mut shard_blocks = Vec::new();
        let mut doc_id_blocks = Vec::new();
        shard_blocks.resize_with(self.shards.len(), Vec::new);
        doc_id_blocks.resize_with(self.shards.len(), Vec::new);

        for (doc_id, ts) in documents {
            total_docs += 1;
            let shard_id = self.get_shard_id(doc_id);
            doc_id_blocks[shard_id].push(doc_id);
            shard_blocks[shard_id].push((doc_id, ts));
        }

        let mut error = None;
        let mut successful_ids = Vec::with_capacity(total_docs);

        let shard_iter = shard_blocks.into_iter().zip(doc_id_blocks).enumerate();
        for (shard_id, (documents, doc_ids)) in shard_iter {
            let res = self.shards[shard_id]
                .mark_many_as_tombstone(keyspace, documents.into_iter())
                .await;

            if let Err(e) = res {
                successful_ids.extend(e.successful_doc_ids().iter().copied());
                error = Some(e.into_inner());
            } else {
                successful_ids.extend(doc_ids);
            }
        }

        if let Some(e) = error {
            Err(BulkMutationError::new(e, successful_ids))
        } else {
            Ok(())
        }
    }

    async fn get(
        &self,
        keyspace: &str,
        doc_id: Key,
    ) -> std::result::Result<Option<Document>, Self::Error> {
        let shard_id = self.get_shard_id(doc_id);
        self.shards[shard_id].get(keyspace, doc_id).await
    }

    async fn multi_get(
        &self,
        keyspace: &str,
        doc_ids: impl Iterator<Item = Key> + Send,
    ) -> std::result::Result<Self::DocsIter, Self::Error> {
        let mut shard_blocks = Vec::new();
        shard_blocks.resize_with(self.shards.len(), Vec::new);

        for doc_id in doc_ids {
            let shard_id = self.get_shard_id(doc_id);
            shard_blocks[shard_id].push(doc_id);
        }

        let mut returned_docs = Vec::new();
        for (shard_id, doc_ids) in shard_blocks.into_iter().enumerate() {
            let docs = self.shards[shard_id]
                .multi_get(keyspace, doc_ids.into_iter())
                .await?;
            returned_docs.extend(docs);
        }

        Ok(returned_docs.into_iter())
    }
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;

    use super::*;

    #[tokio::test]
    async fn test_sharded_storage() {
        let path = temp_dir().join(uuid::Uuid::new_v4().to_string());
        let store = ShardedStorage::open_in_dir(path)
            .await
            .expect("Create storage");

        datacake::cluster::test_suite::run_test_suite(store).await;
    }
}
