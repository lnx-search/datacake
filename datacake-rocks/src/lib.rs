#[macro_use]
extern crate tracing;

mod error;
mod metastore;
mod shard;

use std::path::Path;

use datacake_cluster::{
    Datastore,
    Document,
    HLCTimestamp,
    Key,
    Metastore,
    StateChanges,
};
pub use error::RocksStoreError;
#[cfg(feature = "cache")]
use moka::sync::Cache;

use crate::metastore::MetastoreHandle;
use crate::shard::{RawKey, ShardHandle};

/// Opens a new or existing store using a default set of options.
///
/// If the number of shards are different to the number of shards that
/// was previously used for the given directory. The system will use
/// the already set number of shards.
///
/// An optional cache size can be set after creation in order to use a LRU cache
/// to cache frequently access documents. The cache size is a optimistic
/// estimate to limit memory usage. This requires the `cache` feature to be enabled.
pub async fn open_store(
    num_shard: usize,
    base_path: &Path,
) -> Result<RocksStore, RocksStoreError> {
    let mut options = rocksdb::Options::default();
    options.create_if_missing(true);
    options.set_max_total_wal_size(1 << 30);
    options.set_blob_compaction_readahead_size(4 << 20);
    options.set_optimize_filters_for_hits(true);

    open_store_with_options(num_shard, options, base_path).await
}

/// Opens a new or existing store using the provided options.
///
/// If the number of shards are different to the number of shards that
/// was previously used for the given directory. The system will use
/// the already set number of shards.
///
/// An optional cache size can be provided in order to use a LRU cache
/// to cache frequently access documents. The cache size is a optimistic
/// estimate to limit memory usage. This requires the `cache` feature to be enabled.
pub async fn open_store_with_options(
    num_shard: usize,
    options: rocksdb::Options,
    base_path: &Path,
) -> Result<RocksStore, RocksStoreError> {
    let base_path = base_path.to_path_buf();

    let (metadata, storage_shards) = tokio::task::spawn_blocking(move || {
        let meta_path = base_path.join("metadata");
        let metadata = metastore::start_metastore(options.clone(), &meta_path)?;

        let mut shards = vec![];
        for shard_id in 0..num_shard {
            let shard_path = base_path.join(format!("shard-{}", shard_id));
            let shard = shard::start_shard(shard_id, &options, &shard_path)?;

            shards.push(shard);
        }

        Ok::<_, RocksStoreError>((metadata, shards))
    })
    .await
    .expect("Spawn background thread.")?;

    Ok(RocksStore {
        metadata,
        storage_shards,

        #[cfg(feature = "cache")]
        cache: Cache::new(0),
    })
}

/// A document and metadata store which can be used as a drop in store for a Datacake cluster.
///
/// The store is built upon RocksDB, the metadata is stored in a separate RocksDB instance
/// than the primary documents.
/// The documents themselves are split into shards, where each shard is a new RocksDB instance.
/// The number of shards can be configured, but cannot be adjusted once the store is created
/// without creating a new store and copying over all entries.
pub struct RocksStore {
    metadata: MetastoreHandle,
    storage_shards: Vec<ShardHandle>,

    #[cfg(feature = "cache")]
    cache: Cache<Key, Document>,
}

impl RocksStore {
    #[cfg(feature = "cache")]
    /// Set the the store's LRU cache size in bytes.
    ///
    /// The size of each entry is a best effort estimate
    /// and do not directly track the size used in memory by allocations.
    pub fn set_cache_size(&mut self, cache_size_byte: u64) {
        let cache = Cache::builder()
            .weigher(|_k, v: &Document| {
                let total = mem::size_of::<Key>()
                    + mem::size_of::<Key>()
                    + mem::size_of::<HLCTimestamp>()
                    + v.data.len();

                total as u32
            })
            .max_capacity(cache_size_byte)
            .build();

        self.cache = cache;
    }

    fn get_shard_id(&self, key: Key) -> usize {
        (key % self.storage_shards.len() as u64) as usize
    }

    fn group_and_serialize_keys(
        &self,
        doc_ids: &[Key],
    ) -> Result<Vec<Vec<RawKey>>, RocksStoreError> {
        let mut shard_grouped_ids = Vec::with_capacity(self.storage_shards.len());
        shard_grouped_ids.resize(self.storage_shards.len(), vec![]);

        for doc_id in doc_ids {
            let shard_id = self.get_shard_id(*doc_id);

            let raw = rkyv::to_bytes::<_, 8>(doc_id)
                .expect("Serializing of key should be infallible");

            shard_grouped_ids[shard_id].push(raw.into_vec());
        }

        Ok(shard_grouped_ids)
    }
}

#[async_trait::async_trait]
impl Metastore for RocksStore {
    type Error = RocksStoreError;

    async fn get_keys(&self, shard_id: usize) -> Result<StateChanges, Self::Error> {
        self.metadata.get_keys(false, shard_id).await
    }

    async fn update_keys(
        &self,
        shard_id: usize,
        states: Vec<(Key, HLCTimestamp)>,
    ) -> Result<(), Self::Error> {
        self.metadata.update_keys(false, shard_id, states).await
    }

    async fn remove_keys(
        &self,
        shard_id: usize,
        states: Vec<Key>,
    ) -> Result<(), Self::Error> {
        self.metadata.delete_keys(false, shard_id, states).await
    }

    async fn get_tombstone_keys(
        &self,
        shard_id: usize,
    ) -> Result<StateChanges, Self::Error> {
        self.metadata.get_keys(true, shard_id).await
    }

    async fn update_tombstone_keys(
        &self,
        shard_id: usize,
        states: Vec<(Key, HLCTimestamp)>,
    ) -> Result<(), Self::Error> {
        self.metadata.update_keys(true, shard_id, states).await
    }

    async fn remove_tombstone_keys(
        &self,
        shard_id: usize,
        states: Vec<Key>,
    ) -> Result<(), Self::Error> {
        self.metadata.delete_keys(true, shard_id, states).await
    }
}

#[async_trait::async_trait]
impl Datastore for RocksStore {
    async fn get_documents(
        &self,
        doc_ids: &[Key],
    ) -> Result<Vec<Document>, Self::Error> {
        let shard_grouped_ids = self.group_and_serialize_keys(doc_ids)?;

        let mut documents = Vec::with_capacity(doc_ids.len());
        for (shard_id, mut doc_ids) in shard_grouped_ids.into_iter().enumerate() {
            if doc_ids.is_empty() {
                continue;
            }

            // Small optimisation for where there's only one doc to get.
            if doc_ids.len() == 1 {
                let maybe_doc = self.storage_shards[shard_id]
                    .get_document(doc_ids.remove(0))
                    .await?;

                if let Some(doc) = maybe_doc {
                    documents.push(doc);
                }
            } else {
                let docs = self.storage_shards[shard_id].get_documents(doc_ids).await?;

                documents.extend(docs);
            }
        }

        Ok(documents)
    }

    async fn get_document(&self, doc_id: Key) -> Result<Option<Document>, Self::Error> {
        let shard_id = self.get_shard_id(doc_id);

        let raw = rkyv::to_bytes::<_, 8>(&doc_id)
            .expect("Serializing of key should be infallible");

        self.storage_shards[shard_id]
            .get_document(raw.into_vec())
            .await
    }

    async fn upsert_documents(
        &self,
        documents: Vec<Document>,
    ) -> Result<(), Self::Error> {
        let mut shard_grouped_ids =
            Vec::<Vec<(RawKey, Vec<u8>)>>::with_capacity(self.storage_shards.len());
        shard_grouped_ids.resize(self.storage_shards.len(), vec![]);

        for doc in documents {
            let shard_id = self.get_shard_id(doc.id);

            let raw_id = rkyv::to_bytes::<_, 8>(&doc.id)
                .map_err(|_| RocksStoreError::SerializationError)?;

            let raw_doc = rkyv::to_bytes::<_, 1024>(&doc)
                .map_err(|_| RocksStoreError::SerializationError)?;

            shard_grouped_ids[shard_id].push((raw_id.into_vec(), raw_doc.into_vec()));
        }

        for (shard_id, mut doc_ids) in shard_grouped_ids.into_iter().enumerate() {
            if doc_ids.is_empty() {
                continue;
            }

            // Small optimisation for where there's only one doc to get.
            if doc_ids.len() == 1 {
                let (key, doc) = doc_ids.remove(0);
                self.storage_shards[shard_id]
                    .upsert_document(key, doc)
                    .await?;
            } else {
                self.storage_shards[shard_id]
                    .upsert_documents(doc_ids)
                    .await?;
            }
        }

        Ok(())
    }

    async fn upsert_document(&self, doc: Document) -> Result<(), Self::Error> {
        let shard_id = self.get_shard_id(doc.id);

        let raw_id = rkyv::to_bytes::<_, 8>(&doc.id)
            .map_err(|_| RocksStoreError::SerializationError)?;

        let raw_doc = rkyv::to_bytes::<_, 1024>(&doc)
            .map_err(|_| RocksStoreError::SerializationError)?;

        self.storage_shards[shard_id]
            .upsert_document(raw_id.into_vec(), raw_doc.into_vec())
            .await
    }

    async fn delete_documents(&self, doc_ids: &[Key]) -> Result<(), Self::Error> {
        let shard_grouped_ids = self.group_and_serialize_keys(doc_ids)?;

        for (shard_id, mut doc_ids) in shard_grouped_ids.into_iter().enumerate() {
            if doc_ids.is_empty() {
                continue;
            }

            // Small optimisation for where there's only one doc to get.
            if doc_ids.len() == 1 {
                self.storage_shards[shard_id]
                    .delete_document(doc_ids.remove(0))
                    .await?;
            } else {
                self.storage_shards[shard_id]
                    .delete_documents(doc_ids)
                    .await?;
            }
        }

        Ok(())
    }

    async fn delete_document(&self, doc_id: Key) -> Result<(), Self::Error> {
        let shard_id = self.get_shard_id(doc_id);

        let raw_id = rkyv::to_bytes::<_, 8>(&doc_id)
            .map_err(|_| RocksStoreError::SerializationError)?;

        self.storage_shards[shard_id]
            .delete_document(raw_id.into_vec())
            .await
    }
}
