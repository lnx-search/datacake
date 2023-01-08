use std::collections::HashMap;
use std::error::Error;
use std::mem;
use std::ops::AddAssign;

use datacake_crdt::{HLCTimestamp, Key};
use parking_lot::{Mutex, RwLock};

use crate::core::DocumentMetadata;
use crate::storage::BulkMutationError;
use crate::{Document, PutContext, Storage, SyncStorage};

/// A wrapping type around another `Storage` implementation that
/// logs all the activity going into and out of the store.
///
/// This is a very useful system for debugging issues with your store.
pub struct InstrumentedStorage<S: Storage>(pub S);

impl<S: Storage + Clone> Clone for InstrumentedStorage<S> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[async_trait::async_trait]
impl<S: SyncStorage> Storage for InstrumentedStorage<S> {
    type Error = S::Error;
    type DocsIter = S::DocsIter;
    type MetadataIter = S::MetadataIter;

    async fn get_keyspace_list(&self) -> Result<Vec<String>, Self::Error> {
        info!("get_keyspace_list");
        self.0.get_keyspace_list().await
    }

    async fn iter_metadata(
        &self,
        keyspace: &str,
    ) -> Result<Self::MetadataIter, Self::Error> {
        info!(keyspace = keyspace, "iter_metadata");
        self.0.iter_metadata(keyspace).await
    }

    async fn remove_tombstones(
        &self,
        keyspace: &str,
        keys: impl Iterator<Item = Key> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        let keys = keys.collect::<Vec<_>>();
        info!(keyspace = keyspace, keys = ?keys, "remove_many_metadata");
        self.0.remove_tombstones(keyspace, keys.into_iter()).await
    }

    async fn put_with_ctx(
        &self,
        keyspace: &str,
        document: Document,
        ctx: Option<&PutContext>,
    ) -> Result<(), Self::Error> {
        info!(keyspace = keyspace, document = ?document, "put_with_ctx");
        self.0.put_with_ctx(keyspace, document, ctx).await
    }

    async fn put(&self, keyspace: &str, document: Document) -> Result<(), Self::Error> {
        info!(keyspace = keyspace, document = ?document, "put");
        self.0.put(keyspace, document).await
    }

    async fn multi_put_with_ctx(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = Document> + Send,
        ctx: Option<&PutContext>,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        let documents = documents.collect::<Vec<_>>();
        info!(keyspace = keyspace, documents = ?documents, "put_with_ctx");
        self.0
            .multi_put_with_ctx(keyspace, documents.into_iter(), ctx)
            .await
    }

    async fn multi_put(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = Document> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        let documents = documents.collect::<Vec<_>>();
        info!(keyspace = keyspace, documents = ?documents, "multi_put");
        self.0.multi_put(keyspace, documents.into_iter()).await
    }

    async fn mark_as_tombstone(
        &self,
        keyspace: &str,
        doc_id: Key,
        timestamp: HLCTimestamp,
    ) -> Result<(), Self::Error> {
        info!(keyspace = keyspace, doc_id = doc_id, timestamp = %timestamp, "mark_as_tombstone");
        self.0.mark_as_tombstone(keyspace, doc_id, timestamp).await
    }

    async fn mark_many_as_tombstone(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = DocumentMetadata> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        let documents = documents.collect::<Vec<_>>();
        info!(keyspace = keyspace, documents = ?documents, "mark_many_as_tombstone");
        self.0
            .mark_many_as_tombstone(keyspace, documents.into_iter())
            .await
    }

    async fn get(
        &self,
        keyspace: &str,
        doc_id: Key,
    ) -> Result<Option<Document>, Self::Error> {
        info!(keyspace = keyspace, doc_id = doc_id, "get");
        self.0.get(keyspace, doc_id).await
    }

    async fn multi_get(
        &self,
        keyspace: &str,
        doc_ids: impl Iterator<Item = Key> + Send,
    ) -> Result<Self::DocsIter, Self::Error> {
        let doc_ids = doc_ids.collect::<Vec<_>>();
        info!(keyspace = keyspace, doc_ids = ?doc_ids, "multi_get");
        self.0.multi_get(keyspace, doc_ids.into_iter()).await
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct MockError(pub String);
impl MockError {
    pub fn from(e: impl Error) -> Self {
        Self(e.to_string())
    }
}

#[derive(Default)]
struct MockCounters {
    expected_method_counts: HashMap<&'static str, usize>,
    actual_counts: Mutex<HashMap<&'static str, usize>>,
}

impl MockCounters {
    fn register_expected_counts(&mut self, name: &'static str, count: usize) {
        self.expected_method_counts.insert(name, count);
    }

    fn inc(&self, name: &'static str) {
        let mut lock = self.actual_counts.lock();
        let count = lock.entry(name).or_default();
        count.add_assign(&1);
    }
}

impl Drop for MockCounters {
    fn drop(&mut self) {
        if std::thread::panicking() {
            return;
        }

        let lock = self.actual_counts.lock();
        for (name, count) in mem::take(&mut self.expected_method_counts) {
            let actual_count = lock.get(name).copied().unwrap_or_default();

            assert_eq!(
                actual_count, count,
                "Expected method {:?} to be called {} times, but was called {} times.",
                name, count, actual_count,
            );
        }
    }
}

macro_rules! mock_storage_methods {
    ($name:ident { $($field_name:tt: params = ($($param:ty,)*) returns = $returntp:ty => $method_name:ident)* }) => {
        #[derive(Default)]
        /// A mock storage handler for testing handlers.
        pub struct $name {
            mock_counters: MockCounters,
            #[allow(clippy::type_complexity)]
            $($field_name: Option<(Box<dyn Fn($($param,)*) -> $returntp + Send + Sync>, &'static str)>,)*
        }

        impl $name {
            $(
                #[allow(clippy::type_complexity)]
                pub fn $method_name(
                    mut self,
                    count: usize,
                    cb: impl Fn($($param,)*) -> $returntp + Send + Sync + 'static
                ) -> Self{
                    self.$field_name = Some((Box::new(cb), stringify!($field_name)));
                    self.mock_counters.register_expected_counts(stringify!($field_name), count);
                    self
                }
            )*
        }
    };
}

mock_storage_methods!(MockStorage {
    get_keyspace_list:
        params = ()
        returns = Result<Vec<String>, MockError>
        => expect_get_keyspace_list
    iter_metadata:
        params = (&str,)
        returns = Result<std::vec::IntoIter<(Key, HLCTimestamp, bool)>, MockError>
        => expect_iter_metadata
    remove_tombstones:
        params = (&str, Box<dyn Iterator<Item = Key> + Send>,)
        returns = Result<(), BulkMutationError<MockError>>
        => expect_remove_tombstones
    put_with_ctx:
        params = (&str, Document, Option<&PutContext>,)
        returns = Result<(), MockError>
        => expect_put_with_ctx
    put:
        params = (&str, Document,)
        returns = Result<(), MockError>
        => expect_put
    multi_put_with_ctx:
        params = (&str, Box<dyn Iterator<Item = Document> + Send>, Option<&PutContext>,)
        returns = Result<(), BulkMutationError<MockError>>
        => expect_multi_put_with_ctx
    multi_put:
        params = (&str, Box<dyn Iterator<Item = Document> + Send>,)
        returns = Result<(), BulkMutationError<MockError>>
        => expect_multi_put
    mark_as_tombstone:
        params = (&str, Key, HLCTimestamp,)
        returns = Result<(), MockError>
        => expect_mark_as_tombstone
    mark_many_as_tombstone:
        params = (&str, Box<dyn Iterator<Item = DocumentMetadata> + Send>,)
        returns = Result<(), BulkMutationError<MockError>>
        => expect_mark_many_as_tombstone
    get:
        params = (&str, Key,)
        returns = Result<Option<Document>, MockError>
        => expect_get
    multi_get:
        params = (&str, Box<dyn Iterator<Item = Key> + Send>,)
        returns = Result<std::vec::IntoIter<Document>, MockError>
        => expect_multi_get
});

#[async_trait::async_trait]
impl Storage for MockStorage {
    type Error = MockError;
    type DocsIter = std::vec::IntoIter<Document>;
    type MetadataIter = std::vec::IntoIter<(Key, HLCTimestamp, bool)>;

    async fn get_keyspace_list(&self) -> Result<Vec<String>, Self::Error> {
        if let Some((expected, name)) = self.get_keyspace_list.as_ref() {
            self.mock_counters.inc(name);
            return (*expected)();
        }
        panic!("Storage operation was not expected to be called.");
    }

    async fn iter_metadata(
        &self,
        keyspace: &str,
    ) -> Result<Self::MetadataIter, Self::Error> {
        if let Some((expected, name)) = self.iter_metadata.as_ref() {
            self.mock_counters.inc(name);
            return (*expected)(keyspace);
        }
        panic!("iter_metadata operation was not expected to be called.");
    }

    async fn remove_tombstones(
        &self,
        keyspace: &str,
        keys: impl Iterator<Item = Key> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        if let Some((expected, name)) = self.remove_tombstones.as_ref() {
            self.mock_counters.inc(name);
            #[allow(clippy::needless_collect)]
            let items = keys.collect::<Vec<_>>();
            return (*expected)(keyspace, Box::new(items.into_iter()));
        }
        panic!("remove_tombstones operation was not expected to be called.");
    }

    async fn put_with_ctx(
        &self,
        keyspace: &str,
        document: Document,
        ctx: Option<&PutContext>,
    ) -> Result<(), Self::Error> {
        if let Some((expected, name)) = self.put_with_ctx.as_ref() {
            self.mock_counters.inc(name);
            return (*expected)(keyspace, document, ctx);
        }
        panic!("put_with_ctx operation was not expected to be called.");
    }

    async fn put(&self, keyspace: &str, document: Document) -> Result<(), Self::Error> {
        if let Some((expected, name)) = self.put.as_ref() {
            self.mock_counters.inc(name);
            return (*expected)(keyspace, document);
        }
        panic!("put operation was not expected to be called.");
    }

    async fn multi_put_with_ctx(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = Document> + Send,
        ctx: Option<&PutContext>,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        if let Some((expected, name)) = self.multi_put_with_ctx.as_ref() {
            self.mock_counters.inc(name);
            #[allow(clippy::needless_collect)]
            let items = documents.collect::<Vec<_>>();
            return (*expected)(keyspace, Box::new(items.into_iter()), ctx);
        }
        panic!("multi_put_with_ctx operation was not expected to be called.");
    }

    async fn multi_put(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = Document> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        if let Some((expected, name)) = self.multi_put.as_ref() {
            self.mock_counters.inc(name);
            #[allow(clippy::needless_collect)]
            let items = documents.collect::<Vec<_>>();
            return (*expected)(keyspace, Box::new(items.into_iter()));
        }
        panic!("multi_put operation was not expected to be called.");
    }

    async fn mark_as_tombstone(
        &self,
        keyspace: &str,
        doc_id: Key,
        timestamp: HLCTimestamp,
    ) -> Result<(), Self::Error> {
        if let Some((expected, name)) = self.mark_as_tombstone.as_ref() {
            self.mock_counters.inc(name);
            return (*expected)(keyspace, doc_id, timestamp);
        }
        panic!("mark_as_tombstone operation was not expected to be called.");
    }

    async fn mark_many_as_tombstone(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = DocumentMetadata> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        if let Some((expected, name)) = self.mark_many_as_tombstone.as_ref() {
            self.mock_counters.inc(name);
            #[allow(clippy::needless_collect)]
            let items = documents.collect::<Vec<_>>();
            return (*expected)(keyspace, Box::new(items.into_iter()));
        }
        panic!("mark_many_as_tombstone operation was not expected to be called.");
    }

    async fn get(
        &self,
        keyspace: &str,
        doc_id: Key,
    ) -> Result<Option<Document>, Self::Error> {
        if let Some((expected, name)) = self.get.as_ref() {
            self.mock_counters.inc(name);
            return (*expected)(keyspace, doc_id);
        }
        panic!("get operation was not expected to be called.");
    }

    async fn multi_get(
        &self,
        keyspace: &str,
        doc_ids: impl Iterator<Item = Key> + Send,
    ) -> Result<Self::DocsIter, Self::Error> {
        if let Some((expected, name)) = self.multi_get.as_ref() {
            self.mock_counters.inc(name);
            #[allow(clippy::needless_collect)]
            let items = doc_ids.collect::<Vec<_>>();
            return (*expected)(keyspace, Box::new(items.into_iter()));
        }
        panic!("multi_get operation was not expected to be called.");
    }
}

#[derive(Debug, Default)]
/// A in-memory storage implementor.
///
/// This is not suitable for any sort of real world usage outside of testing.
pub struct MemStore {
    #[allow(clippy::complexity)]
    metadata: RwLock<HashMap<String, HashMap<Key, (HLCTimestamp, bool)>>>,
    data: RwLock<HashMap<String, HashMap<Key, Document>>>,
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct MemStoreError(#[from] pub anyhow::Error);

#[async_trait::async_trait]
impl Storage for MemStore {
    type Error = MemStoreError;
    type DocsIter = std::vec::IntoIter<Document>;
    type MetadataIter = std::vec::IntoIter<(Key, HLCTimestamp, bool)>;

    async fn get_keyspace_list(&self) -> Result<Vec<String>, Self::Error> {
        Ok(self.metadata.read().keys().cloned().collect())
    }

    async fn iter_metadata(
        &self,
        keyspace: &str,
    ) -> Result<Self::MetadataIter, Self::Error> {
        if let Some(ks) = self.metadata.read().get(keyspace) {
            return Ok(ks
                .iter()
                .map(|(k, (ts, tombstone))| (*k, *ts, *tombstone))
                .collect::<Vec<_>>()
                .into_iter());
        };

        Ok(Vec::new().into_iter())
    }

    async fn remove_tombstones(
        &self,
        keyspace: &str,
        keys: impl Iterator<Item = Key> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        if let Some(ks) = self.metadata.write().get_mut(keyspace) {
            for key in keys {
                ks.remove(&key);
            }
        }

        Ok(())
    }

    async fn put(&self, keyspace: &str, document: Document) -> Result<(), Self::Error> {
        self.multi_put(keyspace, [document].into_iter())
            .await
            .map_err(|e| e.inner)
    }

    async fn multi_put(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = Document> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        let documents = documents.collect::<Vec<_>>();
        self.data
            .write()
            .entry(keyspace.to_string())
            .and_modify(|entries| {
                for doc in documents.clone() {
                    entries.insert(doc.id(), doc);
                }
            })
            .or_insert_with(|| {
                HashMap::from_iter(
                    documents.clone().into_iter().map(|doc| (doc.id(), doc)),
                )
            });
        self.metadata
            .write()
            .entry(keyspace.to_string())
            .and_modify(|entries| {
                for doc in documents.clone() {
                    entries.insert(doc.id(), (doc.last_updated(), false));
                }
            })
            .or_insert_with(|| {
                HashMap::from_iter(
                    documents
                        .into_iter()
                        .map(|doc| (doc.id(), (doc.last_updated(), false))),
                )
            });

        Ok(())
    }

    async fn mark_as_tombstone(
        &self,
        keyspace: &str,
        doc_id: Key,
        timestamp: HLCTimestamp,
    ) -> Result<(), Self::Error> {
        self.mark_many_as_tombstone(
            keyspace,
            [DocumentMetadata {
                id: doc_id,
                last_updated: timestamp,
            }]
            .into_iter(),
        )
        .await
        .map_err(|e| e.inner)
    }

    async fn mark_many_as_tombstone(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = DocumentMetadata> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        let docs = documents.collect::<Vec<_>>();
        self.data
            .write()
            .entry(keyspace.to_string())
            .and_modify(|entries| {
                for doc in docs.iter() {
                    entries.remove(&doc.id);
                }
            });
        self.metadata
            .write()
            .entry(keyspace.to_string())
            .and_modify(|entries| {
                for doc in docs {
                    entries.insert(doc.id, (doc.last_updated, true));
                }
            });

        Ok(())
    }

    async fn get(
        &self,
        keyspace: &str,
        doc_id: Key,
    ) -> Result<Option<Document>, Self::Error> {
        Ok(self
            .data
            .read()
            .get(keyspace)
            .and_then(|ks| ks.get(&doc_id).cloned()))
    }

    async fn multi_get(
        &self,
        keyspace: &str,
        doc_ids: impl Iterator<Item = Key> + Send,
    ) -> Result<Self::DocsIter, Self::Error> {
        let mut docs = Vec::new();

        if let Some(ks) = self.data.read().get(keyspace) {
            for doc_id in doc_ids {
                if let Some(doc) = ks.get(&doc_id) {
                    docs.push(doc.clone());
                }
            }
        }

        Ok(docs.into_iter())
    }
}

unsafe impl Send for MemStore {}
unsafe impl Sync for MemStore {}
