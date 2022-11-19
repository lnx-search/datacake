use std::fmt::{Debug, Display};

use async_trait::async_trait;
use datacake_crdt::{HLCTimestamp, Key};

use crate::core::Document;

#[async_trait]
pub trait Storage {
    type Error: Display + Debug;
    type DocsIter: Iterator<Item = Document>;
    type MetadataIter: Iterator<Item = (Key, HLCTimestamp, bool)>;

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
        pairs: impl Iterator<Item = (Key, HLCTimestamp)> + Send,
        is_tombstone: bool,
    ) -> Result<(), Self::Error>;

    /// Retrieves an iterator producing all values contained within the metadata store.
    async fn iter_metadata(
        &self,
        keyspace: &str,
    ) -> Result<Self::MetadataIter, Self::Error>;

    /// Remove a set of keys from the metadata store.
    ///
    /// If the given `keyspace` does not exist, it should be created. A new keyspace name should
    /// not result in an error being returned by the storage trait.
    async fn remove_many_metadata(
        &self,
        keyspace: &str,
        keys: impl Iterator<Item = Key> + Send,
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
        documents: impl Iterator<Item = Document> + Send,
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
        doc_ids: impl Iterator<Item = Key> + Send,
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
        doc_ids: impl Iterator<Item = Key> + Send,
    ) -> Result<Self::DocsIter, Self::Error>;
}

#[cfg(any(test, feature = "test-utils"))]
pub mod test_suite {
    use std::any::type_name;
    use std::collections::HashSet;
    use std::hash::Hash;

    use async_trait::async_trait;
    use datacake_crdt::{get_unix_timestamp_ms, HLCTimestamp, Key};

    use crate::core::Document;
    use crate::storage::Storage;

    pub struct InstrumentedStorage<S: Storage>(pub S);

    #[async_trait]
    impl<S: Storage + Send + Sync + 'static> Storage for InstrumentedStorage<S> {
        type Error = S::Error;
        type DocsIter = S::DocsIter;
        type MetadataIter = S::MetadataIter;

        async fn set_metadata(
            &self,
            keyspace: &str,
            key: Key,
            ts: HLCTimestamp,
            is_tombstone: bool,
        ) -> Result<(), Self::Error> {
            info!(keyspace = keyspace, key = key, ts = %ts, is_tombstone = is_tombstone, "set_metadata");
            self.0.set_metadata(keyspace, key, ts, is_tombstone).await
        }

        async fn set_many_metadata(
            &self,
            keyspace: &str,
            pairs: impl Iterator<Item = (Key, HLCTimestamp)> + Send,
            is_tombstone: bool,
        ) -> Result<(), Self::Error> {
            let pairs = pairs.collect::<Vec<_>>();
            info!(keyspace = keyspace, pairs = ?pairs, is_tombstone = is_tombstone, "set_many_metadata");
            self.0
                .set_many_metadata(keyspace, pairs.into_iter(), is_tombstone)
                .await
        }

        async fn iter_metadata(&self, keyspace: &str) -> Result<Self::MetadataIter, Self::Error> {
            info!(keyspace = keyspace, "iter_metadata");
            self.0
                .iter_metadata(keyspace)
                .await
        }

        async fn remove_many_metadata(
            &self,
            keyspace: &str,
            keys: impl Iterator<Item = Key> + Send,
        ) -> Result<(), Self::Error> {
            let keys = keys.collect::<Vec<_>>();
            info!(keyspace = keyspace, keys = ?keys, "remove_many_metadata");
            self.0.remove_many_metadata(keyspace, keys.into_iter()).await
        }

        async fn put(
            &self,
            keyspace: &str,
            document: Document,
        ) -> Result<(), Self::Error> {
            info!(keyspace = keyspace, document = ?document, "put");
            self.0.put(keyspace, document).await
        }

        async fn multi_put(
            &self,
            keyspace: &str,
            documents: impl Iterator<Item = Document> + Send,
        ) -> Result<(), Self::Error> {
            let documents = documents.collect::<Vec<_>>();
            info!(keyspace = keyspace, documents = ?documents, "multi_put");
            self.0.multi_put(keyspace, documents.into_iter()).await
        }

        async fn del(&self, keyspace: &str, doc_id: Key) -> Result<(), Self::Error> {
            info!(keyspace = keyspace, doc_id = doc_id, "del");
            self.0.del(keyspace, doc_id).await
        }

        async fn multi_del(
            &self,
            keyspace: &str,
            doc_ids: impl Iterator<Item = Key> + Send,
        ) -> Result<(), Self::Error> {
            let doc_ids = doc_ids.collect::<Vec<_>>();
            info!(keyspace = keyspace, doc_ids = ?doc_ids, "multi_del");
            self.0.multi_del(keyspace, doc_ids.into_iter()).await
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

    #[async_trait]
    pub trait ForTest: Send + Sync + 'static {
        async fn new_for_test() -> Self;
    }

    async fn run_test_suite<S: Storage + ForTest>() {
        let mut clock = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);
        let storage = S::new_for_test().await;
        info!("Starting test suite for storage: {}", type_name::<S>());

        let storage = InstrumentedStorage(storage);

        test_keyspace_semantics(&storage, &mut clock).await;
        info!("test_keyspace_semantics OK");

        test_basic_metadata_test(&storage, &mut clock).await;
        info!("test_basic_metadata_test OK");

        test_basic_persistence_test(&storage, &mut clock).await;
        info!("test_basic_persistence_test OK");

        test_tombstone_semantics(&storage, &mut clock).await;
        info!("test_tombstone_semantics OK")
    }

    #[instrument(name = "test_keyspace_semantics", skip(storage))]
    async fn test_keyspace_semantics<S: Storage>(storage: &S, clock: &mut HLCTimestamp) {
        info!("Starting test");

        let res = storage
            .iter_metadata("first-keyspace")
            .await;
        if let Err(e) = res {
            panic!("Iterating through keyspace metadata should return OK. Got {:?}", e);
        }

        let metadata = storage
            .iter_metadata("first-keyspace")
            .await
            .expect("Produce metadata iterator.")
            .collect::<HashSet<(Key, HLCTimestamp, bool)>>();
        assert_eq!(metadata, to_hashset([]), "New keyspace should be empty.");

        let res = storage
            .set_metadata("first-keyspace", 1, clock.send().unwrap(), false)
            .await;
        assert!(
            res.is_ok(),
            "Setting metadata on a new keyspace should not error. Got {:?}",
            res
        );

        let res = storage
            .set_metadata("first-keyspace", 2, clock.send().unwrap(), false)
            .await;
        assert!(
            res.is_ok(),
            "Setting metadata on a existing keyspace should not error. Got {:?}",
            res
        );

        let metadata = storage
            .iter_metadata("first-keyspace")
            .await
            .expect("Produce metadata iterator.")
            .collect::<HashSet<(Key, HLCTimestamp, bool)>>();
        assert_eq!(metadata.len(), 2, "First keyspace should contain 2 entries.");

        let metadata = storage
            .iter_metadata("second-keyspace")
            .await
            .expect("Produce metadata iterator.")
            .collect::<HashSet<(Key, HLCTimestamp, bool)>>();
        assert_eq!(metadata, to_hashset([]), "Second keyspace should be empty.");
    }

    #[instrument(name = "test_basic_metadata_test", skip(storage))]
    async fn test_basic_metadata_test<S: Storage>(storage: &S, clock: &mut HLCTimestamp) {
        info!("Starting test");

        let key_1_ts = clock.send().unwrap();
        storage
            .set_metadata("metadata-test-keyspace", 1, key_1_ts, false)
            .await
            .expect("Set metadata entry 1.");
        let key_2_ts = clock.send().unwrap();
        storage
            .set_metadata("metadata-test-keyspace", 2, key_2_ts, false)
            .await
            .expect("Set metadata entry 2.");
        let key_3_ts = clock.send().unwrap();
        storage
            .set_metadata("metadata-test-keyspace", 3, key_3_ts, true)
            .await
            .expect("Set metadata entry 3.");

        let metadata = storage
            .iter_metadata("metadata-test_keyspace")
            .await
            .expect("Produce metadata iterator.")
            .collect::<HashSet<(Key, HLCTimestamp, bool)>>();
        assert_eq!(
            metadata,
            to_hashset([
                (1, key_1_ts, false),
                (2, key_2_ts, false),
                (3, key_3_ts, true),
            ]),
            "Persisted metadata entries should match expected values."
        );

        storage
            .remove_many_metadata("metadata-test-keyspace", [1, 2].into_iter())
            .await
            .expect("Remove metadata entries.");
        let metadata = storage
            .iter_metadata("metadata-test_keyspace")
            .await
            .expect("Produce metadata iterator.")
            .collect::<HashSet<(Key, HLCTimestamp, bool)>>();
        assert_eq!(
            metadata,
            to_hashset([
                (3, key_3_ts, true),
            ]),
            "Persisted metadata entries should match expected values after removal."
        );

        let key_3_ts = clock.send().unwrap();
        storage
            .set_metadata("metadata-test-keyspace", 3, key_3_ts, false)
            .await
            .expect("Set metadata entry 3.");
        let metadata = storage
            .iter_metadata("metadata-test_keyspace")
            .await
            .expect("Produce metadata iterator.")
            .collect::<HashSet<(Key, HLCTimestamp, bool)>>();
        assert_eq!(
            metadata,
            to_hashset([
                (3, key_3_ts, false),
            ]),
            "Persisted metadata entries should match expected values after update."
        );

        let res = storage
            .remove_many_metadata("metadata-test-keyspace", [1, 2, 3].into_iter())
            .await;
        assert!(res.is_ok(), "Expected successful removal of given metadata keys. Got: {:?}", res);
    }

    #[instrument(name = "test_basic_persistence_test", skip(s))]
    async fn test_basic_persistence_test<S: Storage>(s: &S, clock: &mut HLCTimestamp) {
        info!("Starting test");
    }

    #[instrument(name = "test_tombstone_semantics", skip(s))]
    async fn test_tombstone_semantics<S: Storage>(s: &S, clock: &mut HLCTimestamp) {
        info!("Starting test");
    }

    fn to_hashset<T: Hash + Eq>(iter: impl IntoIterator<Item = T>) -> HashSet<T> {
        iter.into_iter().collect()
    }
}
