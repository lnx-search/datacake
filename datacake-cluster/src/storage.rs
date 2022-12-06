use std::borrow::Cow;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use datacake_crdt::{HLCTimestamp, Key};
use tonic::transport::Channel;

use crate::core::Document;

/// A utility for tracking the progress a task has made.
pub struct ProgressWatcher {
    inner: ProgressTracker,
    timeout: Duration,
    last_tick: Instant,
    last_observed_counter: u64,
}

impl ProgressWatcher {
    pub fn new(inner: ProgressTracker, timeout: Duration) -> Self {
        Self {
            inner,
            timeout,
            last_tick: Instant::now(),
            last_observed_counter: 0,
        }
    }

    /// Checks if the task has expired or made progress.
    pub fn has_expired(&mut self) -> bool {
        if self.is_done() {
            return false;
        }

        let counter = self.inner.progress_counter.load(Ordering::Relaxed);

        if counter > self.last_observed_counter {
            self.last_tick = Instant::now();
            self.last_observed_counter = counter;
            return false;
        }

        self.last_tick.elapsed() > self.timeout
    }

    /// Returns if the task is complete or not.
    pub fn is_done(&self) -> bool {
        self.inner.done.load(Ordering::Relaxed)
    }
}

#[derive(Default, Debug, Clone)]
/// A simple atomic counter to indicate to supervisors that the given
/// operation is making progress.
///
/// This can be used in order to prevent supervisors timing out tasks
/// because they have not been completed within the target time frame.
pub struct ProgressTracker {
    pub(crate) progress_counter: Arc<AtomicU64>,
    pub(crate) done: Arc<AtomicBool>,
}

impl ProgressTracker {
    /// Adds a marker to the progress tracker.
    ///
    /// This is so any supervisors don't accidentally cancel or abort a task if it's
    /// taking longer than it expected.
    pub fn register_progress(&self) {
        self.progress_counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Marks the task as complete.
    pub fn set_done(&self) {
        self.done.store(true, Ordering::Relaxed);
    }
}

#[derive(Clone)]
/// Additional information related to the operation which can be useful.
///
/// This can be very useful if you wish to extend Datacake's storage system
/// in order to support objects which don't fit in memory etc...
pub struct PutContext {
    // Info relating to the task itself.
    pub(crate) progress: ProgressTracker,

    // Info relating to the remote node.
    pub(crate) remote_node_id: Cow<'static, str>,
    pub(crate) remote_addr: SocketAddr,
    pub(crate) remote_rpc_channel: Channel,
}

impl PutContext {
    #[inline]
    /// Adds a marker to the progress tracker.
    ///
    /// This is so any supervisors don't accidentally cancel or abort a task if it's
    /// taking longer than it expected.
    pub fn register_progress(&self) {
        self.progress.register_progress()
    }

    #[inline]
    /// The unique ID of the remote node.
    pub fn remote_node_id(&self) -> &str {
        self.remote_node_id.as_ref()
    }

    #[inline]
    /// The socket address of the remote node.
    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    #[inline]
    /// The existing connection channel which can be used
    /// to communicate with services ran by the Datacake server.
    ///
    /// Additional services can be registered to the server ran by Datacake
    /// using the `ServiceRegistry` trait.
    pub fn remote_channel(&self) -> &Channel {
        &self.remote_rpc_channel
    }
}

#[derive(Debug, thiserror::Error)]
#[error("The operation was not completely successful due to error {inner}")]
/// An error which occurred while mutating the state not allowing the operation
/// to proceed any further but also having some part of the operation complete.
pub struct BulkMutationError<E>
where
    E: Error + Send + 'static,
{
    pub(crate) inner: E,
    pub(crate) successful_doc_ids: Vec<Key>,
}

impl<E> BulkMutationError<E>
where
    E: Error + Send + 'static,
{
    /// Creates a new mutation error from the provided inner error.
    ///
    /// This essentially means that what ever change that was going to happen
    /// was atomic and has therefore been revered.
    ///
    /// WARNING:
    /// *You should under no circumstances return an empty mutation error if **any**
    /// part of the state has been mutated and will not be reversed. Doing so will lead
    /// to state divergence within the cluster*
    pub fn empty_with_error(error: E) -> Self {
        Self::new(error, Vec::new())
    }

    /// Creates a new mutation error from the provided inner error.
    ///
    /// This essentially means that although we ran into an error, we were able to
    /// complete some part of the operation on some documents.
    ///
    /// WARNING:
    /// *You should under no circumstances return an empty mutation error if **any**
    /// part of the state has been mutated and will not be reversed. Doing so will lead
    /// to state divergence within the cluster*
    pub fn new(error: E, successful_doc_ids: Vec<Key>) -> Self {
        Self {
            inner: error,
            successful_doc_ids,
        }
    }

    #[inline]
    /// The cause of the error.
    pub fn cause(&self) -> &E {
        &self.inner
    }

    #[inline]
    /// Consumes the error returning the inner error.
    pub fn into_inner(self) -> E {
        self.inner
    }

    #[inline]
    /// The document ids which the operation was successful on.
    pub fn successful_doc_ids(&self) -> &[Key] {
        &self.successful_doc_ids
    }
}

// TODO: Add default methods with more complicated handlers in order to allow room for lnx stuff.
#[async_trait]
/// The generic storage trait which encapsulates all the required persistence logic.
///
/// A test suite is available for ensuring correct behavour of stores.
pub trait Storage {
    type Error: Error + Send + Sync + 'static;
    type DocsIter: Iterator<Item = Document>;
    type MetadataIter: Iterator<Item = (Key, HLCTimestamp, bool)>;

    /// Retrieves all keyspace currently persisted.
    async fn get_keyspace_list(&self) -> Result<Vec<String>, Self::Error>;

    /// Retrieves an iterator producing all values contained within the store.
    ///
    /// This should contain the document ID, when it was last updated and if it's a tombstone or not.
    async fn iter_metadata(
        &self,
        keyspace: &str,
    ) -> Result<Self::MetadataIter, Self::Error>;

    /// Remove a set of keys which are marked as tombstones store.
    ///
    /// If the given `keyspace` does not exist, it should be created. A new keyspace name should
    /// not result in an error being returned by the storage trait.
    async fn remove_tombstones(
        &self,
        keyspace: &str,
        keys: impl Iterator<Item = Key> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>>;

    /// Inserts or updates a document in the persistent store.
    ///
    /// This is the base call for any `put` operation, and is passed the additional
    /// [PutContext] parameter which can provided additional information.
    ///
    /// In the case the context is `None`, this indicates that the operation originates
    /// from the local node itself. If context is `Some(ctx)` then it has originated from
    /// a remote node.
    ///
    /// If the given `keyspace` does not exist, it should be created. A new keyspace name should
    /// not result in an error being returned by the storage trait.
    ///
    /// NOTE:
    ///     It is the implementors responsibility to ensure that this operation is atomic and durable.
    ///     Partially setting the document metadata and failing to also set the data can lead to
    ///     split sate and the system will fail to converge unless a new operation comes in to modify
    ///     the document again.
    async fn put_with_ctx(
        &self,
        keyspace: &str,
        document: Document,
        _ctx: Option<&PutContext>,
    ) -> Result<(), Self::Error> {
        self.put(keyspace, document).await
    }

    /// Inserts or updates a document in the persistent store.
    ///
    /// If the given `keyspace` does not exist, it should be created. A new keyspace name should
    /// not result in an error being returned by the storage trait.
    ///
    /// NOTE:
    ///     It is the implementors responsibility to ensure that this operation is atomic and durable.
    ///     Partially setting the document metadata and failing to also set the data can lead to
    ///     split sate and the system will fail to converge unless a new operation comes in to modify
    ///     the document again.
    async fn put(&self, keyspace: &str, document: Document) -> Result<(), Self::Error>;

    /// Inserts or updates a set of documents in the persistent store.
    ///
    /// This is the base call for any `multi_put` operation, and is passed the additional
    /// [PutContext] parameter which can provided additional information.
    ///
    /// In the case the context is `None`, this indicates that the operation originates
    /// from the local node itself. If context is `Some(ctx)` then it has originated from
    /// a remote node.
    ///
    /// If the given `keyspace` does not exist, it should be created. A new keyspace name should
    /// not result in an error being returned by the storage trait.
    async fn multi_put_with_ctx(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = Document> + Send,
        _ctx: Option<&PutContext>,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        self.multi_put(keyspace, documents).await
    }

    /// Inserts or updates a set of documents in the persistent store.
    ///
    /// If the given `keyspace` does not exist, it should be created. A new keyspace name should
    /// not result in an error being returned by the storage trait.
    async fn multi_put(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = Document> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>>;

    /// Marks a document in the store as a tombstone.
    ///
    /// If the document does not exist this should be a no-op.
    ///
    /// If the given `keyspace` does not exist, it should be created. A new keyspace name should
    /// not result in an error being returned by the storage trait.
    ///
    /// NOTE:
    ///     This operation is permitted to delete the actual value of the document, but there
    ///     must be a marker indicating that the given document has been marked as deleted at
    ///     the provided timestamp.
    async fn mark_as_tombstone(
        &self,
        keyspace: &str,
        doc_id: Key,
        timestamp: HLCTimestamp,
    ) -> Result<(), Self::Error>;

    /// Marks a set of documents in the store as a tombstone.
    ///
    /// If the document does not exist this should be a no-op.
    ///
    /// If the given `keyspace` does not exist, it should be created. A new keyspace name should
    /// not result in an error being returned by the storage trait.
    ///
    /// NOTE:
    ///     This operation is permitted to delete the actual value of the document, but there
    ///     must be a marker indicating that the given document has been marked as deleted at
    ///     the provided timestamp.
    async fn mark_many_as_tombstone(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = (Key, HLCTimestamp)> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>>;

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

    use datacake_crdt::{get_unix_timestamp_ms, HLCTimestamp, Key};

    use crate::core::Document;
    use crate::storage::Storage;
    use crate::test_utils::InstrumentedStorage;

    #[tokio::test]
    async fn test_suite_semantics() {
        use crate::test_utils::MemStore;
        let _ = tracing_subscriber::fmt::try_init();
        run_test_suite(MemStore::default()).await
    }

    pub async fn run_test_suite<S: Storage + Send + Sync + 'static>(storage: S) {
        let mut clock = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);
        info!("Starting test suite for storage: {}", type_name::<S>());

        let storage = InstrumentedStorage(storage);

        test_keyspace_semantics(&storage, &mut clock).await;
        info!("test_keyspace_semantics OK");

        test_basic_persistence_test(&storage, &mut clock).await;
        info!("test_basic_persistence_test OK");

        test_basic_metadata_test(&storage, &mut clock).await;
        info!("test_basic_metadata_test OK");
    }

    #[instrument(name = "test_keyspace_semantics", skip(storage))]
    async fn test_keyspace_semantics<S: Storage + Sync>(
        storage: &S,
        clock: &mut HLCTimestamp,
    ) {
        info!("Starting test");

        static KEYSPACE: &str = "first-keyspace";

        let res = storage.iter_metadata(KEYSPACE).await;
        if let Err(e) = res {
            panic!(
                "Iterating through keyspace metadata should return OK. Got {:?}",
                e
            );
        }

        let metadata = storage
            .iter_metadata(KEYSPACE)
            .await
            .expect("Produce metadata iterator.")
            .collect::<HashSet<(Key, HLCTimestamp, bool)>>();
        assert_eq!(metadata, to_hashset([]), "New keyspace should be empty.");

        let doc = Document::new(1, clock.send().unwrap(), Vec::new());
        let res = storage.put_with_ctx(KEYSPACE, doc, None).await;
        assert!(
            res.is_ok(),
            "Setting metadata on a new keyspace should not error. Got {:?}",
            res
        );

        let doc = Document::new(2, clock.send().unwrap(), Vec::new());
        let res = storage.put_with_ctx(KEYSPACE, doc, None).await;
        assert!(
            res.is_ok(),
            "Setting metadata on a existing keyspace should not error. Got {:?}",
            res
        );

        let metadata = storage
            .iter_metadata(KEYSPACE)
            .await
            .expect("Produce metadata iterator.")
            .collect::<HashSet<(Key, HLCTimestamp, bool)>>();
        assert_eq!(
            metadata.len(),
            2,
            "First keyspace should contain 2 entries."
        );

        let keyspace_list = storage
            .get_keyspace_list()
            .await
            .expect("Get keyspace list");
        assert_eq!(
            keyspace_list,
            vec![KEYSPACE.to_string()],
            "Returned keyspace list (left) should match value provided (right)."
        );

        let metadata = storage
            .iter_metadata("second-keyspace")
            .await
            .expect("Produce metadata iterator.")
            .collect::<HashSet<(Key, HLCTimestamp, bool)>>();
        assert_eq!(metadata, to_hashset([]), "Second keyspace should be empty.");
    }

    #[instrument(name = "test_basic_metadata_test", skip(storage))]
    async fn test_basic_metadata_test<S: Storage>(
        storage: &S,
        clock: &mut HLCTimestamp,
    ) {
        info!("Starting test");

        static KEYSPACE: &str = "metadata-test-keyspace";

        let mut doc_1 = Document::new(1, clock.send().unwrap(), Vec::new());
        let mut doc_2 = Document::new(2, clock.send().unwrap(), Vec::new());
        let mut doc_3 = Document::new(3, clock.send().unwrap(), Vec::new());
        storage
            .multi_put(
                KEYSPACE,
                [doc_1.clone(), doc_2.clone(), doc_3.clone()].into_iter(),
            )
            .await
            .expect("Put documents");

        doc_3.last_updated = clock.send().unwrap();
        storage
            .mark_as_tombstone(KEYSPACE, doc_3.id, doc_3.last_updated)
            .await
            .expect("Mark document as tombstone.");

        let metadata = storage
            .iter_metadata(KEYSPACE)
            .await
            .expect("Produce metadata iterator.")
            .collect::<HashSet<(Key, HLCTimestamp, bool)>>();
        assert_eq!(
            metadata,
            to_hashset([
                (doc_1.id, doc_1.last_updated, false),
                (doc_2.id, doc_2.last_updated, false),
                (doc_3.id, doc_3.last_updated, true),
            ]),
            "Persisted metadata entries should match expected values."
        );

        doc_1.last_updated = clock.send().unwrap();
        doc_2.last_updated = clock.send().unwrap();
        storage
            .mark_many_as_tombstone(
                KEYSPACE,
                [
                    (doc_1.id, doc_1.last_updated),
                    (doc_2.id, doc_2.last_updated),
                ]
                .into_iter(),
            )
            .await
            .expect("Mark documents as tombstones.");
        let metadata = storage
            .iter_metadata(KEYSPACE)
            .await
            .expect("Produce metadata iterator.")
            .collect::<HashSet<(Key, HLCTimestamp, bool)>>();
        assert_eq!(
            metadata,
            to_hashset([
                (doc_1.id, doc_1.last_updated, true),
                (doc_2.id, doc_2.last_updated, true),
                (doc_3.id, doc_3.last_updated, true),
            ]),
            "Persisted metadata entries should match expected values."
        );

        storage
            .remove_tombstones(KEYSPACE, [1, 2].into_iter())
            .await
            .expect("Remove tombstone entries.");
        let metadata = storage
            .iter_metadata(KEYSPACE)
            .await
            .expect("Produce metadata iterator.")
            .collect::<HashSet<(Key, HLCTimestamp, bool)>>();
        assert_eq!(
            metadata,
            to_hashset([(doc_3.id, doc_3.last_updated, true)]),
            "Persisted metadata entries should match expected values after removal."
        );

        doc_1.last_updated = clock.send().unwrap();
        doc_2.last_updated = clock.send().unwrap();
        doc_3.last_updated = clock.send().unwrap();
        storage
            .multi_put(
                KEYSPACE,
                [doc_1.clone(), doc_2.clone(), doc_3.clone()].into_iter(),
            )
            .await
            .expect("Set metadata entry 3.");
        let metadata = storage
            .iter_metadata(KEYSPACE)
            .await
            .expect("Produce metadata iterator.")
            .collect::<HashSet<(Key, HLCTimestamp, bool)>>();
        assert_eq!(
            metadata,
            to_hashset([
                (doc_1.id, doc_1.last_updated, false),
                (doc_2.id, doc_2.last_updated, false),
                (doc_3.id, doc_3.last_updated, false),
            ]),
            "Persisted metadata entries should match expected values after update."
        );

        doc_1.last_updated = clock.send().unwrap();
        doc_2.last_updated = clock.send().unwrap();
        doc_3.last_updated = clock.send().unwrap();
        storage
            .mark_many_as_tombstone(
                KEYSPACE,
                [
                    (doc_1.id, doc_1.last_updated),
                    (doc_2.id, doc_2.last_updated),
                    (doc_3.id, doc_3.last_updated),
                ]
                .into_iter(),
            )
            .await
            .expect("Mark documents as tombstones.");
        let res = storage
            .remove_tombstones(KEYSPACE, [1, 2, 3].into_iter())
            .await;
        assert!(
            res.is_ok(),
            "Expected successful removal of given metadata keys. Got: {:?}",
            res
        );

        let metadata = storage
            .iter_metadata(KEYSPACE)
            .await
            .expect("Produce metadata iterator.")
            .count();
        assert_eq!(
            metadata, 0,
            "Persisted metadata entries should be empty after tombstone purge."
        );

        doc_1.last_updated = clock.send().unwrap();
        doc_2.last_updated = clock.send().unwrap();
        doc_3.last_updated = clock.send().unwrap();
        let doc_4_ts = clock.send().unwrap();
        storage
            .mark_many_as_tombstone(
                KEYSPACE,
                [
                    (doc_1.id, doc_1.last_updated),
                    (doc_2.id, doc_2.last_updated),
                    (doc_3.id, doc_3.last_updated),
                    (4, doc_4_ts),
                ]
                .into_iter(),
            )
            .await
            .expect("Mark documents as tombstones.");
        let metadata = storage
            .iter_metadata(KEYSPACE)
            .await
            .expect("Produce metadata iterator.")
            .collect::<HashSet<(Key, HLCTimestamp, bool)>>();
        assert_eq!(
            metadata,
            to_hashset([
                (doc_1.id, doc_1.last_updated, true),
                (doc_2.id, doc_2.last_updated, true),
                (doc_3.id, doc_3.last_updated, true),
                (4, doc_4_ts, true),
            ]),
            "Persisted tombstones should be tracked."
        );
    }

    #[instrument(name = "test_basic_persistence_test", skip(storage))]
    async fn test_basic_persistence_test<S: Storage + Sync>(
        storage: &S,
        clock: &mut HLCTimestamp,
    ) {
        info!("Starting test");

        static KEYSPACE: &str = "persistence-test-keyspace";

        let res = storage.get(KEYSPACE, 1).await;
        assert!(
            res.is_ok(),
            "Expected successful get request. Got: {:?}",
            res
        );
        assert!(
            res.unwrap().is_none(),
            "Expected no document to be returned."
        );

        #[allow(clippy::needless_collect)]
        let res = storage
            .multi_get(KEYSPACE, [1, 2, 3].into_iter())
            .await
            .expect("Expected successful get request.")
            .collect::<Vec<_>>();
        assert!(res.is_empty(), "Expected no document to be returned.");

        let mut doc_1 =
            Document::new(1, clock.send().unwrap(), b"Hello, world!".to_vec());
        let mut doc_2 = Document::new(2, clock.send().unwrap(), Vec::new());
        let mut doc_3 = Document::new(
            3,
            clock.send().unwrap(),
            b"Hello, from document 3!".to_vec(),
        );
        let doc_3_updated = Document::new(
            3,
            clock.send().unwrap(),
            b"Hello, from document 3 With an update!".to_vec(),
        );

        storage
            .put_with_ctx(KEYSPACE, doc_1.clone(), None)
            .await
            .expect("Put document in persistent store.");
        let res = storage.get(KEYSPACE, 1).await;
        assert!(
            res.is_ok(),
            "Expected successful get request. Got: {:?}",
            res
        );
        let doc = res
            .unwrap()
            .expect("Expected document to be returned after inserting doc.");
        assert_eq!(doc, doc_1, "Returned document should match.");

        storage
            .multi_put(KEYSPACE, [doc_3.clone(), doc_2.clone()].into_iter())
            .await
            .expect("Put document in persistent store.");
        let res = storage
            .multi_get(KEYSPACE, [1, 2, 3].into_iter())
            .await
            .expect("Expected successful get request.")
            .collect::<HashSet<_>>();
        assert_eq!(
            res,
            to_hashset([doc_1.clone(), doc_2.clone(), doc_3.clone()]),
            "Documents returned should match provided."
        );

        storage
            .put_with_ctx(KEYSPACE, doc_3_updated.clone(), None)
            .await
            .expect("Put updated document in persistent store.");
        let res = storage
            .get(KEYSPACE, 3)
            .await
            .expect("Get updated document.");
        let doc = res.expect("Expected document to be returned after updating doc.");
        assert_eq!(doc, doc_3_updated, "Returned document should match.");

        doc_2.last_updated = clock.send().unwrap();
        storage
            .mark_as_tombstone(KEYSPACE, doc_2.id, doc_2.last_updated)
            .await
            .expect("Mark document as tombstone.");
        let res = storage.get(KEYSPACE, 2).await;
        assert!(
            res.is_ok(),
            "Expected successful get request. Got: {:?}",
            res
        );
        assert!(
            res.unwrap().is_none(),
            "Expected no document to be returned."
        );

        doc_1.last_updated = clock.send().unwrap();
        doc_2.last_updated = clock.send().unwrap();
        storage
            .mark_many_as_tombstone(
                KEYSPACE,
                [
                    (doc_1.id, doc_1.last_updated),
                    (doc_2.id, doc_2.last_updated),
                    (4, clock.send().unwrap()),
                ]
                .into_iter(),
            )
            .await
            .expect("Merk documents as tombstones");
        let res = storage
            .multi_get(KEYSPACE, [1, 2, 3].into_iter())
            .await
            .expect("Expected successful get request.")
            .collect::<HashSet<_>>();
        assert_eq!(
            res,
            to_hashset([doc_3_updated]),
            "Expected returned documents to match.",
        );

        doc_3.last_updated = clock.send().unwrap();
        storage
            .mark_as_tombstone(KEYSPACE, doc_3.id, doc_3.last_updated)
            .await
            .expect("Delete documents from store.");
        #[allow(clippy::needless_collect)]
        let res = storage
            .multi_get(KEYSPACE, [1, 2, 3].into_iter())
            .await
            .expect("Expected successful get request.")
            .collect::<Vec<_>>();
        assert!(res.is_empty(), "Expected no documents to be returned.");
    }

    fn to_hashset<T: Hash + Eq>(iter: impl IntoIterator<Item = T>) -> HashSet<T> {
        iter.into_iter().collect()
    }
}
