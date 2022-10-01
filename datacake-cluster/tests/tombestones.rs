use std::sync::Arc;
use std::time::Duration;

use anyhow::{Error, Result};
use datacake_cluster::test_utils::MemStore;
use datacake_cluster::{test_utils, Datastore, Document, Metastore, NUMBER_OF_SHARDS};
use datacake_crdt::{HLCTimestamp, Key, StateChanges};
use parking_lot::Mutex;

#[derive(Default, Clone)]
pub struct MockDataStore {
    inner: Arc<MemStore>,
    tombstones: Arc<Mutex<Vec<Key>>>,
    deleted: Arc<Mutex<Vec<Key>>>,
}

#[tonic::async_trait]
impl Metastore for MockDataStore {
    type Error = <MemStore as Metastore>::Error;

    async fn get_keys(&self, shard_id: usize) -> Result<StateChanges, Error> {
        self.inner.get_keys(shard_id).await
    }

    async fn update_keys(
        &self,
        shard_id: usize,
        states: Vec<(Key, HLCTimestamp)>,
    ) -> Result<(), Error> {
        self.inner.update_keys(shard_id, states).await
    }

    async fn remove_keys(&self, shard_id: usize, states: Vec<Key>) -> Result<(), Error> {
        self.inner.remove_keys(shard_id, states).await
    }

    async fn get_tombstone_keys(&self, shard_id: usize) -> Result<StateChanges, Error> {
        self.inner.get_tombstone_keys(shard_id).await
    }

    async fn update_tombstone_keys(
        &self,
        shard_id: usize,
        states: Vec<(Key, HLCTimestamp)>,
    ) -> Result<(), Error> {
        self.tombstones.lock().extend(states.iter().map(|v| v.0));

        self.inner.update_tombstone_keys(shard_id, states).await
    }

    async fn remove_tombstone_keys(
        &self,
        shard_id: usize,
        states: Vec<Key>,
    ) -> Result<(), Error> {
        self.deleted.lock().extend(states.iter().copied());

        self.inner.remove_tombstone_keys(shard_id, states).await
    }
}

#[tonic::async_trait]
impl Datastore for MockDataStore {
    async fn get_documents(&self, doc_ids: &[Key]) -> Result<Vec<Document>, Error> {
        self.inner.get_documents(doc_ids).await
    }

    async fn upsert_documents(&self, documents: Vec<Document>) -> Result<(), Error> {
        self.inner.upsert_documents(documents).await
    }

    async fn delete_documents(&self, doc_ids: &[Key]) -> Result<(), Error> {
        self.inner.delete_documents(doc_ids).await
    }
}

#[tokio::test]
async fn test_documents_marked_as_tombstones() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let store = MockDataStore::default();
    let node = test_utils::make_test_node_with_store(
        "node-1",
        "127.0.0.1:8000",
        "127.0.0.1:8100",
        vec![],
        store.clone(),
    )
    .await?;

    let handle = node.handle();

    // Mark a key as deleted. It doesn't matter if the key exists or not.
    handle.delete(1).await?;

    let tombstones = store.tombstones.lock();
    assert_eq!(
        &*tombstones,
        &[1],
        "Expected document to be marked as a tombstone."
    );

    let dead = store.deleted.lock();
    assert_eq!(
        &*dead, &[0; 0],
        "Expected no documents to be marked as dead."
    );

    Ok(())
}

#[tokio::test]
async fn test_documents_purged_after_observation() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let store = MockDataStore::default();
    let node = test_utils::make_test_node_with_store(
        "node-1",
        "127.0.0.1:8001",
        "127.0.0.1:8101",
        vec![],
        store.clone(),
    )
    .await?;

    let handle = node.handle();

    // Mark a key as deleted. It doesn't matter if the key exists or not.
    handle.delete(1).await?;

    // We insert a new document so that our 'observed' operations go beyond our
    // original delete. Once we've observed another operation from the node that
    // marked the document as deleted; we can safely remove it completely.
    let same_shard_id = (NUMBER_OF_SHARDS + 1) as u64;
    handle.insert(same_shard_id, vec![123]).await?;

    // Wait for the background task to trigger the purge.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let dead = store.deleted.lock();
    assert_eq!(&*dead, &[1], "Expected no documents to be marked as dead.");

    Ok(())
}
