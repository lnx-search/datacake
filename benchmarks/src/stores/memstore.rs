//! Copied from the test_utils file in the EC store.
use std::collections::HashMap;

use datacake::crdt::{HLCTimestamp, Key};
use datacake::eventual_consistency::{
    BulkMutationError,
    Document,
    DocumentMetadata,
    Storage,
};
use parking_lot::RwLock;

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
            .map_err(|e| e.into_inner())
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
        .map_err(|e| e.into_inner())
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
