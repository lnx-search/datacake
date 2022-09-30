use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Error;
use datacake_crdt::{HLCTimestamp, Key, OrSWotSet, StateChanges};

use crate::rpc::{DataHandler, Document};
use crate::shard::ShardGroupHandle;
use crate::{Datastore, NUMBER_OF_SHARDS};

#[derive(Clone)]
pub struct StandardDataHandler<DS: Datastore> {
    shard_group: ShardGroupHandle,
    datastore: Arc<DS>,
}

impl<DS: Datastore> StandardDataHandler<DS> {
    pub(crate) fn new(shard_group: ShardGroupHandle, datastore: Arc<DS>) -> Self {
        Self {
            shard_group,
            datastore,
        }
    }

    pub(crate) async fn load_initial_shard_states(&self) -> Result<(), Error> {
        for shard_id in 0..NUMBER_OF_SHARDS {
            let mut set = OrSWotSet::default();

            let mut live_docs = self.datastore.get_keys(shard_id).await?;

            // Ensure they are sorted as to not accidentally ignore state.
            live_docs.sort_by_key(|(_, v)| *v);
            for (k, v) in live_docs {
                set.insert(k, v);
            }

            let mut tombstones = self.datastore.get_tombstone_keys(shard_id).await?;
            tombstones.sort_by_key(|(_, v)| *v);
            for (k, v) in tombstones {
                set.delete(k, v);
            }

            self.shard_group.merge(shard_id, set).await?;
            self.shard_group.compress_state(shard_id).await?;
        }

        Ok(())
    }
}

#[tonic::async_trait]
impl<DS: Datastore> DataHandler for StandardDataHandler<DS> {
    async fn get_documents(&self, doc_ids: &[Key]) -> Result<Vec<Document>, Error> {
        self.datastore.get_documents(doc_ids).await
    }

    async fn get_document(&self, doc_id: Key) -> Result<Option<Document>, Error> {
        self.datastore.get_document(doc_id).await
    }

    async fn upsert_documents(
        &self,
        docs: Vec<(Key, HLCTimestamp, Vec<u8>)>,
    ) -> Result<(), Error> {
        let mut shard_changes = HashMap::<usize, StateChanges>::new();
        let mut documents = vec![];
        for (doc_id, ts, data) in docs {
            let shard_id = crate::shard::get_shard_id(doc_id);

            shard_changes
                .entry(shard_id)
                .or_default()
                .push((doc_id, ts));

            let doc = Document {
                id: doc_id,
                last_modified: ts,
                data,
            };

            documents.push(doc);
        }

        for (shard_id, changes) in shard_changes {
            let num_docs = changes.len();
            self.shard_group.set_many(shard_id, changes.clone()).await?;
            self.datastore
                .update_keys(shard_id, changes.clone())
                .await?;
            self.datastore
                .remove_tombstone_keys(
                    shard_id,
                    changes.into_iter().map(|v| v.0).collect(),
                )
                .await?;

            debug!(
                shard_id = shard_id,
                num_docs = num_docs,
                "Adding document to doc store."
            );
        }

        // TODO: Consider the issues that could happen if the datastore fails
        //  to atomically update each shard?
        self.datastore.upsert_documents(documents).await
    }

    async fn upsert_document(&self, doc: Document) -> Result<(), Error> {
        let shard_id = crate::shard::get_shard_id(doc.id);

        self.shard_group
            .set(shard_id, doc.id, doc.last_modified)
            .await?;
        self.datastore
            .update_keys(shard_id, vec![(doc.id, doc.last_modified)])
            .await?;

        debug!(
            shard_id = shard_id,
            num_docs = 1,
            "Adding document to doc store."
        );
        self.datastore.upsert_document(doc).await
    }

    async fn mark_tombstone_documents(
        &self,
        changes: Vec<(Key, HLCTimestamp)>,
    ) -> Result<(), Error> {
        let mut shard_changes = HashMap::<usize, StateChanges>::new();
        let mut doc_ids = vec![];

        for (doc_id, ts) in changes {
            let shard_id = crate::shard::get_shard_id(doc_id);

            doc_ids.push(doc_id);
            shard_changes
                .entry(shard_id)
                .or_default()
                .push((doc_id, ts));
        }

        for (shard_id, changes) in shard_changes {
            let num_docs = changes.len();
            self.shard_group
                .delete_many(shard_id, changes.clone())
                .await?;
            self.datastore
                .remove_keys(shard_id, changes.iter().map(|v| v.0).collect())
                .await?;
            self.datastore
                .update_tombstone_keys(shard_id, changes)
                .await?;

            debug!(
                shard_id = shard_id,
                num_docs = num_docs,
                "Marked documents as tombstones."
            );
        }

        self.datastore.delete_documents(&doc_ids).await
    }

    async fn mark_tombstone_document(
        &self,
        doc_id: Key,
        ts: HLCTimestamp,
    ) -> Result<(), Error> {
        let shard_id = crate::shard::get_shard_id(doc_id);

        self.shard_group.delete(shard_id, doc_id, ts).await?;
        self.datastore
            .update_tombstone_keys(shard_id, vec![(doc_id, ts)])
            .await?;
        self.datastore.remove_keys(shard_id, vec![doc_id]).await?;

        debug!(
            shard_id = shard_id,
            num_docs = 1,
            "Marked document as tombstones."
        );
        self.datastore.delete_document(doc_id).await
    }

    async fn clear_tombstone_documents(&self, doc_ids: Vec<Key>) -> Result<(), Error> {
        let mut shard_changes = HashMap::<usize, Vec<Key>>::new();

        for doc_id in doc_ids {
            let shard_id = crate::shard::get_shard_id(doc_id);

            shard_changes.entry(shard_id).or_default().push(doc_id);
        }

        for (shard_id, changes) in shard_changes {
            debug!(shard_id = shard_id, num_docs = %changes.len(), "Purged observed document deletes.");
            self.datastore
                .remove_tombstone_keys(shard_id, changes)
                .await?;
        }

        Ok(())
    }
}
