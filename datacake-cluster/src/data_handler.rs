use std::collections::HashMap;
use std::sync::Arc;

use datacake_crdt::{HLCTimestamp, Key, OrSWotSet, StateChanges};

use crate::rpc::{DataHandler, Document};
use crate::shard::ShardGroupHandle;
use crate::{Clock, Datastore, NUMBER_OF_SHARDS};
use crate::error::DatacakeError;

#[derive(Clone)]
pub struct StandardDataHandler<DS: Datastore> {
    shard_group: ShardGroupHandle,
    datastore: Arc<DS>,
    clock: Clock,
}

impl<DS: Datastore> StandardDataHandler<DS> {
    pub(crate) fn new(
        shard_group: ShardGroupHandle,
        datastore: Arc<DS>,
        clock: Clock,
    ) -> Self {
        Self {
            shard_group,
            datastore,
            clock,
        }
    }

    pub(crate) async fn load_initial_shard_states(&self) -> Result<(), DatacakeError<DS::Error>> {
        for shard_id in 0..NUMBER_OF_SHARDS {
            let mut set = OrSWotSet::default();

            let mut live_docs = self.datastore
                .get_keys(shard_id)
                .await
                .map_err(DatacakeError::DatastoreError)?;

            // Ensure they are sorted as to not accidentally ignore state.
            live_docs.sort_by_key(|(_, v)| *v);
            for (k, v) in live_docs {
                set.insert(k, v);
            }

            let mut tombstones = self.datastore
                .get_tombstone_keys(shard_id)
                .await
                .map_err(DatacakeError::DatastoreError)?;

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
    type Error = DS::Error;

    async fn get_documents(&self, doc_ids: &[Key]) -> Result<Vec<Document>, DatacakeError<Self::Error>> {
        self.datastore
            .get_documents(doc_ids)
            .await
            .map_err(DatacakeError::DatastoreError)
    }

    async fn get_document(&self, doc_id: Key) -> Result<Option<Document>, DatacakeError<Self::Error>> {
        self.datastore
            .get_document(doc_id)
            .await
            .map_err(DatacakeError::DatastoreError)
    }

    async fn upsert_documents(
        &self,
        docs: Vec<(Key, HLCTimestamp, Vec<u8>)>,
    ) -> Result<(), DatacakeError<Self::Error>> {
        let mut shard_changes = HashMap::<usize, StateChanges>::new();
        let mut documents = vec![];
        for (doc_id, ts, data) in docs {
            self.clock.register_ts(ts).await;

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
                .await
                .map_err(DatacakeError::DatastoreError)?;
            self.datastore
                .remove_tombstone_keys(
                    shard_id,
                    changes.into_iter().map(|v| v.0).collect(),
                )
                .await
                .map_err(DatacakeError::DatastoreError)?;

            debug!(
                shard_id = shard_id,
                num_docs = num_docs,
                "Adding document to doc store."
            );
        }

        // TODO: Consider the issues that could happen if the datastore fails
        //  to atomically update each shard?
        self.datastore
            .upsert_documents(documents)
            .await
            .map_err(DatacakeError::DatastoreError)
    }

    async fn upsert_document(&self, doc: Document) -> Result<(), DatacakeError<Self::Error>> {
        let shard_id = crate::shard::get_shard_id(doc.id);

        self.shard_group
            .set(shard_id, doc.id, doc.last_modified)
            .await?;
        self.datastore
            .update_keys(shard_id, vec![(doc.id, doc.last_modified)])
            .await
            .map_err(DatacakeError::DatastoreError)?;

        debug!(
            shard_id = shard_id,
            num_docs = 1,
            "Adding document to doc store."
        );

        self.datastore
            .upsert_document(doc)
            .await
            .map_err(DatacakeError::DatastoreError)
    }

    async fn mark_tombstone_documents(
        &self,
        changes: Vec<(Key, HLCTimestamp)>,
    ) -> Result<(), DatacakeError<Self::Error>> {
        let mut shard_changes = HashMap::<usize, StateChanges>::new();
        let mut doc_ids = vec![];

        for (doc_id, ts) in changes {
            self.clock.register_ts(ts).await;

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
                .await
                .map_err(DatacakeError::DatastoreError)?;
            self.datastore
                .update_tombstone_keys(shard_id, changes)
                .await
                .map_err(DatacakeError::DatastoreError)?;

            debug!(
                shard_id = shard_id,
                num_docs = num_docs,
                "Marked documents as tombstones."
            );
        }

        self.datastore
            .delete_documents(&doc_ids)
            .await
            .map_err(DatacakeError::DatastoreError)
    }

    async fn mark_tombstone_document(
        &self,
        doc_id: Key,
        ts: HLCTimestamp,
    ) -> Result<(), DatacakeError<Self::Error>> {
        let shard_id = crate::shard::get_shard_id(doc_id);

        self.shard_group.delete(shard_id, doc_id, ts).await?;
        self.datastore
            .update_tombstone_keys(shard_id, vec![(doc_id, ts)])
            .await
            .map_err(DatacakeError::DatastoreError)?;
        self.datastore
            .remove_keys(shard_id, vec![doc_id])
            .await
            .map_err(DatacakeError::DatastoreError)?;

        debug!(
            shard_id = shard_id,
            num_docs = 1,
            "Marked document as tombstones."
        );

        self.datastore
            .delete_document(doc_id)
            .await
            .map_err(DatacakeError::DatastoreError)
    }

    async fn clear_tombstone_documents(&self, doc_ids: Vec<Key>) -> Result<(), DatacakeError<Self::Error>> {
        let mut shard_changes = HashMap::<usize, Vec<Key>>::new();

        for doc_id in doc_ids {
            let shard_id = crate::shard::get_shard_id(doc_id);

            shard_changes.entry(shard_id).or_default().push(doc_id);
        }

        for (shard_id, changes) in shard_changes {
            debug!(shard_id = shard_id, num_docs = %changes.len(), "Purged observed document deletes.");
            self.datastore
                .remove_tombstone_keys(shard_id, changes)
                .await
                .map_err(DatacakeError::DatastoreError)?;
        }

        Ok(())
    }
}
