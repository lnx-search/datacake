use std::collections::HashMap;

use anyhow::Error;
use datacake_crdt::{HLCTimestamp, Key, StateChanges};
use parking_lot::RwLock;
use tonic::async_trait;

use crate::{Datastore, Document, Metastore, NUMBER_OF_SHARDS};

fn get_shard_map() -> HashMap<usize, HashMap<Key, HLCTimestamp>> {
    let mut map = HashMap::new();

    for i in 0..NUMBER_OF_SHARDS {
        map.insert(i, HashMap::new());
    }

    map
}

pub struct MemStore {
    metadata: RwLock<HashMap<usize, HashMap<Key, HLCTimestamp>>>,
    tombstones: RwLock<HashMap<usize, HashMap<Key, HLCTimestamp>>>,
    documents: RwLock<HashMap<Key, Document>>,
}

impl Default for MemStore {
    fn default() -> Self {
        Self {
            metadata: RwLock::new(get_shard_map()),
            tombstones: RwLock::new(get_shard_map()),
            documents: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl Metastore for MemStore {
    type Error = Error;
    
    async fn get_keys(&self, shard_id: usize) -> Result<StateChanges, Error> {
        let map = self.metadata.read().get(&shard_id).unwrap().clone();

        Ok(map.into_iter().collect())
    }

    async fn update_keys(
        &self,
        shard_id: usize,
        states: Vec<(Key, HLCTimestamp)>,
    ) -> Result<(), Error> {
        let mut lock = self.metadata.write();
        let map = lock.get_mut(&shard_id).unwrap();

        for (k, ts) in states {
            map.insert(k, ts);
        }

        Ok(())
    }

    async fn remove_keys(&self, shard_id: usize, states: Vec<Key>) -> Result<(), Error> {
        let mut lock = self.metadata.write();
        let map = lock.get_mut(&shard_id).unwrap();

        for k in states {
            map.remove(&k);
        }

        Ok(())
    }

    async fn get_tombstone_keys(&self, shard_id: usize) -> Result<StateChanges, Error> {
        let map = self.tombstones.read().get(&shard_id).unwrap().clone();

        Ok(map.into_iter().collect())
    }

    async fn update_tombstone_keys(
        &self,
        shard_id: usize,
        states: Vec<(Key, HLCTimestamp)>,
    ) -> Result<(), Error> {
        let mut lock = self.tombstones.write();
        let map = lock.get_mut(&shard_id).unwrap();

        for (k, ts) in states {
            map.insert(k, ts);
        }

        Ok(())
    }

    async fn remove_tombstone_keys(
        &self,
        shard_id: usize,
        states: Vec<Key>,
    ) -> Result<(), Error> {
        let mut lock = self.tombstones.write();
        let map = lock.get_mut(&shard_id).unwrap();

        for k in states {
            map.remove(&k);
        }

        Ok(())
    }
}

#[async_trait]
impl Datastore for MemStore {
    async fn get_documents(&self, doc_ids: &[Key]) -> Result<Vec<Document>, Error> {
        let map = self.documents.read();
        let mut docs = vec![];

        for key in doc_ids {
            if let Some(doc) = map.get(key) {
                docs.push(doc.clone());
            }
        }

        Ok(docs)
    }

    async fn upsert_documents(&self, documents: Vec<Document>) -> Result<(), Error> {
        let mut map = self.documents.write();

        for doc in documents {
            map.insert(doc.id, doc);
        }

        Ok(())
    }

    async fn delete_documents(&self, doc_ids: &[Key]) -> Result<(), Error> {
        let mut map = self.documents.write();

        for id in doc_ids {
            map.remove(id);
        }

        Ok(())
    }
}
