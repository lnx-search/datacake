use std::sync::Arc;

use bytes::Bytes;
use datacake_crdt::{BadState, HLCTimestamp, Key, OrSWotSet, StateChanges};

use super::actor::ShardHandle;
use crate::shard::actor::{DeadShard, ShardActor};
use crate::StateWatcherHandle;

pub const NUMBER_OF_SHARDS: usize = 32;

/// Creates a shard group.
pub async fn create_shard_group(state_watcher: StateWatcherHandle) -> ShardGroupHandle {
    let mut handles = vec![];

    for shard_id in 0..NUMBER_OF_SHARDS {
        let handle = ShardActor::start(shard_id, state_watcher.clone()).await;
        handles.push(handle);
    }

    ShardGroupHandle::new(handles)
}

#[derive(Clone)]
pub struct ShardGroupHandle {
    handles: Arc<Vec<ShardHandle>>,
}

impl ShardGroupHandle {
    fn new(handles: Vec<ShardHandle>) -> Self {
        Self {
            handles: handles.into(),
        }
    }

    pub async fn set(
        &self,
        shard_id: usize,
        key: Key,
        timestamp: HLCTimestamp,
    ) -> Result<(), DeadShard> {
        self.handles[shard_id].set(key, timestamp).await
    }

    pub async fn set_many(
        &self,
        shard_id: usize,
        key_ts_pairs: StateChanges,
    ) -> Result<(), DeadShard> {
        self.handles[shard_id].set_many(key_ts_pairs).await
    }

    pub async fn delete(
        &self,
        shard_id: usize,
        key: Key,
        timestamp: HLCTimestamp,
    ) -> Result<(), DeadShard> {
        self.handles[shard_id].delete(key, timestamp).await
    }

    pub async fn delete_many(
        &self,
        shard_id: usize,
        key_ts_pairs: StateChanges,
    ) -> Result<(), DeadShard> {
        self.handles[shard_id].delete_many(key_ts_pairs).await
    }

    pub async fn get_serialized_set(
        &self,
        shard_id: usize,
    ) -> Result<Result<(Bytes, u64), BadState>, DeadShard> {
        self.handles[shard_id].get_serialized().await
    }

    pub async fn merge(
        &self,
        shard_id: usize,
        set: OrSWotSet,
    ) -> Result<(), DeadShard> {
        self.handles[shard_id].merge(set).await
    }

    pub async fn purge(
        &self,
        shard_id: usize,
    ) -> Result<Vec<Key>, DeadShard> {
        self.handles[shard_id].purge().await
    }

    pub async fn diff(
        &self,
        shard_id: usize,
        set: OrSWotSet,
    ) -> Result<(StateChanges, StateChanges), DeadShard> {
        self.handles[shard_id].diff(set).await
    }

    pub async fn compress_state(&self, shard_id: usize) -> Result<(), DeadShard> {
        self.handles[shard_id].compress_state().await
    }
}
