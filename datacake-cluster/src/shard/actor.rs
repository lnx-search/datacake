use std::time::{Duration, Instant};

use bytes::Bytes;
use datacake_crdt::{
    get_unix_timestamp_ms,
    BadState,
    HLCTimestamp,
    Key,
    OrSWotSet,
    StateChanges,
};
use tokio::sync::oneshot;
use tokio::time::timeout;

use crate::shard::base::LocalShard;
use crate::StateWatcherHandle;

pub type StateChangeTs = u64;

pub struct ShardActor {
    inner: LocalShard,
    shard_id: usize,

    changes: StateWatcherHandle,
    events_rx: flume::Receiver<Event>,
    has_changed: bool,
    last_triggered_change: Instant,
}

impl ShardActor {
    pub async fn start(shard_id: usize, changes: StateWatcherHandle) -> ShardHandle {
        let (events_tx, events_rx) = flume::bounded(50);

        let slf = Self {
            inner: LocalShard::default(),
            shard_id,
            events_rx,
            changes,
            has_changed: false,
            last_triggered_change: Instant::now(),
        };

        tokio::spawn(slf.run_actor());

        ShardHandle { events_tx }
    }

    async fn run_actor(mut self) {
        let is_abort = loop {
            self.check_last_change().await;

            let fut = timeout(Duration::from_secs(60), self.events_rx.recv_async());

            let event = match fut.await {
                Ok(Ok(event)) => event,
                Ok(Err(_)) => break false,
                Err(_) => {
                    if let Err(e) = self.inner.try_compress_data().await {
                        warn!(error = ?e, "Failed to compress memory buffer due to invalid state. This is likely a bug.");
                    }
                    continue;
                },
            };

            if !event.needs_decompressed_view() {
                self.handle_event(event).await;
                continue;
            }

            if let Err(e) = self.inner.try_decompress_data().await {
                error!(error = ?e, "Compressed memory state is invalid. This suggests that the shard's state has been invalidated. This is a bug.");

                // We can't trust our state anymore, the best thing we can do is exist and let the error bubble up.
                break true;
            }

            self.handle_event(event).await;
        };

        if is_abort {
            error!("Shutting down actor unexpectedly. This was likely caused to the shard's state being corrupted.");
        } else {
            debug!("Shutting down actor.");
        }
    }

    fn set_changed(&mut self) {
        self.has_changed = true;
    }

    async fn check_last_change(&mut self) {
        if self.has_changed {
            let _ = self
                .changes
                .change(self.shard_id, get_unix_timestamp_ms())
                .await;
            self.last_triggered_change = Instant::now();
        }

        self.has_changed = false;
    }

    async fn handle_event(&mut self, event: Event) {
        match event {
            Event::Set { key, timestamp } => {
                self.inner.insert(key, timestamp);
                self.set_changed();
            },
            Event::BulkSet { key_ts_pairs } => {
                for (key, ts) in key_ts_pairs {
                    self.inner.insert(key, ts);
                }
                self.set_changed();
            },
            Event::Delete { key, timestamp } => {
                self.inner.delete(key, timestamp);
                self.set_changed();
            },
            Event::BulkDelete { key_ts_pairs } => {
                for (key, ts) in key_ts_pairs {
                    self.inner.delete(key, ts);
                }
                self.set_changed();
            },
            Event::GetSerialized { tx } => {
                let data = self.inner.transportable_buffer().await;
                let _ = tx.send(data);
            },
            Event::Merge { set, tx } => {
                self.inner.merge(set);
                let purged_keys = self.inner.purge_old_deletes();
                let _ = tx.send(purged_keys);
            },
            Event::Diff { set, tx } => {
                let diff = self.inner.diff(&set);
                let _ = tx.send(diff);
            },
            Event::Compress => {
                let _ = self.inner.try_compress_data();
            },
        }
    }
}

#[derive(Debug)]
pub enum Event {
    Set {
        key: Key,
        timestamp: HLCTimestamp,
    },
    BulkSet {
        key_ts_pairs: StateChanges,
    },
    Delete {
        key: Key,
        timestamp: HLCTimestamp,
    },
    BulkDelete {
        key_ts_pairs: StateChanges,
    },
    GetSerialized {
        tx: oneshot::Sender<Result<(Bytes, u64), BadState>>,
    },
    Merge {
        set: OrSWotSet,
        tx: oneshot::Sender<Vec<Key>>,
    },
    Diff {
        set: OrSWotSet,
        tx: oneshot::Sender<(StateChanges, StateChanges)>,
    },
    Compress,
}

impl Event {
    fn needs_decompressed_view(&self) -> bool {
        !matches!(self, Self::Compress | Self::GetSerialized { .. })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("The shard has shutdown due to an unrecoverable error.")]
pub struct DeadShard;

#[derive(Clone)]
pub struct ShardHandle {
    events_tx: flume::Sender<Event>,
}

impl ShardHandle {
    pub async fn set(&self, key: Key, timestamp: HLCTimestamp) -> Result<(), DeadShard> {
        self.events_tx
            .send_async(Event::Set { key, timestamp })
            .await
            .map_err(|_| DeadShard)
    }

    pub async fn set_many(&self, key_ts_pairs: StateChanges) -> Result<(), DeadShard> {
        self.events_tx
            .send_async(Event::BulkSet { key_ts_pairs })
            .await
            .map_err(|_| DeadShard)
    }

    pub async fn delete(
        &self,
        key: Key,
        timestamp: HLCTimestamp,
    ) -> Result<(), DeadShard> {
        self.events_tx
            .send_async(Event::Delete { key, timestamp })
            .await
            .map_err(|_| DeadShard)
    }

    pub async fn delete_many(
        &self,
        key_ts_pairs: StateChanges,
    ) -> Result<(), DeadShard> {
        self.events_tx
            .send_async(Event::BulkDelete { key_ts_pairs })
            .await
            .map_err(|_| DeadShard)
    }

    pub async fn get_serialized(
        &self,
    ) -> Result<Result<(Bytes, u64), BadState>, DeadShard> {
        let (tx, rx) = oneshot::channel();

        self.events_tx
            .send_async(Event::GetSerialized { tx })
            .await
            .map_err(|_| DeadShard)?;

        rx.await.map_err(|_| DeadShard)
    }

    pub async fn merge(&self, set: OrSWotSet) -> Result<Vec<Key>, DeadShard> {
        let (tx, rx) = oneshot::channel();

        self.events_tx
            .send_async(Event::Merge { set, tx })
            .await
            .map_err(|_| DeadShard)?;

        rx.await.map_err(|_| DeadShard)
    }

    pub async fn diff(
        &self,
        set: OrSWotSet,
    ) -> Result<(StateChanges, StateChanges), DeadShard> {
        let (tx, rx) = oneshot::channel();

        self.events_tx
            .send_async(Event::Diff { set, tx })
            .await
            .map_err(|_| DeadShard)?;

        rx.await.map_err(|_| DeadShard)
    }

    pub async fn compress_state(&self) -> Result<(), DeadShard> {
        self.events_tx
            .send_async(Event::Compress)
            .await
            .map_err(|_| DeadShard)
    }
}
