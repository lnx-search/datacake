use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytecheck::CheckBytes;
use datacake_crdt::{get_unix_timestamp_ms, HLCTimestamp, Key, OrSWotSet, StateChanges};
use parking_lot::RwLock;
use rkyv::{Archive, Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::storage::Storage;

#[derive(Archive, Serialize, Deserialize, Clone, Debug, Hash, Eq, PartialEq)]
#[archive_attr(derive(CheckBytes, Hash, Eq, PartialEq))]
/// A wrapper around a `Cow<'static, str>`.
///
/// This is needed so that we can serialize the whole keyspace map due to the ways
/// `rkyv` can serialize Cow's, we need to explicitly say how we want it to behave.
pub struct CounterKey(#[with(rkyv::with::AsOwned)] pub Cow<'static, str>);

#[derive(Archive, Serialize, Deserialize, Debug, Default)]
#[archive_attr(derive(CheckBytes))]
#[repr(C)]
pub struct KeyspaceTimestamps(pub HashMap<CounterKey, Arc<AtomicU64>>);

impl KeyspaceTimestamps {
    /// Works out the entries which are different in the current timestamps vs the provided on.
    ///
    /// This compares both the keys and the values.
    pub fn diff(&self, other: &Self) -> impl Iterator<Item = Cow<'static, str>> {
        let entries = self.iter().chain(other.iter());
        let mut processed = HashMap::with_capacity(self.len());

        for (key, v) in entries {
            let val = v.load(Ordering::Relaxed);
            processed
                .entry(key.0.clone())
                .and_modify(|existing: &mut (u64, usize, bool)| {
                    existing.1 += 1;

                    if existing.0 != val {
                        existing.2 = true;
                    }
                })
                .or_insert_with(|| (val, 1, false));
        }

        processed
            .into_iter()
            .filter(|(_, (_, counter, is_diff))| *is_diff || (*counter != 2))
            .map(|(key, _)| key)
    }
}

impl Deref for KeyspaceTimestamps {
    type Target = HashMap<CounterKey, Arc<AtomicU64>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for KeyspaceTimestamps {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// A collection of several keyspace states.
///
/// The group keeps track of what keyspace was updated and when it was last updated,
/// along with creation of new states for a new keyspace.
pub struct KeyspaceGroup<S: Storage> {
    storage: Arc<S>,
    keyspace_timestamps: Arc<RwLock<KeyspaceTimestamps>>,
    group: Arc<RwLock<HashMap<Cow<'static, str>, KeyspaceState<S>>>>,
}

impl<S: Storage> Clone for KeyspaceGroup<S> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            keyspace_timestamps: self.keyspace_timestamps.clone(),
            group: self.group.clone(),
        }
    }
}

impl<S: Storage> KeyspaceGroup<S> {
    /// Creates a new, empty keyspace group with a given storage implementation.
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            keyspace_timestamps: Default::default(),
            group: Default::default(),
        }
    }

    #[inline]
    /// Gets a reference to the keyspace storage implementation.
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Serializes the set of keyspace and their applicable timestamps of when they were last updated.
    ///
    /// These timestamps should only be compared against timestamps created by the same node, comparing
    /// them against timestamps created by different nodes can cause issues due to clock drift, etc...
    pub fn serialize_keyspace_counters(&self) -> Result<Vec<u8>, CorruptedState> {
        let guard = self.keyspace_timestamps.read();
        rkyv::to_bytes::<_, 4096>(guard.deref())
            .map_err(|_| CorruptedState)
            .map(|buf| buf.into_vec())
    }

    /// Get a handle to a given keyspace.
    pub fn get_keyspace(&self, name: &str) -> Option<KeyspaceState<S>> {
        let guard = self.group.read();
        guard.get(name).cloned()
    }

    /// Get a handle to a given keyspace.
    ///
    /// If the keyspace does not exist, it is created.
    pub async fn get_or_create_keyspace(&self, name: &str) -> KeyspaceState<S> {
        {
            let guard = self.group.read();
            if let Some(state) = guard.get(name) {
                return state.clone();
            }
        }

        self.add_state(name.to_string(), OrSWotSet::default()).await
    }

    /// Loads a set of existing keyspace states.
    pub async fn load_states(
        &self,
        states: Vec<(impl Into<Cow<'static, str>>, OrSWotSet)>,
    ) {
        let mut counters = Vec::new();
        let mut created_states = Vec::new();
        for (name, state) in states {
            let name = name.into();
            let update_counter = Arc::new(AtomicU64::new(0));

            let state = KeyspaceState::spawn(
                self.storage.clone(),
                name.clone(),
                state,
                update_counter.clone(),
            )
            .await;

            counters.push((name.clone(), update_counter));
            created_states.push((name, state));
        }

        {
            let mut guard = self.group.write();
            for (name, state) in created_states {
                guard.insert(name, state);
            }
        }

        {
            let mut guard = self.keyspace_timestamps.write();
            for (name, state) in counters {
                guard.insert(CounterKey(name.clone()), state);
            }
        }
    }

    /// Adds a new keyspace to the state groups.
    pub async fn add_state(
        &self,
        name: impl Into<Cow<'static, str>>,
        state: OrSWotSet,
    ) -> KeyspaceState<S> {
        let name = name.into();
        let update_counter = Arc::new(AtomicU64::new(0));

        let state = KeyspaceState::spawn(
            self.storage.clone(),
            name.clone(),
            state,
            update_counter.clone(),
        )
        .await;

        {
            let mut guard = self.group.write();
            guard.insert(name.clone(), state.clone());
        }

        {
            let mut guard = self.keyspace_timestamps.write();
            guard.insert(CounterKey(name), update_counter);
        }

        state
    }
}

pub struct KeyspaceState<S: Storage> {
    keyspace: Cow<'static, str>,
    tx: flume::Sender<Op>,
    storage: Arc<S>,
    update_counter: Arc<AtomicU64>,
}

impl<S: Storage> Clone for KeyspaceState<S> {
    fn clone(&self) -> Self {
        Self {
            keyspace: self.keyspace.clone(),
            tx: self.tx.clone(),
            storage: self.storage.clone(),
            update_counter: self.update_counter.clone(),
        }
    }
}

impl<S: Storage> KeyspaceState<S> {
    /// Spawns a new keyspace actor managing a given OrSwotSet set.
    pub async fn spawn(
        storage: Arc<S>,
        keyspace: Cow<'static, str>,
        state: OrSWotSet,
        update_counter: Arc<AtomicU64>,
    ) -> Self {
        let (tx, rx) = flume::bounded(10);

        tokio::spawn(run_state_actor(keyspace.clone(), state, rx));

        Self {
            keyspace,
            tx,
            storage,
            update_counter,
        }
    }

    #[inline]
    /// Gets the timestamp which the keyspace was last modified.
    pub fn last_updated(&self) -> u64 {
        self.update_counter.load(Ordering::Relaxed)
    }

    #[inline]
    /// Gets a reference to the keyspace storage implementation.
    pub fn storage(&self) -> &S {
        &self.storage
    }
    
    #[inline]
    /// The name of the keyspace.
    pub fn name(&self) -> &str {
        self.keyspace.as_ref()
    }
    
    /// Sets a entry in the set.
    pub async fn put(&self, key: Key, ts: HLCTimestamp) -> Result<(), S::Error> {
        self.update_counter
            .store(get_unix_timestamp_ms(), Ordering::Relaxed);

        self.storage
            .set_metadata(&self.keyspace, key, ts, false)
            .await?;

        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::Set { key, ts, tx })
            .await
            .expect("Contact keyspace actor");

        let _ = rx.await;

        Ok(())
    }

    /// Sets multiple keys in the set.
    pub async fn multi_put(&self, key_ts_pairs: StateChanges) -> Result<(), S::Error> {
        self.update_counter
            .store(get_unix_timestamp_ms(), Ordering::Relaxed);

        self.storage
            .set_many_metadata(&self.keyspace, key_ts_pairs.iter().cloned(), false)
            .await?;

        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::MultiSet { key_ts_pairs, tx })
            .await
            .expect("Contact keyspace actor");

        let _ = rx.await;

        Ok(())
    }

    /// Removes a entry in the set.
    pub async fn del(&self, key: Key, ts: HLCTimestamp) -> Result<(), S::Error> {
        self.update_counter
            .store(get_unix_timestamp_ms(), Ordering::Relaxed);

        self.storage
            .set_metadata(&self.keyspace, key, ts, true)
            .await?;

        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::Del { key, ts, tx })
            .await
            .expect("Contact keyspace actor");

        let _ = rx.await;
        Ok(())
    }

    /// Removes multiple keys in the set.
    pub async fn multi_del(&self, key_ts_pairs: StateChanges) -> Result<(), S::Error> {
        self.update_counter
            .store(get_unix_timestamp_ms(), Ordering::Relaxed);

        self.storage
            .set_many_metadata(&self.keyspace, key_ts_pairs.iter().cloned(), true)
            .await?;

        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::MultiDel { key_ts_pairs, tx })
            .await
            .expect("Contact keyspace actor");

        let _ = rx.await;
        Ok(())
    }

    /// Gets a serialized copy of the keyspace state.
    pub async fn serialize(&self) -> Result<Vec<u8>, CorruptedState> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::Serialize { tx })
            .await
            .expect("Contact keyspace actor");

        rx.await.expect("Get actor response")
    }

    /// Purges any tombstones which have had operations observed after them.
    ///
    /// This is used to clear the state of deletes reducing memory usage.
    pub async fn purge_tombstones(&self) -> Result<(), S::Error> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::PurgeDeletes { tx })
            .await
            .expect("Contact keyspace actor");

        let keys = rx.await.expect("Get actor response");
        self.storage
            .remove_many_metadata(&self.keyspace, keys.into_iter())
            .await
    }

    /// Calculates the difference between the current state and another provided state.
    pub async fn diff(&self, set: OrSWotSet) -> (StateChanges, StateChanges) {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::Diff { set, tx })
            .await
            .expect("Contact keyspace actor");

        rx.await.expect("Get actor response")
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Failed to (de)serialize state.")]
pub struct CorruptedState;

enum Op {
    Set {
        key: Key,
        ts: HLCTimestamp,
        tx: oneshot::Sender<()>,
    },
    MultiSet {
        key_ts_pairs: StateChanges,
        tx: oneshot::Sender<()>,
    },
    Del {
        key: Key,
        ts: HLCTimestamp,
        tx: oneshot::Sender<()>,
    },
    MultiDel {
        key_ts_pairs: StateChanges,
        tx: oneshot::Sender<()>,
    },
    Serialize {
        tx: oneshot::Sender<Result<Vec<u8>, CorruptedState>>,
    },
    PurgeDeletes {
        tx: oneshot::Sender<Vec<Key>>,
    },
    Diff {
        set: OrSWotSet,
        tx: oneshot::Sender<(StateChanges, StateChanges)>,
    }
}

#[instrument("keyspace-state", skip_all)]
async fn run_state_actor(
    keyspace: Cow<'static, str>,
    mut state: OrSWotSet,
    tasks: flume::Receiver<Op>,
) {
    info!(keyspace = %keyspace, "Starting keyspace actor.");

    while let Ok(op) = tasks.recv_async().await {
        match op {
            Op::Set { key, ts, tx } => {
                state.insert(key, ts);
                let _ = tx.send(());
            },
            Op::MultiSet { key_ts_pairs, tx } => {
                for (key, ts) in key_ts_pairs {
                    state.insert(key, ts);
                }
                let _ = tx.send(());
            },
            Op::Del { key, ts, tx } => {
                state.delete(key, ts);
                let _ = tx.send(());
            },
            Op::MultiDel { key_ts_pairs, tx } => {
                for (key, ts) in key_ts_pairs {
                    state.delete(key, ts);
                }
                let _ = tx.send(());
            },
            Op::Serialize { tx } => {
                let res = rkyv::to_bytes::<_, 4096>(&state)
                    .map(|buf| buf.into_vec())
                    .map_err(|_| CorruptedState);
                let _ = tx.send(res);
            },
            Op::PurgeDeletes { tx } => {
                let keys = state.purge_old_deletes();
                let _ = tx.send(keys);
            },
            Op::Diff { set, tx } => {
                let diff = state.diff(&set);
                let _ = tx.send(diff);
            }
        }
    }

    info!(keyspace = %keyspace, "All keyspace handles have been dropped, shutting down actor.");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_diff() {
        let set1 = KeyspaceTimestamps::default();
        let set2 = KeyspaceTimestamps::default();
        let diff = set1.diff(&set2).collect::<Vec<_>>();
        assert_eq!(
            diff,
            Vec::<Cow<str>>::new(),
            "Set difference should be the same as expected."
        );

        let mut set1 = KeyspaceTimestamps::default();
        set1.insert(
            CounterKey(Cow::Borrowed("key-1")),
            Arc::new(AtomicU64::new(1)),
        );
        let mut set2 = KeyspaceTimestamps::default();
        set2.insert(
            CounterKey(Cow::Borrowed("key-2")),
            Arc::new(AtomicU64::new(1)),
        );
        let diff = set1.diff(&set2).collect::<Vec<_>>();
        assert_eq!(
            diff,
            vec![Cow::Borrowed("key-1"), Cow::Borrowed("key-2"),],
            "Set difference should be the same as expected.",
        );

        let mut set1 = KeyspaceTimestamps::default();
        set1.insert(
            CounterKey(Cow::Borrowed("key-1")),
            Arc::new(AtomicU64::new(1)),
        );
        set1.insert(
            CounterKey(Cow::Borrowed("key-2")),
            Arc::new(AtomicU64::new(1)),
        );
        let mut set2 = KeyspaceTimestamps::default();
        set2.insert(
            CounterKey(Cow::Borrowed("key-2")),
            Arc::new(AtomicU64::new(1)),
        );
        let diff = set1.diff(&set2).collect::<Vec<_>>();
        assert_eq!(
            diff,
            vec![Cow::Borrowed("key-1")],
            "Set difference should be the same as expected.",
        );

        let mut set1 = KeyspaceTimestamps::default();
        set1.insert(
            CounterKey(Cow::Borrowed("key-1")),
            Arc::new(AtomicU64::new(1)),
        );
        set1.insert(
            CounterKey(Cow::Borrowed("key-2")),
            Arc::new(AtomicU64::new(2)),
        );
        let mut set2 = KeyspaceTimestamps::default();
        set2.insert(
            CounterKey(Cow::Borrowed("key-2")),
            Arc::new(AtomicU64::new(1)),
        );
        let diff = set1.diff(&set2).collect::<Vec<_>>();
        assert_eq!(
            diff,
            vec![Cow::Borrowed("key-1"), Cow::Borrowed("key-2"),],
            "Set difference should be the same as expected.",
        );

        let mut set1 = KeyspaceTimestamps::default();
        set1.insert(
            CounterKey(Cow::Borrowed("key-1")),
            Arc::new(AtomicU64::new(1)),
        );
        set1.insert(
            CounterKey(Cow::Borrowed("key-2")),
            Arc::new(AtomicU64::new(2)),
        );
        let mut set2 = KeyspaceTimestamps::default();
        set2.insert(
            CounterKey(Cow::Borrowed("key-1")),
            Arc::new(AtomicU64::new(3)),
        );
        let diff = set1.diff(&set2).collect::<Vec<_>>();
        assert_eq!(
            diff,
            vec![Cow::Borrowed("key-1"), Cow::Borrowed("key-2"),],
            "Set difference should be the same as expected.",
        );
    }
}
