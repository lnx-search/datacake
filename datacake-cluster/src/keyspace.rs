use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytecheck::CheckBytes;
use datacake_crdt::{get_unix_timestamp_ms, HLCTimestamp, Key, OrSWotSet, StateChanges};
use parking_lot::RwLock;
use rkyv::{Archive, Deserialize, Serialize};
use tokio::sync::oneshot;
use tokio::time::{interval, Instant};

use crate::storage::Storage;
use crate::Clock;

pub const NUM_SOURCES: usize = 2;

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
    clock: Clock,
    storage: Arc<S>,
    keyspace_timestamps: Arc<RwLock<KeyspaceTimestamps>>,
    group: Arc<RwLock<HashMap<Cow<'static, str>, KeyspaceState<S>>>>,
}

impl<S: Storage> Clone for KeyspaceGroup<S> {
    fn clone(&self) -> Self {
        Self {
            clock: self.clock.clone(),
            storage: self.storage.clone(),
            keyspace_timestamps: self.keyspace_timestamps.clone(),
            group: self.group.clone(),
        }
    }
}

impl<S> KeyspaceGroup<S>
where
    S: Storage + Send + Sync + Default + 'static,
{
    #[cfg(any(test, feature = "test-utils"))]
    #[allow(unused)]
    pub async fn new_for_test() -> Self {
        let clock = Clock::new(0);
        let storage = Arc::new(S::default());

        Self::new(storage, clock).await
    }
}

impl<S> KeyspaceGroup<S>
where
    S: Storage + Send + Sync + 'static,
{
    /// Creates a new, empty keyspace group with a given storage implementation.
    pub async fn new(storage: Arc<S>, clock: Clock) -> Self {
        let slf = Self {
            clock,
            storage,
            keyspace_timestamps: Default::default(),
            group: Default::default(),
        };

        tokio::spawn(purge_tombstones_task(slf.clone()));

        slf
    }
    #[inline]
    /// Gets a reference to the keyspace storage implementation.
    pub fn storage(&self) -> &S {
        &self.storage
    }

    #[inline]
    /// The clock used by the given keyspace group.
    pub fn clock(&self) -> &Clock {
        &self.clock
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

    /// Loads existing states from the given storage implementation.
    pub async fn load_states_from_storage(&self) -> Result<(), S::Error> {
        let start = Instant::now();
        let mut states = HashMap::new();

        for keyspace in self.storage.get_keyspace_list().await? {
            let keyspace = Cow::Owned(keyspace);
            let mut state = OrSWotSet::default();

            let mut entries = self
                .storage
                .iter_metadata(&keyspace)
                .await?
                .collect::<Vec<(Key, HLCTimestamp, bool)>>();

            // Must be time ordered to avoid skipping entries.
            entries.sort_by_key(|entry| entry.1);

            for (key, ts, tombstone) in entries {
                if tombstone {
                    state.delete(key, ts);
                } else {
                    state.insert(key, ts);
                }
            }

            states.entry(keyspace).or_insert(state);
        }

        info!(
            elapsed = ?start.elapsed(),
            keyspace_count = states.len(),
            "Loaded persisted state from storage.",
        );

        self.load_states(states.into_iter()).await;

        Ok(())
    }

    /// Loads a set of existing keyspace states.
    pub async fn load_states(
        &self,
        states: impl Iterator<Item = (impl Into<Cow<'static, str>>, OrSWotSet<NUM_SOURCES>)>,
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
        state: OrSWotSet<NUM_SOURCES>,
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
        state: OrSWotSet<NUM_SOURCES>,
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

    #[cfg(test)]
    /// Creates a new keyspace state for a test.
    pub async fn for_test(storage: Arc<S>) -> Self {
        Self::spawn(
            storage,
            Cow::Borrowed("test-keyspace"),
            Default::default(),
            Default::default(),
        )
        .await
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
    pub async fn put<SS: StateSource>(&self, key: Key, ts: HLCTimestamp) {
        self.update_counter
            .store(get_unix_timestamp_ms(), Ordering::Relaxed);

        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::Set {
                source: SS::source_id(),
                key,
                ts,
                tx,
            })
            .await
            .expect("Contact keyspace actor");

        let _ = rx.await;
    }

    /// Sets multiple keys in the set.
    pub async fn multi_put<SS: StateSource>(&self, key_ts_pairs: StateChanges) {
        self.update_counter
            .store(get_unix_timestamp_ms(), Ordering::Relaxed);

        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::MultiSet {
                source: SS::source_id(),
                key_ts_pairs,
                tx,
            })
            .await
            .expect("Contact keyspace actor");

        let _ = rx.await;
    }

    /// Removes a entry in the set.
    pub async fn del<SS: StateSource>(&self, key: Key, ts: HLCTimestamp) {
        self.update_counter
            .store(get_unix_timestamp_ms(), Ordering::Relaxed);

        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::Del {
                source: SS::source_id(),
                key,
                ts,
                tx,
            })
            .await
            .expect("Contact keyspace actor");

        let _ = rx.await;
    }

    /// Removes multiple keys in the set.
    pub async fn multi_del<SS: StateSource>(&self, key_ts_pairs: StateChanges) {
        self.update_counter
            .store(get_unix_timestamp_ms(), Ordering::Relaxed);

        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::MultiDel {
                source: SS::source_id(),
                key_ts_pairs,
                tx,
            })
            .await
            .expect("Contact keyspace actor");

        let _ = rx.await;
    }

    /// Get a given key's timestamp.
    ///
    /// If the key does not exist, `None` is returned.
    ///
    /// Note: A key is counted as not existing if it's marked as a tombstone.
    pub async fn get(&self, key: Key) -> Option<HLCTimestamp> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::GetKey { key, tx })
            .await
            .expect("Contact keyspace actor");

        rx.await.expect("Get actor response.")
    }

    /// Get set of key's timestamps.
    ///
    /// If the key does not exist, it is not apart of the map.
    ///
    /// Note: A key is counted as not existing if it's marked as a tombstone.
    pub async fn get_many(&self, keys: Vec<Key>) -> HashMap<Key, HLCTimestamp> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::GetKeys { keys, tx })
            .await
            .expect("Contact keyspace actor");

        rx.await.expect("Get actor response.")
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

        let changes = rx.await.expect("Get actor response");

        let res = self
            .storage
            .remove_tombstones(&self.keyspace, changes.iter().map(|(key, _)| *key))
            .await;

        // The operation may have been partially successful. We should re-mark
        // any tombstones which weren't successful.
        if let Err(error) = res {
            let tombstones = changes
                .into_iter()
                .filter(|(key, _)| !error.successful_doc_ids.contains(key))
                .collect();

            self.tx
                .send_async(Op::AddTombstones { tombstones })
                .await
                .expect("Contact keyspace actor");

            return Err(error.inner);
        }

        Ok(())
    }

    /// Calculates the difference between the current state and another provided state.
    pub async fn diff(
        &self,
        set: OrSWotSet<NUM_SOURCES>,
    ) -> (StateChanges, StateChanges) {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::Diff { set, tx })
            .await
            .expect("Contact keyspace actor");

        rx.await.expect("Get actor response")
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[allow(unused)]
    /// Calculates the symmetrical difference between the two sets.
    ///
    /// This is only exposed as a test utility as it should not be used as part of
    /// the synchronisation process.
    pub async fn symetrical_diff(
        &self,
        set: OrSWotSet<NUM_SOURCES>,
    ) -> (StateChanges, StateChanges) {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::SymDiff { set, tx })
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
        source: usize,
        key: Key,
        ts: HLCTimestamp,
        tx: oneshot::Sender<()>,
    },
    MultiSet {
        source: usize,
        key_ts_pairs: StateChanges,
        tx: oneshot::Sender<()>,
    },
    Del {
        source: usize,
        key: Key,
        ts: HLCTimestamp,
        tx: oneshot::Sender<()>,
    },
    MultiDel {
        source: usize,
        key_ts_pairs: StateChanges,
        tx: oneshot::Sender<()>,
    },
    GetKey {
        key: Key,
        tx: oneshot::Sender<Option<HLCTimestamp>>,
    },
    GetKeys {
        keys: Vec<Key>,
        tx: oneshot::Sender<HashMap<Key, HLCTimestamp>>,
    },
    Serialize {
        tx: oneshot::Sender<Result<Vec<u8>, CorruptedState>>,
    },
    PurgeDeletes {
        tx: oneshot::Sender<StateChanges>,
    },
    AddTombstones {
        tombstones: StateChanges,
    },
    Diff {
        set: OrSWotSet<NUM_SOURCES>,
        tx: oneshot::Sender<(StateChanges, StateChanges)>,
    },
    #[cfg(any(test, feature = "test-utils"))]
    #[allow(unused)]
    SymDiff {
        set: OrSWotSet<NUM_SOURCES>,
        tx: oneshot::Sender<(StateChanges, StateChanges)>,
    },
}

async fn purge_tombstones_task<S>(handle: KeyspaceGroup<S>)
where
    S: Storage + Send + Sync + 'static,
{
    let mut interval = interval(Duration::from_secs(60 * 60)); // 1 hour.

    loop {
        interval.tick().await;

        let keyspace_set = {
            let lock = handle.group.read();
            lock.deref().clone()
        };

        for (name, state) in keyspace_set {
            if let Err(e) = state.purge_tombstones().await {
                warn!(error = ?e, keyspace = %name, "Failed to purge tombstones from state.");
            }
        }
    }
}

#[instrument("keyspace-state", skip_all)]
async fn run_state_actor(
    keyspace: Cow<'static, str>,
    mut state: OrSWotSet<NUM_SOURCES>,
    tasks: flume::Receiver<Op>,
) {
    info!(keyspace = %keyspace, "Starting keyspace actor.");

    while let Ok(op) = tasks.recv_async().await {
        match op {
            Op::Set {
                source,
                key,
                ts,
                tx,
            } => {
                state.insert_with_source(source, key, ts);
                let _ = tx.send(());
            },
            Op::MultiSet {
                source,
                key_ts_pairs,
                tx,
            } => {
                for (key, ts) in key_ts_pairs {
                    state.insert_with_source(source, key, ts);
                }
                let _ = tx.send(());
            },
            Op::Del {
                source,
                key,
                ts,
                tx,
            } => {
                state.delete_with_source(source, key, ts);
                let _ = tx.send(());
            },
            Op::MultiDel {
                source,
                key_ts_pairs,
                tx,
            } => {
                for (key, ts) in key_ts_pairs {
                    state.delete_with_source(source, key, ts);
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
            Op::AddTombstones { tombstones } => {
                state.add_raw_tombstones(tombstones);
            },
            Op::Diff { set, tx } => {
                let diff = state.diff(&set);
                let _ = tx.send(diff);
            },
            #[cfg(any(test, feature = "test-utils"))]
            Op::SymDiff { set, tx } => {
                let (change_left, removal_left) = state.diff(&set);
                let (change_right, removal_right) = set.diff(&state);

                let changes = change_left.into_iter().chain(change_right).collect();
                let removals = removal_left.into_iter().chain(removal_right).collect();

                let _ = tx.send((changes, removals));
            },
            Op::GetKey { key, tx } => {
                let ts = state.get(&key).copied();
                let _ = tx.send(ts);
            },
            Op::GetKeys { keys, tx } => {
                let mut timestamps = HashMap::with_capacity(keys.len());
                for key in keys {
                    if let Some(ts) = state.get(&key) {
                        timestamps.insert(key, *ts);
                    }
                }
                let _ = tx.send(timestamps);
            },
        }
    }

    info!(keyspace = %keyspace, "All keyspace handles have been dropped, shutting down actor.");
}

pub trait StateSource {
    fn source_id() -> usize;
}

#[derive(Debug, Copy, Clone)]
pub struct ConsistencySource;

#[derive(Debug, Copy, Clone)]
pub struct ReplicationSource;

impl StateSource for ConsistencySource {
    fn source_id() -> usize {
        0
    }
}

impl StateSource for ReplicationSource {
    fn source_id() -> usize {
        1
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::test_utils::MemStore;

    #[derive(Debug, Copy, Clone)]
    pub struct TestSource;

    impl StateSource for TestSource {
        fn source_id() -> usize {
            0
        }
    }

    #[derive(Debug, Copy, Clone)]
    pub struct TestSource2;

    impl StateSource for TestSource2 {
        fn source_id() -> usize {
            1
        }
    }

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
        let diff = set1.diff(&set2).collect::<HashSet<_>>();
        assert_eq!(
            diff,
            HashSet::from_iter(vec![Cow::Borrowed("key-2"), Cow::Borrowed("key-1")]),
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
        let diff = set1.diff(&set2).collect::<HashSet<_>>();
        assert_eq!(
            diff,
            HashSet::from_iter(vec![Cow::Borrowed("key-1"), Cow::Borrowed("key-2")]),
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
        let diff = set1.diff(&set2).collect::<HashSet<_>>();
        assert_eq!(
            diff,
            HashSet::from_iter(vec![Cow::Borrowed("key-2"), Cow::Borrowed("key-1")]),
            "Set difference should be the same as expected.",
        );
    }

    #[tokio::test]
    async fn test_keyspace_single_action() {
        let storage = Arc::new(MemStore::default());
        let clock = Clock::new(0);
        let keyspace = KeyspaceState::spawn(
            storage,
            Cow::Borrowed("test-keyspace"),
            OrSWotSet::default(),
            Arc::new(AtomicU64::new(0)),
        )
        .await;

        assert!(
            keyspace.get(123).await.is_none(),
            "Non-existent key should return None."
        );

        // Insert key into state.
        let ts = clock.get_time().await;
        keyspace.put::<TestSource>(123, ts).await;
        let val = keyspace.get(123).await;
        assert_eq!(val, Some(ts));

        // Update key in state.
        let ts = clock.get_time().await;
        keyspace.put::<TestSource>(123, ts).await;
        let val = keyspace.get(123).await;
        assert_eq!(val, Some(ts));

        // Delete key with timestamp that's observed.
        keyspace.del::<TestSource>(123, ts).await;
        let val = keyspace.get(123).await;
        assert_eq!(val, Some(ts), "Values should be untouched by delete.");

        // Delete key
        let ts = clock.get_time().await;
        keyspace.del::<TestSource>(123, ts).await;
        let val = keyspace.get(123).await;
        assert_eq!(val, None, "Values not exist.");

        // Check symmetric diff
        let (changes, removals) = keyspace.symetrical_diff(OrSWotSet::default()).await;
        assert_eq!(changes, vec![]);
        assert_eq!(removals, vec![(123, ts)]);
    }

    #[tokio::test]
    async fn test_keyspace_bulk_actions() {
        let storage = Arc::new(MemStore::default());
        let clock = Clock::new(0);
        let keyspace = KeyspaceState::spawn(
            storage,
            Cow::Borrowed("test-keyspace"),
            OrSWotSet::default(),
            Arc::new(AtomicU64::new(0)),
        )
        .await;

        assert!(
            keyspace.get_many(vec![123]).await.is_empty(),
            "Non-existent key should return None."
        );

        // Multi-Insert key into state.
        let ts = clock.get_time().await;
        keyspace.multi_put::<TestSource>(vec![(123, ts)]).await;
        let val = keyspace.get(123).await;
        assert_eq!(val, Some(ts));

        // Multi-Update key in state.
        let ts = clock.get_time().await;
        keyspace.multi_put::<TestSource>(vec![(123, ts)]).await;
        let val = keyspace.get(123).await;
        assert_eq!(val, Some(ts));

        // Multi-Delete key with timestamp that's observed.
        keyspace.multi_del::<TestSource>(vec![(123, ts)]).await;
        let val = keyspace.get(123).await;
        assert_eq!(val, Some(ts), "Values should be untouched by delete.");

        // Multi-Delete key
        let ts = clock.get_time().await;
        keyspace.multi_del::<TestSource>(vec![(123, ts)]).await;
        let val = keyspace.get(123).await;
        assert_eq!(val, None, "Values not exist.");

        // Check symmetric diff
        let (changes, removals) = keyspace.symetrical_diff(OrSWotSet::default()).await;
        assert_eq!(changes, vec![]);
        assert_eq!(removals, vec![(123, ts)]);
    }

    #[tokio::test]
    async fn test_keyspace_unordered_events() {
        let storage = Arc::new(MemStore::default());
        let keyspace = KeyspaceState::spawn(
            storage,
            Cow::Borrowed("test-keyspace"),
            OrSWotSet::default(),
            Arc::new(AtomicU64::new(0)),
        )
        .await;

        let initial_ts = HLCTimestamp::new(101, 0, 0);
        keyspace.put::<TestSource>(123, initial_ts).await;
        assert_eq!(keyspace.get(123).await, Some(initial_ts));

        let old_ts = HLCTimestamp::new(100, 0, 0);
        keyspace.put::<TestSource>(123, old_ts).await;
        assert_eq!(keyspace.get(123).await, Some(initial_ts));

        let newer_ts = HLCTimestamp::new(102, 0, 0);
        keyspace.put::<TestSource>(123, newer_ts).await;
        assert_eq!(keyspace.get(123).await, Some(newer_ts));

        // This should be ignored as we've observed newer events from this source.
        keyspace.put::<TestSource>(321, old_ts).await;
        assert_eq!(keyspace.get(321).await, None);

        let put_ts = HLCTimestamp::new(103, 0, 0);
        keyspace.put::<TestSource>(321, put_ts).await;
        assert_eq!(keyspace.get(321).await, Some(put_ts));

        let newer_ts = HLCTimestamp::new(104, 0, 0);
        keyspace.del::<TestSource>(123, newer_ts).await;
        assert!(keyspace.get(123).await.is_none());

        keyspace.put::<TestSource>(123, old_ts).await;
        assert!(keyspace.get(123).await.is_none());

        // Ensure that calling `del` again with an older timestamp does not overwrite our newer timestamp.
        keyspace.del::<TestSource>(123, old_ts).await;
        assert!(keyspace.get(123).await.is_none());
        keyspace.put::<TestSource>(123, old_ts).await;
        assert!(keyspace.get(123).await.is_none());

        // Deleting another key which has an older timestamp should be affected as they're from the same source.f
        keyspace.del::<TestSource>(321, old_ts).await;
        assert!(keyspace.get(321).await.is_some());

        // Deleting another key which has an older timestamp should not be affected as they're from different sources.
        // NOTE: The rules of last write wins still apply here.
        keyspace.del::<TestSource2>(321, newer_ts).await;
        assert!(keyspace.get(321).await.is_none());
    }
}
