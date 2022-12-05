use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_utils::atomic::AtomicCell;
use datacake_crdt::{HLCTimestamp, Key, OrSWotSet};
use parking_lot::RwLock;
use puppet::ActorMailbox;
use tokio::time::interval;

use super::NUM_SOURCES;
use crate::keyspace::messages::PurgeDeletes;
use crate::keyspace::KeyspaceActor;
use crate::rpc::datacake_api;
use crate::{Clock, Storage};

const PURGE_DELETES_INTERVAL: Duration = if cfg!(test) {
    Duration::from_secs(1)
} else {
    Duration::from_secs(60 * 60) // 1 Hour
};
type KeyspaceMap<S> = BTreeMap<Cow<'static, str>, ActorMailbox<KeyspaceActor<S>>>;

pub struct KeyspaceGroup<S>
where
    S: Storage + Send + Sync + 'static,
{
    clock: Clock,
    storage: Arc<S>,
    keyspace_timestamps: Arc<RwLock<KeyspaceTimestamps>>,
    group: Arc<RwLock<KeyspaceMap<S>>>,
}

impl<S> Clone for KeyspaceGroup<S>
where
    S: Storage + Send + Sync + 'static,
{
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

        tokio::spawn(keyspace_purge_task(slf.clone()));

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
    pub async fn get_keyspace_info(&self) -> datacake_api::KeyspaceInfo {
        let ts = self.clock.get_time().await;
        let lock = self.keyspace_timestamps.read();

        let mut timestamps = HashMap::with_capacity(lock.len());
        for (name, ts) in lock.iter() {
            timestamps
                .insert(name.to_string(), datacake_api::Timestamp::from(ts.load()));
        }

        datacake_api::KeyspaceInfo {
            timestamp: Some(ts.into()),
            keyspace_timestamps: timestamps,
        }
    }

    /// Get a handle to a given keyspace.
    ///
    /// If the keyspace does not exist, it is created.
    pub async fn get_or_create_keyspace(
        &self,
        name: &str,
    ) -> ActorMailbox<KeyspaceActor<S>> {
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
        let mut states = BTreeMap::new();

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
            let ts = self.clock.get_time().await;
            let update_counter = Arc::new(AtomicCell::new(ts));

            let state = super::spawn_keyspace(
                name.clone(),
                self.storage.clone(),
                self.clock.clone(),
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
                guard.insert(name.clone(), state);
            }
        }
    }

    /// Adds a new keyspace to the state groups.
    pub async fn add_state(
        &self,
        name: impl Into<Cow<'static, str>>,
        state: OrSWotSet<NUM_SOURCES>,
    ) -> ActorMailbox<KeyspaceActor<S>> {
        let name = name.into();
        let ts = self.clock.get_time().await;
        let update_counter = Arc::new(AtomicCell::new(ts));

        let state = super::spawn_keyspace(
            name.clone(),
            self.storage.clone(),
            self.clock.clone(),
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
            guard.insert(name, update_counter);
        }

        state
    }
}

async fn keyspace_purge_task<S>(handle: KeyspaceGroup<S>)
where
    S: Storage + Send + Sync + 'static,
{
    let mut interval = interval(PURGE_DELETES_INTERVAL); // 1 hour.

    loop {
        interval.tick().await;

        let keyspace_set = {
            let lock = handle.group.read();
            lock.deref().clone()
        };

        for (name, state) in keyspace_set {
            if let Err(e) = state.send(PurgeDeletes(PhantomData::<S>::default())).await {
                warn!(error = ?e, keyspace = %name, "Failed to purge tombstones from state.");
            }
        }
    }
}

#[derive(Clone, Default, Debug)]
pub struct KeyspaceTimestamps(
    pub BTreeMap<Cow<'static, str>, Arc<AtomicCell<HLCTimestamp>>>,
);

impl KeyspaceTimestamps {
    pub fn diff(&self, other: &Self) -> impl Iterator<Item = Cow<'static, str>> {
        let entries = self.iter().chain(other.iter());
        let mut processed = HashMap::with_capacity(self.len());

        for (key, v) in entries {
            let val = v.load();
            processed
                .entry(key.clone())
                .and_modify(|existing: &mut (HLCTimestamp, usize, bool)| {
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
    type Target = BTreeMap<Cow<'static, str>, Arc<AtomicCell<HLCTimestamp>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for KeyspaceTimestamps {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<datacake_api::KeyspaceInfo> for KeyspaceTimestamps {
    fn from(info: datacake_api::KeyspaceInfo) -> Self {
        let mut timestamps = Self::default();
        for (keyspace, ts) in info.keyspace_timestamps {
            let ts = HLCTimestamp::from(ts);
            timestamps.insert(Cow::Owned(keyspace), Arc::new(AtomicCell::new(ts)));
        }
        timestamps
    }
}
