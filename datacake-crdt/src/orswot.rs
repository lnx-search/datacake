use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::mem;

use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};

use crate::timestamp::HLCTimestamp;

pub type Key = u64;
pub type StateChanges = Vec<(Key, HLCTimestamp)>;

#[derive(Debug)]
pub struct BadState;

#[derive(Serialize, Deserialize, Archive, Debug, Default, Clone)]
#[archive(compare(PartialEq))]
#[archive_attr(derive(CheckBytes, Debug))]
pub struct NodeVersions {
    nodes_max_stamps: HashMap<u32, HLCTimestamp>,
}

impl NodeVersions {
    fn try_update_max_stamp(&mut self, ts: HLCTimestamp) -> bool {
        match self.nodes_max_stamps.entry(ts.node()) {
            Entry::Occupied(mut entry) => {
                // We have already observed these events at some point from this node.
                // This means we can no longer trust that this key is in fact still valid.
                if &ts < entry.get() {
                    return false;
                }

                entry.insert(ts);
            },
            Entry::Vacant(v) => {
                v.insert(ts);
            },
        }

        true
    }

    fn is_ts_beyond_last_observed_event(&self, ts: HLCTimestamp) -> bool {
        self.nodes_max_stamps
            .get(&ts.node())
            .map(|v| &ts < v)
            .unwrap_or_default()
    }
}

#[derive(Serialize, Deserialize, Archive, Debug, Default, Clone)]
#[archive(compare(PartialEq))]
#[archive_attr(derive(CheckBytes, Debug))]
pub struct OrSWotSet {
    entries: BTreeMap<Key, HLCTimestamp>,
    dead: HashMap<Key, HLCTimestamp>,
    versions: NodeVersions,
}

impl OrSWotSet {
    pub fn from_bytes(data: &[u8]) -> Result<Self, BadState> {
        // TODO: Use check bytes
        let archived = unsafe { rkyv::archived_root::<Self>(data) };

        let deserialized: Self = archived.deserialize(&mut rkyv::Infallible).unwrap();

        Ok(deserialized)
    }

    pub fn as_bytes(&self) -> Result<Vec<u8>, BadState> {
        Ok(rkyv::to_bytes::<_, 2048>(self)
            .map_err(|_| BadState)?
            .into_vec())
    }

    /// Calculated the determinisitic diffrence between two sets, returning the
    /// modified keys and the deleted keys.
    ///
    /// This follows the same logic as `set.merge(&other)` but does not modify
    /// the state of the main set.
    pub fn diff(&self, other: &OrSWotSet) -> (StateChanges, StateChanges) {
        let mut changed_keys = vec![];
        let mut removed_keys = vec![];

        let mut versions = self.versions.clone();

        let base_entries = other.entries.iter().map(|(k, ts)| (*k, *ts, false));

        let mut entries_log = Vec::from_iter(base_entries);
        entries_log.extend(other.dead.iter().map(|(k, ts)| (*k, *ts, true)));

        // It's important we go in time/event order. Otherwise we may incorrect merge the set.
        entries_log.sort_by_key(|v| v.1);

        for (key, ts, is_delete) in entries_log {
            // We've already observed the operation.
            if versions.is_ts_beyond_last_observed_event(ts) && is_delete {
                continue;
            }
            versions.try_update_max_stamp(ts);

            if is_delete {
                if let Some(entry) = self.entries.get(&key) {
                    if &ts >= entry {
                        removed_keys.push((key, ts));
                        continue;
                    }
                }

                if !self.dead.contains_key(&key) {
                    removed_keys.push((key, ts));
                }

                continue;
            }

            // If our own entry is newer, we use that.
            if let Some(existing_ts) = self.entries.get(&key) {
                if &ts < existing_ts {
                    continue;
                }
            }

            // Have we already marked the document as dead. And if so, is it newer than this op?
            if let Some(deleted) = self.dead.get(&key) {
                if &ts < deleted {
                    continue;
                }
            }

            changed_keys.push((key, ts));
        }

        (changed_keys, removed_keys)
    }

    /// Merges another set with the current set.
    ///
    /// In this case any conflicts are deterministically resolved via the key's [HLCTimestamp]
    /// any deletes are tracked or ignored depending on this timestamp due to the nature
    /// of the ORSWOT CRDT.
    pub fn merge(&mut self, other: OrSWotSet) {
        let base_entries = other.entries.into_iter().map(|(k, ts)| (k, ts, false));

        let remote_versions = other.versions;
        let mut entries_log = Vec::from_iter(base_entries);
        entries_log.extend(other.dead.into_iter().map(|(k, ts)| (k, ts, true)));

        // It's important we go in time/event order. Otherwise we may incorrect merge the set.
        entries_log.sort_by_key(|v| v.1);

        let mut old_entries = mem::take(&mut self.entries);

        for (key, ts, is_delete) in entries_log {
            // We've already observed the operation.
            if self.versions.is_ts_beyond_last_observed_event(ts) && is_delete {
                continue;
            }
            self.versions.try_update_max_stamp(ts);

            if is_delete {
                if let Some(entry) = self.entries.remove(&key) {
                    if ts < entry {
                        self.entries.insert(key, entry);
                        continue;
                    }
                }

                self.dead
                    .entry(key)
                    .and_modify(|v| {
                        if *v < ts {
                            (*v) = ts;
                        }
                    })
                    .or_insert_with(|| ts);

                continue;
            }

            let mut timestamp = ts;

            // If our own entry is newer, we use that.
            if let Some(delete_ts) = old_entries.remove(&key) {
                if delete_ts < ts {
                    timestamp = delete_ts;
                }
            }

            // Have we already marked the document as dead. And if so, is it newer than this op?
            if let Some(deleted) = self.dead.remove(&key) {
                if timestamp < deleted {
                    self.dead.insert(key, deleted);
                    continue;
                }
            }

            self.entries.insert(key, timestamp);
        }

        // The remaining entries in our map are either entries which we need to remove,
        // or entries that the other node is currently missing.
        for (key, ts) in old_entries {
            // We've observed all the events upto and beyond this timestamp.
            // We can rely on this check to see if we delete or keep the entry,
            // as this is a `<` bounds check rather than `<=`. Which means
            // if the entry happens to have been the most recent event observed, it won't
            // be `true` and therefore be kept.
            if remote_versions.is_ts_beyond_last_observed_event(ts) {
                continue;
            }

            if let Some(deleted) = self.dead.remove(&key) {
                if ts < deleted {
                    self.dead.insert(key, deleted);
                    continue;
                }
            }

            self.entries.insert(key, ts);
        }
    }

    pub fn get(&mut self, k: &Key) -> Option<&HLCTimestamp> {
        self.entries.get(k)
    }

    pub fn purge_old_deletes(&mut self) -> Vec<Key> {
        let mut deleted_keys = vec![];
        for (k, stamp) in mem::take(&mut self.dead) {
            if !self.versions.is_ts_beyond_last_observed_event(stamp) {
                self.dead.insert(k, stamp);
            } else {
                deleted_keys.push(k);
            }
        }

        deleted_keys
    }

    pub fn insert(&mut self, k: Key, ts: HLCTimestamp) -> bool {
        let mut has_set = false;

        if !self.versions.try_update_max_stamp(ts) {
            return has_set;
        }

        if let Some(deleted_ts) = self.dead.remove(&k) {
            // Our deleted timestamp is newer, so we don't want to adjust our markings.
            if deleted_ts >= ts {
                self.dead.insert(k, deleted_ts);
                return has_set;
            }
        }

        self.entries
            .entry(k)
            .and_modify(|v| {
                if *v < ts {
                    has_set = true;
                    (*v) = ts;
                }
            })
            .or_insert_with(|| {
                has_set = true;
                ts
            });

        has_set
    }

    pub fn delete(&mut self, k: Key, ts: HLCTimestamp) -> bool {
        let mut has_set = false;

        if !self.versions.try_update_max_stamp(ts) {
            return has_set;
        }

        if let Some(existing_ts) = self.entries.remove(&k) {
            // Our deleted timestamp is newer, so we don't want to adjust our markings.
            if existing_ts >= ts {
                self.entries.insert(k, existing_ts);
                return has_set;
            }
        }

        self.dead
            .entry(k)
            .and_modify(|v| {
                if *v < ts {
                    has_set = true;
                    (*v) = ts;
                }
            })
            .or_insert_with(|| {
                has_set = true;
                ts
            });

        has_set
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::timestamp::get_unix_timestamp_ms;

    #[test]
    fn test_basic_insert_merge() {
        let mut node_a = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);
        let mut node_b = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 1);

        // We create our new set for node a.
        let mut node_a_set = OrSWotSet::default();

        // We add a new set of entries into our set.
        node_a_set.insert(1, node_a.send().unwrap());
        node_a_set.insert(2, node_a.send().unwrap());
        node_a_set.insert(3, node_a.send().unwrap());

        // We create our new state on node b's side.
        let mut node_b_set = OrSWotSet::default();

        // We add a new set of entries into our set.
        node_b_set.insert(1, node_b.send().unwrap());
        node_b_set.insert(4, node_b.send().unwrap());

        node_a_set.merge(node_b_set);

        assert!(
            node_a_set.dead.is_empty(),
            "Expected no entries to be marked as dead."
        );

        assert!(
            node_a_set.entries.get(&1).is_some(),
            "Expected entry with key 1 to exist."
        );
        assert!(
            node_a_set.entries.get(&2).is_some(),
            "Expected entry with key 2 to exist."
        );
        assert!(
            node_a_set.entries.get(&3).is_some(),
            "Expected entry with key 3 to exist."
        );
        assert!(
            node_a_set.entries.get(&4).is_some(),
            "Expected entry with key 4 to exist."
        );
    }

    #[test]
    fn test_same_time_conflict_convergence() {
        let mut node_a = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);
        let mut node_b = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 1);

        // We create our new set for node a.
        let mut node_a_set = OrSWotSet::default();

        // We add a new set of entries into our set.
        // It's important that our `3` key is first here, as it means the counter
        // of the HLC timestamp will mean the delete succeeds.
        node_a_set.insert(3, node_a.send().unwrap());
        node_a_set.insert(1, node_a.send().unwrap());
        node_a_set.insert(2, node_a.send().unwrap());

        // We create our new state on node b's side.
        let mut node_b_set = OrSWotSet::default();

        // We add a new set of entries into our set.
        // These entries effectively happen at the same time as node A in our test, just because
        // of the execution speed.
        node_b_set.insert(1, node_b.send().unwrap());
        node_b_set.delete(3, node_b.send().unwrap());

        // When merged, the set should mark key `3` as deleted
        // and ignore the insert on the original set.
        node_a_set.merge(node_b_set.clone());

        assert!(
            node_a_set.dead.contains_key(&3),
            "SET A: Expected key 3 to be marked as dead."
        );

        assert!(
            node_a_set.entries.get(&1).is_some(),
            "SET A: Expected entry with key 1 to exist."
        );
        assert!(
            node_a_set.entries.get(&2).is_some(),
            "SET A: Expected entry with key 2 to exist."
        );
        assert!(
            node_a_set.entries.get(&3).is_none(),
            "SET A: Expected entry with key 3 to NOT exist."
        );

        // If we now merge set A with set B. They should align.
        node_b_set.merge(node_a_set);

        assert!(
            node_b_set.dead.contains_key(&3),
            "SET B: Expected key 3 to be marked as dead."
        );

        assert!(
            node_b_set.entries.get(&1).is_some(),
            "SET B: Expected entry with key 1 to exist."
        );
        assert!(
            node_b_set.entries.get(&2).is_some(),
            "SET B: Expected entry with key 2 to exist."
        );
        assert!(
            node_b_set.entries.get(&3).is_none(),
            "SET B: Expected entry with key 3 to NOT exist."
        );
    }

    #[test]
    fn test_basic_delete_merge() {
        let mut node_a = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);
        let mut node_b = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 1);

        // We create our new set for node a.
        let mut node_a_set = OrSWotSet::default();

        // We add a new set of entries into our set.
        node_a_set.insert(1, node_a.send().unwrap());
        node_a_set.insert(2, node_a.send().unwrap());
        node_a_set.insert(3, node_a.send().unwrap());

        std::thread::sleep(Duration::from_millis(1));

        // We create our new state on node b's side.
        let mut node_b_set = OrSWotSet::default();

        // We add a new set of entries into our set.
        node_b_set.insert(1, node_b.send().unwrap());
        node_b_set.delete(3, node_b.send().unwrap());

        node_a_set.merge(node_b_set.clone());

        assert!(
            node_a_set.dead.contains_key(&3),
            "Expected key 3 to be marked as dead."
        );

        assert!(
            node_a_set.entries.get(&1).is_some(),
            "Expected entry with key 1 to exist."
        );
        assert!(
            node_a_set.entries.get(&2).is_some(),
            "Expected entry with key 2 to exist."
        );
        assert!(
            node_a_set.entries.get(&3).is_none(),
            "Expected entry with key 3 to NOT exist."
        );
    }

    #[test]
    fn test_purge_delete_merge() {
        let mut node_a = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);
        let mut node_b = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 1);

        // We create our new set for node a.
        let mut node_a_set = OrSWotSet::default();

        // We add a new set of entries into our set.
        node_a_set.insert(1, node_a.send().unwrap());
        node_a_set.insert(2, node_a.send().unwrap());
        node_a_set.insert(3, node_a.send().unwrap());

        std::thread::sleep(Duration::from_millis(1));

        // We create our new state on node b's side.
        let mut node_b_set = OrSWotSet::default();

        // We add a new set of entries into our set.
        node_b_set.insert(1, node_b.send().unwrap());
        node_b_set.delete(3, node_b.send().unwrap());

        node_a_set.merge(node_b_set.clone());

        node_a_set.insert(4, node_a.send().unwrap());

        // We must observe another event from node b.
        node_b_set.insert(4, node_b.send().unwrap());
        node_a_set.merge(node_b_set.clone());

        node_a_set.purge_old_deletes();

        assert!(
            node_a_set.dead.is_empty(),
            "Expected dead entries to be empty."
        );

        assert!(
            node_a_set.entries.get(&1).is_some(),
            "Expected entry with key 1 to exist."
        );
        assert!(
            node_a_set.entries.get(&2).is_some(),
            "Expected entry with key 2 to exist."
        );
        assert!(
            node_a_set.entries.get(&3).is_none(),
            "Expected entry with key 3 to NOT exist."
        );
        assert!(
            node_a_set.entries.get(&4).is_some(),
            "Expected entry with key 4 to exist."
        );
    }

    #[test]
    fn test_purge_some_entries() {
        let mut node_a = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);
        let mut node_b = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 1);

        // We create our new set for node a.
        let mut node_a_set = OrSWotSet::default();

        // We add a new set of entries into our set.
        node_a_set.insert(1, node_a.send().unwrap());
        node_a_set.insert(2, node_a.send().unwrap());
        node_a_set.insert(3, node_a.send().unwrap());

        std::thread::sleep(Duration::from_millis(1));

        // We create our new state on node b's side.
        let mut node_b_set = OrSWotSet::default();

        // We add a new set of entries into our set.
        node_b_set.insert(1, node_b.send().unwrap());
        node_b_set.delete(3, node_b.send().unwrap());

        node_a_set.merge(node_b_set.clone());

        node_a_set.insert(4, node_a.send().unwrap());

        // Delete entry 2 from set a.
        node_a_set.delete(2, node_a.send().unwrap());

        // 'observe' a new op happening from node a.
        node_a_set.insert(5, node_a.send().unwrap());

        node_a_set.merge(node_b_set.clone());

        // We expect our deletion of key `2` to be removed, but not key `3`
        node_a_set.purge_old_deletes();

        node_b_set.merge(node_a_set.clone());
        node_a_set.purge_old_deletes();

        assert!(
            node_a_set.dead.get(&3).is_some(),
            "SET A: Expected key 3 to be left in dead set."
        );
        assert!(
            node_a_set.dead.get(&2).is_none(),
            "SET A: Expected key 2 to be purged from dead set."
        );

        assert!(
            node_a_set.entries.get(&1).is_some(),
            "SET A: Expected entry with key 1 to exist."
        );
        assert!(
            node_a_set.entries.get(&2).is_none(),
            "SET A: Expected entry with key 2 to exist."
        );
        assert!(
            node_a_set.entries.get(&3).is_none(),
            "SET A: Expected entry with key 3 to NOT exist."
        );
        assert!(
            node_a_set.entries.get(&4).is_some(),
            "SET A: Expected entry with key 4 to exist."
        );

        assert!(
            node_b_set.dead.get(&3).is_some(),
            "SET B: Expected key 3 to be left in dead set."
        );
        assert!(
            node_b_set.dead.get(&2).is_none(),
            "SET B: Expected key 2 to be purged from dead set."
        );

        assert!(
            node_b_set.entries.get(&1).is_some(),
            "SET B: Expected entry with key 1 to exist."
        );
        assert!(
            node_b_set.entries.get(&2).is_none(),
            "SET B: Expected entry with key 2 to exist."
        );
        assert!(
            node_b_set.entries.get(&3).is_none(),
            "SET B: Expected entry with key 3 to NOT exist."
        );
        assert!(
            node_b_set.entries.get(&4).is_some(),
            "SET B: Expected entry with key 4 to exist."
        );
    }

    #[test]
    fn test_insert_no_op() {
        let mut node_a = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);
        let old_ts = node_a.send().unwrap();

        // Wait a period of time to make the ts we just created 'old'
        std::thread::sleep(Duration::from_millis(200));

        // We create our new set for node a.
        let mut node_a_set = OrSWotSet::default();

        let did_add = node_a_set.insert(1, node_a.send().unwrap());
        assert!(did_add, "Expected entry insert to be added.");

        let did_add = node_a_set.insert(1, old_ts);
        assert!(
            !did_add,
            "Expected entry insert with old timestamp to be ignored"
        );
    }

    #[test]
    fn test_delete_no_op() {
        let mut node_a = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);
        let old_ts = node_a.send().unwrap();

        // Wait a period of time to make the ts we just created 'old'
        std::thread::sleep(Duration::from_millis(200));

        // We create our new set for node a.
        let mut node_a_set = OrSWotSet::default();

        let did_add = node_a_set.insert(1, node_a.send().unwrap());
        assert!(did_add, "Expected entry insert to be added.");

        let did_add = node_a_set.delete(1, old_ts);
        assert!(
            !did_add,
            "Expected entry delete with old timestamp to be ignored"
        );
    }

    #[test]
    fn test_set_diff() {
        let mut node_a = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);
        let mut node_b = HLCTimestamp::new(get_unix_timestamp_ms() + 5000, 0, 1);

        let mut node_a_set = OrSWotSet::default();
        let mut node_b_set = OrSWotSet::default();

        let insert_ts_1 = node_a.send().unwrap();
        node_a_set.insert(1, insert_ts_1);

        std::thread::sleep(Duration::from_millis(500));

        let delete_ts_3 = node_a.send().unwrap();
        node_a_set.delete(3, delete_ts_3);

        let insert_ts_2 = node_b.send().unwrap();
        node_b_set.insert(2, insert_ts_2);

        let (changed, removed) = node_a_set.diff(&node_b_set);

        assert_eq!(
            changed,
            vec![(2, insert_ts_2)],
            "Expected set a to only be marked as missing key `2`"
        );
        assert!(
            removed.is_empty(),
            "Expected set a to not be missing any delete markers."
        );

        let (changed, removed) = node_b_set.diff(&node_a_set);
        assert_eq!(
            changed,
            vec![(1, insert_ts_1)],
            "Expected set b to have key `1` marked as changed."
        );
        assert_eq!(
            removed,
            vec![(3, delete_ts_3)],
            "Expected set b to have key `3` marked as deleted."
        );
    }

    #[test]
    fn test_set_diff_with_conflicts() {
        let mut node_a = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);
        let mut node_b = HLCTimestamp::new(get_unix_timestamp_ms() + 5000, 0, 1);

        let mut node_a_set = OrSWotSet::default();
        let mut node_b_set = OrSWotSet::default();

        // This should get overriden by node b.
        node_a_set.insert(1, node_a.send().unwrap());
        node_a_set.insert(2, node_a.send().unwrap());

        std::thread::sleep(Duration::from_millis(500));

        let delete_ts_3 = node_a.send().unwrap();
        node_a_set.delete(3, delete_ts_3);

        let insert_ts_2 = node_b.send().unwrap();
        node_b_set.insert(2, insert_ts_2);

        let insert_ts_1 = node_b.send().unwrap();
        node_b_set.insert(1, insert_ts_1);

        let (changed, removed) = node_a_set.diff(&node_b_set);

        assert_eq!(
            changed,
            vec![(2, insert_ts_2), (1, insert_ts_1)],
            "Expected set a to be marked as updating keys `1, 2`"
        );
        assert!(
            removed.is_empty(),
            "Expected set a to not be missing any delete markers."
        );

        let (changed, removed) = node_b_set.diff(&node_a_set);
        assert_eq!(
            changed,
            vec![],
            "Expected set b to have no changed keys marked."
        );
        assert_eq!(
            removed,
            vec![(3, delete_ts_3)],
            "Expected set b to have key `3` marked as deleted."
        );
    }
}
