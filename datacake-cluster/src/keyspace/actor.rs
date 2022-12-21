use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;

use crossbeam_utils::atomic::AtomicCell;
use datacake_crdt::{HLCTimestamp, OrSWotSet, StateChanges};
use puppet::{puppet_actor, ActorMailbox};
use datacake_node::Clock;

use super::messages::{Del, Diff, MultiDel, MultiSet, Set, SymDiff};
use crate::keyspace::messages::{CorruptedState, PurgeDeletes, Serialize, NUM_SOURCES};
use crate::keyspace::LastUpdated;
use crate::storage::BulkMutationError;
use crate::Storage;

/// Spawns a new keyspace actor, returning the actor's mailbox.
pub async fn spawn_keyspace<S>(
    name: Cow<'static, str>,
    storage: Arc<S>,
    clock: Clock,
    state: OrSWotSet<NUM_SOURCES>,
    change_timestamp: Arc<AtomicCell<HLCTimestamp>>,
) -> ActorMailbox<KeyspaceActor<S>>
where
    S: Storage + Send + Sync + 'static,
{
    let ks = KeyspaceActor {
        name: name.clone(),
        clock,
        storage,
        state,
        change_timestamp,
    };

    ks.spawn_actor_with_name(name).await
}

pub struct KeyspaceActor<S>
where
    S: Storage + Send + Sync + 'static,
{
    name: Cow<'static, str>,
    clock: Clock,
    storage: Arc<S>,
    state: OrSWotSet<NUM_SOURCES>,
    change_timestamp: Arc<AtomicCell<HLCTimestamp>>,
}

#[puppet_actor]
impl<S> KeyspaceActor<S>
where
    S: Storage + Send + Sync + 'static,
{
    async fn inc_change_timestamp(&self) {
        let ts = self.clock.get_time().await;
        self.change_timestamp.store(ts);
    }

    #[puppet]
    /// Sets a document value in the store.
    ///
    /// If the document is not the newest the store has seen thus far, it is a no-op.
    async fn on_set(&mut self, msg: Set<S>) -> Result<(), S::Error> {
        // We have something newer.
        if !self.state.will_apply(msg.doc.id, msg.doc.last_updated) {
            return Ok(());
        }

        let doc_id = msg.doc.id;
        let ts = msg.doc.last_updated;

        self.storage
            .put_with_ctx(&self.name, msg.doc, msg.ctx.as_ref())
            .await?;

        // The change has gone through, let's apply our memory state.
        self.state.insert_with_source(msg.source, doc_id, ts);
        self.inc_change_timestamp().await;
        Ok(())
    }

    #[puppet]
    /// Sets several documents value in the store.
    ///
    /// If a document is not the newest the store has seen thus far, it is a no-op.
    async fn on_multi_set(
        &mut self,
        msg: MultiSet<S>,
    ) -> Result<(), BulkMutationError<S::Error>> {
        let mut valid_entries = Vec::with_capacity(msg.docs.len());

        // Only select docs to be inserted if they're able to be applied.
        let docs = msg
            .docs
            .into_iter()
            .filter(|doc| self.state.will_apply(doc.id, doc.last_updated))
            .map(|doc| {
                valid_entries.push((doc.id, doc.last_updated));
                doc
            });

        let res = self
            .storage
            .multi_put_with_ctx(&self.name, docs, msg.ctx.as_ref())
            .await;

        // Ensure the insertion order into the set is correct.
        valid_entries.sort_by_key(|entry| entry.1);
        self.inc_change_timestamp().await;

        if let Err(error) = res {
            let successful_ids = HashSet::<_>::from_iter(error.successful_doc_ids());
            let successful_entries = valid_entries
                .into_iter()
                .filter(|entry| successful_ids.contains(&entry.0));

            for (doc_id, ts) in successful_entries {
                self.state.insert_with_source(msg.source, doc_id, ts);
            }
            Err(error)
        } else {
            for (doc_id, ts) in valid_entries {
                self.state.insert_with_source(msg.source, doc_id, ts);
            }
            Ok(())
        }
    }

    #[puppet]
    /// Removes a document value in the store.
    ///
    /// If the document is not the newest the store has seen thus far, it is a no-op.
    async fn on_del(&mut self, msg: Del<S>) -> Result<(), S::Error> {
        // We have something newer.
        if !self.state.will_apply(msg.doc_id, msg.ts) {
            return Ok(());
        }

        self.storage
            .mark_as_tombstone(&self.name, msg.doc_id, msg.ts)
            .await?;

        // The change has gone through, let's apply our memory state.
        self.state
            .delete_with_source(msg.source, msg.doc_id, msg.ts);
        self.inc_change_timestamp().await;
        Ok(())
    }

    #[puppet]
    /// Removes several documents value in the store.
    ///
    /// If a document is not the newest the store has seen thus far, it is a no-op.
    async fn on_multi_del(
        &mut self,
        msg: MultiDel<S>,
    ) -> Result<(), BulkMutationError<S::Error>> {
        let mut valid_entries = Vec::with_capacity(msg.key_ts_pairs.len());

        // Only select docs to be inserted if they're able to be applied.
        let docs = msg
            .key_ts_pairs
            .into_iter()
            .filter(|doc| self.state.will_apply(doc.0, doc.1))
            .map(|doc| {
                valid_entries.push((doc.0, doc.1));
                doc
            });

        let res = self.storage.mark_many_as_tombstone(&self.name, docs).await;

        // Ensure the insertion order into the set is correct.
        valid_entries.sort_by_key(|entry| entry.1);
        self.inc_change_timestamp().await;

        if let Err(error) = res {
            let successful_ids = HashSet::<_>::from_iter(error.successful_doc_ids());
            let successful_entries = valid_entries
                .into_iter()
                .filter(|entry| successful_ids.contains(&entry.0));

            for (doc_id, ts) in successful_entries {
                self.state.delete_with_source(msg.source, doc_id, ts);
            }
            Err(error)
        } else {
            for (doc_id, ts) in valid_entries {
                self.state.delete_with_source(msg.source, doc_id, ts);
            }
            Ok(())
        }
    }

    #[puppet]
    async fn on_purge_tombstones(
        &mut self,
        _msg: PurgeDeletes<S>,
    ) -> Result<(), S::Error> {
        let changes = self.state.purge_old_deletes();

        let res = self
            .storage
            .remove_tombstones(&self.name, changes.iter().map(|(key, _)| *key))
            .await;

        // The operation may have been partially successful. We should re-mark
        // any tombstones which weren't successful.
        if let Err(error) = res {
            let tombstones = changes
                .into_iter()
                .filter(|(key, _)| !error.successful_doc_ids.contains(key))
                .collect();

            self.state.add_raw_tombstones(tombstones);

            return Err(error.inner);
        }

        Ok(())
    }

    #[puppet]
    async fn on_diff(&self, msg: Diff) -> (StateChanges, StateChanges) {
        self.state.diff(&msg.0)
    }

    #[puppet]
    async fn on_sym_diff(&self, msg: SymDiff) -> (StateChanges, StateChanges) {
        let (change_left, removal_left) = self.state.diff(&msg.0);
        let (change_right, removal_right) = msg.0.diff(&self.state);

        let changes = change_left.into_iter().chain(change_right).collect();
        let removals = removal_left.into_iter().chain(removal_right).collect();

        (changes, removals)
    }

    #[puppet]
    async fn on_serialize(&self, _msg: Serialize) -> Result<Vec<u8>, CorruptedState> {
        rkyv::to_bytes::<_, 4096>(&self.state)
            .map(|buf| buf.into_vec())
            .map_err(|_| CorruptedState)
    }

    #[puppet]
    async fn on_last_updated(&self, _msg: LastUpdated) -> HLCTimestamp {
        self.change_timestamp.load()
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Reverse;

    use datacake_crdt::get_unix_timestamp_ms;

    use super::*;
    use crate::test_utils::MockStorage;
    use crate::Document;

    async fn make_actor(
        clock: Clock,
        storage: MockStorage,
    ) -> KeyspaceActor<MockStorage> {
        let ts = clock.get_time().await;
        KeyspaceActor {
            name: Cow::Borrowed("my-keyspace"),
            clock,
            storage: Arc::new(storage),
            state: OrSWotSet::default(),
            change_timestamp: Arc::new(AtomicCell::new(ts)),
        }
    }

    #[tokio::test]
    async fn test_on_set() {
        let clock = Clock::new(0);

        let doc_1 = Document::new(1, clock.get_time().await, b"Hello, world 1".to_vec());
        let doc_2 = Document::new(2, clock.get_time().await, b"Hello, world 2".to_vec());
        let doc_3 = Document::new(3, clock.get_time().await, b"Hello, world 3".to_vec());

        let docs = [doc_1.clone(), doc_2.clone(), doc_3.clone()];

        let mock_store =
            MockStorage::default().expect_put_with_ctx(3, move |keyspace, doc, ctx| {
                assert_eq!(keyspace, "my-keyspace");
                assert!(ctx.is_none());
                assert!(docs.contains(&doc));

                Ok(())
            });

        let mut keyspace = make_actor(clock, mock_store).await;

        keyspace
            .on_set(Set {
                source: 0,
                doc: doc_1,
                ctx: None,
                _marker: Default::default(),
            })
            .await
            .expect("Put operation should be successful.");
        keyspace
            .on_set(Set {
                source: 1,
                doc: doc_2,
                ctx: None,
                _marker: Default::default(),
            })
            .await
            .expect("Put operation should be successful.");
        keyspace
            .on_set(Set {
                source: 0,
                doc: doc_3,
                ctx: None,
                _marker: Default::default(),
            })
            .await
            .expect("Put operation should be successful.");
    }

    #[tokio::test]
    async fn test_on_set_old_ts() {
        let clock = Clock::new(0);

        let old_ts = HLCTimestamp::new(get_unix_timestamp_ms() - 3_700_000, 0, 0);
        let doc_1 = Document::new(1, clock.get_time().await, b"Hello, world 1".to_vec());
        let doc_2 = Document::new(2, clock.get_time().await, b"Hello, world 2".to_vec());
        let doc_3 = Document::new(3, old_ts, b"Hello, world 3".to_vec());

        let docs = [doc_1.clone(), doc_2.clone()];

        let mock_store =
            MockStorage::default().expect_put_with_ctx(2, move |keyspace, doc, ctx| {
                assert_eq!(keyspace, "my-keyspace");
                assert!(ctx.is_none());
                assert!(docs.contains(&doc));

                Ok(())
            });

        let mut keyspace = make_actor(clock, mock_store).await;

        keyspace
            .on_set(Set {
                source: 0,
                doc: doc_1,
                ctx: None,
                _marker: Default::default(),
            })
            .await
            .expect("Put operation should be successful.");

        // We use source `1` here so it advances our minimum safe TS.
        keyspace
            .on_set(Set {
                source: 1,
                doc: doc_2,
                ctx: None,
                _marker: Default::default(),
            })
            .await
            .expect("Put operation should be successful.");

        // Doc 3 should return OK by not be stored.
        keyspace
            .on_set(Set {
                source: 0,
                doc: doc_3,
                ctx: None,
                _marker: Default::default(),
            })
            .await
            .expect("Put operation should be successful.");
    }

    #[tokio::test]
    async fn test_on_multi_set() {
        let clock = Clock::new(0);

        let doc_1 = Document::new(1, clock.get_time().await, b"Hello, world 1".to_vec());
        let doc_2 = Document::new(2, clock.get_time().await, b"Hello, world 2".to_vec());
        let doc_3 = Document::new(3, clock.get_time().await, b"Hello, world 3".to_vec());

        let docs = [doc_1.clone(), doc_2.clone(), doc_3.clone()];

        let mock_store = MockStorage::default().expect_multi_put_with_ctx(
            1,
            move |keyspace, mut docs_iter, ctx| {
                assert_eq!(keyspace, "my-keyspace");
                assert!(ctx.is_none());
                assert!(docs_iter.all(|doc| docs.contains(&doc)));

                Ok(())
            },
        );

        let mut keyspace = make_actor(clock, mock_store).await;
        keyspace
            .on_multi_set(MultiSet {
                source: 0,
                docs: vec![doc_1.clone(), doc_2.clone(), doc_3.clone()],
                ctx: None,
                _marker: Default::default(),
            })
            .await
            .expect("Put operation should be successful.");
    }

    #[tokio::test]
    async fn test_on_multi_set_old_ts() {
        let clock = Clock::new(0);

        let old_ts = HLCTimestamp::new(get_unix_timestamp_ms() - 3_700_000, 0, 0);
        let doc_1 = Document::new(1, clock.get_time().await, b"Hello, world 1".to_vec());
        let doc_2 = Document::new(2, clock.get_time().await, b"Hello, world 2".to_vec());
        let doc_3 = Document::new(3, clock.get_time().await, b"Hello, world 3".to_vec());
        let doc_4 = Document::new(4, old_ts, b"Hello, world 4".to_vec());

        let docs = [doc_2.clone()];

        let mock_store = MockStorage::default()
            .expect_multi_put_with_ctx(1, move |keyspace, mut docs_iter, ctx| {
                assert_eq!(keyspace, "my-keyspace");
                assert!(ctx.is_none());
                assert!(docs_iter.all(|doc| docs.contains(&doc)));

                Ok(())
            })
            .expect_put_with_ctx(2, move |keyspace, doc, ctx| {
                assert_eq!(keyspace, "my-keyspace");
                assert!(ctx.is_none());
                assert!(doc.id == doc_3.id || doc.id == doc_1.id);

                Ok(())
            });

        let mut keyspace = make_actor(clock, mock_store).await;
        keyspace
            .on_set(Set {
                source: 1,
                doc: doc_3,
                ctx: None,
                _marker: Default::default(),
            })
            .await
            .expect("Put operation should be successful.");
        keyspace
            .on_set(Set {
                source: 0,
                doc: doc_1,
                ctx: None,
                _marker: Default::default(),
            })
            .await
            .expect("Put operation should be successful.");

        // Doc 4 should be ignored as it's too old past the set's forgiveness period.
        keyspace
            .on_multi_set(MultiSet {
                source: 0,
                docs: vec![doc_2.clone(), doc_4.clone()],
                ctx: None,
                _marker: Default::default(),
            })
            .await
            .expect("Put operation should be successful.");
    }

    #[tokio::test]
    async fn test_on_multi_set_unordered_events() {
        let clock = Clock::new(0);

        let old_ts = HLCTimestamp::new(get_unix_timestamp_ms() - 3_700_000, 0, 0);
        let doc_1 = Document::new(1, clock.get_time().await, b"Hello, world 1".to_vec());
        let doc_2 = Document::new(2, clock.get_time().await, b"Hello, world 2".to_vec());
        let doc_3 = Document::new(3, clock.get_time().await, b"Hello, world 3".to_vec());
        let doc_4 = Document::new(4, old_ts, b"Hello, world 4".to_vec());

        let docs = [doc_4.clone(), doc_2.clone(), doc_1.clone(), doc_3.clone()];

        let mock_store = MockStorage::default().expect_multi_put_with_ctx(
            1,
            move |keyspace, mut docs_iter, ctx| {
                assert_eq!(keyspace, "my-keyspace");
                assert!(ctx.is_none());
                assert!(docs_iter.all(|doc| docs.contains(&doc)));

                Ok(())
            },
        );

        let mut keyspace = make_actor(clock, mock_store).await;

        // All docs should be stored as their timestamps should be re-ordered.
        keyspace
            .on_multi_set(MultiSet {
                source: 0,
                docs: vec![doc_4.clone(), doc_2.clone(), doc_1.clone(), doc_3.clone()],
                ctx: None,
                _marker: Default::default(),
            })
            .await
            .expect("Put operation should be successful.");

        assert!(keyspace.state.get(&doc_1.id).is_some());
        assert!(keyspace.state.get(&doc_2.id).is_some());
        assert!(keyspace.state.get(&doc_3.id).is_some());
        assert!(keyspace.state.get(&doc_4.id).is_some());
    }

    #[tokio::test]
    async fn test_on_del() {
        let clock = Clock::new(0);

        let doc_1 = Document::new(1, clock.get_time().await, b"Hello, world 1".to_vec());
        let doc_2 = Document::new(2, clock.get_time().await, b"Hello, world 2".to_vec());
        let doc_3 = Document::new(3, clock.get_time().await, b"Hello, world 3".to_vec());

        let docs = [doc_1.clone(), doc_2.clone(), doc_3.clone()];

        let delete_ts = clock.get_time().await;
        let mock_store = MockStorage::default()
            .expect_multi_put_with_ctx(1, move |keyspace, mut docs_iter, ctx| {
                assert_eq!(keyspace, "my-keyspace");
                assert!(ctx.is_none());
                assert!(docs_iter.all(|doc| docs.contains(&doc)));

                Ok(())
            })
            .expect_mark_as_tombstone(1, move |keyspace, doc_id, ts| {
                assert_eq!(keyspace, "my-keyspace");
                assert_eq!(doc_id, doc_1.id);
                assert_eq!(ts, delete_ts);

                Ok(())
            });

        let mut keyspace = make_actor(clock, mock_store).await;

        keyspace
            .on_multi_set(MultiSet {
                source: 0,
                docs: vec![doc_1.clone(), doc_2.clone(), doc_3.clone()],
                ctx: None,
                _marker: Default::default(),
            })
            .await
            .expect("Put operation should be successful.");
        keyspace
            .on_del(Del {
                source: 0,
                doc_id: doc_1.id,
                ts: delete_ts,
                _marker: Default::default(),
            })
            .await
            .expect("Put operation should be successful.");
    }

    #[tokio::test]
    async fn test_on_multi_del() {
        let clock = Clock::new(0);

        let doc_1 = Document::new(1, clock.get_time().await, b"Hello, world 1".to_vec());
        let doc_2 = Document::new(2, clock.get_time().await, b"Hello, world 2".to_vec());
        let doc_3 = Document::new(3, clock.get_time().await, b"Hello, world 3".to_vec());

        let doc_ids = [doc_1.id, doc_2.id];
        let docs = [doc_1.clone(), doc_2.clone(), doc_3.clone()];

        let mock_store = MockStorage::default()
            .expect_multi_put_with_ctx(1, move |keyspace, mut docs_iter, ctx| {
                assert_eq!(keyspace, "my-keyspace");
                assert!(ctx.is_none());
                assert!(docs_iter.all(|doc| docs.contains(&doc)));

                Ok(())
            })
            .expect_mark_many_as_tombstone(1, move |keyspace, mut docs| {
                assert_eq!(keyspace, "my-keyspace");
                assert!(docs.all(|doc| doc_ids.contains(&doc.0)));

                Ok(())
            });

        let mut keyspace = make_actor(clock.clone(), mock_store).await;

        keyspace
            .on_multi_set(MultiSet {
                source: 0,
                docs: vec![doc_1.clone(), doc_2.clone(), doc_3.clone()],
                ctx: None,
                _marker: Default::default(),
            })
            .await
            .expect("Put operation should be successful.");
        keyspace
            .on_multi_del(MultiDel {
                source: 0,
                key_ts_pairs: vec![
                    (doc_1.id, clock.get_time().await),
                    (doc_2.id, clock.get_time().await),
                ],
                _marker: Default::default(),
            })
            .await
            .expect("Put operation should be successful.");
    }

    #[tokio::test]
    async fn test_on_del_unordered_events() {
        let clock = Clock::new(0);

        let doc_1 = Document::new(1, clock.get_time().await, b"Hello, world 1".to_vec());
        let doc_2 = Document::new(2, clock.get_time().await, b"Hello, world 2".to_vec());
        let doc_3 = Document::new(3, clock.get_time().await, b"Hello, world 3".to_vec());

        let docs = [doc_1.clone(), doc_2.clone(), doc_3.clone()];

        let deletes_expected = vec![
            (doc_3.id, clock.get_time().await),
            (doc_1.id, clock.get_time().await),
        ];

        let deletes_expected_clone = deletes_expected.clone();
        let mock_store = MockStorage::default()
            .expect_multi_put_with_ctx(1, move |keyspace, mut docs_iter, ctx| {
                assert_eq!(keyspace, "my-keyspace");
                assert!(ctx.is_none());
                assert!(docs_iter.all(|doc| docs.contains(&doc)));

                Ok(())
            })
            .expect_mark_many_as_tombstone(1, move |keyspace, mut docs_iter| {
                assert_eq!(keyspace, "my-keyspace");
                assert!(docs_iter.all(|doc| deletes_expected_clone.contains(&doc)));

                Ok(())
            });

        let mut keyspace = make_actor(clock, mock_store).await;

        keyspace
            .on_multi_set(MultiSet {
                source: 0,
                docs: vec![doc_1.clone(), doc_2.clone(), doc_3.clone()],
                ctx: None,
                _marker: Default::default(),
            })
            .await
            .expect("Put operation should be successful.");
        keyspace
            .on_multi_del(MultiDel {
                source: 0,
                key_ts_pairs: deletes_expected.clone(),
                _marker: Default::default(),
            })
            .await
            .expect("Put operation should be successful.");

        assert!(keyspace.state.get(&doc_2.id).is_some());
        assert!(!keyspace.state.delete(doc_1.id, doc_1.last_updated));
        assert!(!keyspace.state.delete(doc_3.id, doc_3.last_updated));

        // Push the safe timestamp forwards.
        keyspace.state.insert_with_source(
            1,
            5,
            HLCTimestamp::new(get_unix_timestamp_ms() + 3_700_000, 0, 0),
        );
        keyspace.state.insert_with_source(
            0,
            2,
            HLCTimestamp::new(get_unix_timestamp_ms() + 3_700_000, 1, 0),
        );

        let mut changes = keyspace.state.purge_old_deletes();
        // Needed because it may not be ordered.
        changes.sort_by_key(|change| Reverse(change.0));
        assert_eq!(changes, deletes_expected);
    }
}
