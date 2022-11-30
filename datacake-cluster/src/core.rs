use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};

use bytes::Bytes;
use datacake_crdt::{HLCTimestamp, Key};

use crate::keyspace::StateSource;
use crate::rpc::datacake_api;
use crate::{error, KeyspaceGroup, PutContext, Storage};

#[derive(Clone)]
pub struct Document {
    /// The unique id of the document.
    pub id: Key,

    /// The timestamp of when the document was last updated.
    pub last_updated: HLCTimestamp,

    /// The raw binary data of the document's value.
    pub data: Bytes,
}

impl Document {
    /// A convenience method for passing data values which can be sent as bytes.
    pub fn new(id: Key, last_updated: HLCTimestamp, data: impl Into<Bytes>) -> Self {
        Self {
            id,
            last_updated,
            data: data.into(),
        }
    }
}

impl Eq for Document {}

impl PartialEq for Document {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.last_updated == other.last_updated
            && self.data == other.data
    }
}

impl Hash for Document {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}

impl Debug for Document {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut f = f.debug_struct("Document");
        f.field("id", &self.id);
        f.field("last_updated", &self.last_updated);

        #[cfg(any(test, feature = "test-utils"))]
        {
            f.field("data", &self.data);
        }

        f.finish()
    }
}

impl From<datacake_api::Document> for Document {
    fn from(doc: datacake_api::Document) -> Self {
        let metadata = doc.metadata.unwrap();
        let last_updated = metadata.last_updated.unwrap();
        Self {
            id: metadata.id,
            last_updated: last_updated.into(),
            data: doc.data,
        }
    }
}

impl From<Document> for datacake_api::Document {
    fn from(doc: Document) -> Self {
        Self {
            metadata: Some(datacake_api::DocumentMetadata {
                id: doc.id,
                last_updated: Some(doc.last_updated.into()),
            }),
            data: doc.data,
        }
    }
}

impl From<datacake_api::Timestamp> for HLCTimestamp {
    fn from(ts: datacake_api::Timestamp) -> Self {
        Self::new(ts.millis, ts.counter as u16, ts.node_id)
    }
}

impl From<HLCTimestamp> for datacake_api::Timestamp {
    fn from(ts: HLCTimestamp) -> Self {
        Self {
            millis: ts.millis(),
            counter: ts.counter() as u32,
            node_id: ts.node(),
        }
    }
}

/// Inserts or updates the given keyspace locally and updates the given keyspace state.
pub(crate) async fn put_data<SS, S>(
    keyspace: &str,
    document: Document,
    group: &KeyspaceGroup<S>,
    ctx: Option<&PutContext>,
) -> Result<(), error::DatacakeError<S::Error>>
where
    SS: StateSource + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    let doc_id = document.id;
    let last_updated = document.last_updated;

    group
        .storage()
        .put_with_ctx(keyspace, document, ctx)
        .await?;

    let keyspace = group.get_or_create_keyspace(keyspace).await;

    keyspace.put::<SS>(doc_id, last_updated).await;

    Ok(())
}

/// Inserts or updates many entries in the given keyspace
/// locally and updates the given keyspace state.
pub(crate) async fn put_many_data<SS, S>(
    keyspace: &str,
    documents: impl Iterator<Item = Document> + Send,
    group: &KeyspaceGroup<S>,
    ctx: Option<&PutContext>,
) -> Result<(), error::DatacakeError<S::Error>>
where
    SS: StateSource + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    let mut entries = Vec::new();
    let documents = documents.map(|doc| {
        entries.push((doc.id, doc.last_updated));
        doc
    });

    let res = group
        .storage()
        .multi_put_with_ctx(keyspace, documents, ctx)
        .await;

    let keyspace = group.get_or_create_keyspace(keyspace).await;
    entries.sort_by_key(|entry| entry.1);

    if let Err(error) = res {
        let entries = entries
            .into_iter()
            .filter(|(key, _)| error.successful_doc_ids.contains(key))
            .collect();

        keyspace.multi_put::<SS>(entries).await;
        return Err(error::DatacakeError::StorageError(error.inner));
    }

    keyspace.multi_put::<SS>(entries).await;

    Ok(())
}

/// Removes a document from the local keyspace and updates the given keyspace state.
pub(crate) async fn del_data<SS, S>(
    keyspace: &str,
    doc_id: Key,
    last_updated: HLCTimestamp,
    group: &KeyspaceGroup<S>,
) -> Result<(), error::DatacakeError<S::Error>>
where
    SS: StateSource + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    let keyspace = group.get_or_create_keyspace(keyspace).await;
    if let Some(timestamp) = keyspace.get(doc_id).await {
        if last_updated <= timestamp {
            return Ok(());
        }
    }

    group
        .storage()
        .mark_as_tombstone(keyspace.name(), doc_id, last_updated)
        .await?;

    keyspace.del::<SS>(doc_id, last_updated).await;

    Ok(())
}

/// Removes multiple documents from the local keyspace and updates the given keyspace state.
pub(crate) async fn del_many_data<SS, S>(
    keyspace: &str,
    documents: impl Iterator<Item = (Key, HLCTimestamp)> + Send,
    group: &KeyspaceGroup<S>,
) -> Result<(), error::DatacakeError<S::Error>>
where
    SS: StateSource + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    let mut entries = Vec::new();
    let documents = documents
        .map(|doc| {
            entries.push(doc);
            doc.0
        })
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.1);

    let keyspace = group.get_or_create_keyspace(keyspace).await;
    let timestamps = keyspace.get_many(documents.clone()).await;

    let valid_updates = entries
        .iter()
        .filter(|(key, last_updated)| {
            if let Some(timestamp) = timestamps.get(key) {
                if last_updated <= timestamp {
                    return false;
                }
            }

            true
        })
        .copied();

    let res = group
        .storage()
        .mark_many_as_tombstone(keyspace.name(), valid_updates)
        .await;

    if let Err(error) = res {
        let entries = entries
            .into_iter()
            .filter(|(key, _)| error.successful_doc_ids.contains(key))
            .collect();

        keyspace.multi_del::<SS>(entries).await;
        return Err(error::DatacakeError::StorageError(error.inner));
    }

    keyspace.multi_del::<SS>(entries).await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use datacake_crdt::OrSWotSet;

    use super::*;
    use crate::storage::mem_store::MemStore;

    #[derive(Debug, Copy, Clone)]
    pub struct TestSource;

    impl StateSource for TestSource {
        fn source_id() -> usize {
            0
        }
    }

    #[tokio::test]
    async fn test_put_data() {
        static KEYSPACE: &str = "put-data-keyspace";
        let group = KeyspaceGroup::<MemStore>::new_for_test().await;
        let clock = group.clock();

        // Test insert
        let doc_ts = clock.get_time().await;
        let doc = Document::new(1, doc_ts, b"hello, world".to_vec());
        put_data::<TestSource, _>(KEYSPACE, doc.clone(), &group, None)
            .await
            .expect("Put new data should be successful.");

        let keyspace = group.get_or_create_keyspace(KEYSPACE).await;
        let (additions, removals) = keyspace.symetrical_diff(OrSWotSet::default()).await;
        assert_eq!(
            additions,
            vec![(1, doc_ts)],
            "State additions should match."
        );
        assert_eq!(removals, Vec::new(), "State removals should match.");

        let mut new_state = OrSWotSet::default();
        new_state.insert(1, doc_ts);
        let (additions, removals) = keyspace.symetrical_diff(new_state.clone()).await;
        assert!(additions.is_empty(), "State should be inline.");
        assert!(removals.is_empty(), "State should be inline.");

        let stored_doc = keyspace
            .storage()
            .get(KEYSPACE, 1)
            .await
            .expect("get doc")
            .expect("Document should not be `None`");
        assert_eq!(stored_doc, doc, "Stored documents should match.");

        // Test updating an existing document.
        let updated_doc_ts = clock.get_time().await;
        let updated_doc = Document::new(1, updated_doc_ts, b"hello, world".to_vec());
        put_data::<TestSource, _>(KEYSPACE, updated_doc.clone(), &group, None)
            .await
            .expect("Put new data should be successful.");

        let keyspace = group.get_or_create_keyspace(KEYSPACE).await;
        let (additions, removals) = keyspace.symetrical_diff(OrSWotSet::default()).await;
        assert_eq!(
            additions,
            vec![(1, updated_doc_ts)],
            "State additions should match."
        );
        assert_eq!(removals, Vec::new(), "State removals should match.");

        let (additions, removals) = keyspace.symetrical_diff(new_state).await;
        assert_eq!(
            additions,
            vec![(1, updated_doc_ts)],
            "The state should have been updated with the updated doc timestamp."
        );
        assert!(removals.is_empty(), "State should be inline.");

        let stored_doc = keyspace
            .storage()
            .get(KEYSPACE, 1)
            .await
            .expect("get doc")
            .expect("Document should not be `None`");
        assert_eq!(stored_doc, updated_doc, "Stored documents should match.");

        // Test inserting and updating.
        let doc_1 = Document::new(
            1,
            clock.get_time().await,
            b"hello, world from doc 1".to_vec(),
        );
        let doc_2 = Document::new(
            2,
            clock.get_time().await,
            b"hello, world from doc 2".to_vec(),
        );
        let doc_3 = Document::new(
            3,
            clock.get_time().await,
            b"hello, world from doc 3".to_vec(),
        );
        put_data::<TestSource, _>(KEYSPACE, doc_1.clone(), &group, None)
            .await
            .expect("Put new data should be successful.");
        put_data::<TestSource, _>(KEYSPACE, doc_2.clone(), &group, None)
            .await
            .expect("Put new data should be successful.");
        put_data::<TestSource, _>(KEYSPACE, doc_3.clone(), &group, None)
            .await
            .expect("Put new data should be successful.");

        let keyspace = group.get_or_create_keyspace(KEYSPACE).await;
        let (additions, removals) = keyspace.symetrical_diff(OrSWotSet::default()).await;
        assert_eq!(
            additions,
            vec![
                (1, doc_1.last_updated),
                (2, doc_2.last_updated),
                (3, doc_3.last_updated),
            ],
            "State additions should match.",
        );
        assert_eq!(removals, Vec::new(), "State removals should match.");
    }

    #[tokio::test]
    async fn test_put_many_data() {
        static KEYSPACE: &str = "put-many-data-keyspace";
        let group = KeyspaceGroup::<MemStore>::new_for_test().await;
        let clock = group.clock();

        // Test insert
        let doc_ts = clock.get_time().await;
        let doc = Document::new(1, doc_ts, b"hello, world".to_vec());
        put_many_data::<TestSource, _>(
            KEYSPACE,
            vec![doc.clone()].into_iter(),
            &group,
            None,
        )
        .await
        .expect("Put new data should be successful.");

        let keyspace = group.get_or_create_keyspace(KEYSPACE).await;
        let (additions, removals) = keyspace.symetrical_diff(OrSWotSet::default()).await;
        assert_eq!(
            additions,
            vec![(1, doc_ts)],
            "State additions should match."
        );
        assert_eq!(removals, Vec::new(), "State removals should match.");

        let mut new_state = OrSWotSet::default();
        new_state.insert(1, doc_ts);
        let (additions, removals) = keyspace.symetrical_diff(new_state.clone()).await;
        assert!(additions.is_empty(), "State should be inline.");
        assert!(removals.is_empty(), "State should be inline.");

        let stored_doc = keyspace
            .storage()
            .get(KEYSPACE, 1)
            .await
            .expect("get doc")
            .expect("Document should not be `None`");
        assert_eq!(stored_doc, doc, "Stored documents should match.");

        // Test updating an existing document.
        let updated_doc_ts = clock.get_time().await;
        let updated_doc = Document::new(1, updated_doc_ts, b"hello, world".to_vec());
        put_many_data::<TestSource, _>(
            KEYSPACE,
            vec![updated_doc.clone()].into_iter(),
            &group,
            None,
        )
        .await
        .expect("Put new data should be successful.");

        let keyspace = group.get_or_create_keyspace(KEYSPACE).await;
        let (additions, removals) = keyspace.symetrical_diff(OrSWotSet::default()).await;
        assert_eq!(
            additions,
            vec![(1, updated_doc_ts)],
            "State additions should match."
        );
        assert_eq!(removals, Vec::new(), "State removals should match.");

        let (additions, removals) = keyspace.symetrical_diff(new_state).await;
        assert_eq!(
            additions,
            vec![(1, updated_doc_ts)],
            "The state should have been updated with the updated doc timestamp."
        );
        assert!(removals.is_empty(), "State should be inline.");

        let stored_doc = keyspace
            .storage()
            .get(KEYSPACE, 1)
            .await
            .expect("get doc")
            .expect("Document should not be `None`");
        assert_eq!(stored_doc, updated_doc, "Stored documents should match.");

        // Test inserting and updating.
        let doc_1 = Document::new(
            1,
            clock.get_time().await,
            b"hello, world from doc 1".to_vec(),
        );
        let doc_2 = Document::new(
            2,
            clock.get_time().await,
            b"hello, world from doc 2".to_vec(),
        );
        let doc_3 = Document::new(
            3,
            clock.get_time().await,
            b"hello, world from doc 3".to_vec(),
        );
        put_many_data::<TestSource, _>(
            KEYSPACE,
            vec![doc_1.clone(), doc_2.clone(), doc_3.clone()].into_iter(),
            &group,
            None,
        )
        .await
        .expect("Put new data should be successful.");

        let keyspace = group.get_or_create_keyspace(KEYSPACE).await;
        let (additions, removals) = keyspace.symetrical_diff(OrSWotSet::default()).await;
        assert_eq!(
            additions,
            vec![
                (1, doc_1.last_updated),
                (2, doc_2.last_updated),
                (3, doc_3.last_updated),
            ],
            "State additions should match.",
        );
        assert_eq!(removals, Vec::new(), "State removals should match.");
    }

    #[tokio::test]
    async fn test_del_data() {
        static KEYSPACE: &str = "del-data-keyspace";
        let group = KeyspaceGroup::<MemStore>::new_for_test().await;
        let clock = group.clock();

        let doc_1 = Document::new(
            1,
            clock.get_time().await,
            b"hello, world from doc 1".to_vec(),
        );
        let doc_2 = Document::new(
            2,
            clock.get_time().await,
            b"hello, world from doc 2".to_vec(),
        );
        let doc_3 = Document::new(
            3,
            clock.get_time().await,
            b"hello, world from doc 3".to_vec(),
        );
        put_many_data::<TestSource, _>(
            KEYSPACE,
            vec![doc_1.clone(), doc_2.clone(), doc_3.clone()].into_iter(),
            &group,
            None,
        )
        .await
        .expect("Put new data should be successful.");

        // The new timestamp is newer than the insert so this should be successful.
        let delete_timestamp = clock.get_time().await;
        del_data::<TestSource, _>(KEYSPACE, doc_1.id, delete_timestamp, &group)
            .await
            .expect("Delete doc.");

        let keyspace = group.get_or_create_keyspace(KEYSPACE).await;
        let (additions, removals) = keyspace.symetrical_diff(OrSWotSet::default()).await;
        assert_eq!(
            additions,
            vec![
                (doc_2.id, doc_2.last_updated),
                (doc_3.id, doc_3.last_updated),
            ],
            "State additions should match.",
        );
        assert_eq!(
            removals,
            vec![(doc_1.id, delete_timestamp)],
            "State removals should match."
        );

        // This should not be deleted as it has the same timestamp as the insert, and inserts always wins.
        del_data::<TestSource, _>(KEYSPACE, doc_2.id, doc_2.last_updated, &group)
            .await
            .expect("Delete doc.");

        let keyspace = group.get_or_create_keyspace(KEYSPACE).await;
        let (additions, removals) = keyspace.symetrical_diff(OrSWotSet::default()).await;
        assert_eq!(
            additions,
            vec![
                (doc_2.id, doc_2.last_updated),
                (doc_3.id, doc_3.last_updated),
            ],
            "State additions should match.",
        );
        assert_eq!(
            removals,
            vec![(doc_1.id, delete_timestamp)],
            "State removals should match."
        );

        // This should not be deleted as the timestamp is older than the insert.
        let new_timestamp = HLCTimestamp::new(500, 0, 0);
        del_data::<TestSource, _>(KEYSPACE, doc_2.id, new_timestamp, &group)
            .await
            .expect("Delete doc.");

        let keyspace = group.get_or_create_keyspace(KEYSPACE).await;
        let (additions, removals) = keyspace.symetrical_diff(OrSWotSet::default()).await;
        assert_eq!(
            additions,
            vec![
                (doc_2.id, doc_2.last_updated),
                (doc_3.id, doc_3.last_updated),
            ],
            "State additions should match.",
        );
        assert_eq!(
            removals,
            vec![(doc_1.id, delete_timestamp)],
            "State removals should match."
        );
    }

    #[tokio::test]
    async fn test_del_many_data() {
        static KEYSPACE: &str = "del-many-data-keyspace";
        let group = KeyspaceGroup::<MemStore>::new_for_test().await;
        let clock = group.clock();

        let doc_1 = Document::new(
            1,
            clock.get_time().await,
            b"hello, world from doc 1".to_vec(),
        );
        let doc_2 = Document::new(
            2,
            clock.get_time().await,
            b"hello, world from doc 2".to_vec(),
        );
        let doc_3 = Document::new(
            3,
            clock.get_time().await,
            b"hello, world from doc 3".to_vec(),
        );
        put_many_data::<TestSource, _>(
            KEYSPACE,
            vec![doc_1.clone(), doc_2.clone(), doc_3.clone()].into_iter(),
            &group,
            None,
        )
        .await
        .expect("Put new data should be successful.");

        // The new timestamp is newer than the insert so this should be successful.
        let delete_timestamp = clock.get_time().await;
        del_many_data::<TestSource, _>(
            KEYSPACE,
            vec![(doc_1.id, delete_timestamp)].into_iter(),
            &group,
        )
        .await
        .expect("Delete doc.");

        let keyspace = group.get_or_create_keyspace(KEYSPACE).await;
        let (additions, removals) = keyspace.symetrical_diff(OrSWotSet::default()).await;
        assert_eq!(
            additions,
            vec![
                (doc_2.id, doc_2.last_updated),
                (doc_3.id, doc_3.last_updated),
            ],
            "State additions should match.",
        );
        assert_eq!(
            removals,
            vec![(doc_1.id, delete_timestamp)],
            "State removals should match."
        );

        // This should not be deleted as it has the same timestamp as the insert, and inserts always wins.
        del_many_data::<TestSource, _>(
            KEYSPACE,
            vec![(doc_2.id, doc_2.last_updated)].into_iter(),
            &group,
        )
        .await
        .expect("Delete doc.");

        let keyspace = group.get_or_create_keyspace(KEYSPACE).await;
        let (additions, removals) = keyspace.symetrical_diff(OrSWotSet::default()).await;
        assert_eq!(
            additions,
            vec![
                (doc_2.id, doc_2.last_updated),
                (doc_3.id, doc_3.last_updated),
            ],
            "State additions should match.",
        );
        assert_eq!(
            removals,
            vec![(doc_1.id, delete_timestamp)],
            "State removals should match."
        );

        // This should not be deleted as the timestamp is older than the insert.
        let new_timestamp = HLCTimestamp::new(500, 0, 0);
        del_many_data::<TestSource, _>(
            KEYSPACE,
            vec![(doc_2.id, new_timestamp)].into_iter(),
            &group,
        )
        .await
        .expect("Delete doc.");

        let keyspace = group.get_or_create_keyspace(KEYSPACE).await;
        let (additions, removals) = keyspace.symetrical_diff(OrSWotSet::default()).await;
        assert_eq!(
            additions,
            vec![
                (doc_2.id, doc_2.last_updated),
                (doc_3.id, doc_3.last_updated),
            ],
            "State additions should match.",
        );
        assert_eq!(
            removals,
            vec![(doc_1.id, delete_timestamp)],
            "State removals should match."
        );
    }
}
