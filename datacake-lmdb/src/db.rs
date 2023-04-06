use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::path::Path;

use datacake_crdt::{HLCTimestamp, Key};
use datacake_eventual_consistency::{Document, DocumentMetadata};
use flume::{self, Receiver, Sender};
use futures::channel::oneshot;
use heed::byteorder::LittleEndian;
use heed::types::{ByteSlice, Str, Unit, U64};
use heed::{Database, Env, EnvOpenOptions};

type KvDB = Database<U64<LittleEndian>, ByteSlice>;
type MetaDB = Database<U64<LittleEndian>, U64<LittleEndian>>;
type KeyspaceDB = Database<Str, Unit>;
type DatabaseKeyspace = BTreeMap<String, (KvDB, MetaDB)>;
type Task = Box<dyn FnOnce(&Env, &KeyspaceDB, &mut DatabaseKeyspace) + Send + 'static>;

const DEFAULT_MAP_SIZE: usize = 10 << 20;
const MAX_NUM_DBS: u32 = 250;
const CAPACITY: usize = 10;

#[derive(Debug, Clone)]
/// A asynchronous wrapper around a LMDB database.
///
/// These operations will be ran in a background thread preventing
/// any IO operations from blocking the async context.
pub struct StorageHandle {
    tx: Sender<Task>,
    env: Env,
}

impl StorageHandle {
    /// Connects to the LMDB database.
    ///
    /// This spawns 1 background threads with actions being executed within that thread.
    ///
    /// This approach reduces the affect of writes blocking reads and vice-versa.
    ///
    /// If the database does not already exist it will be created.
    ///
    /// ```rust
    /// use datacake_lmdb::StorageHandle;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let storage = StorageHandle::open("./my-lmdb-data").await.expect("Create database");
    /// # drop(storage);
    /// # let _ = std::fs::remove_dir_all("./my-lmdb-data");
    /// # }
    /// ```
    pub async fn open(path: impl AsRef<Path>) -> heed::Result<Self> {
        let (tx, env) = setup_database(path).await?;
        Ok(Self { tx, env })
    }

    #[inline]
    /// Get the current heed environment.
    pub fn env(&self) -> &Env {
        &self.env
    }

    /// Get the current keyspace list.
    pub(crate) async fn keyspace_list(&self) -> heed::Result<Vec<String>> {
        let (tx, rx) = oneshot::channel();

        let cb = move |env: &Env,
                       keyspace_list: &KeyspaceDB,
                       _databases: &mut DatabaseKeyspace| {
            let res = read_keyspace_list(env, keyspace_list);
            let _ = tx.send(res);
        };

        self.tx
            .send_async(Box::new(cb))
            .await
            .expect("send message");

        rx.await.unwrap()
    }

    /// Execute a PUT operation on the DB.
    pub(crate) async fn put_kv(
        &self,
        keyspace: &str,
        doc: Document,
    ) -> heed::Result<()> {
        self.submit_task(keyspace, move |env: &Env, kv: &KvDB, meta: &MetaDB| {
            let mut txn = env.write_txn()?;
            kv.put(&mut txn, &doc.id(), doc.data())?;
            meta.put(&mut txn, &doc.id(), &doc.last_updated().as_u64())?;
            txn.commit()?;
            Ok(())
        })
        .await
    }

    /// Execute a many PUT operations on the DB.
    pub(crate) async fn put_many_kv(
        &self,
        keyspace: &str,
        docs: impl Iterator<Item = Document>,
    ) -> heed::Result<()> {
        let docs = Vec::from_iter(docs);

        self.submit_task(keyspace, move |env: &Env, kv: &KvDB, meta: &MetaDB| {
            let mut txn = env.write_txn()?;
            for doc in docs {
                kv.put(&mut txn, &doc.id(), doc.data())?;
                meta.put(&mut txn, &doc.id(), &doc.last_updated().as_u64())?;
            }
            txn.commit()?;
            Ok(())
        })
        .await
    }

    /// Get the metadata list from the DB.
    pub(crate) async fn get_metadata(
        &self,
        keyspace: &str,
    ) -> heed::Result<Vec<(Key, HLCTimestamp, bool)>> {
        self.submit_task(keyspace, move |env: &Env, kv: &KvDB, meta: &MetaDB| {
            let mut entries = Vec::new();
            let txn = env.read_txn()?;

            for pair in meta.iter(&txn)? {
                let (id, ts) = pair?;

                let is_tombstone = kv.get(&txn, &id)?.is_none();
                entries.push((id, HLCTimestamp::from_u64(ts), is_tombstone));
            }

            Ok(entries)
        })
        .await
    }

    /// Mark an entry as a tombstone.
    pub(crate) async fn mark_tombstone(
        &self,
        keyspace: &str,
        key: Key,
        ts: HLCTimestamp,
    ) -> heed::Result<()> {
        self.submit_task(keyspace, move |env: &Env, kv: &KvDB, meta: &MetaDB| {
            let mut txn = env.write_txn()?;
            kv.delete(&mut txn, &key)?;
            meta.put(&mut txn, &key, &ts.as_u64())?;
            txn.commit()?;
            Ok(())
        })
        .await
    }

    /// Mark an entry as a tombstone.
    pub(crate) async fn mark_many_as_tombstone(
        &self,
        keyspace: &str,
        docs: impl Iterator<Item = DocumentMetadata>,
    ) -> heed::Result<()> {
        let docs = Vec::from_iter(docs);

        self.submit_task(keyspace, move |env: &Env, kv: &KvDB, meta: &MetaDB| {
            let mut txn = env.write_txn()?;
            for doc in docs {
                kv.delete(&mut txn, &doc.id)?;
                meta.put(&mut txn, &doc.id, &doc.last_updated.as_u64())?;
            }
            txn.commit()?;
            Ok(())
        })
        .await
    }

    /// Clear a tombstone entry.
    pub(crate) async fn remove_tombstones(
        &self,
        keyspace: &str,
        keys: impl Iterator<Item = Key>,
    ) -> heed::Result<()> {
        let keys = Vec::from_iter(keys);

        self.submit_task(keyspace, move |env: &Env, _kv: &KvDB, meta: &MetaDB| {
            let mut txn = env.write_txn()?;
            for key in keys {
                meta.delete(&mut txn, &key)?; // Our entry will already be removed.
            }
            txn.commit()?;
            Ok(())
        })
        .await
    }

    /// Execute a PUT operation on the DB.
    pub(crate) async fn get(
        &self,
        keyspace: &str,
        key: u64,
    ) -> heed::Result<Option<Document>> {
        self.submit_task(keyspace, move |env: &Env, kv: &KvDB, meta: &MetaDB| {
            let txn = env.read_txn()?;
            if let Some(doc) = kv.get(&txn, &key)? {
                let ts = meta.get(&txn, &key)?.unwrap();
                Ok(Some(Document::new(key, HLCTimestamp::from_u64(ts), doc)))
            } else {
                Ok(None)
            }
        })
        .await
    }

    /// Execute a PUT operation on the DB.
    pub(crate) async fn get_many(
        &self,
        keyspace: &str,
        keys: impl Iterator<Item = Key>,
    ) -> heed::Result<Vec<Document>> {
        let keys = Vec::from_iter(keys);

        self.submit_task(keyspace, move |env: &Env, kv: &KvDB, meta: &MetaDB| {
            let mut docs = Vec::with_capacity(keys.len());
            let txn = env.read_txn()?;
            for key in keys {
                if let Some(doc) = kv.get(&txn, &key)? {
                    let ts = meta.get(&txn, &key)?.unwrap();
                    docs.push(Document::new(key, HLCTimestamp::from_u64(ts), doc));
                }
            }

            Ok(docs)
        })
        .await
    }

    /// Submits a writer task to execute on the KV store.
    ///
    /// This executes the callback on the memory view connection which should be
    /// significantly faster to modify or read.
    async fn submit_task<CB, T>(&self, keyspace: &str, inner: CB) -> heed::Result<T>
    where
        T: Send + 'static,
        CB: FnOnce(&Env, &KvDB, &MetaDB) -> heed::Result<T> + Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let keyspace = keyspace.to_owned();

        let cb = move |env: &Env,
                       keyspace_list: &KeyspaceDB,
                       databases: &mut DatabaseKeyspace| {
            let res = match databases.entry(keyspace) {
                Entry::Vacant(entry) => {
                    match try_create_dbs(env, keyspace_list, entry.key()) {
                        Ok(dbs) => {
                            let (kv, meta) = entry.insert(dbs);
                            inner(env, kv, meta)
                        },
                        Err(e) => Err(e),
                    }
                },
                Entry::Occupied(existing) => {
                    let (kv, meta) = existing.get();
                    inner(env, kv, meta)
                },
            };

            let _ = tx.send(res);
        };

        self.tx
            .send_async(Box::new(cb))
            .await
            .expect("send message");

        rx.await.unwrap()
    }
}

fn try_create_dbs(
    env: &Env,
    keyspace_list: &KeyspaceDB,
    keyspace: &str,
) -> heed::Result<(KvDB, MetaDB)> {
    let kv_name = format!("datacake-{keyspace}-kv");
    let meta_name = format!("datacake-{keyspace}-meta");

    let mut txn = env.write_txn()?;
    keyspace_list.put(&mut txn, keyspace, &())?;
    let kv_db = env.create_database(&mut txn, Some(&kv_name))?;
    let meta_db = env.create_database(&mut txn, Some(&meta_name))?;
    txn.commit()?;

    Ok((kv_db, meta_db))
}

fn read_keyspace_list(
    env: &Env,
    keyspace_list: &KeyspaceDB,
) -> heed::Result<Vec<String>> {
    let mut list = Vec::new();
    let txn = env.read_txn()?;

    for key in keyspace_list.iter(&txn)? {
        let (keyspace, _) = key?;
        list.push(keyspace.to_owned());
    }

    Ok(list)
}

async fn setup_database(path: impl AsRef<Path>) -> heed::Result<(Sender<Task>, Env)> {
    let path = path.as_ref().to_path_buf();
    let (tx, rx) = flume::bounded(CAPACITY);

    let env = tokio::task::spawn_blocking(move || setup_disk_handle(&path, rx))
        .await
        .expect("spawn background runner")?;

    Ok((tx, env))
}

fn setup_disk_handle(path: &Path, tasks: Receiver<Task>) -> heed::Result<Env> {
    if !path.exists() {
        let _ = std::fs::create_dir_all(path); // Attempt to create the directory.
    }

    let env = EnvOpenOptions::new()
        .map_size(DEFAULT_MAP_SIZE)
        .max_dbs(MAX_NUM_DBS)
        .open(path)?;

    let mut txn = env.write_txn()?;
    let keyspace_list = env.create_database(&mut txn, Some("datacake-keyspace"))?;
    txn.commit()?;

    let env2 = env.clone();
    std::thread::spawn(move || run_tasks(env, tasks, keyspace_list));

    Ok(env2)
}

/// Runs all tasks received with a mutable reference to the given connection.
fn run_tasks(env: Env, tasks: Receiver<Task>, keyspace_list: KeyspaceDB) {
    let mut dbs = DatabaseKeyspace::new();
    while let Ok(task) = tasks.recv() {
        (task)(&env, &keyspace_list, &mut dbs);
    }
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::path::PathBuf;

    use uuid::Uuid;

    use super::*;

    fn get_path() -> PathBuf {
        let path = temp_dir().join(Uuid::new_v4().to_string());
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[tokio::test]
    async fn test_db_creation() {
        StorageHandle::open(get_path())
            .await
            .expect("Database should open OK.");
    }

    #[tokio::test]
    async fn test_db_put_and_get() {
        let handle = StorageHandle::open(get_path())
            .await
            .expect("Database should open OK.");

        let doc1 = Document::new(1, HLCTimestamp::from_u64(0), b"Hello".as_ref());
        handle
            .put_kv("test", doc1.clone())
            .await
            .expect("Put new doc");

        // Test keyspace dont overlap
        let doc2 = Document::new(1, HLCTimestamp::from_u64(2), b"Hello 2".as_ref());
        handle
            .put_kv("test2", doc2.clone())
            .await
            .expect("Put new doc");

        let doc3 = Document::new(1, HLCTimestamp::from_u64(3), b"Hello 3".as_ref());
        handle
            .put_kv("test3", doc3.clone())
            .await
            .expect("Put new doc");

        let fetched_doc_1 = handle
            .get("test", 1)
            .await
            .expect("Get doc")
            .expect("Doc exists");
        let fetched_doc_2 = handle
            .get("test2", 1)
            .await
            .expect("Get doc")
            .expect("Doc exists");
        let fetched_doc_3 = handle
            .get("test3", 1)
            .await
            .expect("Get doc")
            .expect("Doc exists");

        assert_eq!(
            [doc1, doc2, doc3],
            [fetched_doc_1, fetched_doc_2, fetched_doc_3],
            "Documents returned should not overlap and match."
        );
    }

    #[tokio::test]
    async fn test_db_put_get_many() {
        let handle = StorageHandle::open(get_path())
            .await
            .expect("Database should open OK.");

        let docs = vec![
            Document::new(1, HLCTimestamp::from_u64(0), b"Hello".as_ref()),
            Document::new(2, HLCTimestamp::from_u64(0), b"Hello".as_ref()),
            Document::new(3, HLCTimestamp::from_u64(0), b"Hello".as_ref()),
        ];
        handle
            .put_many_kv("test", docs.clone().into_iter())
            .await
            .expect("Put new docs");

        let fetched_docs = handle
            .get_many("test", [1, 2, 3].into_iter())
            .await
            .expect("Get docs");

        assert_eq!(docs, fetched_docs, "Documents returned should match.");
    }

    #[tokio::test]
    async fn test_db_mark_tombstone() {
        let handle = StorageHandle::open(get_path())
            .await
            .expect("Database should open OK.");

        let doc1 = Document::new(1, HLCTimestamp::from_u64(0), b"Hello".as_ref());
        handle
            .put_kv("test", doc1.clone())
            .await
            .expect("Put new doc");
        assert!(
            handle.get("test", 1).await.expect("Get doc").is_some(),
            "Document should exist"
        );

        // Mark it as a tombstone so we shouldn't get it returned anymore.
        handle
            .mark_tombstone("test", doc1.id(), HLCTimestamp::from_u64(1))
            .await
            .expect("Put new doc");
        assert!(
            handle.get("test", 1).await.expect("Get doc").is_none(),
            "Document should not exist"
        );

        // Add a doc simulating an update and check it get's re-inserted.
        handle
            .put_kv("test", doc1.clone())
            .await
            .expect("Put new doc");
        assert!(
            handle.get("test", 1).await.expect("Get doc").is_some(),
            "Document should exist"
        );
    }
}
