use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Error, Result};
use datacake_cluster::{
    ConnectionCfg,
    DatacakeCluster,
    Datastore,
    Document,
    HLCTimestamp,
    Key,
    Metastore,
    StateChanges,
    NUMBER_OF_SHARDS,
};
use sled::Mode;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialise our logger.
    // Datacake has a fairly active DEBUG log level, which gives a lot of info
    // around what phase of state replication it is at.
    tracing_subscriber::fmt::init();

    let node_1_store = Store::new(1);

    // Our gossip address and rpc address must be different.
    // The gossip address will use UDP vs the TCP RPC connection.
    let node_1_cfg = make_connection_config(
        "127.0.0.1:8000".parse().unwrap(),
        "127.0.0.1:8100".parse().unwrap(),
    );

    // We connect the first node, it is safe to connect a node with a set of seed nodes
    // that may not be online yet.
    let node_1 = DatacakeCluster::connect(
        // This value must be unique
        "node-1",
        // Typically you would probably only have one cluster,
        // currently this does not affect anything outside of gossip.
        "cluster-1",
        // Our connection config we just made.
        node_1_cfg,
        // We say that the node-2 (the node we will create after this) will be one of our
        // seeds.
        vec!["127.0.0.1:8001".to_string()],
        // We pass in our datastore instance. This is where Datacake will send and receive all of
        // our state changes.
        node_1_store,
    )
    .await?;

    // In the real world this would likely be another
    // instance of your application on another machine.
    let node_2_store = Store::new(2);
    let node_2_cfg = make_connection_config(
        "127.0.0.1:8001".parse().unwrap(),
        "127.0.0.1:8101".parse().unwrap(),
    );
    let node_2 = DatacakeCluster::connect(
        // It is super important that these are unique,
        // otherwise the system will not propagate state.
        "node-2",
        "cluster-1",
        node_2_cfg,
        vec!["127.0.0.1:8000".to_string()],
        node_2_store,
    )
    .await?;

    // We can use our handles to manipulate our data store's state.
    // These handles are cheap to clone.
    let node_1_handle = node_1.handle();
    let node_2_handle = node_2.handle();

    // Let's insert a document from node-1.
    node_1_handle.insert(123, b"Hello, world!".to_vec()).await?;

    // In theory if all nodes are operational and live this is much quicker
    // at aligning the state. But for demonstration purposes we will give our
    // nodes some time to sync up.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let maybe_doc = node_2_handle.get(123).await?;
    assert!(maybe_doc.is_some(), "Expected our document to exist.");

    let doc = maybe_doc.unwrap();
    assert_eq!(doc.id, 123);
    assert_eq!(doc.data, b"Hello, world!");

    Ok(())
}

/// A simple helper method for the purposes of testing.
///
/// In the real world you would want to set the public address and the listen address'
/// to different values as nodes will likely be on different networks.
fn make_connection_config(
    gossip_addr: SocketAddr,
    rpc_addr: SocketAddr,
) -> ConnectionCfg {
    ConnectionCfg {
        gossip_public_addr: gossip_addr,
        gossip_listen_addr: gossip_addr,
        rpc_public_addr: rpc_addr,
        rpc_listen_addr: rpc_addr,
    }
}

/// This is our example store.
///
/// It's a simple wrapper around a [sled::Db] with a set of trees/keyspace created
/// for each metadata shard, although in production you may want to change this to
/// better suite your data organisation or what's best suited for the store of your
/// choice.
pub struct Store {
    /// The live documents metadata.
    ///
    /// The index of the tree in the array corresponds to the shard id.
    keys: Vec<sled::Tree>,

    /// The documents that have been marked as tombstones.
    ///
    /// The index of the tree in the array corresponds to the shard id.
    dead: Vec<sled::Tree>,

    /// Our main db, this stores all of our documents directly.
    db: sled::Db,
}

impl Store {
    fn new(id: usize) -> Self {
        let db = sled::Config::new()
            .mode(Mode::HighThroughput)
            .temporary(true)
            .path(format!("./data/{}", id))
            .open()
            .unwrap();

        let mut keys = vec![];
        let mut dead = vec![];

        // This is a simple way to organise all of our metadata with minimal setup
        // you may wish to do something different for an actual project or a different
        // datastore.
        for i in 0..NUMBER_OF_SHARDS {
            let k = db.open_tree(format!("metadata-keys-{}", i)).unwrap();
            let d = db.open_tree(format!("metadata-dead-{}", i)).unwrap();

            keys.push(k);
            dead.push(d);
        }

        Self { keys, dead, db }
    }
}

/// Creates a simple insert [sled::Batch] from the given set of changes.
///
/// Each (Key, Timestamp) pair is serialized with `rkyv` to it's raw data,
/// and is then added to the batch.
fn metadata_updates_to_batch(changes: Vec<(Key, HLCTimestamp)>) -> sled::Batch {
    let mut batch = sled::Batch::default();

    for (k, ts) in changes {
        let key = rkyv::to_bytes::<_, 8>(&k).unwrap();
        let val = rkyv::to_bytes::<_, 24>(&ts).unwrap();

        batch.insert(key.into_vec(), val.into_vec());
    }

    batch
}

/// Deserialized the raw key value pair into their Rust types.
fn get_key_ts_pair(key: &[u8], value: &[u8]) -> (Key, HLCTimestamp) {
    let key = rkyv::from_bytes::<Key>(key).unwrap();
    let ts = rkyv::from_bytes::<HLCTimestamp>(value).unwrap();

    (key, ts)
}

/// Creates a simple remove [sled::Batch] from the given set of changes.
///
/// Each (Key, Timestamp) pair is serialized with `rkyv` to it's raw data,
/// and is then added to the batch.
fn metadata_removes_to_batch(changes: Vec<Key>) -> sled::Batch {
    let mut batch = sled::Batch::default();

    for k in changes {
        let key = rkyv::to_bytes::<_, 8>(&k).unwrap();
        batch.remove(key.into_vec());
    }

    batch
}

#[async_trait::async_trait]
impl Metastore for Store {
    type Error = Error;

    /// This is called when our cluster first starts up, where each shard's state is
    /// fetched from the persisted store, these keys contribute to the live documents
    /// in the set.
    ///
    /// It's important that the state returned is correct, otherwise during synchronisations
    /// data may be incorrectly changed during the merging of two node's states.
    async fn get_keys(
        &self,
        shard_id: usize,
    ) -> std::result::Result<StateChanges, Error> {
        let data = self.keys[shard_id].iter();

        let mut state = vec![];
        for data in data {
            let (key, value) = data?;
            state.push(get_key_ts_pair(&key, &value));
        }

        Ok(state)
    }

    /// This should persist any changes to the live documents metadata state.
    ///
    /// This is called very often, so it's a good idea to try and optimise this as
    /// best as possible.
    ///
    /// The cluster will attempt to batch these changes by shard during bulk updates,
    /// but individual updates to documents will result in this method being triggered
    /// every time.
    async fn update_keys(
        &self,
        shard_id: usize,
        states: Vec<(Key, HLCTimestamp)>,
    ) -> std::result::Result<(), Error> {
        let batch = metadata_updates_to_batch(states);
        self.keys[shard_id].apply_batch(batch)?;

        Ok(())
    }

    /// This should remove a given set of document keys from the live documents metadata.
    ///
    /// This is called very often, so it's a good idea to try and optimise this as
    /// best as possible.
    ///
    /// The cluster will attempt to batch these changes by shard during bulk updates,
    /// but individual updates to documents will result in this method being triggered
    /// every time.
    async fn remove_keys(
        &self,
        shard_id: usize,
        states: Vec<Key>,
    ) -> std::result::Result<(), Error> {
        let batch = metadata_removes_to_batch(states);
        self.keys[shard_id].apply_batch(batch)?;

        Ok(())
    }

    /// Similar to [Metastore::get_keys], this method gets called when the cluster first
    /// starts up, where each shard's tombstone state is fetched from the persisted store,
    /// these keys contribute to the dead documents in the set.
    ///
    /// It's important that tombstone metadata is correctly persisted and not pre-emptively removed
    /// as it can result in previously removed documents being re-added if a node which is behind
    /// has yet to observe the remove.
    async fn get_tombstone_keys(
        &self,
        shard_id: usize,
    ) -> std::result::Result<StateChanges, Error> {
        let data = self.dead[shard_id].iter();

        let mut state = vec![];
        for data in data {
            let (key, value) = data?;
            state.push(get_key_ts_pair(&key, &value));
        }

        Ok(state)
    }

    /// Similar to [Metastore::update_keys], this should persist any changes to
    /// the dead/tombstone documents metadata state.
    ///
    /// This is called very often, so it's a good idea to try and optimise this as
    /// best as possible.
    ///
    /// The cluster will attempt to batch these changes by shard during bulk updates,
    /// but individual updates to documents will result in this method being triggered
    /// every time.
    async fn update_tombstone_keys(
        &self,
        shard_id: usize,
        states: Vec<(Key, HLCTimestamp)>,
    ) -> std::result::Result<(), Error> {
        let batch = metadata_updates_to_batch(states);
        self.dead[shard_id].apply_batch(batch)?;

        Ok(())
    }

    /// Unlike [Metastore::remove_keys], this is only called after a synchronisation merge
    /// where some tombstones can be completely removed as we have internally.
    ///
    /// Once this is completed, it is safe to remove any trace of the document and it's metadata.
    ///
    /// The cluster will attempt to batch these changes by shard during bulk updates,
    /// but individual updates to documents will result in this method being triggered
    /// every time.
    async fn remove_tombstone_keys(
        &self,
        shard_id: usize,
        states: Vec<Key>,
    ) -> std::result::Result<(), Error> {
        let batch = metadata_removes_to_batch(states);
        self.dead[shard_id].apply_batch(batch)?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl Datastore for Store {
    /// This gets called when ever you call `get` or `get_many` on a [datacake_cluster::DatacakeHandle]
    /// this is also used when the nodes attempt to synchronise their state.
    ///
    /// In this example we've gone with a very simple approach to fetching multiple documents,
    /// but you may wish to changes this to something which is better suited for your workload.
    async fn get_documents(
        &self,
        doc_ids: &[Key],
    ) -> std::result::Result<Vec<Document>, Error> {
        let mut docs = vec![];

        for id in doc_ids {
            let key = rkyv::to_bytes::<_, 8>(id).unwrap();
            let value = match self.db.get(&key)? {
                None => continue,
                Some(v) => v,
            };

            let doc = rkyv::from_bytes::<Document>(&value).unwrap();
            docs.push(doc);
        }

        Ok(docs)
    }

    /// This gets called when ever you call `insert` or `insert_many` on a [datacake_cluster::DatacakeHandle]
    /// this is also used when the nodes attempt to synchronise their state.
    ///
    /// The raw data you insert with the handle methods are wrapped in a [datacake_cluster::Document]
    /// which contains the required metadata to keep things simple.
    /// This wrapper type supports rkyv's (de)serialization for easy handling.
    async fn upsert_documents(
        &self,
        documents: Vec<Document>,
    ) -> std::result::Result<(), Error> {
        let mut batch = sled::Batch::default();

        for docs in documents {
            let key = rkyv::to_bytes::<_, 8>(&docs.id).unwrap();
            let doc = rkyv::to_bytes::<_, 1024>(&docs).unwrap();

            batch.insert(key.into_vec(), doc.into_vec());
        }

        self.db.apply_batch(batch)?;

        Ok(())
    }

    /// This gets called when ever you call `delete` or `delete_many` on a [datacake_cluster::DatacakeHandle]
    /// this is also used when the nodes attempt to synchronise their state.
    ///
    /// It is safe to remove the document data itself during this process.
    /// However, you should to proactively modify the metadata state of the documents.
    async fn delete_documents(&self, doc_ids: &[Key]) -> std::result::Result<(), Error> {
        let mut batch = sled::Batch::default();

        for id in doc_ids {
            let doc = rkyv::to_bytes::<_, 1024>(id).unwrap();

            batch.remove(doc.into_vec());
        }

        self.db.apply_batch(batch)?;

        Ok(())
    }
}
