#[macro_use]
extern crate tracing;

mod clock;
mod data_handler;
mod manager;
mod node;
mod rpc;
mod shard;
mod shared;
mod wrappers;

mod tasks;
#[cfg(feature = "test-utils")]
pub mod test_utils;
pub mod error;

use std::fmt::{Debug, Display};
use std::mem;
use std::sync::Arc;

use bytes::Bytes;
pub use datacake_crdt::{HLCTimestamp, Key, OrSWotSet, StateChanges, TimestampError};
pub use manager::ConnectionCfg;
pub use rpc::Document;
pub use shard::NUMBER_OF_SHARDS;
use tokio::time::{timeout_at, Duration, Instant};
pub use wrappers::{Datastore, Metastore};

use crate::clock::Clock;
use crate::data_handler::StandardDataHandler;
use crate::error::DatacakeError;
use crate::manager::DatacakeClusterManager;
use crate::rpc::{ClientCluster, DataHandler};
use crate::shard::state::StateWatcherHandle;

/// A fully managed eventually consistent state controller.
///
/// The [DatacakeCluster] manages all RPC and state propagation for
/// a given application, where the only setup required is the
/// RPC based configuration and the required handler traits
/// which wrap the application itself.
///
/// Datacake essentially acts as a frontend wrapper around a datastore
/// to make is distributed.
pub struct DatacakeCluster<DS: Datastore> {
    manager: Option<DatacakeClusterManager<DS::Error>>,
    handle: DatacakeHandle<DS::Error>,
}

impl<DS: Datastore> DatacakeCluster<DS> {
    /// Starts the Datacake cluster, connecting to the targeted seed nodes.
    ///
    /// When connecting to the cluster, the `node_id` **must be unique** otherwise
    /// the cluster will incorrectly propagate state and not become consistent.
    ///
    /// Typically you will only have one cluster and therefore only have one `cluster_id`
    /// which should be the same for each node in the cluster.
    /// Currently the `cluster_id` is not handled by anything other than
    /// [chitchat](https://docs.rs/chitchat/0.4.1/chitchat/)
    ///
    /// No seed nodes need to be current live for the cluster to start correctly,
    /// but they are required in order for nodes to discover one-another and share
    /// their basic state.
    pub async fn connect(
        node_id: impl Into<String>,
        cluster_id: impl Into<String>,
        connection_cfg: ConnectionCfg,
        seed_nodes: Vec<String>,
        datastore: DS,
    ) -> Result<Self, DatacakeError<DS::Error>> {
        let node_id = node_id.into();
        let cluster_id = cluster_id.into();

        let clock = Clock::new(crc32fast::hash(node_id.as_bytes()));
        let datastore = Arc::new(datastore);

        let shard_changes_watcher = shard::state::state_watcher().await;

        let shard_group = shard::create_shard_group(shard_changes_watcher.clone()).await;
        let handler = StandardDataHandler::new(shard_group.clone(), datastore.clone(), clock.clone());

        // Initialise the shard groups loading from the persisted state.
        handler.load_initial_shard_states().await?;

        let data_handler = Arc::new(handler) as Arc<dyn DataHandler<Error = DS::Error>>;

        let manager = DatacakeClusterManager::connect(
            node_id,
            connection_cfg,
            cluster_id,
            seed_nodes,
            data_handler.clone(),
            shard_group,
            shard_changes_watcher,
        )
        .await?;

        let nodes = manager.rpc_nodes().clone();

        Ok(Self {
            manager: Some(manager),
            handle: DatacakeHandle {
                data_handler: data_handler.clone(),
                nodes,
                clock,
            },
        })
    }

    /// Safely shuts the current node down and leaves the cluster.
    pub async fn shutdown(mut self) -> Result<(), DatacakeError<DS::Error>> {
        if let Some(manager) = mem::take(&mut self.manager) {
            manager.shutdown().await?;
        }

        Ok(())
    }

    #[inline]
    /// Creates a new [DatacakeHandle].
    pub fn handle(&self) -> DatacakeHandle<DS::Error> {
        self.handle.clone()
    }
}

impl<DS: Datastore> Drop for DatacakeCluster<DS> {
    fn drop(&mut self) {
        if let Some(manager) = self.manager.take() {
            tokio::spawn(async move {
                if let Err(e) = manager.shutdown().await {
                    warn!(
                        error = ?e,
                        "Failed to shut down Datacake cluster correctly, not all network connections may have closed correctly."
                    );
                }
            });
        }
    }
}

/// A cheap to clone, threadsafe handle for interacting
/// with your underlying datastore and the distribution system.
pub struct DatacakeHandle<E>
where
    E: Display + Debug + Send + Sync + 'static
{
    data_handler: Arc<dyn DataHandler<Error = E>>,
    nodes: ClientCluster,
    clock: Clock,
}

impl<E> Clone for DatacakeHandle<E>
where
    E: Display + Debug + Send + Sync + 'static
{
    fn clone(&self) -> Self {
        Self {
            data_handler: self.data_handler.clone(),
            nodes: self.nodes.clone(),
            clock: self.clock.clone(),
        }
    }
}

impl<E> DatacakeHandle<E>
where
    E: Display + Debug + Send + Sync + 'static
{
    async fn broadcast_upsert_to_nodes(
        &self,
        docs: Vec<(Key, HLCTimestamp, Bytes)>,
    ) -> Result<(), DatacakeError<E>> {
        let docs = Arc::new(docs);
        let nodes = self.nodes.get_all_clients();
        let (completed_tx, completed_rx) = flume::bounded(nodes.len());

        for (node_id, client) in nodes {
            let tx = completed_tx.clone();
            let docs = docs.clone();

            tokio::spawn(async move {
                match client.data.upsert(docs).await {
                    Ok(_) => {
                        let _ = tx.send_async(()).await;
                    },
                    Err(e) => {
                        warn!(
                            target_node_id = %node_id,
                            error = ?e,
                            "Node failed to handle upsert request."
                        );
                    },
                };
            });
        }

        let deadline = Instant::now() + Duration::from_millis(2);
        while (timeout_at(deadline, completed_rx.recv_async()).await).is_ok() {
            continue;
        }

        Ok(())
    }

    async fn broadcast_delete_to_nodes(
        &self,
        docs: Vec<(Key, HLCTimestamp)>,
    ) -> Result<(), DatacakeError<E>> {
        let docs = Arc::new(docs);
        let nodes = self.nodes.get_all_clients();
        let (completed_tx, completed_rx) = flume::bounded(nodes.len());

        for (node_id, client) in nodes {
            let tx = completed_tx.clone();
            let docs = docs.clone();

            tokio::spawn(async move {
                match client.data.delete(docs).await {
                    Ok(_) => {
                        let _ = tx.send_async(()).await;
                    },
                    Err(e) => {
                        warn!(
                            target_node_id = %node_id,
                            error = ?e,
                            "Node failed to handle delete request."
                        );
                    },
                };
            });
        }

        let deadline = Instant::now() + Duration::from_millis(2);
        while (timeout_at(deadline, completed_rx.recv_async()).await).is_ok() {
            continue;
        }

        Ok(())
    }

    #[inline]
    /// The number of nodes currently connected to the cluster.
    pub fn live_nodes_count(&self) -> usize {
        self.nodes.live_nodes_count()
    }

    /// Get a single document with a given id.
    pub async fn get(&self, id: Key) -> Result<Option<Document>, DatacakeError<E>> {
        self.data_handler.get_document(id).await
    }

    /// Get many documents from a set of ids.
    pub async fn get_many(&self, ids: &[Key]) -> Result<Vec<Document>, DatacakeError<E>> {
        self.data_handler.get_documents(ids).await
    }

    /// Insert a single document into the datastore.
    pub async fn insert(&self, id: Key, data: Vec<u8>) -> Result<(), DatacakeError<E>> {
        let last_modified = self.clock.get_time().await;

        self.data_handler
            .upsert_document(Document {
                id,
                last_modified,
                data: data.clone(),
            })
            .await?;

        self.broadcast_upsert_to_nodes(vec![(id, last_modified, Bytes::from(data))])
            .await
    }

    /// Insert multiple documents into the datastore at once.
    pub async fn insert_many(
        &self,
        documents: impl Iterator<Item = (Key, Vec<u8>)>,
    ) -> Result<(), DatacakeError<E>> {
        let mut docs = vec![];
        for (doc_id, data) in documents {
            let ts = self.clock.get_time().await;
            docs.push((doc_id, ts, data));
        }

        self.data_handler.upsert_documents(docs.clone()).await?;
        self.broadcast_upsert_to_nodes(
            docs.into_iter()
                .map(|(k, ts, data)| (k, ts, Bytes::from(data)))
                .collect(),
        )
        .await
    }

    /// Delete a document from the datastore with a given id.
    pub async fn delete(&self, id: Key) -> Result<(), DatacakeError<E>> {
        let last_modified = self.clock.get_time().await;

        self.data_handler
            .mark_tombstone_document(id, last_modified)
            .await?;

        self.broadcast_delete_to_nodes(vec![(id, last_modified)])
            .await
    }

    /// Delete many documents from the datastore from the set of ids.
    pub async fn delete_many(&self, ids: &[Key]) -> Result<(), DatacakeError<E>> {
        let mut doc_changes = vec![];
        for id in ids {
            let ts = self.clock.get_time().await;
            doc_changes.push((*id, ts));
        }

        self.data_handler
            .mark_tombstone_documents(doc_changes.clone())
            .await?;

        self.broadcast_delete_to_nodes(doc_changes).await
    }
}
