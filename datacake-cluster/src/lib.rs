#[macro_use]
extern crate tracing;

mod clock;
mod core;
pub mod error;
mod keyspace;
mod node;
mod poller;
mod rpc;
mod storage;

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use chitchat::FailureDetectorConfig;
use datacake_crdt::{get_unix_timestamp_ms, Key};
#[cfg(feature = "test-utils")]
pub use storage::test_suite;
pub use storage::Storage;
use tokio_stream::wrappers::WatchStream;
use tokio_stream::StreamExt;

use crate::clock::Clock;
use crate::core::Document;
use crate::keyspace::KeyspaceGroup;
use crate::node::{ClusterMember, DatacakeNode};
use crate::poller::ShutdownHandle;
use crate::rpc::{Context, GrpcTransport, RpcNetwork};

const POLLING_INTERVAL_DURATION: Duration = Duration::from_secs(1);

/// A fully managed eventually consistent state controller.
///
/// The [DatacakeCluster] manages all RPC and state propagation for
/// a given application, where the only setup required is the
/// RPC based configuration and the required handler traits
/// which wrap the application itself.
///
/// Datacake essentially acts as a frontend wrapper around a datastore
/// to make is distributed.
pub struct DatacakeCluster<S>
where
    S: Storage + Send + Sync + 'static,
{
    node: DatacakeNode,
    network: RpcNetwork,
    group: KeyspaceGroup<S>,
    clock: Clock,
}

impl<S> DatacakeCluster<S>
where
    S: Storage + Send + Sync + 'static,
{
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
    /// No seed nodes need to be live at the time of connecting for the cluster to start correctly,
    /// but they are required in order for nodes to discover one-another and share
    /// their basic state.
    pub async fn connect(
        node_id: impl Into<String>,
        cluster_id: impl Into<String>,
        listen_addr: SocketAddr,
        public_addr: SocketAddr,
        seed_nodes: Vec<String>,
        datastore: S,
    ) -> Result<Self, error::DatacakeError<S::Error>> {
        let node_id = node_id.into();
        let cluster_id = cluster_id.into();

        let clock = Clock::new(crc32fast::hash(node_id.as_bytes()));
        let storage = Arc::new(datastore);

        let group = KeyspaceGroup::new(storage.clone());
        let network = RpcNetwork::default();

        // Load the keyspace states.
        group.load_states_from_storage().await?;

        let node = connect_node(
            node_id.clone(),
            cluster_id.clone(),
            group.clone(),
            network.clone(),
            listen_addr,
            public_addr,
            seed_nodes,
        )
        .await?;

        setup_poller(group.clone(), network.clone(), &node).await?;

        info!(
            node_id = %node_id,
            cluster_id = %cluster_id,
            listen_addr = %listen_addr,
            "Datacake cluster connected."
        );

        Ok(Self {
            node,
            network,
            group,
            clock,
        })
    }

    /// Shuts down the cluster and cleans up any connections.
    pub async fn shutdown(self) {
        self.node.shutdown().await;
    }

    /// Creates a new handle to the underlying storage system.
    ///
    /// Changes applied to the handle are distributed across the cluster.
    pub fn handle(&self) -> DatacakeHandle<S> {
        DatacakeHandle {
            network: self.network.clone(),
            group: self.group.clone(),
            clock: self.clock.clone(),
        }
    }
}


/// A cheaply cloneable handle to control the data store.
pub struct DatacakeHandle<S>
where
    S: Storage + Send + Sync + 'static,
{
    network: RpcNetwork,
    group: KeyspaceGroup<S>,
    clock: Clock,
}

impl<S> Clone for DatacakeHandle<S>
where
    S: Storage + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            network: self.network.clone(),
            group: self.group.clone(),
            clock: self.clock.clone(),
        }
    }
}

impl<S> DatacakeHandle<S>
where
    S: Storage + Send + Sync + 'static,
{
    /// Retrieves a document from the underlying storage.
    pub async fn get(&self, keyspace: &str, doc_id: Key) -> Result<Option<Document>, S::Error> {
        let storage = self.group.storage();
        storage.get(keyspace, doc_id).await
    }

    /// Retrieves a set of documents from the underlying storage.
    ///
    /// If a document does not exist with the given ID, it is simply not part
    /// of the returned iterator.
    pub async fn get_many<I, T>(
        &self,
        keyspace: &str,
        doc_ids: I,
    ) -> Result<S::DocsIter, S::Error>
    where
        T: Iterator<Item = Key> + Send,
        I: IntoIterator<IntoIter = T> + Send
    {
        let storage = self.group.storage();
        storage.multi_get(keyspace, doc_ids.into_iter()).await
    }

    /// Insert or update a single document into the datastore.
    pub async fn put(
        &self,
        keyspace: &str,
        doc_id: Key,
        data: Vec<u8>,
    ) -> Result<(), error::DatacakeError<S::Error>> {
        let last_updated = self.clock.get_time().await;
        let document = Document {
            id: doc_id,
            last_updated,
            data
        };

        core::put_data(keyspace, document, &self.group).await?;

        Ok(())
    }

    /// Insert or update multiple documents into the datastore at once.
    pub async fn put_many(
        &self,
        keyspace: &str,
        documents: Vec<(Key, Vec<u8>)>,
    ) -> Result<(), error::DatacakeError<S::Error>> {
        let mut docs = Vec::with_capacity(documents.len());
        for (id, data) in documents {
            let last_updated = self.clock.get_time().await;
            docs.push(Document {
                id,
                last_updated,
                data,
            });
        }

        core::put_many_data(keyspace, docs.into_iter(), &self.group).await?;

        Ok(())
    }

    /// Delete a document from the datastore with a given doc ID.
    pub async fn del(
        &self,
        keyspace: &str,
        doc_id: Key,
    ) -> Result<(), error::DatacakeError<S::Error>> {
        let last_updated = self.clock.get_time().await;

        core::del_data(keyspace, doc_id, last_updated, &self.group).await?;

        Ok(())
    }


    /// Delete multiple documents from the datastore from the set of doc IDs.
    pub async fn del_many(
        &self,
        keyspace: &str,
        doc_ids: Vec<Key>,
    ) -> Result<(), error::DatacakeError<S::Error>> {
        let mut docs = Vec::with_capacity(doc_ids.len());
        for id in doc_ids {
            let last_updated = self.clock.get_time().await;
            docs.push((id, last_updated));
        }

        core::del_many_data(keyspace, docs.into_iter(), &self.group).await?;

        Ok(())
    }
}


/// Connects to the chitchat cluster.
///
/// The node will attempt to establish connections to the seed nodes and
/// will broadcast the node's public address to communicate.
async fn connect_node<S>(
    node_id: String,
    cluster_id: String,
    group: KeyspaceGroup<S>,
    network: RpcNetwork,
    listen_addr: SocketAddr,
    public_addr: SocketAddr,
    seed_nodes: Vec<String>,
) -> Result<DatacakeNode, error::DatacakeError<S::Error>>
where
    S: Storage + Send + Sync + 'static,
{
    let (chitchat_tx, chitchat_rx) = flume::bounded(1000);
    let context = Context {
        chitchat_messages: chitchat_tx,
        keyspace_group: group,
    };
    let transport = GrpcTransport::new(network.clone(), context, chitchat_rx);

    let me = ClusterMember::new(node_id, get_unix_timestamp_ms(), public_addr);
    let node = DatacakeNode::connect(
        me,
        listen_addr,
        cluster_id,
        seed_nodes,
        FailureDetectorConfig::default(),
        &transport,
    )
    .await?;

    Ok(node)
}

/// Starts the background task which watches for membership changes
/// intern starting and stopping polling services for each member.
async fn setup_poller<S>(
    keyspace_group: KeyspaceGroup<S>,
    network: RpcNetwork,
    node: &DatacakeNode,
) -> Result<(), error::DatacakeError<S::Error>>
where
    S: Storage + Send + Sync + 'static,
{
    let changes = node.member_change_watcher();
    tokio::spawn(watch_membership_changes(keyspace_group, network, changes));
    Ok(())
}

/// Watches for changes in the cluster membership.
///
/// When nodes leave and join, pollers are stopped and started as required.
async fn watch_membership_changes<S>(
    keyspace_group: KeyspaceGroup<S>,
    network: RpcNetwork,
    mut changes: WatchStream<Vec<ClusterMember>>,
) where
    S: Storage + Send + Sync + 'static,
{
    let mut poller_handles = HashMap::<SocketAddr, ShutdownHandle>::new();
    let mut last_network_set = HashSet::new();
    while let Some(members) = changes.next().await {
        let new_network_set = members
            .iter()
            .map(|member| (member.node_id.clone(), member.public_addr))
            .collect::<HashSet<_>>();

        // Remove client no longer apart of the network.
        for (node_id, addr) in last_network_set.difference(&new_network_set) {
            info!(
                target_node_id = %node_id,
                target_addr = %addr,
                "Node is no longer part of cluster."
            );

            network.disconnect(*addr);

            if let Some(handle) = poller_handles.remove(addr) {
                handle.kill();
            }
        }

        // Add new clients for each new node.
        for (node_id, addr) in new_network_set.difference(&last_network_set) {
            info!(
                target_node_id = %node_id,
                target_addr = %addr,
                "Node has connected to the cluster."
            );

            let channel = match network.get_or_connect(*addr).await {
                Ok(channel) => channel,
                Err(e) => {
                    error!(
                        error = ?e,
                        target_node_id = %node_id,
                        target_addr = %addr,
                        "Failed to establish network connection to node despite membership just changing. Is the system configured correctly?"
                    );
                    warn!(
                        target_node_id = %node_id,
                        target_addr = %addr,
                        "Node poller is starting with lazy connection, this may continue to error if a connection cannot be re-established.",
                    );

                    network.connect_lazy(*addr)
                },
            };

            let state = poller::NodePollerState::new(
                Cow::Owned(node_id.clone()),
                *addr,
                keyspace_group.clone(),
                channel,
                POLLING_INTERVAL_DURATION,
            );
            let handle = state.shutdown_handle();
            tokio::spawn(poller::node_poller(state));

            if let Some(handle) = poller_handles.insert(*addr, handle) {
                handle.kill();
            };
        }

        last_network_set = new_network_set;
    }
}
