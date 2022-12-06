#[macro_use]
extern crate tracing;

mod clock;
mod core;
pub mod error;
mod keyspace;
mod node;
mod nodes_selector;
mod replication;
mod rpc;
mod statistics;
mod storage;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::fmt::Display;
use std::future::Future;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chitchat::transport::Transport;
use chitchat::FailureDetectorConfig;
use datacake_crdt::Key;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use itertools::Itertools;
pub use nodes_selector::{
    Consistency,
    ConsistencyError,
    DCAwareSelector,
    NodeSelector,
    NodeSelectorHandle,
};
pub use statistics::ClusterStatistics;
#[cfg(feature = "test-utils")]
pub use storage::test_suite;
pub use storage::{BulkMutationError, ProgressTracker, PutContext, Storage};
use tokio_stream::wrappers::WatchStream;

pub use self::core::Document;
use crate::clock::Clock;
use crate::keyspace::{
    Del,
    KeyspaceGroup,
    MultiDel,
    MultiSet,
    Set,
    CONSISTENCY_SOURCE_ID,
};
use crate::node::{ClusterMember, DatacakeNode};
use crate::replication::{
    MembershipChanges,
    Mutation,
    ReplicationCycleContext,
    ReplicationHandle,
    TaskDistributor,
    TaskServiceContext,
};
use crate::rpc::{
    ConsistencyClient,
    Context,
    DefaultRegistry,
    GrpcTransport,
    RpcNetwork,
    ServiceRegistry,
    TIMEOUT_LIMIT,
};

pub static DEFAULT_DATA_CENTER: &str = "datacake-dc-unknown";
pub static DEFAULT_CLUSTER_ID: &str = "datacake-cluster-unknown";
const DEFAULT_REPAIR_INTERVAL: Duration = if cfg!(any(test, feature = "test-utils")) {
    Duration::from_secs(1)
} else {
    Duration::from_secs(60 * 60) // 1 Hour
};

/// Non-required configurations for the datacake cluster node.
pub struct ClusterOptions {
    cluster_id: String,
    data_center: Cow<'static, str>,
    repair_interval: Duration,
}

impl Default for ClusterOptions {
    fn default() -> Self {
        Self {
            cluster_id: DEFAULT_CLUSTER_ID.to_string(),
            data_center: Cow::Borrowed(DEFAULT_DATA_CENTER),
            repair_interval: DEFAULT_REPAIR_INTERVAL,
        }
    }
}

impl ClusterOptions {
    /// Set the cluster id for the given node.
    pub fn with_cluster_id(mut self, cluster_id: impl Display) -> Self {
        self.cluster_id = cluster_id.to_string();
        self
    }

    /// Set the data center the node belongs to.
    pub fn with_data_center(mut self, dc: impl Display) -> Self {
        self.data_center = Cow::Owned(dc.to_string());
        self
    }

    pub fn with_repair_interval(mut self, interval: Duration) -> Self {
        self.repair_interval = interval;
        self
    }
}

#[derive(Debug, Clone)]
/// Configuration for the cluster network.
pub struct ConnectionConfig {
    /// The binding address for the RPC server to bind and listen on.
    ///
    /// This is often `0.0.0.0` + your chosen port.
    pub listen_addr: SocketAddr,

    /// The public address to be broadcast to other cluster members.
    ///
    /// This is normally the machine's public IP address and the port the server is listening on.
    pub public_addr: SocketAddr,

    /// A set of initial seed nodes which the node will attempt to connect to and learn of any
    /// other members in the cluster.
    ///
    /// Normal `2` or `3` seeds is fine when running a multi-node cluster.
    /// Having only `1` seed can be dangerous if both nodes happen to go down but the seed
    /// does not restart before this node, as it will be unable to re-join the cluster.
    pub seed_nodes: Vec<String>,
}

impl ConnectionConfig {
    /// Creates a new connection config.
    pub fn new(
        listen_addr: SocketAddr,
        public_addr: SocketAddr,
        seeds: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Self {
        Self {
            listen_addr,
            public_addr,
            seed_nodes: seeds
                .into_iter()
                .map(|seed| seed.as_ref().to_string())
                .collect(),
        }
    }
}

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
    node_selector: NodeSelectorHandle,
    task_service: TaskDistributor,
    statistics: ClusterStatistics,
    _transport: Box<dyn Transport>,
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
    pub async fn connect<DS>(
        node_id: impl Into<String>,
        connection_cfg: ConnectionConfig,
        datastore: S,
        node_selector: DS,
        options: ClusterOptions,
    ) -> Result<Self, error::DatacakeError<S::Error>>
    where
        DS: NodeSelector + Send + 'static,
    {
        Self::connect_with_registry(
            node_id,
            connection_cfg,
            datastore,
            node_selector,
            DefaultRegistry,
            options,
        )
        .await
    }

    /// Starts the Datacake cluster with a custom service registry, connecting to the targeted seed nodes.
    ///
    /// A custom service registry can be used in order to add additional GRPC services to the
    /// RPC server in order to avoid listening on multiple addresses.
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
    pub async fn connect_with_registry<DS, R>(
        node_id: impl Into<String>,
        connection_cfg: ConnectionConfig,
        datastore: S,
        node_selector: DS,
        service_registry: R,
        options: ClusterOptions,
    ) -> Result<Self, error::DatacakeError<S::Error>>
    where
        DS: NodeSelector + Send + 'static,
        R: ServiceRegistry + Send + Sync + Clone + 'static,
    {
        let node_id = node_id.into();

        let clock = Clock::new(crc32fast::hash(node_id.as_bytes()));
        let storage = Arc::new(datastore);

        let group = KeyspaceGroup::new(storage.clone(), clock.clone()).await;
        let network = RpcNetwork::default();
        let statistics = ClusterStatistics::default();
        statistics.num_live_members.store(1, Ordering::Relaxed);
        statistics.num_data_centers.store(1, Ordering::Relaxed);

        // Load the keyspace states.
        group.load_states_from_storage().await?;

        let selector = nodes_selector::start_node_selector(
            connection_cfg.public_addr,
            options.data_center.clone(),
            node_selector,
        )
        .await;

        let cluster_info = ClusterInfo {
            listen_addr: connection_cfg.listen_addr,
            public_addr: connection_cfg.public_addr,
            seed_nodes: connection_cfg.seed_nodes,
            data_center: options.data_center.as_ref(),
        };
        let (node, transport) = connect_node(
            node_id.clone(),
            options.cluster_id.clone(),
            group.clone(),
            network.clone(),
            cluster_info,
            service_registry,
            statistics.clone(),
        )
        .await?;

        let task_ctx = TaskServiceContext {
            clock: group.clock().clone(),
            network: network.clone(),
            local_node_id: Cow::Owned(node_id.clone()),
            public_node_addr: node.public_addr,
        };
        let replication_ctx = ReplicationCycleContext {
            repair_interval: options.repair_interval,
            group: group.clone(),
            network: network.clone(),
        };
        let task_service = replication::start_task_distributor_service(task_ctx).await;
        let repair_service = replication::start_replication_cycle(replication_ctx).await;

        setup_poller(
            task_service.clone(),
            repair_service,
            network.clone(),
            &node,
            selector.clone(),
            statistics.clone(),
        )
        .await;

        info!(
            node_id = %node_id,
            cluster_id = %options.cluster_id,
            listen_addr = %connection_cfg.listen_addr,
            "Datacake cluster connected."
        );

        Ok(Self {
            node,
            network,
            group,
            clock,
            statistics,
            task_service,
            node_selector: selector,
            // Needs to live to run the network.
            _transport: transport,
        })
    }

    /// Shuts down the cluster and cleans up any connections.
    pub async fn shutdown(self) {
        self.node.shutdown().await;
    }

    #[inline]
    /// Gets the live cluster statistics.
    pub fn statistics(&self) -> &ClusterStatistics {
        &self.statistics
    }

    /// Creates a new handle to the underlying storage system.
    ///
    /// Changes applied to the handle are distributed across the cluster.
    pub fn handle(&self) -> DatacakeHandle<S> {
        DatacakeHandle {
            node_id: Cow::Owned(self.node.node_id.clone()),
            public_addr: self.node.public_addr,
            network: self.network.clone(),
            group: self.group.clone(),
            clock: self.clock.clone(),
            node_selector: self.node_selector.clone(),
            task_service: self.task_service.clone(),
            statistics: self.statistics.clone(),
        }
    }

    /// Creates a new handle to the underlying storage system with a preset keyspace.
    ///
    /// Changes applied to the handle are distributed across the cluster.
    pub fn handle_with_keyspace(
        &self,
        keyspace: impl Into<String>,
    ) -> DatacakeKeyspaceHandle<S> {
        DatacakeKeyspaceHandle {
            inner: self.handle(),
            keyspace: Cow::Owned(keyspace.into()),
        }
    }

    /// Waits for the provided set of node_ids to be part of the cluster.
    pub async fn wait_for_nodes(
        &self,
        node_ids: &[impl AsRef<str>],
        timeout: Duration,
    ) -> Result<(), anyhow::Error> {
        self.node
            .wait_for_members(
                |members| {
                    node_ids.iter().all(|node| {
                        members
                            .iter()
                            .map(|m| m.node_id.as_str())
                            .contains(&node.as_ref())
                    })
                },
                timeout,
            )
            .await
    }
}

/// A cheaply cloneable handle to control the data store.
pub struct DatacakeHandle<S>
where
    S: Storage + Send + Sync + 'static,
{
    node_id: Cow<'static, str>,
    public_addr: SocketAddr,
    network: RpcNetwork,
    group: KeyspaceGroup<S>,
    clock: Clock,
    node_selector: NodeSelectorHandle,
    task_service: TaskDistributor,
    statistics: ClusterStatistics,
}

impl<S> Clone for DatacakeHandle<S>
where
    S: Storage + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id.clone(),
            public_addr: self.public_addr,
            network: self.network.clone(),
            group: self.group.clone(),
            clock: self.clock.clone(),
            node_selector: self.node_selector.clone(),
            task_service: self.task_service.clone(),
            statistics: self.statistics.clone(),
        }
    }
}

impl<S> DatacakeHandle<S>
where
    S: Storage + Send + Sync + 'static,
{
    #[inline]
    /// Gets the live cluster statistics.
    pub fn statistics(&self) -> &ClusterStatistics {
        &self.statistics
    }

    /// Creates a new handle to the underlying storage system with a preset keyspace.
    ///
    /// Changes applied to the handle are distributed across the cluster.
    pub fn with_keyspace(
        &self,
        keyspace: impl Into<String>,
    ) -> DatacakeKeyspaceHandle<S> {
        DatacakeKeyspaceHandle {
            inner: self.clone(),
            keyspace: Cow::Owned(keyspace.into()),
        }
    }

    /// Retrieves a document from the underlying storage.
    pub async fn get(
        &self,
        keyspace: &str,
        doc_id: Key,
    ) -> Result<Option<Document>, S::Error> {
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
        I: IntoIterator<IntoIter = T> + Send,
    {
        let storage = self.group.storage();
        storage.multi_get(keyspace, doc_ids.into_iter()).await
    }

    /// Insert or update a single document into the datastore.
    pub async fn put<D>(
        &self,
        keyspace: &str,
        doc_id: Key,
        data: D,
        consistency: Consistency,
    ) -> Result<(), error::DatacakeError<S::Error>>
    where
        D: Into<Bytes>,
    {
        let nodes = self
            .node_selector
            .get_nodes(consistency)
            .await
            .map_err(error::DatacakeError::ConsistencyError)?;

        let node_id = self.node_id.clone();
        let node_addr = self.public_addr;
        let last_updated = self.clock.get_time().await;
        let document = Document::new(doc_id, last_updated, data);

        info!(node_id = %self.node_id, doc_id = doc_id, ts = %last_updated, "Putting document");

        let keyspace = self.group.get_or_create_keyspace(keyspace).await;
        let msg = Set {
            source: CONSISTENCY_SOURCE_ID,
            doc: document.clone(),
            ctx: None,
            _marker: PhantomData::<S>::default(),
        };
        keyspace.send(msg).await?;

        // Register mutation with the distributor service.
        self.task_service.mutation(Mutation::Put {
            keyspace: Cow::Owned(keyspace.name().to_string()),
            doc: document.clone(),
        });

        let factory = |node| {
            let clock = self.group.clock().clone();
            let keyspace = keyspace.name().to_string();
            let document = document.clone();
            let node_id = node_id.clone();
            async move {
                let channel = self
                    .network
                    .get_or_connect(node)
                    .await
                    .map_err(|e| error::DatacakeError::TransportError(node, e))?;

                let mut client = ConsistencyClient::new(clock, channel);

                client
                    .put(keyspace, document, &node_id, node_addr)
                    .await
                    .map_err(|e| error::DatacakeError::RpcError(node, e))?;

                Ok::<_, error::DatacakeError<S::Error>>(())
            }
        };

        handle_consistency_distribution::<S, _, _>(nodes, factory).await
    }

    /// Insert or update multiple documents into the datastore at once.
    pub async fn put_many<I, T, D>(
        &self,
        keyspace: &str,
        documents: I,
        consistency: Consistency,
    ) -> Result<(), error::DatacakeError<S::Error>>
    where
        D: Into<Bytes>,
        T: Iterator<Item = (Key, D)> + Send,
        I: IntoIterator<IntoIter = T> + Send,
    {
        let nodes = self
            .node_selector
            .get_nodes(consistency)
            .await
            .map_err(error::DatacakeError::ConsistencyError)?;

        let node_id = self.node_id.clone();
        let node_addr = self.public_addr;
        let last_updated = self.clock.get_time().await;
        let docs = documents
            .into_iter()
            .map(|(id, data)| Document::new(id, last_updated, data))
            .collect::<Vec<_>>();

        let keyspace = self.group.get_or_create_keyspace(keyspace).await;
        let msg = MultiSet {
            source: CONSISTENCY_SOURCE_ID,
            docs: docs.clone(),
            ctx: None,
            _marker: PhantomData::<S>::default(),
        };
        keyspace.send(msg).await?;

        // Register mutation with the distributor service.
        self.task_service.mutation(Mutation::MultiPut {
            keyspace: Cow::Owned(keyspace.name().to_string()),
            docs: docs.clone(),
        });

        let factory = |node| {
            let clock = self.group.clock().clone();
            let keyspace = keyspace.name().to_string();
            let documents = docs.clone();
            let node_id = node_id.clone();
            let node_addr = node_addr;
            async move {
                let channel = self
                    .network
                    .get_or_connect(node)
                    .await
                    .map_err(|e| error::DatacakeError::TransportError(node, e))?;

                let mut client = ConsistencyClient::new(clock, channel);

                client
                    .multi_put(keyspace, documents.into_iter(), &node_id, node_addr)
                    .await
                    .map_err(|e| error::DatacakeError::RpcError(node, e))?;

                Ok::<_, error::DatacakeError<S::Error>>(())
            }
        };

        handle_consistency_distribution::<S, _, _>(nodes, factory).await
    }

    /// Delete a document from the datastore with a given doc ID.
    pub async fn del(
        &self,
        keyspace: &str,
        doc_id: Key,
        consistency: Consistency,
    ) -> Result<(), error::DatacakeError<S::Error>> {
        let nodes = self
            .node_selector
            .get_nodes(consistency)
            .await
            .map_err(error::DatacakeError::ConsistencyError)?;

        let last_updated = self.clock.get_time().await;

        let keyspace = self.group.get_or_create_keyspace(keyspace).await;
        let msg = Del {
            source: CONSISTENCY_SOURCE_ID,
            doc_id,
            ts: last_updated,
            _marker: PhantomData::<S>::default(),
        };
        keyspace.send(msg).await?;

        // Register mutation with the distributor service.
        self.task_service.mutation(Mutation::Del {
            keyspace: Cow::Owned(keyspace.name().to_string()),
            doc_id,
            ts: last_updated,
        });

        let factory = |node| {
            let clock = self.group.clock().clone();
            let keyspace = keyspace.name().to_string();
            async move {
                let channel = self
                    .network
                    .get_or_connect(node)
                    .await
                    .map_err(|e| error::DatacakeError::TransportError(node, e))?;

                let mut client = ConsistencyClient::new(clock, channel);

                client
                    .del(keyspace, doc_id, last_updated)
                    .await
                    .map_err(|e| error::DatacakeError::RpcError(node, e))?;

                Ok::<_, error::DatacakeError<S::Error>>(())
            }
        };

        handle_consistency_distribution::<S, _, _>(nodes, factory).await
    }

    /// Delete multiple documents from the datastore from the set of doc IDs.
    pub async fn del_many<I, T>(
        &self,
        keyspace: &str,
        doc_ids: I,
        consistency: Consistency,
    ) -> Result<(), error::DatacakeError<S::Error>>
    where
        T: Iterator<Item = Key> + Send,
        I: IntoIterator<IntoIter = T> + Send,
    {
        let nodes = self
            .node_selector
            .get_nodes(consistency)
            .await
            .map_err(error::DatacakeError::ConsistencyError)?;

        let last_updated = self.clock.get_time().await;
        let docs = doc_ids
            .into_iter()
            .map(|id| (id, last_updated))
            .collect::<Vec<_>>();

        let keyspace = self.group.get_or_create_keyspace(keyspace).await;
        let msg = MultiDel {
            source: CONSISTENCY_SOURCE_ID,
            key_ts_pairs: docs.clone(),
            _marker: PhantomData::<S>::default(),
        };
        keyspace.send(msg).await?;

        // Register mutation with the distributor service.
        self.task_service.mutation(Mutation::MultiDel {
            keyspace: Cow::Owned(keyspace.name().to_string()),
            docs: docs.clone(),
        });

        let factory = |node| {
            let clock = self.group.clock().clone();
            let keyspace = keyspace.name().to_string();
            let docs = docs.clone();
            async move {
                let channel = self
                    .network
                    .get_or_connect(node)
                    .await
                    .map_err(|e| error::DatacakeError::TransportError(node, e))?;

                let mut client = ConsistencyClient::new(clock, channel);

                client
                    .multi_del(keyspace, docs.into_iter())
                    .await
                    .map_err(|e| error::DatacakeError::RpcError(node, e))?;

                Ok::<_, error::DatacakeError<S::Error>>(())
            }
        };

        handle_consistency_distribution::<S, _, _>(nodes, factory).await
    }
}

/// A convenience wrapper which creates a new handle with a preset keyspace.
pub struct DatacakeKeyspaceHandle<S>
where
    S: Storage + Send + Sync + 'static,
{
    inner: DatacakeHandle<S>,
    keyspace: Cow<'static, str>,
}

impl<S> Clone for DatacakeKeyspaceHandle<S>
where
    S: Storage + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            keyspace: self.keyspace.clone(),
        }
    }
}

impl<S> DatacakeKeyspaceHandle<S>
where
    S: Storage + Send + Sync + 'static,
{
    /// Retrieves a document from the underlying storage.
    pub async fn get(&self, doc_id: Key) -> Result<Option<Document>, S::Error> {
        self.inner.get(self.keyspace.as_ref(), doc_id).await
    }

    /// Retrieves a set of documents from the underlying storage.
    ///
    /// If a document does not exist with the given ID, it is simply not part
    /// of the returned iterator.
    pub async fn get_many<I, T>(&self, doc_ids: I) -> Result<S::DocsIter, S::Error>
    where
        T: Iterator<Item = Key> + Send,
        I: IntoIterator<IntoIter = T> + Send,
    {
        self.inner.get_many(self.keyspace.as_ref(), doc_ids).await
    }

    /// Insert or update a single document into the datastore.
    pub async fn put(
        &self,
        doc_id: Key,
        data: Vec<u8>,
        consistency: Consistency,
    ) -> Result<(), error::DatacakeError<S::Error>> {
        self.inner
            .put(self.keyspace.as_ref(), doc_id, data, consistency)
            .await
    }

    /// Insert or update multiple documents into the datastore at once.
    pub async fn put_many<I, T>(
        &self,
        documents: I,
        consistency: Consistency,
    ) -> Result<(), error::DatacakeError<S::Error>>
    where
        T: Iterator<Item = (Key, Vec<u8>)> + Send,
        I: IntoIterator<IntoIter = T> + Send,
    {
        self.inner
            .put_many(self.keyspace.as_ref(), documents, consistency)
            .await
    }

    /// Delete a document from the datastore with a given doc ID.
    pub async fn del(
        &self,
        doc_id: Key,
        consistency: Consistency,
    ) -> Result<(), error::DatacakeError<S::Error>> {
        self.inner
            .del(self.keyspace.as_ref(), doc_id, consistency)
            .await
    }

    /// Delete multiple documents from the datastore from the set of doc IDs.
    pub async fn del_many<I, T>(
        &self,
        doc_ids: I,
        consistency: Consistency,
    ) -> Result<(), error::DatacakeError<S::Error>>
    where
        T: Iterator<Item = Key> + Send,
        I: IntoIterator<IntoIter = T> + Send,
    {
        self.inner
            .del_many(self.keyspace.as_ref(), doc_ids, consistency)
            .await
    }
}

struct ClusterInfo<'a> {
    listen_addr: SocketAddr,
    public_addr: SocketAddr,
    seed_nodes: Vec<String>,
    data_center: &'a str,
}

/// Connects to the chitchat cluster.
///
/// The node will attempt to establish connections to the seed nodes and
/// will broadcast the node's public address to communicate.
async fn connect_node<S, R>(
    node_id: String,
    cluster_id: String,
    group: KeyspaceGroup<S>,
    network: RpcNetwork,
    cluster_info: ClusterInfo<'_>,
    service_registry: R,
    statistics: ClusterStatistics,
) -> Result<(DatacakeNode, Box<dyn Transport>), error::DatacakeError<S::Error>>
where
    S: Storage + Send + Sync + 'static,
    R: ServiceRegistry + Send + Sync + Clone + 'static,
{
    let (chitchat_tx, chitchat_rx) = flume::bounded(1000);
    let context = Context {
        chitchat_messages: chitchat_tx,
        keyspace_group: group,
        service_registry,
        network,
    };
    let transport = GrpcTransport::new(context, chitchat_rx);

    let me =
        ClusterMember::new(node_id, cluster_info.public_addr, cluster_info.data_center);
    let node = DatacakeNode::connect(
        me,
        cluster_info.listen_addr,
        cluster_id,
        cluster_info.seed_nodes,
        FailureDetectorConfig::default(),
        &transport,
        statistics,
    )
    .await?;

    Ok((node, Box::new(transport)))
}

/// Starts the background task which watches for membership changes
/// intern starting and stopping polling services for each member.
async fn setup_poller(
    task_service: TaskDistributor,
    repair_service: ReplicationHandle,
    network: RpcNetwork,
    node: &DatacakeNode,
    node_selector: NodeSelectorHandle,
    statistics: ClusterStatistics,
) {
    let changes = node.member_change_watcher();
    let self_node_id = Cow::Owned(node.node_id.clone());
    tokio::spawn(watch_membership_changes(
        self_node_id,
        task_service,
        repair_service,
        network,
        node_selector,
        changes,
        statistics,
    ));
}

/// Watches for changes in the cluster membership.
///
/// When nodes leave and join, pollers are stopped and started as required.
async fn watch_membership_changes(
    self_node_id: Cow<'static, str>,
    task_service: TaskDistributor,
    repair_service: ReplicationHandle,
    network: RpcNetwork,
    node_selector: NodeSelectorHandle,
    mut changes: WatchStream<Vec<ClusterMember>>,
    statistics: ClusterStatistics,
) {
    let mut last_network_set = HashSet::new();
    while let Some(members) = changes.next().await {
        info!(
            self_node_id = %self_node_id,
            num_members = members.len(),
            "Cluster membership has changed."
        );

        let new_network_set = members
            .iter()
            .filter(|member| member.node_id != self_node_id.as_ref())
            .map(|member| (member.node_id.clone(), member.public_addr))
            .collect::<HashSet<_>>();

        {
            let mut data_centers = BTreeMap::<Cow<'static, str>, Vec<SocketAddr>>::new();
            for member in members.iter() {
                let dc = Cow::Owned(member.data_center.clone());
                data_centers.entry(dc).or_default().push(member.public_addr);
            }

            statistics
                .num_data_centers
                .store(data_centers.len() as u64, Ordering::Relaxed);
            node_selector.set_nodes(data_centers).await;
        }

        let mut membership_changes = MembershipChanges::default();
        // Remove client no longer apart of the network.
        for (node_id, addr) in last_network_set.difference(&new_network_set) {
            info!(
                self_node_id = %self_node_id,
                target_node_id = %node_id,
                target_addr = %addr,
                "Node is no longer part of cluster."
            );

            network.disconnect(*addr);
            membership_changes.left.push(Cow::Owned(node_id.clone()));
        }

        // Add new clients for each new node.
        for (node_id, addr) in new_network_set.difference(&last_network_set) {
            info!(
                self_node_id = %self_node_id,
                target_node_id = %node_id,
                target_addr = %addr,
                "Node has connected to the cluster."
            );

            membership_changes
                .joined
                .push((Cow::Owned(node_id.clone()), *addr));
        }

        task_service.membership_change(membership_changes.clone());
        repair_service.membership_change(membership_changes.clone());

        last_network_set = new_network_set;
    }
}

async fn handle_consistency_distribution<S, CB, F>(
    nodes: Vec<SocketAddr>,
    factory: CB,
) -> Result<(), error::DatacakeError<S::Error>>
where
    S: Storage,
    CB: FnMut(SocketAddr) -> F,
    F: Future<Output = Result<(), error::DatacakeError<S::Error>>>,
{
    let mut num_success = 0;
    let num_required = nodes.len();

    let mut requests = nodes
        .into_iter()
        .map(factory)
        .collect::<FuturesUnordered<_>>();

    while let Some(res) = requests.next().await {
        match res {
            Ok(()) => {
                num_success += 1;
            },
            Err(error::DatacakeError::RpcError(node, error)) => {
                error!(
                    error = ?error,
                    target_node = %node,
                    "Replica failed to acknowledge change to meet consistency level requirement."
                );
            },
            Err(error::DatacakeError::TransportError(node, error)) => {
                error!(
                    error = ?error,
                    target_node = %node,
                    "Replica failed to acknowledge change to meet consistency level requirement."
                );
            },
            Err(other) => {
                error!(
                    error = ?other,
                    "Failed to send action to replica due to unknown error.",
                );
            },
        }
    }

    if num_success != num_required {
        Err(error::DatacakeError::ConsistencyError(
            ConsistencyError::ConsistencyFailure {
                responses: num_success,
                required: num_required,
                timeout: TIMEOUT_LIMIT,
            },
        ))
    } else {
        Ok(())
    }
}
