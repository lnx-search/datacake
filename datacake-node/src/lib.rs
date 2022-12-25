pub(crate) mod clock;
mod error;
mod extension;
mod node;
mod nodes_selector;
mod rpc;
mod statistics;

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;

use chitchat::transport::Transport;
use chitchat::FailureDetectorConfig;
pub use clock::Clock;
use datacake_rpc::{RpcService, Server};
pub use error::NodeError;
pub use extension::ClusterExtension;
use futures::StreamExt;
pub use node::{ChitchatNode, ClusterMember};
pub use nodes_selector::{
    Consistency,
    ConsistencyError,
    DCAwareSelector,
    NodeSelector,
    NodeSelectorHandle,
};
pub use rpc::network::RpcNetwork;
pub use statistics::ClusterStatistics;
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;
use tracing::info;

use crate::rpc::chitchat_transport::ChitchatTransport;
use crate::rpc::services::chitchat_impl::ChitchatService;

pub static DEFAULT_CLUSTER_ID: &str = "datacake-cluster-unknown";
pub static DEFAULT_DATA_CENTER: &str = "datacake-dc-unknown";

/// Build a datacake node using provided settings.
pub struct DatacakeNodeBuilder<S> {
    node_id: String,
    connection_cfg: ConnectionConfig,
    cluster_id: String,
    data_center: Cow<'static, str>,
    node_selector: S,
}

impl<S> DatacakeNodeBuilder<S>
where
    S: NodeSelector + Send + 'static,
{
    /// Create a new node builder.
    pub fn new(
        node_id: impl Into<String>,
        connection_cfg: ConnectionConfig,
        selector: S,
    ) -> Self {
        Self {
            node_id: node_id.into(),
            connection_cfg,
            cluster_id: DEFAULT_CLUSTER_ID.to_string(),
            data_center: Cow::Borrowed(DEFAULT_DATA_CENTER),
            node_selector: selector,
        }
    }

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
    pub async fn connect(self) -> Result<DatacakeNode, NodeError> {
        let clock = Clock::new(crc32fast::hash(self.node_id.as_bytes()));

        let statistics = ClusterStatistics::default();
        let network = RpcNetwork::default();

        let rpc_server = Server::listen(self.connection_cfg.listen_addr).await?;
        let selector = nodes_selector::start_node_selector(
            self.connection_cfg.public_addr,
            self.data_center.clone(),
            self.node_selector,
        )
        .await;

        let cluster_info = ClusterInfo {
            listen_addr: self.connection_cfg.listen_addr,
            public_addr: self.connection_cfg.public_addr,
            seed_nodes: self.connection_cfg.seed_nodes,
            data_center: self.data_center.as_ref(),
        };
        let (node, transport) = connect_node(
            self.node_id.clone(),
            self.cluster_id.clone(),
            clock.clone(),
            network.clone(),
            cluster_info,
            &rpc_server,
            statistics.clone(),
        )
        .await?;

        let (tx, membership_changes) = watch::channel(MembershipChange::default());
        tokio::spawn(watch_membership_changes(
            Cow::Owned(self.node_id.clone()),
            network.clone(),
            selector.clone(),
            statistics.clone(),
            node.member_change_watcher(),
            tx,
        ));

        info!(
            node_id = %self.node_id,
            cluster_id = %self.cluster_id,
            listen_addr = %self.connection_cfg.listen_addr,
            "Datacake cluster connected."
        );

        Ok(DatacakeNode {
            rpc_server,
            node,
            network,
            clock,
            selector,
            membership_changes,
            // Needs to live to run the network.
            _transport: transport,
        })
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

pub struct DatacakeNode {
    node: ChitchatNode,
    rpc_server: Server,
    clock: Clock,
    network: RpcNetwork,
    selector: NodeSelectorHandle,
    membership_changes: watch::Receiver<MembershipChange>,
    _transport: Box<dyn Transport>,
}

impl DatacakeNode {
    /// Shuts down the cluster and cleans up any connections.
    pub async fn shutdown(self) {
        self.node.shutdown().await;
    }

    /// Add a RPC service to the existing RPC system.
    pub fn add_rpc_service<Svc>(&self, service: Svc)
    where
        Svc: RpcService + Send + Sync + 'static,
    {
        self.rpc_server.add_service(service);
    }

    #[inline]
    /// Gets the live cluster statistics.
    pub fn statistics(&self) -> ClusterStatistics {
        self.node.statistics()
    }

    #[inline]
    /// Get access to the cluster clock.
    pub fn clock(&self) -> &Clock {
        &self.clock
    }

    #[inline]
    /// Get access to the current RPC network.
    pub fn network(&self) -> &RpcNetwork {
        &self.network
    }

    #[inline]
    /// Return the cluster member of the node itself.
    pub fn me(&self) -> &ClusterMember {
        self.node.me.as_ref()
    }

    #[inline]
    /// Get a stream of membership changes.
    pub fn membership_changes(&self) -> WatchStream<MembershipChange> {
        WatchStream::new(self.membership_changes.clone())
    }

    #[inline]
    /// Selects a set of nodes using a provided consistency level.
    pub async fn select_nodes(
        &self,
        consistency: Consistency,
    ) -> Result<Vec<SocketAddr>, ConsistencyError> {
        self.selector.get_nodes(consistency).await
    }

    #[inline]
    /// Creates a handle to the cluster providing the core functionality of the node.
    pub fn handle(&self) -> DatacakeHandle {
        DatacakeHandle {
            me: self.node.me.clone(),
            clock: self.clock.clone(),
            network: self.network.clone(),
            selector: self.selector.clone(),
            statistics: self.statistics(),
            membership_changes: self.membership_changes.clone(),
        }
    }
}

#[derive(Clone)]
pub struct DatacakeHandle {
    me: Cow<'static, ClusterMember>,
    clock: Clock,
    network: RpcNetwork,
    selector: NodeSelectorHandle,
    statistics: ClusterStatistics,
    membership_changes: watch::Receiver<MembershipChange>,
}

impl DatacakeHandle {
    #[inline]
    /// Gets the live cluster statistics.
    pub fn statistics(&self) -> ClusterStatistics {
        self.statistics.clone()
    }

    #[inline]
    /// Get access to the cluster clock.
    pub fn clock(&self) -> &Clock {
        &self.clock
    }

    #[inline]
    /// Get access to the current RPC network.
    pub fn network(&self) -> &RpcNetwork {
        &self.network
    }

    #[inline]
    /// Get a stream of membership changes.
    pub fn membership_changes(&self) -> WatchStream<MembershipChange> {
        WatchStream::new(self.membership_changes.clone())
    }

    #[inline]
    /// Return the cluster member of the node itself.
    pub fn me(&self) -> &ClusterMember {
        self.me.as_ref()
    }

    #[inline]
    /// Selects a set of nodes using a provided consistency level.
    pub async fn select_nodes(
        &self,
        consistency: Consistency,
    ) -> Result<Vec<SocketAddr>, ConsistencyError> {
        self.selector.get_nodes(consistency).await
    }
}

#[derive(Clone, Default)]
pub struct MembershipChange {
    pub joined: Vec<ClusterMember>,
    pub left: Vec<ClusterMember>,
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
async fn connect_node(
    node_id: String,
    cluster_id: String,
    clock: Clock,
    network: RpcNetwork,
    cluster_info: ClusterInfo<'_>,
    server: &Server,
    statistics: ClusterStatistics,
) -> Result<(ChitchatNode, Box<dyn Transport>), NodeError> {
    let (chitchat_tx, chitchat_rx) = flume::bounded(1000);

    let service = ChitchatService::new(clock.clone(), chitchat_tx);
    server.add_service(service);

    let transport =
        ChitchatTransport::new(cluster_info.listen_addr, clock, network, chitchat_rx);

    let me = ClusterMember::new(
        node_id,
        cluster_info.public_addr,
        cluster_info.data_center.to_string(),
    );
    let node = ChitchatNode::connect(
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

/// Watches for changes in the cluster membership.
///
/// When nodes leave and join, pollers are stopped and started as required.
async fn watch_membership_changes(
    self_node_id: Cow<'static, str>,
    network: RpcNetwork,
    node_selector: NodeSelectorHandle,
    statistics: ClusterStatistics,
    mut changes: WatchStream<BTreeMap<String, ClusterMember>>,
    membership_changes_tx: watch::Sender<MembershipChange>,
) {
    let mut last_network_set = BTreeSet::new();
    while let Some(members) = changes.next().await {
        info!(
            self_node_id = %self_node_id,
            num_members = members.len(),
            "Cluster membership has changed."
        );

        let mut membership_changes = MembershipChange::default();
        let new_network_set = members
            .iter()
            .filter(|(node_id, _)| node_id != &self_node_id.as_ref())
            .map(|(_, member)| (member.node_id.clone(), member.public_addr))
            .collect::<BTreeSet<_>>();

        {
            let mut data_centers = BTreeMap::<Cow<'static, str>, Vec<SocketAddr>>::new();
            for member in members.values() {
                let dc = Cow::Owned(member.data_center.clone());
                data_centers.entry(dc).or_default().push(member.public_addr);
            }

            statistics
                .num_data_centers
                .store(data_centers.len() as u64, Ordering::Relaxed);
            node_selector.set_nodes(data_centers).await;
        }

        // Remove client no longer apart of the network.
        for (node_id, addr) in last_network_set.difference(&new_network_set) {
            info!(
                self_node_id = %self_node_id,
                target_node_id = %node_id,
                target_addr = %addr,
                "Node is no longer part of cluster."
            );

            network.disconnect(*addr);

            if let Some(member) = members.get(node_id) {
                membership_changes.left.push(member.clone());
            }
        }

        // Add new clients for each new node.
        for (node_id, addr) in new_network_set.difference(&last_network_set) {
            info!(
                self_node_id = %self_node_id,
                target_node_id = %node_id,
                target_addr = %addr,
                "Node has connected to the cluster."
            );

            if let Some(member) = members.get(node_id) {
                membership_changes.joined.push(member.clone());
            }
        }

        let _ = membership_changes_tx.send(membership_changes);
        last_network_set = new_network_set;
    }
}
