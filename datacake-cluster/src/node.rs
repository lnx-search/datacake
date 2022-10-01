use std::fmt::{Debug, Display};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chitchat::transport::Transport;
use chitchat::{
    spawn_chitchat,
    ChitchatConfig,
    ChitchatHandle,
    ClusterStateSnapshot,
    FailureDetectorConfig,
    NodeId,
};
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;
use tokio_stream::StreamExt;
use crate::DatacakeError;

static PUBLIC_RPC_ADDR_KEY: &str = "public_rpc_addr";
const GOSSIP_INTERVAL: Duration = if cfg!(test) {
    Duration::from_millis(200)
} else {
    Duration::from_secs(1)
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterMember {
    /// A unique ID for the given node in the cluster.
    pub node_id: String,
    /// The timestamp(ms) of when the node starts.
    pub generation: u64,
    /// The public address of the node for gossip.
    pub public_gossip_addr: SocketAddr,
    /// The public address of the node for RPC.
    pub public_rpc_addr: SocketAddr,
}

impl ClusterMember {
    pub fn new(
        node_id: String,
        generation: u64,
        public_rpc_addr: SocketAddr,
        public_gossip_addr: SocketAddr,
    ) -> Self {
        Self {
            node_id,
            generation,
            public_gossip_addr,
            public_rpc_addr,
        }
    }

    pub fn chitchat_id(&self) -> String {
        format!("{}/{}", self.node_id, self.generation)
    }
}

impl From<ClusterMember> for NodeId {
    fn from(member: ClusterMember) -> Self {
        Self::new(member.chitchat_id(), member.public_gossip_addr)
    }
}

pub struct DatacakeNode {
    /// The ID of the cluster this node belongs to.
    pub cluster_id: String,
    /// The ID of the current node.
    pub node_id: String,
    /// The public address of the node for gossip.
    pub public_gossip_addr: SocketAddr,

    chitchat_handle: ChitchatHandle,
    members: watch::Receiver<Vec<ClusterMember>>,
    stop: Arc<AtomicBool>,
}

impl DatacakeNode {
    pub async fn connect<E>(
        me: ClusterMember,
        listen_addr: SocketAddr,
        cluster_id: String,
        seed_nodes: Vec<String>,
        failure_detector_config: FailureDetectorConfig,
        transport: &dyn Transport,
    ) -> Result<Self, DatacakeError<E>>
    where
        E: Display + Debug + Send + Sync + 'static
    {
        info!(
            cluster_id = %cluster_id,
            node_id = %me.node_id,
            public_rpc_addr = %me.public_rpc_addr,
            listen_gossip_addr = %listen_addr,
            public_gossip_addr = %me.public_gossip_addr,
            peer_seed_addrs = %seed_nodes.join(", "),
            "Joining cluster."
        );

        let cfg = ChitchatConfig {
            node_id: NodeId::from(me.clone()),
            cluster_id: cluster_id.clone(),
            gossip_interval: GOSSIP_INTERVAL,
            listen_addr,
            seed_nodes,
            failure_detector_config,
        };

        let chitchat_handle = spawn_chitchat(
            cfg,
            vec![(
                PUBLIC_RPC_ADDR_KEY.to_string(),
                me.public_rpc_addr.to_string(),
            )],
            transport,
        )
        .await.map_err(|e| DatacakeError::ChitChatError(e.to_string()))?;

        let chitchat = chitchat_handle.chitchat();
        let (members_tx, members_rx) = watch::channel(Vec::new());

        let cluster = DatacakeNode {
            cluster_id,
            node_id: me.chitchat_id(),
            public_gossip_addr: me.public_gossip_addr,
            chitchat_handle,
            members: members_rx,
            stop: Arc::new(Default::default()),
        };

        let initial_members: Vec<ClusterMember> = vec![me.clone()];
        if members_tx.send(initial_members).is_err() {
            error!("Failed to add itself as the initial member of the cluster.");
        }

        let stop_flag = cluster.stop.clone();
        tokio::spawn(async move {
            let mut node_change_rx = chitchat.lock().await.live_nodes_watcher();

            while let Some(members_set) = node_change_rx.next().await {
                let state_snapshot = chitchat.lock().await.state_snapshot();

                let mut members = members_set
                    .into_iter()
                    .map(|node_id| build_cluster_member(&node_id, &state_snapshot))
                    .filter_map(|member_res| {
                        // Just log an error for members that cannot be built.
                        if let Err(error) = &member_res {
                            error!(
                                error = ?error,
                                "Failed to build cluster member from cluster state, ignoring member.",
                            );
                        }
                        member_res.ok()
                    })
                    .collect::<Vec<_>>();
                members.push(me.clone());

                if stop_flag.load(Ordering::Relaxed) {
                    debug!("Received a stop signal. Stopping.");
                    break;
                }

                if members_tx.send(members).is_err() {
                    // Somehow the cluster has been dropped.
                    error!("Failed to update members list. Stopping.");
                    break;
                }
            }

            Result::<(), DatacakeError<E>>::Ok(())
        });

        Ok(cluster)
    }

    /// Return [WatchStream] for monitoring change of node members.
    pub fn member_change_watcher(&self) -> WatchStream<Vec<ClusterMember>> {
        WatchStream::new(self.members.clone())
    }

    #[cfg(test)]
    /// Returns a list of node members.
    pub fn members(&self) -> Vec<ClusterMember> {
        self.members.borrow().clone()
    }

    /// Leave the cluster.
    pub async fn shutdown(self) {
        info!(self_addr = ?self.public_gossip_addr, "Shutting down the cluster.");
        let result = self.chitchat_handle.shutdown().await;
        if let Err(error) = result {
            error!(self_addr = ?self.public_gossip_addr, error = ?error, "Error while shutting down.");
        }

        self.stop.store(true, Ordering::Relaxed);
    }

    #[cfg(test)]
    /// Convenience method for testing that waits for the predicate to hold true for the cluster's
    /// members.
    pub async fn wait_for_members<F>(
        self: &DatacakeNode,
        mut predicate: F,
        timeout_after: Duration,
    ) -> Result<(), anyhow::Error>
    where
        F: FnMut(&Vec<ClusterMember>) -> bool,
    {
        use tokio::time::timeout;

        timeout(
            timeout_after,
            self.member_change_watcher()
                .skip_while(|members| !predicate(members))
                .next(),
        )
        .await?;
        Ok(())
    }
}

fn build_cluster_member<'a>(
    node_id: &'a NodeId,
    cluster_state_snapshot: &'a ClusterStateSnapshot,
) -> Result<ClusterMember, String> {
    let node_state =
        cluster_state_snapshot
            .node_states
            .get(&node_id.id)
            .ok_or_else(|| {
                format!(
                    "Could not find node ID `{}` in ChitChat state.",
                    node_id.id,
                )
            })?;
    let public_rpc_address = node_state
        .get(PUBLIC_RPC_ADDR_KEY)
        .ok_or_else(|| {
            format!(
                "Could not find `{}` key in node {:?} state.",
                PUBLIC_RPC_ADDR_KEY,
                node_id
            )
        })
        .map(|addr_str| addr_str.parse::<SocketAddr>())?
        .map_err(|e| e.to_string())?;

    let (node_unique_id, generation_str) =
        node_id.id.split_once('/').ok_or_else(|| {
            format!(
                "Failed to create cluster member instance from NodeId {:?}.",
                node_id
            )
        })?;

    let generation = generation_str.parse::<u64>()
        .map_err(|e| e.to_string())?;

    Ok(ClusterMember::new(
        node_unique_id.to_string(),
        generation,
        public_rpc_address,
        node_id.gossip_public_address,
    ))
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use std::sync::atomic::AtomicU16;

    use chitchat::transport::{ChannelTransport, Transport};

    use super::*;

    #[tokio::test]
    async fn test_cluster_single_node() -> Result<()> {
        let _ = tracing_subscriber::fmt::try_init();

        let transport = ChannelTransport::default();
        let cluster = create_node_for_test(Vec::new(), &transport).await?;

        let members: Vec<SocketAddr> = cluster
            .members()
            .iter()
            .map(|member| member.public_gossip_addr)
            .collect();
        let expected_members = vec![cluster.public_gossip_addr];
        assert_eq!(members, expected_members);
        cluster.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_propagated_state() -> Result<()> {
        let _ = tracing_subscriber::fmt::try_init();

        let transport = ChannelTransport::default();
        let node1 = create_node_for_test(Vec::new(), &transport).await?;
        let node_1_gossip_addr = node1.public_gossip_addr.to_string();
        let node2 =
            create_node_for_test(vec![node_1_gossip_addr.clone()], &transport).await?;
        let node3 = create_node_for_test(vec![node_1_gossip_addr], &transport).await?;

        let wait_secs = Duration::from_secs(30);
        for cluster in [&node1, &node2, &node3] {
            cluster
                .wait_for_members(|members| members.len() == 3, wait_secs)
                .await
                .unwrap();
        }

        for member in node1.members() {
            dbg!(&member.public_rpc_addr);
        }

        Ok(())
    }

    fn create_failure_detector_config_for_test() -> FailureDetectorConfig {
        FailureDetectorConfig {
            phi_threshold: 6.0,
            initial_interval: GOSSIP_INTERVAL,
            ..Default::default()
        }
    }

    /// Compute the gRPC port from the chitchat listen address for tests.
    pub fn rpc_addr_from_listen_addr_for_test(listen_addr: SocketAddr) -> SocketAddr {
        let grpc_port = listen_addr.port() + 1u16;
        (listen_addr.ip(), grpc_port).into()
    }

    pub async fn create_node_for_test_with_id(
        node_id: u16,
        cluster_id: String,
        seeds: Vec<String>,
        transport: &dyn Transport,
    ) -> Result<DatacakeNode> {
        let gossip_advertise_addr: SocketAddr = ([127, 0, 0, 1], node_id).into();
        let node_id = format!("node_{node_id}");
        let failure_detector_config = create_failure_detector_config_for_test();
        let node = DatacakeNode::connect::<String>(
            ClusterMember::new(
                node_id,
                1,
                rpc_addr_from_listen_addr_for_test(gossip_advertise_addr),
                gossip_advertise_addr,
            ),
            gossip_advertise_addr,
            cluster_id,
            seeds,
            failure_detector_config,
            transport,
        )
        .await?;
        Ok(node)
    }

    pub async fn create_node_for_test(
        seeds: Vec<String>,
        transport: &dyn Transport,
    ) -> Result<DatacakeNode> {
        static NODE_AUTO_INCREMENT: AtomicU16 = AtomicU16::new(1u16);
        let node_id = NODE_AUTO_INCREMENT.fetch_add(1, Ordering::Relaxed);
        let node = create_node_for_test_with_id(
            node_id,
            "test-cluster".to_string(),
            seeds,
            transport,
        )
        .await?;
        Ok(node)
    }
}
