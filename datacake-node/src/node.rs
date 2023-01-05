use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::Debug;
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
use tracing::{debug, error, info};

use crate::error::NodeError;
use crate::statistics::ClusterStatistics;
use crate::DEFAULT_DATA_CENTER;

static DATA_CENTER_KEY: &str = "data_center";
const GOSSIP_INTERVAL: Duration = if cfg!(test) {
    Duration::from_millis(500)
} else {
    Duration::from_secs(1)
};
pub type NodeMembership = BTreeMap<crate::NodeId, ClusterMember>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterMember {
    /// A unique ID for the given node in the cluster.
    pub node_id: crate::NodeId,
    /// The public address of the nod.
    pub public_addr: SocketAddr,
    /// The data center / availability zone the node is in.
    ///
    /// This is used to select nodes for sending consistency tasks to.
    pub data_center: String,
}

impl ClusterMember {
    pub fn new(
        node_id: crate::NodeId,
        public_addr: SocketAddr,
        data_center: String,
    ) -> Self {
        Self {
            node_id,
            public_addr,
            data_center,
        }
    }

    pub fn chitchat_id(&self) -> String {
        self.node_id.to_string()
    }
}

impl From<ClusterMember> for NodeId {
    fn from(member: ClusterMember) -> Self {
        Self::new(member.chitchat_id(), member.public_addr)
    }
}

pub struct ChitchatNode {
    pub me: Cow<'static, ClusterMember>,
    statistics: ClusterStatistics,
    chitchat_handle: ChitchatHandle,
    members: watch::Receiver<NodeMembership>,
    stop: Arc<AtomicBool>,
}

impl ChitchatNode {
    pub async fn connect(
        me: ClusterMember,
        listen_addr: SocketAddr,
        cluster_id: String,
        seed_nodes: Vec<String>,
        failure_detector_config: FailureDetectorConfig,
        transport: &dyn Transport,
        statistics: ClusterStatistics,
    ) -> Result<Self, NodeError> {
        info!(
            cluster_id = %cluster_id,
            node_id = %me.node_id,
            public_addr = %me.public_addr,
            listen_gossip_addr = %listen_addr,
            peer_seed_addrs = %seed_nodes.join(", "),
            "Joining cluster."
        );

        statistics.num_live_members.store(1, Ordering::Relaxed);
        statistics.num_data_centers.store(1, Ordering::Relaxed);

        let cfg = ChitchatConfig {
            node_id: NodeId::from(me.clone()),
            cluster_id: cluster_id.clone(),
            gossip_interval: GOSSIP_INTERVAL,
            listen_addr,
            seed_nodes,
            failure_detector_config,
            is_ready_predicate: None,
        };

        let chitchat_handle = spawn_chitchat(
            cfg,
            vec![(DATA_CENTER_KEY.to_string(), me.data_center.clone())],
            transport,
        )
        .await
        .map_err(|e| NodeError::ChitChat(e.to_string()))?;

        let chitchat = chitchat_handle.chitchat();
        let (members_tx, members_rx) = watch::channel(BTreeMap::new());

        let cluster = ChitchatNode {
            me: Cow::Owned(me.clone()),
            chitchat_handle,
            statistics: statistics.clone(),
            members: members_rx,
            stop: Arc::new(Default::default()),
        };

        let initial_members: BTreeMap<crate::NodeId, ClusterMember> =
            BTreeMap::from_iter([(me.node_id, me.clone())]);
        if members_tx.send(initial_members).is_err() {
            error!("Failed to add itself as the initial member of the cluster.");
        }

        let stop_flag = cluster.stop.clone();
        tokio::spawn(async move {
            let mut node_change_rx = chitchat.lock().await.ready_nodes_watcher();

            while let Some(members_set) = node_change_rx.next().await {
                let state_snapshot = {
                    let lock = chitchat.lock().await;
                    let dead_member_count = lock.dead_nodes().count();

                    statistics
                        .num_dead_members
                        .store(dead_member_count as u64, Ordering::Relaxed);
                    lock.state_snapshot()
                };

                let mut members = members_set
                    .into_iter()
                    .map(|node_id| build_cluster_member(&node_id, &state_snapshot))
                    .filter_map(|member_res| {
                        match member_res {
                            Ok(member) => {
                                Some((member.node_id.clone(), member))
                            },
                            Err(error) => {
                                error!(
                                    error = ?error,
                                    "Failed to build cluster member from cluster state, ignoring member.",
                                );
                                None
                            },
                        }
                    })
                    .collect::<BTreeMap<_, _>>();
                members.insert(me.node_id.clone(), me.clone());

                statistics
                    .num_live_members
                    .store(members.len() as u64, Ordering::Relaxed);

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

            Result::<(), NodeError>::Ok(())
        });

        Ok(cluster)
    }

    /// Return [WatchStream] for monitoring change of node members.
    pub fn member_change_watcher(&self) -> WatchStream<NodeMembership> {
        WatchStream::new(self.members.clone())
    }

    /// Returns a handle to the members watcher channel.
    pub fn members_watcher(&self) -> watch::Receiver<NodeMembership> {
        self.members.clone()
    }

    #[cfg(test)]
    /// Returns a list of node members.
    pub fn members(&self) -> NodeMembership {
        self.members.borrow().clone()
    }

    #[inline]
    /// Get a handle to the live statistics.
    pub fn statistics(&self) -> ClusterStatistics {
        self.statistics.clone()
    }

    /// Leave the cluster.
    pub async fn shutdown(self) {
        info!(self_addr = ?self.me.public_addr, "Shutting down the cluster.");
        let result = self.chitchat_handle.shutdown().await;
        if let Err(error) = result {
            error!(self_addr = ?self.me.public_addr, error = ?error, "Error while shutting down.");
        }

        self.stop.store(true, Ordering::Relaxed);
    }

    /// Convenience method for testing that waits for the predicate to hold true for the cluster's
    /// members.
    pub async fn wait_for_members<F>(
        self: &ChitchatNode,
        mut predicate: F,
        timeout_after: Duration,
    ) -> Result<(), anyhow::Error>
    where
        F: FnMut(&NodeMembership) -> bool,
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
    chitchat_id: &'a NodeId,
    state: &'a ClusterStateSnapshot,
) -> Result<ClusterMember, String> {
    let node_state = state.node_states.get(&chitchat_id.id).ok_or_else(|| {
        format!(
            "Could not find node ID `{}` in ChitChat state.",
            chitchat_id.id
        )
    })?;

    let data_center = node_state
        .get(DATA_CENTER_KEY)
        .unwrap_or(DEFAULT_DATA_CENTER);

    let node_id = chitchat_id
        .id
        .parse::<crate::NodeId>()
        .map_err(|e| format!("Invalid node ID: {}", e))?;

    Ok(ClusterMember::new(
        node_id,
        chitchat_id.gossip_public_address,
        data_center.to_owned(),
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU8;

    use anyhow::Result;
    use chitchat::transport::{ChannelTransport, Transport};

    use super::*;

    #[tokio::test]
    async fn test_cluster_single_node() -> Result<()> {
        let _ = tracing_subscriber::fmt::try_init();

        let transport = ChannelTransport::default();
        let cluster = create_node_for_test(Vec::new(), &transport).await?;

        let members: Vec<SocketAddr> = cluster
            .members()
            .values()
            .map(|member| member.public_addr)
            .collect();
        let expected_members = vec![cluster.me.public_addr];
        assert_eq!(members, expected_members);
        cluster.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_propagated_state() -> Result<()> {
        let _ = tracing_subscriber::fmt::try_init();

        let transport = ChannelTransport::default();
        let node1 = create_node_for_test(Vec::new(), &transport).await?;
        let node_1_gossip_addr = node1.me.public_addr.to_string();
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

        for (id, member) in node1.members() {
            dbg!(id, &member.public_addr);
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

    pub async fn create_node_for_test_with_id(
        node_id: crate::NodeId,
        cluster_id: String,
        seeds: Vec<String>,
        transport: &dyn Transport,
    ) -> Result<ChitchatNode> {
        let public_addr: SocketAddr = ([127, 0, 0, 1], node_id as u16).into();
        let failure_detector_config = create_failure_detector_config_for_test();
        let node = ChitchatNode::connect(
            ClusterMember::new(node_id, public_addr, "unknown".to_string()),
            public_addr,
            cluster_id,
            seeds,
            failure_detector_config,
            transport,
            ClusterStatistics::default(),
        )
        .await?;
        Ok(node)
    }

    pub async fn create_node_for_test(
        seeds: Vec<String>,
        transport: &dyn Transport,
    ) -> Result<ChitchatNode> {
        static NODE_AUTO_INCREMENT: AtomicU8 = AtomicU8::new(1);
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
