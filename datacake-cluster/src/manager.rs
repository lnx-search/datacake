use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;

use chitchat::transport::UdpTransport;
use chitchat::FailureDetectorConfig;
use datacake_crdt::get_unix_timestamp_ms;
use futures::channel::oneshot;
use futures::StreamExt;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::WatchStream;

use crate::node::{ClusterMember, DatacakeNode};
use crate::rpc::{server, ClientCluster, DataHandler};
use crate::shard::state::StateWatcherHandle;
use crate::shard::{self, ShardGroupHandle};
use crate::tasks::tombstone_purge_task;
use crate::DatacakeError;

/// All network related configs for both gossip and RPC.
pub struct ConnectionCfg {
    /// The address that other nodes can use to connect
    /// to the node's gossip communication.
    pub gossip_public_addr: SocketAddr,

    /// The listen address the node should use for the gossip address.
    ///
    /// Normally this is something along the lines of: `0.0.0.0:9999` or `127.0.0.1:9999`.
    pub gossip_listen_addr: SocketAddr,

    /// The address that other nodes can use to connect
    /// to the node's RPC communication.
    pub rpc_public_addr: SocketAddr,

    /// The listen address the node should use for the gossip address.
    ///
    /// Normally this is something along the lines of: `0.0.0.0:9999` or `127.0.0.1:9999`.
    ///
    /// NOTE:
    ///  This cannot be the same address as the gossip listen address.
    pub rpc_listen_addr: SocketAddr,
}

pub struct DatacakeClusterManager<E> {
    rpc_server_shutdown: oneshot::Sender<()>,
    rpc_clients: ClientCluster,
    tasks: Vec<JoinHandle<()>>,
    node: DatacakeNode,
    _phantom: PhantomData<E>,
}

impl<E> DatacakeClusterManager<E>
where
    E: Display + Debug + Send + Sync + 'static,
{
    pub async fn connect(
        node_id: String,
        connection_cfg: ConnectionCfg,
        cluster_id: String,
        seed_nodes: Vec<String>,
        data_handler: Arc<dyn DataHandler<Error = E>>,
        shard_group: ShardGroupHandle,
        shard_changes_watcher: StateWatcherHandle,
    ) -> Result<Self, DatacakeError<E>> {
        info!(
            cluster_id = %cluster_id,
            node_id = %node_id,
            peer_seed_addrs = %seed_nodes.join(", "),
            num_shards = %shard::NUMBER_OF_SHARDS,
            "Starting Datacake cluster."
        );

        let rpc_clients = ClientCluster::default();
        let rpc_server_shutdown = server::start_rpc_server(
            shard_group.clone(),
            shard_changes_watcher,
            data_handler.clone(),
            connection_cfg.rpc_listen_addr,
        )
        .await?;

        let me = ClusterMember::new(
            node_id,
            get_unix_timestamp_ms(),
            connection_cfg.rpc_public_addr,
            connection_cfg.gossip_public_addr,
        );

        let node_id = me.node_id.clone();
        let node = DatacakeNode::connect(
            me,
            connection_cfg.gossip_listen_addr,
            cluster_id,
            seed_nodes,
            FailureDetectorConfig::default(),
            &UdpTransport,
        )
        .await?;

        let watcher = node.member_change_watcher();
        let state_changes_task = tokio::spawn(watch_for_remote_state_changes(
            node_id.clone(),
            watcher,
            rpc_clients.clone(),
            data_handler.clone(),
            shard_group.clone(),
        ));

        let purge_task = tokio::spawn(tombstone_purge_task(
            node_id.clone(),
            shard_group,
            data_handler,
        ));

        Ok(Self {
            rpc_server_shutdown,
            rpc_clients,
            tasks: vec![purge_task, state_changes_task],
            node,
            _phantom: PhantomData,
        })
    }

    #[inline]
    pub fn rpc_nodes(&self) -> &ClientCluster {
        &self.rpc_clients
    }

    pub async fn shutdown(self) -> Result<(), DatacakeError<E>> {
        self.node.shutdown().await;
        let _ = self.rpc_server_shutdown.send(());

        for task in self.tasks {
            task.abort();
        }

        Ok(())
    }
}

/// Watches any member state changes from the ChitChat cluster.
///
/// * The system first checks for any new member joins and disconnects
///   and attempts to establish the RPC connection.
///
/// * The previous known state of the shard is checked to see if any of it's
///   shards have changed. If they have, the synchronisation process is triggered,
///   otherwise the member is ignored.
async fn watch_for_remote_state_changes<E>(
    self_node_id: String,
    mut changes: WatchStream<Vec<ClusterMember>>,
    rpc_clients: ClientCluster,
    data_handler: Arc<dyn DataHandler<Error = E>>,
    shard_group: ShardGroupHandle,
) where
    E: Display + Debug + Send + Sync + 'static,
{
    let mut shard_states = HashMap::<String, SocketAddr>::new();
    while let Some(members) = changes.next().await {
        info!(
            node_id = %self_node_id,
            num_members = members.len(),
            "Member states have changed! Checking for new and dead members.",
        );

        // Make sure our remote nodes are handled.
        let iterator = members
            .iter()
            .filter(|member| member.node_id != self_node_id)
            .map(|member| (member.node_id.clone(), member.public_rpc_addr));

        let errors = rpc_clients.adjust_connected_clients(iterator).await;
        for (node_id, error) in errors {
            error!(
                node_id = %self_node_id,
                target_node_id = %node_id,
                error = ?error,
                "Failed to connect to remote node member.",
            );
        }

        for member in members
            .into_iter()
            .filter(|member| member.node_id != self_node_id)
        {
            if let Some(previous_addr) = shard_states.get(&member.node_id) {
                if (previous_addr == &member.public_rpc_addr)
                    && rpc_clients.get_client(&member.node_id).is_some()
                {
                    info!(
                        node_id = %self_node_id,
                        target_node_id = %member.node_id,
                        rpc_addr = %member.public_rpc_addr,
                        "Ignoring member setup, node already online.",
                    );
                    continue;
                }
            }

            let client = match rpc_clients.get_client(&member.node_id) {
                None => {
                    warn!(
                        node_id = %self_node_id,
                        target_node_id = %member.node_id,
                        "Potential logical error, node RPC clients should be connected before \
                        reaching this point unless a node has failed to establish a connection."
                    );
                    continue;
                },
                Some(client) => client,
            };

            shard_states.insert(member.node_id.clone(), member.public_rpc_addr);

            info!(
                node_id = %self_node_id,
                target_node_id = %member.node_id,
                rpc_addr = %member.public_rpc_addr,
                "Starting changes poller for node.",
            );

            let node = crate::tasks::NodeInfo {
                rpc: client,
                client_cluster: rpc_clients.clone(),
                data_handler: data_handler.clone(),
                shard_group: shard_group.clone(),
            };

            tokio::spawn(crate::tasks::shard_state_poller_task(member, node));
        }
    }
}
