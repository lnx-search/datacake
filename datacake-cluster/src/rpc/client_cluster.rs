use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use parking_lot::RwLock;

use super::client::{connect_client, DataClient, SynchronisationClient};

#[derive(Clone)]
pub struct Client {
    pub public_addr: SocketAddr,
    pub sync: SynchronisationClient,
    pub data: DataClient,
}

#[derive(Default, Clone)]
pub struct ClientCluster {
    nodes: Arc<RwLock<HashMap<String, Client>>>,
}

impl ClientCluster {
    /// Drops a node's client.
    ///
    /// Once all requests have been handled, this will cause the client to shutdown.
    pub fn disconnect_node(&self, node_id: &str) {
        self.nodes.write().remove(node_id);
    }

    #[inline]
    /// The number of nodes currently connected in the cluster.
    pub fn live_nodes_count(&self) -> usize {
        self.nodes.read().len()
    }

    /// Disconnects any nodes that are not in the given set of node IDs.
    pub async fn adjust_connected_clients(
        &self,
        node_ids: impl Iterator<Item = (impl AsRef<str>, SocketAddr)>,
    ) -> Vec<(String, tonic::transport::Error)> {
        let mut errors = vec![];

        let mut existing_map = { self.nodes.read().clone() };
        let mut new_nodes = HashMap::new();

        for (node_id, remote_addr) in node_ids {
            let node_id = node_id.as_ref();

            if let Some(conn) = existing_map.remove(node_id) {
                if conn.public_addr == remote_addr {
                    new_nodes.insert(node_id.to_string(), conn);
                    continue;
                }
            }

            let res = connect_client(remote_addr).await;

            let client = match res {
                Ok((sync, data)) => {
                    info!(
                        target_node_id = %node_id,
                        remote_addr = %remote_addr,
                        "Connected to remote node!",
                    );

                    Client {
                        public_addr: remote_addr,
                        sync,
                        data,
                    }
                },
                Err(e) => {
                    errors.push((node_id.to_string(), e));
                    continue;
                },
            };

            new_nodes.insert(node_id.to_string(), client);
        }

        {
            let mut lock = self.nodes.write();
            (*lock) = new_nodes;
        }

        errors
    }

    /// Gets a reference to the node's rpc client.
    pub fn get_client(&self, node_id: &str) -> Option<Client> {
        self.nodes.read().get(node_id).cloned()
    }

    pub fn get_all_clients(&self) -> Vec<(String, Client)> {
        self.nodes
            .read()
            .iter()
            .map(|(n, c)| (n.clone(), c.clone()))
            .collect()
    }
}
