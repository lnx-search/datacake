use std::net::SocketAddr;

use anyhow::Result;

use crate::{ConnectionCfg, DatacakeCluster, Datastore};

mod memstore;
pub use memstore::MemStore;

/// A simple helper method for the purposes of testing.
///
/// In the real world you would want to set the public address and the listen address'
/// to different values as nodes will likely be on different networks.
pub fn make_connection_config(
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

pub async fn make_test_node(
    node_id: &str,
    gossip_addr: &str,
    rpc_addr: &str,
    seeds: Vec<&str>,
) -> Result<DatacakeCluster> {
    make_test_node_with_store(node_id, gossip_addr, rpc_addr, seeds, MemStore::default()).await
}

pub async fn make_test_node_with_store<DS: Datastore>(
    node_id: &str,
    gossip_addr: &str,
    rpc_addr: &str,
    seeds: Vec<&str>,
    store: DS,
) -> Result<DatacakeCluster> {
    let cfg =
        make_connection_config(gossip_addr.parse().unwrap(), rpc_addr.parse().unwrap());

    let node = DatacakeCluster::connect(
        node_id,
        "cluster-1",
        cfg,
        seeds.into_iter().map(|v| v.into()).collect(),
        store,
    )
    .await?;

    Ok(node)
}