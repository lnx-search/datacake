use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::time::{interval, Interval};
use tonic::transport::Channel;

use crate::keyspace::KeyspaceTimestamps;
use crate::rpc::{ReplicationClient, RpcNetwork};

/// A actor responsible for continuously polling a node's keyspace state
/// and triggering the required callbacks.
pub struct NodePollerState {
    /// The ID of the node being polled.
    target_node_id: u32,

    /// The target node's RPC address.
    target_rpc_addr: SocketAddr,

    /// The last recorded keyspace timestamps.
    ///
    /// This is used to work out the difference between the old and new states.
    last_keyspace_timestamps: KeyspaceTimestamps,

    /// The remote node's RPC channel.
    channel: Channel,

    /// A shutdown signal to tell the actor to shut down.
    shutdown: Arc<AtomicBool>,

    /// The time interval to elapse between polling the state.
    interval: Interval,
}

impl NodePollerState {
    async fn tick(&mut self) -> bool {
        if self.is_killed() {
            return true;
        }

        self.interval.tick().await;

        self.is_killed()
    }

    fn is_killed(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }
}

#[instrument(name = "node-poller", skip_all)]
async fn node_poller(mut state: NodePollerState) {
    info!(
        target_node_id = %state.target_node_id,
        target_rpc_addr = %state.target_rpc_addr,
        "Polling for state changes..."
    );

    let mut client = ReplicationClient::from(state.channel.clone());

    loop {
        if state.tick().await {
            break;
        }

        if let Err(e) = poll_node(&mut state, &mut client).await {
            error!(
                error = ?e,
                target_node_id = %state.target_node_id,
                target_rpc_addr = %state.target_rpc_addr,
                "Unable to poll node due to error.",
            );
        }
    }

    info!(
        target_node_id = %state.target_node_id,
        target_rpc_addr = %state.target_rpc_addr,
        "Poller has received shutdown signal, Closing."
    );
}

async fn poll_node(
    state: &mut NodePollerState,
    client: &mut ReplicationClient,
) -> Result<(), anyhow::Error> {
    let keyspace_timestamps = client.poll_keyspace().await?;

    Ok(())
}
