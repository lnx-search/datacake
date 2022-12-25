use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use datacake_rpc::Channel;
use parking_lot::RwLock;
use tracing::trace;

#[derive(Clone, Default)]
/// A collection of RPC client connections which can be reused and multiplexed.
pub struct RpcNetwork {
    clients: Arc<RwLock<HashMap<SocketAddr, Channel>>>,
}

impl RpcNetwork {
    /// Attempts to get an already existing connection or creates a new connection.
    pub fn get_or_connect(&self, addr: SocketAddr) -> io::Result<Channel> {
        {
            let guard = self.clients.read();
            if let Some(channel) = guard.get(&addr) {
                return Ok(channel.clone());
            }
        }

        trace!(addr = %addr, "Connect client to network.");
        self.connect(addr)
    }

    /// Connects to a given address and adds it to the clients.
    pub fn connect(&self, addr: SocketAddr) -> io::Result<Channel> {
        let channel = Channel::connect(addr)?;

        {
            let mut guard = self.clients.write();
            guard.insert(addr, channel.clone());
        }

        Ok(channel)
    }

    /// Removes a client from the network.
    pub fn disconnect(&self, addr: SocketAddr) {
        let mut guard = self.clients.write();
        guard.remove(&addr);
    }
}
