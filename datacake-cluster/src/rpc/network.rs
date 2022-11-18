use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tonic::transport::{Channel, Endpoint, Error};

pub const TIMEOUT_LIMIT: Duration = Duration::from_secs(2);
pub const CONNECT_TIMEOUT_LIMIT: Duration = Duration::from_secs(5);

#[derive(Clone)]
/// A collection of RPC client connections which can be reused and multiplexed.
pub struct ClientNetwork {
    clients: Arc<RwLock<HashMap<SocketAddr, Channel>>>,
}

impl ClientNetwork {
    /// Attempts to get an already existing connection or creates a new connection.
    pub async fn get_or_connect(&self, addr: SocketAddr) -> Result<Channel, Error> {
        {
            let guard = self.clients.read();
            if let Some(channel) = guard.get(&addr) {
                return Ok(channel.clone());
            }
        }

        self.connect(addr).await
    }

    /// Connects to a given address and adds it to the clients.
    pub async fn connect(&self, addr: SocketAddr) -> Result<Channel, Error> {
        let uri = format!("http://{}", addr);
        let channel = Endpoint::from_str(&uri)
            .unwrap()
            .timeout(TIMEOUT_LIMIT)
            .connect_timeout(CONNECT_TIMEOUT_LIMIT)
            .connect()
            .await?;

        {
            let mut guard = self.clients.write();
            guard.insert(addr, channel.clone());
        }

        Ok(channel)
    }
}
