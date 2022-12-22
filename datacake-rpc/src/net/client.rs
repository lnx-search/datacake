use std::io;
use std::net::SocketAddr;

use quinn::{ConnectError, Connection, ConnectionError, Endpoint};

use crate::net::ConnectionChannel;

#[derive(Debug, thiserror::Error)]
/// The client failed to establish a connection to the remote server.
pub enum ClientConnectError {
    #[error("Connect Error: {0}")]
    Connect(#[from] ConnectError),

    #[error("Connection Error: {0}")]
    Connection(#[from] ConnectionError),

    #[error("IO Error: {0}")]
    Io(#[from] io::Error),
}

#[derive(Clone)]
/// A raw QUIC client connection which can produce multiplexed streams.
pub struct ClientConnection {
    connection: Connection,
    remote_addr: SocketAddr,
}

impl ClientConnection {
    /// Connects to a remote QUIC server.
    ///
    /// This takes a bind address as this is effectively the binding address
    /// of the UDP socket being used.
    pub async fn connect(
        bind_addr: SocketAddr,
        remote_addr: SocketAddr,
    ) -> Result<Self, ClientConnectError> {
        let cfg = super::tls::configure_client();
        let connection = Endpoint::client(bind_addr)?
            .connect_with(cfg, remote_addr, "rpc.datacake.net")?
            .await?;

        Ok(Self {
            connection,
            remote_addr,
        })
    }

    /// Opens a new channel to be used as part of a RPC message.
    pub(crate) async fn open_channel(&self) -> io::Result<ConnectionChannel> {
        let (send, recv) = self.connection.open_bi().await?;
        Ok(ConnectionChannel {
            remote_addr: self.remote_addr,
            send,
            recv,
            buf: Vec::new(),
        })
    }
}
