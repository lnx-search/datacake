use std::io;
use std::net::SocketAddr;

use quinn::{ConnectError, Connection, ConnectionError, Endpoint, EndpointConfig, TokioRuntime};
use socket2::{Domain, Socket, Type};

use crate::net::{BUFFER_SIZE, ConnectionChannel};

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
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, None)?;
        //socket.set_recv_buffer_size(32 << 10)?;
        //socket.set_send_buffer_size(32 << 10)?;
        socket.bind(&socket2::SockAddr::from(bind_addr))?;

        let endpoint = Endpoint::new(
            EndpointConfig::default(),
            None,
            socket.into(),
            TokioRuntime,
        )?;
        let connection = endpoint
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
            hot_buffer: Box::new([0u8; BUFFER_SIZE]),
            buf: Vec::new(),
        })
    }
}
