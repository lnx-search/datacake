use std::io;
use std::net::SocketAddr;

use quinn::{Connecting, Endpoint, EndpointConfig, TokioRuntime};
use socket2::{Domain, Socket, Type};
use tokio::task::JoinHandle;

use crate::net::{BUFFER_SIZE, ConnectionChannel};
use crate::server::{ServerState, ServerTask};

#[derive(Debug, thiserror::Error)]
pub enum ServerBindError {
    #[error("TLS Error: {0}")]
    /// An error within the TLS configuration occurred.
    Config(String),
    #[error("IO Error: {0}")]
    /// An IO error caused a failure to bind the socket.
    Io(#[from] io::Error),
}

/// Starts the RPC QUIC server.
///
/// This takes a binding socket address and server name.
pub(crate) async fn start_rpc_server(
    bind_addr: SocketAddr,
    state: ServerState,
) -> Result<JoinHandle<()>, ServerBindError> {
    let (cfg, _) = super::tls::configure_server(vec!["rpc.datacake.net".to_string()])
        .map_err(|e| ServerBindError::Config(e.to_string()))?;

    let socket = Socket::new(Domain::IPV4, Type::DGRAM, None)?;
    //socket.set_recv_buffer_size(24 << 10)?;
    //socket.set_send_buffer_size(24 << 10)?;
    socket.bind(&socket2::SockAddr::from(bind_addr))?;

    let endpoint = Endpoint::new(
        EndpointConfig::default(),
        Some(cfg),
        socket.into(),
        TokioRuntime,
    )?;

    let handle = tokio::spawn(async move {
        while let Some(conn) = endpoint.accept().await {
            tokio::spawn(handle_connecting(conn, state.clone()));
        }
    });

    Ok(handle)
}

/// A single connection handler.
///
/// This accepts new streams being created and spawns concurrent tasks to handle
/// them.
async fn handle_connecting(conn: Connecting, state: ServerState) -> io::Result<()> {
    let conn = conn.await?;
    let remote_addr = conn.remote_address();

    while let Ok((send, recv)) = conn.accept_bi().await {
        let channel = ConnectionChannel {
            remote_addr,
            send,
            recv,
            hot_buffer: Box::new([0u8; BUFFER_SIZE]),
            buf: Vec::with_capacity(12 << 10),
        };

        let server = ServerTask::new(channel, state.clone());
        tokio::spawn(server.handle_messages());
    }

    Ok(())
}
