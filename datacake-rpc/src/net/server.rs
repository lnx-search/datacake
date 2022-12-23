use std::io;
use std::io::ErrorKind;
use std::net::SocketAddr;
use s2n_quic::{Connection, Server};

use tokio::task::JoinHandle;

use crate::net::{BUFFER_SIZE, ConnectionChannel};
use crate::net::limits::Limits;
use crate::server::{ServerState, ServerTask};


pub static CERT_PEM: &str = include_str!("../certs/cert.pem");
pub static KEY_PEM: &str = include_str!("../certs/key.pem");

/// Starts the RPC QUIC server.
///
/// This takes a binding socket address and server name.
pub(crate) async fn start_rpc_server(
    bind_addr: SocketAddr,
    state: ServerState,
) -> io::Result<JoinHandle<()>> {
    let mut server = Server::builder()
        .with_limits(Limits::default().limits()).unwrap()
        .with_tls((CERT_PEM, KEY_PEM)).unwrap()
        .with_io(bind_addr)?
        .start()
        .map_err(|e| io::Error::new(ErrorKind::Other, e.to_string()))?;

    let handle = tokio::spawn(async move {
        while let Some(conn) = server.accept().await {
            tokio::spawn(handle_connecting(conn, state.clone()));
        }
    });

    Ok(handle)
}

/// A single connection handler.
///
/// This accepts new streams being created and spawns concurrent tasks to handle
/// them.
async fn handle_connecting(mut conn: Connection, state: ServerState) -> io::Result<()> {
    let remote_addr = conn.remote_addr()?;

    while let Some(stream) = conn.accept_bidirectional_stream().await? {
        let channel = ConnectionChannel {
            remote_addr,
            stream,
            hot_buffer: Box::new([0u8; BUFFER_SIZE]),
            buf: Vec::with_capacity(12 << 10),
        };

        let server = ServerTask::new(channel, state.clone());
        tokio::spawn(server.handle_messages());
    }

    Ok(())
}
