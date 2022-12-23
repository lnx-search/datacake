use std::io;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;

use s2n_quic::{Client, Connection};
use s2n_quic::client::Connect;
use tokio::sync::Mutex;

use crate::net::{BUFFER_SIZE, ConnectionChannel};
use crate::net::limits::Limits;

pub static CERT_PEM: &str = include_str!("../certs/cert.pem");


#[derive(Clone)]
/// A raw QUIC client connection which can produce multiplexed streams.
pub struct ClientConnection {
    connection: Arc<Mutex<Connection>>,
    remote_addr: SocketAddr,
}

impl ClientConnection {
    /// Connects to a remote QUIC server.
    ///
    /// This takes a bind address as this is effectively the binding address
    /// of the UDP socket being used.
    pub async fn connect(
        remote_addr: SocketAddr,
    ) -> io::Result<Self> {
        let client = Client::builder()
            .with_limits(Limits::default().limits()).unwrap()
            .with_io("0.0.0.0:0")?
            .with_tls(CERT_PEM).unwrap()
            .start()
            .map_err(|e| io::Error::new(ErrorKind::Other, e.to_string()))?;

        let connect = Connect::new(remote_addr).with_server_name("localhost");
        let connection = client
            .connect(connect)
            .await
            .map_err(|e| io::Error::new(ErrorKind::Other, e.to_string()))?;

        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            remote_addr,
        })
    }

    /// Opens a new channel to be used as part of a RPC message.
    pub(crate) async fn open_channel(&self) -> io::Result<ConnectionChannel> {
        let mut lock = self.connection.lock().await;
        let stream = lock.open_bidirectional_stream().await?;
        Ok(ConnectionChannel {
            remote_addr: self.remote_addr,
            stream,
            hot_buffer: Box::new([0u8; BUFFER_SIZE]),
            buf: Vec::new(),
        })
    }
}
