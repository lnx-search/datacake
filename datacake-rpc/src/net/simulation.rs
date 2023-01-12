use std::io;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hyper::client::conn::SendRequest;
use hyper::Body;
use tokio::sync::{Mutex, OnceCell};
use tokio::time::timeout;

use crate::net::Error;

#[derive(Clone)]
/// A client used for simulation testing via turmoil.
///
/// This is not a production grade client and is only really meant for testing not
/// performance.
pub struct LazyClient {
    addr: SocketAddr,
    client: Arc<OnceCell<Mutex<SendRequest<Body>>>>,
}

impl LazyClient {
    /// Creates a new lazy client.
    pub fn connect(socket: SocketAddr) -> Self {
        Self {
            addr: socket,
            client: Arc::new(OnceCell::new()),
        }
    }

    /// Ensures the connection is initialised and ready to handle events.
    pub async fn get_or_init(&self) -> Result<&Mutex<SendRequest<Body>>, Error> {
        if let Some(existing) = self.client.get() {
            return Ok(existing);
        }

        let io = timeout(Duration::from_secs(2), turmoil::net::TcpStream::connect(self.addr))
            .await
            .map_err(|_| Error::Io(io::Error::new(ErrorKind::TimedOut, "Failed to connect within deadline")))??;

        let (sender, connection) = hyper::client::conn::Builder::new()
            .http2_keep_alive_while_idle(true)
            .http2_only(true)
            .http2_adaptive_window(true)
            .handshake(io)
            .await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Error in connection: {}", e);
            }
        });

        self.client.set(Mutex::new(sender)).unwrap();
        Ok(self.client.get().unwrap())
    }
}
