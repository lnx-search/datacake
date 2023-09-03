use std::net::SocketAddr;

use http::{HeaderMap, Method, Request, Response};

#[cfg(feature = "simulation")]
use super::simulation::LazyClient;
use crate::body::Body;
use crate::net::Error;
use crate::request::MessageMetadata;

#[derive(Clone)]
/// A raw client connection which can produce multiplexed streams.
pub struct Channel {
    #[cfg(not(feature = "simulation"))]
    connection: hyper::Client<hyper::client::HttpConnector, hyper::Body>,

    #[cfg(feature = "simulation")]
    connection: LazyClient,

    remote_addr: SocketAddr,
}

impl Channel {
    #[cfg(not(feature = "simulation"))]
    /// Connects to a remote RPC server.
    pub fn connect(remote_addr: SocketAddr) -> Self {
        let mut http = hyper::client::HttpConnector::new();
        http.enforce_http(false);
        http.set_nodelay(true);
        http.set_connect_timeout(Some(std::time::Duration::from_secs(2)));

        let client = hyper::Client::builder()
            .http2_keep_alive_while_idle(true)
            .http2_only(true)
            .http2_adaptive_window(true)
            .build(http);

        Self {
            connection: client,
            remote_addr,
        }
    }

    #[cfg(feature = "simulation")]
    /// Connects to a remote RPC server with turmoil simulation enabled.
    pub fn connect(remote_addr: SocketAddr) -> Self {
        let client = LazyClient::connect(remote_addr);

        Self {
            connection: client,
            remote_addr,
        }
    }

    /// Sends a message payload the remote server and gets the response
    /// data back.
    pub(crate) async fn send_parts(
        &self,
        metadata: MessageMetadata,
        headers: HeaderMap,
        body: Body,
    ) -> Result<Response<hyper::Body>, Error> {
        let uri = format!("http://{}{}", self.remote_addr, metadata.to_uri_path(),);
        let mut request = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .body(body.into_inner())
            .unwrap();

        (*request.headers_mut()) = headers;

        #[cfg(not(feature = "simulation"))]
        let resp = self.connection.request(request).await?;
        #[cfg(feature = "simulation")]
        let resp = {
            let conn = self.connection.get_or_init().await?;
            conn.lock().await.send_request(request).await?
        };

        Ok(resp)
    }

    #[inline]
    /// The address of the remote connection.
    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }
}
