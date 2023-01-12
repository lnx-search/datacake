use std::net::SocketAddr;

use rkyv::AlignedVec;
use http::{Method, Request};
use hyper::body::{Bytes, HttpBody};
use hyper::{Body, StatusCode};

use crate::net::Error;

#[cfg(feature = "turmoil")]
use super::simulation::LazyClient;

use crate::request::MessageMetadata;

#[derive(Clone)]
/// A raw client connection which can produce multiplexed streams.
pub struct Channel {
    #[cfg(not(feature = "turmoil"))]
    connection: hyper::Client<hyper::client::HttpConnector, Body>,

    #[cfg(feature = "turmoil")]
    connection: LazyClient,

    remote_addr: SocketAddr,
}

impl Channel {
    #[cfg(not(feature = "turmoil"))]
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

    #[cfg(feature = "turmoil")]
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
    pub(crate) async fn send_msg(
        &self,
        metadata: MessageMetadata,
        msg: Bytes,
    ) -> Result<Result<AlignedVec, AlignedVec>, Error> {
        let uri = format!(
            "http://{}{}",
            self.remote_addr,
            crate::to_uri_path(&metadata.service_name, &metadata.path),
        );
        let request = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .body(Body::from(msg))
            .unwrap();

        #[cfg(not(feature = "turmoil"))]
        let resp = self.connection.request(request).await?;
        #[cfg(feature = "turmoil")]
        let resp = {
            let conn = self.connection
                .get_or_init()
                .await?;
            conn.lock()
                .await
                .send_request(request)
                .await?
        };

        let (req, mut body) = resp.into_parts();

        let size = body.size_hint().lower();
        let mut buffer = AlignedVec::with_capacity(size as usize);
        while let Some(chunk) = body.data().await {
            buffer.extend_from_slice(&chunk?);
        }

        if req.status == StatusCode::OK {
            Ok(Ok(buffer))
        } else {
            Ok(Err(buffer))
        }
    }

    #[inline]
    /// The address of the remote connection.
    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }
}
