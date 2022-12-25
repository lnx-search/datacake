use std::io;
use std::net::SocketAddr;

use http::{Method, Request};
use hyper::body::{Bytes, HttpBody};
use hyper::client::HttpConnector;
use hyper::{Body, Client, StatusCode};
use rkyv::AlignedVec;

use crate::request::MessageMetadata;

#[derive(Clone)]
/// A raw client connection which can produce multiplexed streams.
pub struct Channel {
    connection: Client<HttpConnector, Body>,
    remote_addr: SocketAddr,
}

impl Channel {
    /// Connects to a remote RPC server.
    pub async fn connect(remote_addr: SocketAddr) -> io::Result<Self> {
        let mut http = HttpConnector::new();
        http.enforce_http(false);
        http.set_nodelay(true);

        let client = hyper::Client::builder()
            .http2_only(true)
            .http2_adaptive_window(true)
            .build(http);

        Ok(Self {
            connection: client,
            remote_addr,
        })
    }

    /// Sends a message payload the remote server and gets the response
    /// data back.
    pub(crate) async fn send_msg(
        &self,
        metadata: MessageMetadata,
        msg: Bytes,
    ) -> Result<Result<AlignedVec, AlignedVec>, hyper::Error> {
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

        let resp = self.connection.request(request).await?;
        let (req, mut body) = resp.into_parts();

        let size = body.size_hint().upper().unwrap_or(1024);
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
