use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;
use chitchat::serialize::Serializable;
use chitchat::transport::{Socket, Transport};
use chitchat::ChitchatMessage;
use parking_lot::Mutex;
use tokio::sync::oneshot;

use crate::rpc::chitchat_transport_api::chitchat_transport_client::ChitchatTransportClient;
use crate::rpc::chitchat_transport_api::ChitchatRpcMessage;
use crate::rpc::network::ClientNetwork;

#[derive(Clone)]
/// Chitchat compatible transport built on top of an existing GRPC connection.
///
/// This allows us to maintain a single connection rather than both a UDP and TCP connection.
pub struct GrpcTransport(Arc<GrpcTransportInner>);

impl Deref for GrpcTransport {
    type Target = GrpcTransportInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[async_trait]
impl Transport for GrpcTransport {
    async fn open(&self, listen_addr: SocketAddr) -> anyhow::Result<Box<dyn Socket>> {
        let shutdown =
            super::server::connect_server(listen_addr, self.ctx.clone()).await?;

        {
            self.shutdown_handles.lock().push(shutdown);
        }

        Ok(Box::new(GrpcConnection {
            self_addr: listen_addr,
            network: self.network.clone(),
            messages: self.messages.clone(),
        }))
    }
}

pub struct GrpcTransportInner {
    network: ClientNetwork,
    ctx: super::server::Context,
    shutdown_handles: Mutex<Vec<oneshot::Sender<()>>>,
    messages: flume::Receiver<(SocketAddr, ChitchatMessage)>,
}

pub struct GrpcConnection {
    self_addr: SocketAddr,
    network: ClientNetwork,
    messages: flume::Receiver<(SocketAddr, ChitchatMessage)>,
}

#[async_trait]
impl Socket for GrpcConnection {
    async fn send(
        &mut self,
        to: SocketAddr,
        msg: ChitchatMessage,
    ) -> anyhow::Result<()> {
        let message = msg.serialize_to_vec();
        let source = self.self_addr.serialize_to_vec();

        let channel = self.network.get_or_connect(to).await?;
        let mut client = ChitchatTransportClient::new(channel);
        client
            .send_msg(ChitchatRpcMessage { message, source })
            .await?;

        Ok(())
    }

    async fn recv(&mut self) -> anyhow::Result<(SocketAddr, ChitchatMessage)> {
        self.messages
            .recv_async()
            .await
            .map_err(anyhow::Error::from)
    }
}
