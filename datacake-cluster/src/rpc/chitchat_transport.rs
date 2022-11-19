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
use crate::rpc::network::RpcNetwork;
use crate::storage::Storage;

#[derive(Clone)]
/// Chitchat compatible transport built on top of an existing GRPC connection.
///
/// This allows us to maintain a single connection rather than both a UDP and TCP connection.
pub struct GrpcTransport<S: Storage>(Arc<GrpcTransportInner<S>>);

impl<S: Storage> GrpcTransport<S> {
    /// Creates a new GRPC transport instances.
    pub fn new(
        network: RpcNetwork,
        ctx: super::server::Context<S>,
        shutdown_handles: Mutex<Vec<oneshot::Sender<()>>>,
        messages: flume::Receiver<(SocketAddr, ChitchatMessage)>,
    ) -> Self {
        Self(Arc::new(GrpcTransportInner {
            network,
            ctx,
            shutdown_handles,
            messages,
        }))
    }
}

impl<S: Storage> Deref for GrpcTransport<S> {
    type Target = GrpcTransportInner<S>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[async_trait]
impl<S: Storage + Sync + Send + 'static> Transport for GrpcTransport<S> {
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

pub struct GrpcTransportInner<S: Storage> {
    /// The RPC clients available to this cluster.
    network: RpcNetwork,

    /// Context to be passed when binding a new RPC server instance.
    ctx: super::server::Context<S>,

    /// The set of server handles that should be kept alive until the system shuts down.
    shutdown_handles: Mutex<Vec<oneshot::Sender<()>>>,

    /// Received messages to be sent to the Chitchat cluster.
    messages: flume::Receiver<(SocketAddr, ChitchatMessage)>,
}

pub struct GrpcConnection {
    self_addr: SocketAddr,
    network: RpcNetwork,
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
