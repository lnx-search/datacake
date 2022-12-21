use std::io::ErrorKind;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;
use chitchat::serialize::Serializable;
use chitchat::transport::{Socket, Transport};
use chitchat::ChitchatMessage;
use futures::io;
use parking_lot::Mutex;
use tokio::sync::oneshot;
use tracing::{info, trace};

use crate::rpc::datacake_api::chitchat_transport_client::ChitchatTransportClient;
use crate::rpc::datacake_api::ChitchatRpcMessage;
use crate::rpc::network::RpcNetwork;
use crate::rpc::server::ServiceRegistry;
use crate::Clock;

#[derive(Clone)]
/// Chitchat compatible transport built on top of an existing GRPC connection.
///
/// This allows us to maintain a single connection rather than both a UDP and TCP connection.
pub struct GrpcTransport<R>(Arc<GrpcTransportInner<R>>)
where
    R: ServiceRegistry + Clone;

impl<R> GrpcTransport<R>
where
    R: ServiceRegistry + Clone,
{
    /// Creates a new GRPC transport instances.
    pub fn new(
        ctx: super::server::Context<R>,
        messages: flume::Receiver<(SocketAddr, ChitchatMessage)>,
    ) -> Self {
        Self(Arc::new(GrpcTransportInner {
            ctx,
            shutdown_handles: Default::default(),
            messages,
        }))
    }
}

impl<R> Deref for GrpcTransport<R>
where
    R: ServiceRegistry + Clone,
{
    type Target = GrpcTransportInner<R>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[async_trait]
impl<R> Transport for GrpcTransport<R>
where
    R: ServiceRegistry + Send + Sync + Clone + 'static,
{
    async fn open(
        &self,
        listen_addr: SocketAddr,
    ) -> Result<Box<dyn Socket>, anyhow::Error> {
        info!(listen_addr = %listen_addr, "Starting RPC server.");
        let shutdown =
            super::server::connect_server(listen_addr, self.ctx.clone()).await?;

        {
            self.shutdown_handles.lock().push(shutdown);
        }

        Ok(Box::new(GrpcConnection {
            clock: self.ctx.clock.clone(),
            self_addr: listen_addr,
            network: self.ctx.network.clone(),
            messages: self.messages.clone(),
        }))
    }
}

pub struct GrpcTransportInner<R>
where
    R: ServiceRegistry + Clone,
{
    /// Context to be passed when binding a new RPC server instance.
    ctx: super::server::Context<R>,

    /// The set of server handles that should be kept alive until the system shuts down.
    shutdown_handles: Mutex<Vec<oneshot::Sender<()>>>,

    /// Received messages to be sent to the Chitchat cluster.
    messages: flume::Receiver<(SocketAddr, ChitchatMessage)>,
}

pub struct GrpcConnection {
    clock: Clock,
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
    ) -> Result<(), anyhow::Error> {
        trace!(to = %to, msg = ?msg, "Gossip send");
        let message = msg.serialize_to_vec();
        let source = self.self_addr.serialize_to_vec();

        let channel =
            self.network.get_or_connect(to).await.map_err(|e| {
                io::Error::new(ErrorKind::ConnectionRefused, e.to_string())
            })?;

        let ts = self.clock.get_time().await;
        let mut client = ChitchatTransportClient::new(channel);
        client
            .send_msg(ChitchatRpcMessage {
                message,
                source,
                timestamp: Some(ts.into()),
            })
            .await
            .map_err(|e| io::Error::new(ErrorKind::ConnectionAborted, e.to_string()))?;

        Ok(())
    }

    async fn recv(&mut self) -> Result<(SocketAddr, ChitchatMessage), anyhow::Error> {
        let msg = self.messages.recv_async().await?;
        Ok(msg)
    }
}
