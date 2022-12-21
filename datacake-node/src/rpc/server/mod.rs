mod chitchat_impl;

use std::net::SocketAddr;

use chitchat::ChitchatMessage;
use chitchat_impl::ChitchatService;
use tokio::sync::oneshot;
use tonic::transport::server::Router;
use tonic::transport::{Error, Server};
use tracing::info;

use super::datacake_api::chitchat_transport_server::ChitchatTransportServer;
use super::network::RpcNetwork;
use crate::Clock;

pub struct Context<R>
where
    R: ServiceRegistry + Clone,
{
    pub chitchat_messages: flume::Sender<(SocketAddr, ChitchatMessage)>,
    pub service_registry: R,
    pub network: RpcNetwork,
    pub clock: Clock,
}

impl<R> Clone for Context<R>
where
    R: ServiceRegistry + Clone,
{
    fn clone(&self) -> Self {
        Self {
            chitchat_messages: self.chitchat_messages.clone(),
            service_registry: self.service_registry.clone(),
            network: self.network.clone(),
            clock: self.clock.clone(),
        }
    }
}

/// A custom service registry can be used in order to add additional GRPC services to the
/// RPC server in order to avoid listening on multiple addresses.
pub trait ServiceRegistry {
    /// Adds a set of services, returning the modified router.
    fn register_service(&self, router: Router) -> Router {
        router
    }
}

#[derive(Debug, Copy, Clone)]
/// A registry which adds no additional services to the server.
pub struct DefaultRegistry;
impl ServiceRegistry for DefaultRegistry {}

/// Create a new RPC server with a given context.
pub(crate) async fn connect_server<R>(
    listen_addr: SocketAddr,
    ctx: Context<R>,
) -> Result<oneshot::Sender<()>, Error>
where
    R: ServiceRegistry + Clone,
{
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (ready_tx, ready_rx) = oneshot::channel();

    let chitchat_service =
        ChitchatService::new(ctx.clock.clone(), ctx.chitchat_messages);

    let router =
        Server::builder().add_service(ChitchatTransportServer::new(chitchat_service));

    let router = ctx.service_registry.register_service(router);

    let fut = router.serve_with_shutdown(listen_addr, async move {
        let _ = ready_tx.send(());

        info!(listen_addr = %listen_addr, "RPC server is listening for connections.");
        let _ = shutdown_rx.await;
        info!(listen_addr = %listen_addr, "RPC server is shutting down.")
    });

    let handle = tokio::spawn(fut);

    if ready_rx.await.is_err() {
        return Err(handle.await.unwrap().unwrap_err());
    }

    Ok(shutdown_tx)
}
