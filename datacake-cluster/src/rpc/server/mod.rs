mod chitchat_impl;
mod consistency_impl;
mod replication_impl;

use std::net::SocketAddr;

use chitchat::ChitchatMessage;
use tokio::sync::oneshot;
use tonic::transport::server::Router;
use tonic::transport::{Error, Server};

use crate::keyspace::KeyspaceGroup;
use crate::rpc::datacake_api::chitchat_transport_server::ChitchatTransportServer;
use crate::rpc::datacake_api::consistency_api_server::ConsistencyApiServer;
use crate::rpc::datacake_api::replication_api_server::ReplicationApiServer;
use crate::rpc::server::chitchat_impl::ChitchatService;
use crate::rpc::server::consistency_impl::ConsistencyService;
use crate::rpc::server::replication_impl::ReplicationService;
use crate::storage::Storage;
use crate::RpcNetwork;

pub struct Context<S, R>
where
    S: Storage + Send + Sync + 'static,
    R: ServiceRegistry + Clone,
{
    pub chitchat_messages: flume::Sender<(SocketAddr, ChitchatMessage)>,
    pub keyspace_group: KeyspaceGroup<S>,
    pub service_registry: R,
    pub network: RpcNetwork,
}

impl<S, R> Clone for Context<S, R>
where
    S: Storage + Send + Sync + 'static,
    R: ServiceRegistry + Clone,
{
    fn clone(&self) -> Self {
        Self {
            chitchat_messages: self.chitchat_messages.clone(),
            keyspace_group: self.keyspace_group.clone(),
            service_registry: self.service_registry.clone(),
            network: self.network.clone(),
        }
    }
}

/// A custom service registry can be used in order to add additional GRPC services to the
/// RPC server in order to avoid listening on multiple addresses.
pub trait ServiceRegistry {
    /// Adds a set of services, returning the modified router.
    fn register_service(&self, router: Router) -> Router;
}

#[derive(Debug, Copy, Clone)]
/// A registry which adds no additional services to the server.
pub struct DefaultRegistry;
impl ServiceRegistry for DefaultRegistry {
    fn register_service(&self, router: Router) -> Router {
        router
    }
}

/// Create a new RPC server with a given context.
pub(crate) async fn connect_server<S, R>(
    listen_addr: SocketAddr,
    ctx: Context<S, R>,
) -> Result<oneshot::Sender<()>, Error>
where
    S: Storage + Send + Sync + 'static,
    R: ServiceRegistry + Clone,
{
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (ready_tx, ready_rx) = oneshot::channel();

    let chitchat_service =
        ChitchatService::new(ctx.keyspace_group.clock().clone(), ctx.chitchat_messages);
    let consistency_service =
        ConsistencyService::new(ctx.keyspace_group.clone(), ctx.network.clone());
    let replication_service = ReplicationService::new(ctx.keyspace_group.clone());

    let router = Server::builder()
        .add_service(ChitchatTransportServer::new(chitchat_service))
        .add_service(ConsistencyApiServer::new(consistency_service))
        .add_service(ReplicationApiServer::new(replication_service));

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
