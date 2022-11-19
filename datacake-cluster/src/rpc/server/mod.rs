mod chitchat_impl;
mod consistency_impl;
mod replication_impl;

use std::net::SocketAddr;

use chitchat::ChitchatMessage;
use tokio::sync::oneshot;
use tonic::transport::{Error, Server};
use crate::keyspace::KeyspaceGroup;

use crate::rpc::chitchat_transport_api::chitchat_transport_server::ChitchatTransportServer;
use crate::rpc::datacake_api::consistency_api_server::ConsistencyApiServer;
use crate::rpc::datacake_api::replication_api_server::ReplicationApiServer;
use crate::rpc::server::chitchat_impl::ChitchatService;
use crate::rpc::server::consistency_impl::ConsistencyService;
use crate::rpc::server::replication_impl::ReplicationService;
use crate::storage::Storage;

pub struct Context<S: Storage> {
    pub chitchat_messages: flume::Sender<(SocketAddr, ChitchatMessage)>,
    pub keyspace_group: KeyspaceGroup<S>,
}

impl<S: Storage> Clone for Context<S> {
    fn clone(&self) -> Self {
        Self {
            chitchat_messages: self.chitchat_messages.clone(),
            keyspace_group: self.keyspace_group.clone(),
        }
    }
}

pub(crate) async fn connect_server<S: Storage + Send + Sync + 'static>(
    listen_addr: SocketAddr,
    ctx: Context<S>,
) -> Result<oneshot::Sender<()>, Error> {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (ready_tx, ready_rx) = oneshot::channel();

    let chitchat_service = ChitchatService::new(ctx.chitchat_messages);
    let consistency_service = ConsistencyService::new(ctx.keyspace_group.clone());
    let replication_service = ReplicationService::new(ctx.keyspace_group.clone());

    let fut = Server::builder()
        .add_service(ChitchatTransportServer::new(chitchat_service))
        .add_service(ConsistencyApiServer::new(consistency_service))
        .add_service(ReplicationApiServer::new(replication_service))
        .serve_with_shutdown(
            listen_addr,
            async move {
                let _ = ready_tx.send(());

                info!(listen_addr = %listen_addr, "RPC server is listening for connections.");
                let _ = shutdown_rx.await;
                info!(listen_addr = %listen_addr, "RPC server is shutting down.")
            }
        );

    let handle = tokio::spawn(fut);

    if ready_rx.await.is_err() {
        return Err(handle.await.unwrap().unwrap_err());
    }

    Ok(shutdown_tx)
}
