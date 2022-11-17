mod chitchat_impl;

use std::net::SocketAddr;

use chitchat::ChitchatMessage;
use tokio::sync::oneshot;
use tonic::transport::{Error, Server};

use crate::rpc::chitchat_transport_api::chitchat_transport_server::ChitchatTransportServer;
use crate::rpc::server::chitchat_impl::ChitchatService;

#[derive(Clone)]
pub struct Context {
    pub chitchat_messages: flume::Sender<(SocketAddr, ChitchatMessage)>,
}

pub(crate) async fn connect_server(
    listen_addr: SocketAddr,
    ctx: Context,
) -> Result<oneshot::Sender<()>, Error> {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (ready_tx, ready_rx) = oneshot::channel();
    let chitchat_service = ChitchatService::new(ctx.chitchat_messages);

    let fut = Server::builder()
        .add_service(ChitchatTransportServer::new(chitchat_service))
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
