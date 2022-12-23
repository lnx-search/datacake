use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::task::JoinHandle;

use crate::handler::{HandlerKey, OpaqueMessageHandler, RpcService, ServiceRegistry};
use crate::net::{ConnectionChannel, SendMsgError};
use crate::Status;

/// A RPC server instance.
///
/// This allows for dynamic adding and removal of services.
pub struct Server {
    state: ServerState,
    handle: JoinHandle<()>,
}

impl Server {
    /// Spawns the RPC server task and returns the server handle.
    pub async fn listen(addr: SocketAddr) -> io::Result<Self> {
        let state = ServerState::default();
        let handle =
            crate::net::start_rpc_server(addr, state.clone()).await?;

        Ok(Self { state, handle })
    }

    /// Adds a new service to the live RPC server.
    pub fn add_service<Svc>(&self, service: Svc)
    where
        Svc: RpcService + Send + Sync + 'static,
    {
        let mut registry = ServiceRegistry::new(service);
        Svc::register_handlers(&mut registry);
        let handlers = registry.into_handlers();
        self.state.add_handlers(handlers);
    }

    /// Removes all handlers linked with the given service name.
    pub fn remove_service(&self, service_name: &str) {
        self.state.remove_handlers(service_name);
    }

    /// Signals the server to shutdown.
    pub fn shutdown(&self) {
        self.handle.abort();
    }
}

#[derive(Clone, Default)]
/// Represents the shared state of the RPC server.
pub(crate) struct ServerState {
    handlers: Arc<RwLock<BTreeMap<HandlerKey, Arc<dyn OpaqueMessageHandler>>>>,
}

impl ServerState {
    /// Adds a new set of handlers to the server state.
    ///
    /// Handlers newly added will then be able to handle messages received by
    /// the already running RPC system.
    pub(crate) fn add_handlers(
        &self,
        handlers: BTreeMap<HandlerKey, Arc<dyn OpaqueMessageHandler>>,
    ) {
        let mut lock = self.handlers.write();
        lock.extend(handlers);
    }

    /// Removes a new set of handlers from the server state.
    pub(crate) fn remove_handlers(&self, service: &str) {
        let service = crate::hash(service);

        let mut lock = self.handlers.write();
        lock.retain(|key, _| key.0 != service);
    }

    /// Attempts to get the message handler for a specific service and message.
    pub(crate) fn get_handler(
        &self,
        service: &str,
        path: &str,
    ) -> Option<Arc<dyn OpaqueMessageHandler>> {
        let service = crate::hash(service);
        let path = crate::hash(path);
        let lock = self.handlers.read();
        lock.get(&(service, path)).cloned()
    }
}

/// A single server task as part of a multiplexed connection to the server.
pub(crate) struct ServerTask {
    channel: ConnectionChannel,
    state: ServerState,
}

impl ServerTask {
    /// Creates a new server task.
    pub(crate) fn new(channel: ConnectionChannel, state: ServerState) -> Self {
        Self { channel, state }
    }

    /// Continuously receives messages from the remote client until the connection
    /// is closed.
    pub(crate) async fn handle_messages(mut self) {
        let remote_addr = self.channel.remote_addr();
        while let Ok(Some(Ok((metadata, msg)))) = self.channel.recv_msg().await {
            let handler = self
                .state
                .get_handler(metadata.service_name.as_ref(), metadata.path.as_ref());

            match handler {
                Some(handler) => {
                    let reply = handler.try_handle(remote_addr, msg).await;
                    match self.channel.send_msg(&metadata, &reply).await {
                        Ok(()) => {},
                        Err(SendMsgError::IoError(e)) => {
                            warn!(error = ?e, "Encountered an IO error while handling connection.");
                            break;
                        },
                        Err(SendMsgError::Status(status)) => {
                            if let Err(e) = self.channel.send_error(&status).await {
                                warn!(error = ?e, "Encountered an IO error while handling connection.");
                            };
                        },
                    }
                },
                None => {
                    let status = Status::unavailable(format!(
                        "Unknown handler for service and message: {}/{}",
                        metadata.service_name, metadata.path,
                    ));

                    if let Err(e) = self.channel.send_error(&status).await {
                        warn!(error = ?e, "Encountered an IO error while handling connection.");
                    };
                },
            }
        }
    }
}
