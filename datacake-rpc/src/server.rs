use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use parking_lot::{Mutex, RwLock};
use tokio::task::JoinHandle;

use crate::handler::{HandlerKey, OpaqueMessageHandler, RpcService, ServiceRegistry};

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
        self.state.add_handlers(Svc::service_name(), handlers);
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
    services: Arc<Mutex<BTreeMap<String, BTreeSet<HandlerKey>>>>,
    handlers: Arc<RwLock<BTreeMap<HandlerKey, Arc<dyn OpaqueMessageHandler>>>>,
}

impl ServerState {
    /// Adds a new set of handlers to the server state.
    ///
    /// Handlers newly added will then be able to handle messages received by
    /// the already running RPC system.
    pub(crate) fn add_handlers(
        &self,
        service_name: &str,
        handlers: BTreeMap<HandlerKey, Arc<dyn OpaqueMessageHandler>>,
    ) {
        {
            let mut lock = self.services.lock();
            for key in handlers.keys() {
                lock.entry(service_name.to_string())
                    .or_default()
                    .insert(*key);
            }
        }

        let mut lock = self.handlers.write();
        lock.extend(handlers);
    }

    /// Removes a new set of handlers from the server state.
    pub(crate) fn remove_handlers(&self, service: &str) {
        let uris = {
            match self.services.lock().remove(service) {
                None => return,
                Some(uris) => uris,
            }
        };

        let mut lock = self.handlers.write();
        lock.retain(|key, _| uris.contains(key));
    }

    /// Attempts to get the message handler for a specific service and message.
    pub(crate) fn get_handler(
        &self,
        uri: &str
    ) -> Option<Arc<dyn OpaqueMessageHandler>> {
        let lock = self.handlers.read();
        lock.get(&crate::hash(uri)).cloned()
    }
}
