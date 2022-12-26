use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use tokio::task::JoinHandle;

use crate::handler::{HandlerKey, OpaqueMessageHandler, RpcService, ServiceRegistry};

/// A RPC server instance.
///
/// This is used for listening for inbound connections and handling any RPC messages
/// coming from clients.
///
/// ```rust
/// use bytecheck::CheckBytes;
/// use rkyv::{Archive, Deserialize, Serialize};
/// use datacake_rpc::{Server, Handler, Request, RpcService, ServiceRegistry, Status};
/// use std::net::SocketAddr;
///
/// #[repr(C)]
/// #[derive(Serialize, Deserialize, Archive, PartialEq, Debug)]
/// #[archive(compare(PartialEq))]
/// #[archive_attr(derive(CheckBytes, PartialEq, Debug))]
/// pub struct MyMessage {
///     name: String,
///     age: u32,
/// }
///
/// pub struct EchoService;
///
/// impl RpcService for EchoService {
///     fn register_handlers(registry: &mut ServiceRegistry<Self>) {
///         registry.add_handler::<MyMessage>();
///     }
/// }
///
/// #[datacake_rpc::async_trait]
/// impl Handler<MyMessage> for EchoService {
///     type Reply = MyMessage;
///
///     async fn on_message(&self, msg: Request<MyMessage>) -> Result<Self::Reply, Status> {
///         Ok(msg.to_owned().unwrap())
///     }
/// }
///
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// let bind = "127.0.0.1:8000".parse::<SocketAddr>()?;
/// // Start the RPC server listening on our bind address.
/// let server = Server::listen(bind).await?;
///
/// // Once our server is running we can add or remove services.
/// // Once a service is added it is able to begin handling RPC messages.
/// server.add_service(EchoService);
///
/// // Once a service is removed the server will reject messages for the
/// // service that is no longer registered,
/// server.remove_service(EchoService::service_name());
///
/// // We can add wait() here if we want to listen for messages forever.
/// // server.wait().await;
/// # Ok(())
/// # }
/// ```
pub struct Server {
    state: ServerState,
    handle: JoinHandle<()>,
}

impl Server {
    /// Spawns the RPC server task and returns the server handle.
    pub async fn listen(addr: SocketAddr) -> io::Result<Self> {
        let state = ServerState::default();
        let handle = crate::net::start_rpc_server(addr, state.clone()).await?;

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

    /// Waits until the server exits.
    ///
    /// This typically is just a future that pends forever as the server
    /// will not exit unless an external force triggers it.
    pub async fn wait(self) {
        self.handle.await.expect("Wait for server handle.");
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
        uri: &str,
    ) -> Option<Arc<dyn OpaqueMessageHandler>> {
        let lock = self.handlers.read();
        lock.get(&crate::hash(uri)).cloned()
    }
}
