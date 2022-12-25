use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use bytecheck::CheckBytes;
use rkyv::ser::serializers::AllocSerializer;
use rkyv::validation::validators::DefaultValidator;
use rkyv::{AlignedVec, Archive, Serialize};

use crate::net::Status;
use crate::request::Request;
use crate::{DataView, SCRATCH_SPACE};

/// A specific handler key.
///
/// This is in the format of (service_name, handler_path).
pub type HandlerKey = u64;

/// A registry system used for linking a service's message handlers
/// with the RPC system at runtime.
///
/// Since the RPC system cannot determine what message payload matches
/// with which handler at compile time, it must dynamically link them
/// at runtime.
///
/// Not registering a handler will cause the handler to not be triggered
/// even if a valid message comes through.
///
///
/// ```rust
/// use rkyv::{Archive, Deserialize, Serialize};
/// use datacake_rpc::{Handler, Request, RpcService, ServiceRegistry, Status, RpcClient, Channel};
/// use std::net::SocketAddr;
///
/// #[repr(C)]
/// #[derive(Serialize, Deserialize, Archive, Debug)]
/// #[archive_attr(derive(CheckBytes, Debug))]
/// pub struct MyMessage {
///     name: String,
///     age: u32,
/// }
///
/// #[repr(C)]
/// #[derive(Serialize, Deserialize, Archive, Debug)]
/// #[archive_attr(derive(CheckBytes, Debug))]
/// pub struct MyOtherMessage {
///     age: u32,
/// }
///
/// pub struct EchoService;
///
/// impl RpcService for EchoService {
///     fn register_handlers(registry: &mut ServiceRegistry<Self>) {
///         // Since we've registered the `MyMessage` handler, the RPC system
///         // will dispatch the messages to out handler.
///         //
///         // But because we *haven't* registered our `MyOtherMessage` handler,
///         // even though our service implements the handler, no messages will
///         // be dispatched.
///         registry.add_handler::<MyMessage>();
///
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
/// #[datacake_rpc::async_trait]
/// impl Handler<MyOtherMessage> for EchoService {
///     type Reply = MyOtherMessage;
///
///     async fn on_message(&self, msg: Request<MyOtherMessage>) -> Result<Self::Reply, Status> {
///         Ok(msg.to_owned().unwrap())
///     }
/// }
/// ```
pub struct ServiceRegistry<Svc> {
    handlers: BTreeMap<HandlerKey, Arc<dyn OpaqueMessageHandler>>,
    service: Arc<Svc>,
}

impl<Svc> ServiceRegistry<Svc>
where
    Svc: RpcService + Send + Sync + 'static,
{
    pub(crate) fn new(service: Svc) -> Self {
        Self {
            handlers: BTreeMap::new(),
            service: Arc::new(service),
        }
    }

    /// Consumes the registry into the produced handlers.
    pub(crate) fn into_handlers(
        self,
    ) -> BTreeMap<HandlerKey, Arc<dyn OpaqueMessageHandler>> {
        self.handlers
    }

    /// Adds a new handler to the registry.
    ///
    /// This is done in the form of specifying what message types are handled
    /// by the service via the generic.
    pub fn add_handler<Msg>(&mut self)
    where
        Msg: Archive + Send + Sync + 'static,
        Msg::Archived: CheckBytes<DefaultValidator<'static>> + Send + Sync + 'static,
        Svc: Handler<Msg>,
    {
        let phantom = PhantomHandler {
            handler: self.service.clone(),
            _msg: PhantomData::<Msg>::default(),
        };

        let uri = crate::to_uri_path(Svc::service_name(), <Svc as Handler<Msg>>::path());
        self.handlers.insert(crate::hash(&uri), Arc::new(phantom));
    }
}

/// A standard RPC server that handles messages.
///
/// ```rust
/// use datacake_rpc::{RpcService, ServiceRegistry};
///
/// pub struct MyService;
///
/// impl RpcService for MyService {
///     // This is an optional method which can be used
///     // to avoid naming conflicts between two services.
///     // By default this uses the type name of the service.
///     fn service_name() -> &'static str {
///         "my-lovely-service"
///     }
///
///     fn register_handlers(registry: &mut ServiceRegistry<Self>) {
///         // Register each one of our handlers here.
///     }
/// }
/// ```
pub trait RpcService: Sized {
    /// An optional name of the service.
    ///
    /// This can be used to prevent overlaps or clashes
    /// in handlers as two services may handle the same
    /// message but behave differently, to distinguish between
    /// these services, the message paths also use the service name
    /// to create a unique key.
    fn service_name() -> &'static str {
        std::any::type_name::<Self>()
    }

    /// Register all message handlers for this server with the registry.
    ///
    /// See [ServiceRegistry] for more information.
    fn register_handlers(registry: &mut ServiceRegistry<Self>);
}

#[async_trait]
/// A generic RPC message handler.
///
/// ```rust
/// use rkyv::{Archive, Deserialize, Serialize};
/// use datacake_rpc::{Handler, Request, RpcService, ServiceRegistry, Status, RpcClient, Channel};
/// use std::net::SocketAddr;
///
/// #[repr(C)]
/// #[derive(Serialize, Deserialize, Archive, Debug)]
/// #[archive_attr(derive(CheckBytes, Debug))]
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
/// // Our message must implement `Archive` and have it's archived value
/// // implement check bytes, this is used to provide the zero-copy functionality.
/// #[datacake_rpc::async_trait]
/// impl Handler<MyMessage> for EchoService {
///     // Our reply can be any type that implements `Archive` and `Serialize` as part
///     // of the rkyv package. Here we're just echoing the message back.
///     type Reply = MyMessage;
///
///     // We get passed a `Request` which is a thin wrapper around the `DataView` type.
///     // This means we are simply being given a zero-copy view of the message rather
///     // than a owned value. If you need a owned version which is not tied ot the
///     // request buffer, you can use the `to_owned` method which will attempt to
///     // deserialize the inner message/view.
///     async fn on_message(&self, msg: Request<MyMessage>) -> Result<Self::Reply, Status> {
///         Ok(msg.to_owned().unwrap())
///     }
/// }
/// ```
pub trait Handler<Msg>: RpcService
where
    Msg: Archive,
    Msg::Archived: CheckBytes<DefaultValidator<'static>> + 'static,
{
    /// Our reply can be any type that implements [Archive] and [Serialize] as part
    /// of the [rkyv] package. Here we're just echoing the message back.
    type Reply: Archive + Serialize<AllocSerializer<SCRATCH_SPACE>>;

    /// The path of the message, this is similar to the service name which can
    /// be used to avoid conflicts, by default this uses the name of the message type.
    fn path() -> &'static str {
        std::any::type_name::<Msg>()
    }

    /// Process a message.
    /// We get passed a [Request] which is a thin wrapper around the [DataView] type.
    /// This means we are simply being given a zero-copy view of the message rather
    /// than a owned value. If you need a owned version which is not tied ot the
    /// request buffer, you can use the `to_owned` method which will attempt to
    /// deserialize the inner message/view.
    async fn on_message(&self, msg: Request<Msg>) -> Result<Self::Reply, Status>;
}

#[async_trait]
pub(crate) trait OpaqueMessageHandler: Send + Sync {
    async fn try_handle(
        &self,
        remote_addr: SocketAddr,
        data: AlignedVec,
    ) -> Result<AlignedVec, AlignedVec>;
}

struct PhantomHandler<H, Msg>
where
    H: Send + Sync + 'static,
    Msg: Send + Sync + 'static,
{
    handler: Arc<H>,
    _msg: PhantomData<Msg>,
}

impl<H, Msg> Clone for PhantomHandler<H, Msg>
where
    Msg: Archive + Send + Sync + 'static,
    Msg::Archived: CheckBytes<DefaultValidator<'static>> + Send + Sync + 'static,
    H: Handler<Msg> + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            handler: self.handler.clone(),
            _msg: self._msg,
        }
    }
}

#[async_trait]
impl<H, Msg> OpaqueMessageHandler for PhantomHandler<H, Msg>
where
    Msg: Archive + Send + Sync + 'static,
    Msg::Archived: CheckBytes<DefaultValidator<'static>> + Send + Sync + 'static,
    H: Handler<Msg> + Send + Sync + 'static,
{
    async fn try_handle(
        &self,
        remote_addr: SocketAddr,
        data: AlignedVec,
    ) -> Result<AlignedVec, AlignedVec> {
        let view = match DataView::using(data) {
            Ok(view) => view,
            Err(crate::view::InvalidView) => {
                let status = Status::invalid();
                let error = rkyv::to_bytes::<_, SCRATCH_SPACE>(&status)
                    .unwrap_or_else(|_| AlignedVec::new());
                return Err(error);
            },
        };

        let msg = Request::new(remote_addr, view);

        self.handler
            .on_message(msg)
            .await
            .map(|reply| {
                rkyv::to_bytes::<_, SCRATCH_SPACE>(&reply)
                    .unwrap_or_else(|_| AlignedVec::new())
            })
            .map_err(|status| {
                rkyv::to_bytes::<_, SCRATCH_SPACE>(&status)
                    .unwrap_or_else(|_| AlignedVec::new())
            })
    }
}
