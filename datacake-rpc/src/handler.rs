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
pub type HandlerKey = (u64, u64);

/// A registry system used for linking a service's message handlers
/// with the RPC system at runtime.
///
/// Since the RPC system cannot determine what message payload matches
/// with which handler at compile time, it must dynamically link them
/// at runtime.
///
/// Not registering a handler will cause the handler to not be triggered
/// even if a valid message comes through.
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

        self.handlers.insert(
            (
                crate::hash(Svc::service_name()),
                crate::hash(<Svc as Handler<Msg>>::path()),
            ),
            Arc::new(phantom),
        );
    }
}

/// A standard RPC server that handles messages.
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
///
pub trait Handler<Msg>: RpcService
where
    Msg: Archive,
    Msg::Archived: CheckBytes<DefaultValidator<'static>> + 'static,
{
    type Reply: Archive + Serialize<AllocSerializer<SCRATCH_SPACE>>;

    fn path() -> &'static str {
        std::any::type_name::<Msg>()
    }

    /// Process a message.
    async fn on_message(&self, msg: Request<Msg>) -> Result<Self::Reply, Status>;
}

#[async_trait]
pub(crate) trait OpaqueMessageHandler: Send + Sync {
    async fn try_handle(&self, remote_addr: SocketAddr, data: AlignedVec) -> AlignedVec;
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
    async fn try_handle(&self, remote_addr: SocketAddr, data: AlignedVec) -> AlignedVec {
        let view = match DataView::using(data) {
            Ok(view) => view,
            Err(crate::view::InvalidView) => {
                let status = Status::invalid();
                let error = rkyv::to_bytes::<_, SCRATCH_SPACE>(&status)
                    .unwrap_or_else(|_| AlignedVec::new());
                return error;
            },
        };

        let msg = Request { remote_addr, view };

        match self.handler.on_message(msg).await {
            Ok(reply) => rkyv::to_bytes(&reply).unwrap_or_else(|_| AlignedVec::new()),
            Err(status) => rkyv::to_bytes::<_, SCRATCH_SPACE>(&status)
                .unwrap_or_else(|_| AlignedVec::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytecheck::CheckBytes;
    use rkyv::{Archive, Deserialize, Serialize};

    use super::*;

    #[repr(C)]
    #[derive(Serialize, Deserialize, Archive, PartialEq, Eq, Debug)]
    #[archive(compare(PartialEq))]
    #[archive_attr(derive(CheckBytes, Debug, PartialEq, Eq))]
    pub struct Msg(u32);

    pub struct Test;

    impl RpcService for Test {
        fn register_handlers(registry: &mut ServiceRegistry<Self>) {
            registry.add_handler::<Msg>();
        }
    }

    #[async_trait]
    impl Handler<Msg> for Test {
        type Reply = ();

        async fn on_message(&self, msg: Request<Msg>) -> Result<Self::Reply, Status> {
            todo!()
        }
    }

    #[test]
    fn test_phantom_data() {
        let handler = PhantomHandler {
            handler: Arc::new(Test),
            _msg: PhantomData::<Msg>::default(),
        };
    }
}
