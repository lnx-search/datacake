use std::borrow::Cow;
use std::marker::PhantomData;

use bytecheck::CheckBytes;
use bytes::Bytes;
use rkyv::ser::serializers::AllocSerializer;
use rkyv::validation::validators::DefaultValidator;
use rkyv::{Archive, Serialize};

use crate::handler::{Handler, RpcService};
use crate::net::{Channel, Status};
use crate::request::MessageMetadata;
use crate::{DataView, SCRATCH_SPACE};

/// A type alias for the returned data view of the RPC message reply.
pub type MessageReply<Svc, Msg> = DataView<<Svc as Handler<Msg>>::Reply>;

/// A RPC client handle for a given service.
///
/// ```rust
/// use bytecheck::CheckBytes;
/// use rkyv::{Archive, Deserialize, Serialize};
/// use datacake_rpc::{Handler, Request, RpcService, ServiceRegistry, Status, RpcClient, Channel};
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
/// # use datacake_rpc::Server;
/// # let bind = "127.0.0.1:8000".parse::<SocketAddr>().unwrap();
/// # let server = Server::listen(bind).await.unwrap();
/// # server.add_service(EchoService);
/// let connect = "127.0.0.1:8000".parse::<SocketAddr>()?;
/// let client = Channel::connect(connect);
///
/// let rpc_client = RpcClient::<EchoService>::new(client);
///
/// let msg = MyMessage {
///     name: "Bobby".to_string(),
///     age: 12,
/// };
///
/// let resp = rpc_client.send(&msg).await?;
/// assert_eq!(resp, msg);
/// # Ok(())
/// # }
/// ```
pub struct RpcClient<Svc>
where
    Svc: RpcService,
{
    channel: Channel,
    _p: PhantomData<Svc>,
}

impl<Svc> Clone for RpcClient<Svc>
where
    Svc: RpcService,
{
    fn clone(&self) -> Self {
        Self {
            channel: self.channel.clone(),
            _p: PhantomData,
        }
    }
}
impl<Svc> RpcClient<Svc>
where
    Svc: RpcService,
{
    /// Creates a new RPC client which can handle a new service type.
    ///
    /// [RpcClient]'s are cheap to create and should be preferred over
    /// locking or other synchronization primitives.
    pub fn new(channel: Channel) -> Self {
        Self {
            channel,
            _p: PhantomData,
        }
    }

    /// Creates a new RPC client which can handle a new service type.
    ///
    /// [RpcClient]'s are cheap to create and should be preferred over
    /// locking or other synchronization primitives.
    pub fn new_client<Svc2>(&self) -> RpcClient<Svc2>
    where
        Svc2: RpcService,
    {
        RpcClient {
            channel: self.channel.clone(),
            _p: PhantomData,
        }
    }

    /// Sends a message to the server and wait for a reply.
    pub async fn send<Msg>(&self, msg: &Msg) -> Result<MessageReply<Svc, Msg>, Status>
    where
        Msg: Archive + Serialize<AllocSerializer<SCRATCH_SPACE>>,
        Msg::Archived: CheckBytes<DefaultValidator<'static>> + 'static,
        Svc: Handler<Msg>,
        // Due to some interesting compiler errors, we couldn't use GATs here to enforce
        // this on the trait side, which is a shame.
        <Svc as Handler<Msg>>::Reply:
            Archive + Serialize<AllocSerializer<SCRATCH_SPACE>>,
        <<Svc as Handler<Msg>>::Reply as Archive>::Archived:
            CheckBytes<DefaultValidator<'static>> + 'static,
    {
        let metadata = MessageMetadata {
            service_name: Cow::Borrowed(<Svc as RpcService>::service_name()),
            path: Cow::Borrowed(<Svc as Handler<Msg>>::path()),
        };

        let msg_bytes =
            rkyv::to_bytes::<_, SCRATCH_SPACE>(msg).map_err(|_| Status::invalid())?;

        let result = self
            .channel
            .send_msg(metadata, Bytes::from(msg_bytes.into_vec()))
            .await
            .map_err(Status::connection)?;

        match result {
            Ok(buffer) => {
                let view = DataView::using(buffer).map_err(|_| Status::invalid())?;
                Ok(view)
            },
            Err(buffer) => {
                let status = rkyv::from_bytes(&buffer).map_err(|_| Status::invalid())?;
                Err(status)
            },
        }
    }
}
