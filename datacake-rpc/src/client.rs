use std::future::Future;
use std::marker::PhantomData;
use std::time::Duration;

use http::header::IntoHeaderName;
use http::{HeaderMap, HeaderValue, StatusCode};

use crate::body::{Body, TryAsBody, TryIntoBody};
use crate::handler::{Handler, RpcService};
use crate::net::{Channel, Status};
use crate::request::{MessageMetadata, RequestContents};
use crate::DataView;

/// A type alias for the returned data view of the RPC message reply.
pub type MessageReply<Svc, Msg> =
    <<Svc as Handler<Msg>>::Reply as RequestContents>::Content;

/// A RPC client handle for a given service.
///
/// ```rust
/// use rkyv::{Archive, Deserialize, Serialize};
/// use datacake_rpc::{Handler, Request, RpcService, ServiceRegistry, Status, RpcClient, Channel};
/// use std::net::SocketAddr;
///
/// #[repr(C)]
/// #[derive(Serialize, Deserialize, Archive, PartialEq, Debug)]
/// #[archive(compare(PartialEq), check_bytes)]
/// #[archive_attr(derive(PartialEq, Debug))]
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
    timeout: Option<Duration>,
    _p: PhantomData<Svc>,
}

impl<Svc> Clone for RpcClient<Svc>
where
    Svc: RpcService,
{
    fn clone(&self) -> Self {
        Self {
            channel: self.channel.clone(),
            timeout: self.timeout,
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
            timeout: None,
            _p: PhantomData,
        }
    }

    /// Sets a timeout of a given amount of time.
    ///
    /// If any requests exceed this amount of time `Status::timeout` is returned.
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = Some(timeout);
    }

    #[inline]
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
            timeout: None,
            _p: PhantomData,
        }
    }

    #[inline]
    /// Sends a message to the server and wait for a reply.
    ///
    /// This lets you send messages behind a reference which can help
    /// avoid excess copies when it isn't needed.
    ///
    /// In the event you need to send a [Body] or type which must consume `self`
    /// you can use [Self::send_owned]
    pub fn send<'a, 'slf: 'a, Msg>(
        &'slf self,
        msg: &'a Msg,
    ) -> impl Future<Output = Result<MessageReply<Svc, Msg>, Status>> + 'a
    where
        Msg: RequestContents + TryAsBody,
        Svc: Handler<Msg>,
        // Due to some interesting compiler errors, we couldn't use GATs here to enforce
        // this on the trait side, which is a shame.
        <Svc as Handler<Msg>>::Reply: RequestContents + TryIntoBody,
    {
        let ctx = self.create_rpc_context();
        ctx.send(msg)
    }

    #[inline]
    /// Sends a message to the server and wait for a reply using an owned
    /// message value.
    ///
    /// This allows you to send types implementing [TryIntoBody] like [Body].
    pub fn send_owned<'slf, Msg>(
        &'slf self,
        msg: Msg,
    ) -> impl Future<Output = Result<MessageReply<Svc, Msg>, Status>> + 'slf
    where
        Msg: RequestContents + TryIntoBody + 'slf,
        Svc: Handler<Msg>,
        // Due to some interesting compiler errors, we couldn't use GATs here to enforce
        // this on the trait side, which is a shame.
        <Svc as Handler<Msg>>::Reply: RequestContents + TryIntoBody,
    {
        let ctx = self.create_rpc_context();
        ctx.send_owned(msg)
    }

    #[inline]
    /// Creates a new RPC context which can customise more of
    /// the request than the convenience methods, i.e. Headers.
    pub fn create_rpc_context(&self) -> RpcContext<Svc> {
        RpcContext {
            client: self,
            headers: HeaderMap::new(),
        }
    }
}

/// A configurable RPC context that can be used to
/// fine tune the request/response of the RPC calls.
///
/// This allows you to pass and receive headers while
/// being reasonably convenient and without
/// breaking existing implementations.
pub struct RpcContext<'a, Svc>
where
    Svc: RpcService,
{
    client: &'a RpcClient<Svc>,
    headers: HeaderMap,
}

impl<'a, Svc> RpcContext<'a, Svc>
where
    Svc: RpcService,
{
    /// Set a single request header.
    pub fn set_header<K>(mut self, key: K, value: HeaderValue) -> Self
    where
        K: IntoHeaderName,
    {
        self.headers.insert(key, value);
        self
    }

    /// Set multiple request headers.
    pub fn set_headers<K, I>(mut self, headers: I) -> Self
    where
        K: IntoHeaderName,
        I: IntoIterator<Item = (K, HeaderValue)>,
    {
        for (key, value) in headers {
            self.headers.insert(key, value);
        }
        self
    }

    /// Sends a message to the server and wait for a reply.
    ///
    /// This lets you send messages behind a reference which can help
    /// avoid excess copies when it isn't needed.
    ///
    /// In the event you need to send a [Body] or type which must consume `self`
    /// you can use [Self::send_owned]
    pub async fn send<Msg>(self, msg: &Msg) -> Result<MessageReply<Svc, Msg>, Status>
    where
        Msg: RequestContents + TryAsBody,
        Svc: Handler<Msg>,
        // Due to some interesting compiler errors, we couldn't use GATs here to enforce
        // this on the trait side, which is a shame.
        <Svc as Handler<Msg>>::Reply: RequestContents + TryIntoBody,
    {
        let metadata = MessageMetadata {
            service_name: <Svc as RpcService>::service_name(),
            path: <Svc as Handler<Msg>>::path(),
        };

        let body = msg.try_as_body()?;
        self.send_inner(body, metadata).await
    }

    /// Sends a message to the server and wait for a reply using an owned
    /// message value.
    ///
    /// This allows you to send types implementing [TryIntoBody] like [Body].
    pub async fn send_owned<Msg>(
        self,
        msg: Msg,
    ) -> Result<MessageReply<Svc, Msg>, Status>
    where
        Msg: RequestContents + TryIntoBody,
        Svc: Handler<Msg>,
        // Due to some interesting compiler errors, we couldn't use GATs here to enforce
        // this on the trait side, which is a shame.
        <Svc as Handler<Msg>>::Reply: RequestContents + TryIntoBody,
    {
        let metadata = MessageMetadata {
            service_name: <Svc as RpcService>::service_name(),
            path: <Svc as Handler<Msg>>::path(),
        };

        let body = msg.try_into_body()?;
        self.send_inner(body, metadata).await
    }

    async fn send_inner<Msg>(
        self,
        body: Body,
        metadata: MessageMetadata,
    ) -> Result<MessageReply<Svc, Msg>, Status>
    where
        Msg: RequestContents,
        Svc: Handler<Msg>,
        // Due to some interesting compiler errors, we couldn't use GATs here to enforce
        // this on the trait side, which is a shame.
        <Svc as Handler<Msg>>::Reply: RequestContents + TryIntoBody,
    {
        let future = self.client.channel.send_parts(metadata, self.headers, body);

        let response = match self.client.timeout {
            Some(duration) => tokio::time::timeout(duration, future)
                .await
                .map_err(|_| Status::timeout())?
                .map_err(Status::connection)?,
            None => future.await.map_err(Status::connection)?,
        };

        let (head, body) = response.into_parts();

        if head.status == StatusCode::OK {
            return <<Svc as Handler<Msg>>::Reply>::from_body(Body::new(body)).await;
        }

        let buffer = crate::utils::to_aligned(body)
            .await
            .map_err(|e| Status::internal(e.message()))?;
        let status = DataView::<Status>::using(buffer).map_err(|_| Status::invalid())?;
        Err(status.to_owned().unwrap_or_else(|_| Status::invalid()))
    }
}
