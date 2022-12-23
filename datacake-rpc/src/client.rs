use std::borrow::Cow;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;

use bytecheck::CheckBytes;
use rkyv::ser::serializers::AllocSerializer;
use rkyv::validation::validators::DefaultValidator;
use rkyv::{Archive, Serialize};
use tokio::time::Instant;

use crate::handler::{Handler, RpcService};
use crate::net::{
    ClientConnection,
    ConnectionChannel,
    SendMsgError,
    Status,
};
use crate::request::MessageMetadata;
use crate::{DataView, SCRATCH_SPACE};

pub type MessageReply<Svc, Msg> = DataView<<Svc as Handler<Msg>>::Reply>;

#[derive(Clone)]
/// A RPC QUIC client connection.
///
/// This is a cheap to clone handle to the underlying QUIC connection and
/// can be used to produce [RpcClient]s.
pub struct Client {
    conn: ClientConnection,
}

impl Client {
    /// Connects to a remote RPC server.
    pub async fn connect(remote_addr: SocketAddr) -> io::Result<Self> {
        let conn = ClientConnection::connect(remote_addr).await?;
        Ok(Self { conn })
    }

    /// Creates a new RPC client which can handle a new service type.
    ///
    /// [RpcClient]'s are cheap to create and should be preferred over
    /// locking or other synchronization primitives.
    pub async fn create_rpc_client<Svc>(&self) -> io::Result<RpcClient<Svc>>
    where
        Svc: RpcService,
    {
        let channel = self.conn.open_channel().await?;
        Ok(RpcClient {
            client: self.clone(),
            channel,
            _p: PhantomData,
        })
    }
}

/// A RPC client handle for a given service.
pub struct RpcClient<Svc>
where
    Svc: RpcService,
{
    client: Client,
    channel: ConnectionChannel,
    _p: PhantomData<Svc>,
}

impl<Svc> RpcClient<Svc>
where
    Svc: RpcService,
{
    /// Creates a new RPC client which can handle a new service type.
    ///
    /// [RpcClient]'s are cheap to create and should be preferred over
    /// locking or other synchronization primitives.
    pub async fn new_client<Svc2>(&self) -> io::Result<RpcClient<Svc2>>
    where
        Svc2: RpcService,
    {
        self.client.create_rpc_client().await
    }

    /// Creates a new RPC client which handles the same service type.
    ///
    /// [RpcClient]'s are cheap to create and should be preferred over
    /// locking or other synchronization primitives.
    pub async fn clone_client(&self) -> io::Result<Self> {
        self.client.create_rpc_client().await
    }

    /// Sends a message to the server and wait for a reply.
    pub async fn send<Msg>(
        &mut self,
        msg: &Msg,
    ) -> Result<MessageReply<Svc, Msg>, Status>
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

        self.channel
            .send_msg(&metadata, &msg_bytes)
            .await
            .map_err(|e| match e {
                SendMsgError::IoError(e) => Status::connection(e),
                SendMsgError::Status(s) => s,
            })?;

        let result = self
            .channel
            .recv_msg()
            .await
            .map_err(Status::connection)?
            .ok_or_else(Status::closed)?;

        match result {
            Ok((_, msg)) => {
                let view = DataView::using(msg).map_err(|_| Status::invalid())?;
                Ok(view)
            },
            Err(status) => {
                let status = rkyv::from_bytes(&status).map_err(|_| Status::invalid())?;
                Err(status)
            },
        }
    }
}
