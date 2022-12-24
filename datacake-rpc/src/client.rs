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

pub type MessageReply<Svc, Msg> = DataView<<Svc as Handler<Msg>>::Reply>;

/// A RPC client handle for a given service.
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
