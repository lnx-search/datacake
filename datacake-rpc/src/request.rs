use std::borrow::Cow;
use std::net::SocketAddr;

use bytecheck::CheckBytes;
use rkyv::de::deserializers::SharedDeserializeMap;
use rkyv::validation::validators::DefaultValidator;
use rkyv::{Archive, Deserialize, Serialize};

use crate::view::{DataView, InvalidView};

#[repr(C)]
#[derive(Serialize, Deserialize, Archive)]
#[archive_attr(derive(CheckBytes, Debug, PartialEq, Eq))]
pub(crate) struct MessageMetadata {
    #[with(rkyv::with::AsOwned)]
    /// The name of the service being targeted.
    pub(crate) service_name: Cow<'static, str>,
    #[with(rkyv::with::AsOwned)]
    /// The message name/path.
    pub(crate) path: Cow<'static, str>,
}

pub struct Request<Msg>
where
    Msg: Archive,
    Msg::Archived: CheckBytes<DefaultValidator<'static>> + 'static,
{
    pub(crate) remote_addr: SocketAddr,
    pub(crate) view: DataView<Msg>,
}

impl<Msg> Request<Msg>
where
    Msg: Archive,
    Msg::Archived: CheckBytes<DefaultValidator<'static>> + 'static,
{
    /// Consumes the request into the data view of the message.
    pub fn into_view(self) -> DataView<Msg> {
        self.view
    }

    /// The remote address of the incoming message.
    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }
}

impl<Msg> Request<Msg>
where
    Msg: Archive,
    Msg::Archived: CheckBytes<DefaultValidator<'static>>
        + Deserialize<Msg, SharedDeserializeMap>
        + 'static,
{
    /// Deserializes the view into it's owned value T.
    pub fn to_owned(&self) -> Result<Msg, InvalidView> {
        self.view.to_owned()
    }
}
