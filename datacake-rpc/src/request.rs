use std::borrow::Cow;
use std::net::SocketAddr;

use bytecheck::CheckBytes;
use rkyv::de::deserializers::SharedDeserializeMap;
use rkyv::validation::validators::DefaultValidator;
use rkyv::{Archive, Deserialize, Serialize};

use crate::view::{DataView, InvalidView};

#[repr(C)]
#[derive(Serialize, Deserialize, Archive, Debug)]
#[archive_attr(derive(CheckBytes, Debug, PartialEq, Eq))]
pub struct MessageMetadata {
    #[with(rkyv::with::AsOwned)]
    /// The name of the service being targeted.
    pub(crate) service_name: Cow<'static, str>,
    #[with(rkyv::with::AsOwned)]
    /// The message name/path.
    pub(crate) path: Cow<'static, str>,
}

/// A zero-copy view of the message data and any additional metadata provided
/// by the RPC system.
///
/// The request contains the original request buffer which is used to create
/// the 'view' of the given message type.
pub struct Request<Msg>
where
    Msg: Archive,
    Msg::Archived: CheckBytes<DefaultValidator<'static>> + 'static,
{
    pub(crate) remote_addr: SocketAddr,

    // A small hack around how linters behave to prevent
    // them raising incorrect errors which may be confusing
    // for users who are implementing the required traits.
    #[cfg(debug_assertions)]
    pub(crate) view: Box<DataView<Msg>>,

    #[cfg(not(debug_assertions))]
    pub(crate) view: DataView<Msg>,
}

impl<Msg> Request<Msg>
where
    Msg: Archive,
    Msg::Archived: CheckBytes<DefaultValidator<'static>> + 'static,
{
    pub(crate) fn new(remote_addr: SocketAddr, view: DataView<Msg>) -> Self {
        Self {
            remote_addr,
            #[cfg(debug_assertions)]
            view: Box::new(view),

            #[cfg(not(debug_assertions))]
            view,
        }
    }

    #[cfg(debug_assertions)]
    /// Consumes the request into the data view of the message.
    pub fn into_view(self) -> DataView<Msg> {
        *self.view
    }

    #[cfg(not(debug_assertions))]
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
