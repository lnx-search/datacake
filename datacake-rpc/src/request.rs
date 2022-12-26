use std::borrow::Cow;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;

use bytecheck::CheckBytes;
use rkyv::de::deserializers::SharedDeserializeMap;
use rkyv::validation::validators::DefaultValidator;
use rkyv::{Archive, Deserialize, Serialize};

use crate::view::{DataView, InvalidView};

#[repr(C)]
#[derive(Serialize, Deserialize, Archive, PartialEq)]
#[cfg_attr(test, derive(Debug))]
#[archive_attr(derive(CheckBytes))]
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
    Msg::Archived: 'static,
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

impl<Msg> Debug for Request<Msg>
where
    Msg: Archive,
    Msg::Archived: CheckBytes<DefaultValidator<'static>> + Debug + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Request")
            .field("view", &self.view)
            .field("remote_addr", &self.remote_addr)
            .finish()
    }
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
    Msg::Archived: Deserialize<Msg, SharedDeserializeMap> + 'static,
{
    /// Deserializes the view into it's owned value T.
    pub fn to_owned(&self) -> Result<Msg, InvalidView> {
        self.view.to_owned()
    }
}

#[cfg(feature = "test-utils")]
impl<Msg> Request<Msg>
where
    Msg: Archive
        + Serialize<rkyv::ser::serializers::AllocSerializer<{ crate::SCRATCH_SPACE }>>,
    Msg::Archived: CheckBytes<DefaultValidator<'static>> + 'static,
{
    /// A test utility for creating a mocked request.
    ///
    /// This takes the owned value of the msg and acts like the target request.
    ///
    /// This should be used for testing only.
    pub fn using_owned(msg: Msg) -> Self {
        use std::net::{Ipv4Addr, SocketAddrV4};

        let buf = rkyv::to_bytes::<_, { crate::SCRATCH_SPACE }>(&msg).unwrap();
        let view = DataView::using(buf).unwrap();
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from([127, 0, 0, 1]), 80));
        Self::new(addr, view)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata() {
        let meta = MessageMetadata {
            service_name: Cow::Borrowed("test"),
            path: Cow::Borrowed("demo"),
        };

        let bytes = rkyv::to_bytes::<_, 1024>(&meta).expect("Serialize");
        let copy: MessageMetadata = rkyv::from_bytes(&bytes).expect("Deserialize");
        assert_eq!(meta, copy, "Deserialized value should match");
    }

    #[test]
    fn test_request() {
        let msg = MessageMetadata {
            service_name: Cow::Borrowed("test"),
            path: Cow::Borrowed("demo"),
        };

        let addr = "127.0.0.1:8000".parse().unwrap();
        let bytes = rkyv::to_bytes::<_, 1024>(&msg).expect("Serialize");
        let view: DataView<MessageMetadata, _> =
            DataView::using(bytes).expect("Create view");
        let req = Request::new(addr, view);
        assert_eq!(req.remote_addr(), addr, "Remote addr should match.");
        assert_eq!(
            req.to_owned().unwrap(),
            msg,
            "Deserialized value should match."
        );
    }
}
