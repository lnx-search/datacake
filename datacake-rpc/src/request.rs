use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::ops::Deref;

use async_trait::async_trait;
use rkyv::Archive;

use crate::rkyv_tooling::DataView;
use crate::{Body, Status};

#[async_trait]
/// The deserializer trait for converting the request body into
/// the desired type specified by [Self::Content].
///
/// This trait is automatically implemented for the [Body] type
/// and any type implementing [rkyv]'s (de)serializer traits.
pub trait RequestContents {
    /// The deserialized message type.
    type Content: Send + Sized + 'static;

    async fn from_body(body: Body) -> Result<Self::Content, Status>;
}

#[async_trait]
impl RequestContents for Body {
    type Content = Self;

    async fn from_body(body: Body) -> Result<Self, Status> {
        Ok(body)
    }
}

#[async_trait]
impl<Msg> RequestContents for Msg
where
    Msg: Archive + Send + Sync + 'static,
    Msg::Archived: Send + Sync + 'static,
{
    type Content = DataView<Self>;

    async fn from_body(body: Body) -> Result<Self::Content, Status> {
        let bytes = crate::utils::to_aligned(body.0)
            .await
            .map_err(Status::internal)?;

        DataView::using(bytes).map_err(|_| Status::invalid())
    }
}

#[derive(PartialEq)]
#[cfg_attr(test, derive(Debug))]
pub struct MessageMetadata {
    /// The name of the service being targeted.
    pub(crate) service_name: &'static str,
    /// The message name/path.
    pub(crate) path: &'static str,
}

impl MessageMetadata {
    #[inline]
    /// Produces a uri path for the metadata.
    pub(crate) fn to_uri_path(&self) -> String {
        crate::to_uri_path(self.service_name, self.path)
    }
}

/// A zero-copy view of the message data and any additional metadata provided
/// by the RPC system.
///
/// The request contains the original request buffer which is used to create
/// the 'view' of the given message type.
pub struct Request<Msg>
where
    Msg: RequestContents,
{
    pub(crate) remote_addr: SocketAddr,

    // A small hack to stop linters miss-guiding users
    // into thinking their messages are `!Sized` when in fact they are.
    // We don't want to box in release mode however.
    #[cfg(debug_assertions)]
    pub(crate) view: Box<Msg::Content>,
    #[cfg(not(debug_assertions))]
    pub(crate) view: Msg::Content,
}

impl<Msg> Debug for Request<Msg>
where
    Msg: RequestContents,
    Msg::Content: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Request")
            .field("view", &self.view)
            .field("remote_addr", &self.remote_addr)
            .finish()
    }
}

impl<Msg> Deref for Request<Msg>
where
    Msg: RequestContents,
{
    type Target = Msg::Content;

    fn deref(&self) -> &Self::Target {
        &self.view
    }
}

impl<Msg> Request<Msg>
where
    Msg: RequestContents,
{
    pub(crate) fn new(remote_addr: SocketAddr, view: Msg::Content) -> Self {
        Self {
            remote_addr,
            #[cfg(debug_assertions)]
            view: Box::new(view),
            #[cfg(not(debug_assertions))]
            view,
        }
    }

    #[cfg(debug_assertions)]
    /// Consumes the request into the value of the message.
    pub fn into_inner(self) -> Msg::Content {
        *self.view
    }

    #[cfg(not(debug_assertions))]
    /// Consumes the request into the value of the message.
    pub fn into_inner(self) -> Msg::Content {
        self.view
    }

    /// The remote address of the incoming message.
    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }
}

#[cfg(feature = "test-utils")]
impl<Msg> Request<Msg>
where
    Msg: RequestContents + rkyv::Serialize<crate::rkyv_tooling::DatacakeSerializer>,
{
    /// A test utility for creating a mocked request.
    ///
    /// This takes the owned value of the msg and acts like the target request.
    ///
    /// This should be used for testing only.
    pub async fn using_owned(msg: Msg) -> Self {
        let bytes = crate::rkyv_tooling::to_view_bytes(&msg).unwrap();
        let contents = Msg::from_body(Body::from(bytes.to_vec())).await.unwrap();

        use std::net::{Ipv4Addr, SocketAddrV4};

        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from([127, 0, 0, 1]), 80));
        Self::new(addr, contents)
    }
}
