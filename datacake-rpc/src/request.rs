use std::borrow::Cow;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::ops::Deref;

use async_trait::async_trait;
use bytecheck::CheckBytes;
use rkyv::validation::validators::DefaultValidator;
use rkyv::{Archive, Deserialize, Serialize};

use crate::view::DataView;
use crate::{Body, Status};

#[async_trait]
pub trait RequestContents {
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
    Msg::Archived: CheckBytes<DefaultValidator<'static>> + Send + Sync + 'static,
{
    type Content = DataView<Self>;

    async fn from_body(body: Body) -> Result<Self::Content, Status> {
        let bytes = crate::utils::to_aligned(body.0)
            .await
            .map_err(Status::internal)?;

        DataView::using(bytes).map_err(|_| Status::invalid())
    }
}

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
    Msg: RequestContents
        + rkyv::Serialize<
            rkyv::ser::serializers::AllocSerializer<{ crate::SCRATCH_SPACE }>,
        >,
{
    /// A test utility for creating a mocked request.
    ///
    /// This takes the owned value of the msg and acts like the target request.
    ///
    /// This should be used for testing only.
    pub async fn using_owned(msg: Msg) -> Self {
        let bytes = rkyv::to_bytes(&msg).unwrap();
        let contents = Msg::from_body(Body::from(bytes.to_vec())).await.unwrap();

        use std::net::{Ipv4Addr, SocketAddrV4};

        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from([127, 0, 0, 1]), 80));
        Self::new(addr, contents)
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
        let req = Request::<MessageMetadata>::new(addr, view);
        assert_eq!(req.remote_addr(), addr, "Remote addr should match.");
        assert_eq!(
            req.to_owned().unwrap(),
            msg,
            "Deserialized value should match."
        );
    }
}
