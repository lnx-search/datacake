use std::ops::{Deref, DerefMut};

use rkyv::{Archive, Serialize};

use crate::rkyv_tooling::DatacakeSerializer;
use crate::Status;

/// A wrapper type around the internal [hyper::Body]
pub struct Body(pub(crate) hyper::Body);

impl Body {
    /// Creates a new body.
    pub fn new(inner: hyper::Body) -> Self {
        Self(inner)
    }

    /// Consumes the body returning the inner hyper object.
    pub fn into_inner(self) -> hyper::Body {
        self.0
    }
}

impl<T> From<T> for Body
where
    T: Into<hyper::Body>,
{
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

impl Deref for Body {
    type Target = hyper::Body;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Body {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// The serializer trait converting replies into hyper bodies.
///
/// This is a light abstraction to allow users to be able to
/// stream data across the RPC system which may not fit in memory.
///
/// Any types which implement [TryAsBody] will implement this type.
pub trait TryIntoBody {
    /// Try convert the reply into a body or return an error
    /// status.
    fn try_into_body(self) -> Result<Body, Status>;
}

/// The serializer trait for converting replies into hyper bodies
/// using a reference to self.
///
/// This will work for most implementations but if you want to stream
/// hyper bodies for example, you cannot implement this trait.
pub trait TryAsBody {
    /// Try convert the reply into a body or return an error
    /// status.
    fn try_as_body(&self) -> Result<Body, Status>;
}

impl<T> TryAsBody for T
where
    T: Archive + Serialize<DatacakeSerializer>,
{
    #[inline]
    fn try_as_body(&self) -> Result<Body, Status> {
        crate::rkyv_tooling::to_view_bytes(self)
            .map(|v| Body::from(v.to_vec()))
            .map_err(|e| Status::internal(e.to_string()))
    }
}

impl<T> TryIntoBody for T
where
    T: TryAsBody,
{
    #[inline]
    fn try_into_body(self) -> Result<Body, Status> {
        <Self as TryAsBody>::try_as_body(&self)
    }
}

impl TryIntoBody for Body {
    #[inline]
    fn try_into_body(self) -> Result<Body, Status> {
        Ok(self)
    }
}
