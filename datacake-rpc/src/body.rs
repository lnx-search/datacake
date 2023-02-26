use std::ops::{Deref, DerefMut};

/// A wrapper type around the internal [hyper::Body]
pub struct Body(pub(crate) hyper::Body);

impl Body {
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
