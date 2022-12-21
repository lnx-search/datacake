use std::fmt::{Debug, Display, Formatter};
use std::error::Error;
use bytecheck::CheckBytes;
use rkyv::{Serialize, Deserialize, Archive};


#[repr(C)]
#[derive(Serialize, Deserialize, Archive)]
#[archive_attr(derive(CheckBytes))]
/// Status information around the cause of a message request failing.
///
/// This includes a generic status code, message and any additional
/// information provided by the generic context.
///
/// Additional context for the error can be provided via the generic,
/// but both client and server must be expecting the same context `T`,
/// otherwise this value will be `None`.
pub struct Status<T = ()> {
    /// The generic error code of the request.
    pub code: ErrorCode,
    /// The display message for the error.
    pub message: String,
    /// Additional information about why the request failed.
    pub context: Option<T>
}

impl<T> Display for Status<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl<T> Debug for Status<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Status")
            .field("code", &self.code)
            .field("message", &self.message)
            .finish()
    }
}

impl<T> Error for Status<T> {}


#[repr(C)]
#[derive(Serialize, Deserialize, Archive, PartialEq, Eq, Debug)]
#[archive(compare(PartialEq))]
#[archive_attr(derive(CheckBytes, Debug, PartialEq, Eq))]
/// A generic error code describing the high level reason why the request failed.
pub enum ErrorCode {
    /// The service is ready but cannot handle the provided message
    /// due to it not being registered with the server itself.
    UnknownMessage,
    /// The server is running but the specified service does not exist
    /// or cannot handle messages at this time.
    ServiceUnavailable,
    /// An internal error occurred while processing the message.
    InternalError,
    /// The provided message data is invalid or unable to be deserialized
    /// by the server processing it.
    InvalidPayload,
}