use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};

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
pub struct Status {
    /// The generic error code of the request.
    pub code: ErrorCode,
    /// The display message for the error.
    pub message: String,
}

impl Status {
    /// The server is running but the specified service does not exist
    /// or cannot handle messages at this time.
    pub fn unavailable(msg: impl Display) -> Self {
        Self {
            code: ErrorCode::ServiceUnavailable,
            message: msg.to_string(),
        }
    }

    /// An internal error occurred while processing the message.
    pub fn internal(msg: impl Display) -> Self {
        Self {
            code: ErrorCode::InternalError,
            message: msg.to_string(),
        }
    }

    /// The provided message data is invalid or unable to be deserialized
    /// by the server processing it.
    pub fn invalid() -> Self {
        Self {
            code: ErrorCode::InvalidPayload,
            message: "Invalid message payload was provided to be deserialized."
                .to_string(),
        }
    }

    /// The connection is closed or interrupted during the operation.
    pub fn connection(msg: impl Display) -> Self {
        Self {
            code: ErrorCode::ConnectionError,
            message: msg.to_string(),
        }
    }

    /// The RPC channel was closed before the message could be completed.
    pub fn closed() -> Self {
        Self {
            code: ErrorCode::ConnectionError,
            message: "The connection was closed before the message could be received."
                .to_string(),
        }
    }
}

impl Display for Status {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl Debug for Status {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Status")
            .field("code", &self.code)
            .field("message", &self.message)
            .finish()
    }
}

impl Error for Status {}

#[repr(C)]
#[derive(Serialize, Deserialize, Archive, PartialEq, Eq, Debug)]
#[archive(compare(PartialEq))]
#[archive_attr(derive(CheckBytes, Debug, PartialEq, Eq))]
/// A generic error code describing the high level reason why the request failed.
pub enum ErrorCode {
    /// The server is running but the specified service does not exist
    /// or cannot handle messages at this time.
    ServiceUnavailable,
    /// An internal error occurred while processing the message.
    InternalError,
    /// The provided message data is invalid or unable to be deserialized
    /// by the server processing it.
    InvalidPayload,
    /// The connection is closed or interrupted during the operation.
    ConnectionError,
    /// The RPC channel was closed before the message could be completed.
    ChannelClosed,
}
