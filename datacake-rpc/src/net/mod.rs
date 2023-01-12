mod client;
mod server;
mod status;

#[cfg(feature = "turmoil")]
mod simulation;

use std::io;
pub use client::Channel;
pub(crate) use server::start_rpc_server;
pub use status::{ArchivedErrorCode, ArchivedStatus, ErrorCode, Status};


#[derive(Debug, thiserror::Error)]
/// A failure in an RPC operation.
pub enum Error {
    #[error("IO Error: {0}")]
    /// The system failed to complete operation due to an IO error.
    Io(#[from] io::Error),
    #[error("Hyper Error: {0}")]
    /// The operation failed due an error originating in hyper.
    Hyper(#[from] hyper::Error),
}

