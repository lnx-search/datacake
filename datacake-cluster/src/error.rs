use std::fmt::{Debug, Display};

use thiserror::Error;

// use crate::rpc::RpcError;

#[derive(Debug, Error)]
pub enum DatacakeError<E: Display + Debug> {
    // #[error("A failure occurred within the RPC system: {0}")]
    // RpcError(#[from] RpcError),
    #[error("{0}")]
    ChitChatError(String),

    #[error("An unknown error occurred during the operation: {0}")]
    UnknownError(String),

    #[error(
        "A failure occurred within the user provided `DataStore` implementation: {0}"
    )]
    DatastoreError(E),
}
