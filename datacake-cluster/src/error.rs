use std::error::Error;
use std::fmt::Debug;
use std::net::SocketAddr;

use thiserror::Error;

use crate::nodes_selector::ConsistencyError;

#[derive(Debug, Error)]
pub enum DatacakeError<E: Error> {
    #[error("{0}")]
    ChitChatError(String),

    #[error("An unknown error occurred during the operation: {0}")]
    UnknownError(anyhow::Error),

    #[error(
        "A failure occurred within the user provided `DataStore` implementation: {0}"
    )]
    StorageError(#[from] E),

    #[error("Failed to complete operation due to consistency level failure: {0}")]
    ConsistencyError(ConsistencyError),

    #[error("Transport Error: ({0}) - {1}")]
    TransportError(SocketAddr, tonic::transport::Error),

    #[error("Rpc Error: ({0}) - {1}")]
    RpcError(SocketAddr, tonic::Status),
}
