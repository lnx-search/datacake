use std::error::Error;
use std::fmt::Debug;

use thiserror::Error;

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
}
