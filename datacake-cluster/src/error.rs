use std::fmt::{Debug, Display};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum DatacakeError<E: Display + Debug> {
    #[error("{0}")]
    ChitChatError(String),

    #[error("An unknown error occurred during the operation: {0}")]
    UnknownError(anyhow::Error),

    #[error(
        "A failure occurred within the user provided `DataStore` implementation: {0}"
    )]
    DatastoreError(E),
}
