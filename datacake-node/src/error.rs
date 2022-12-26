use std::fmt::Debug;
use std::io;

use thiserror::Error;

use crate::nodes_selector::ConsistencyError;

#[derive(Debug, Error)]
pub enum NodeError {
    #[error("{0}")]
    /// An error has occurred within Chitchat.
    ChitChat(String),

    #[error("{0}")]
    /// An IO error has occurred,
    IO(#[from] io::Error),

    #[error("Failed to complete operation due to consistency level failure: {0}")]
    /// The operation succeeded on the local node but failed to meet the required
    /// consistency level within the timeout period. (2 seconds)
    Consistency(ConsistencyError),

    #[error("Failed to initialised cluster extension: {0}")]
    Extension(anyhow::Error),
}
