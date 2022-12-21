use std::fmt::Debug;

use thiserror::Error;

use crate::nodes_selector::ConsistencyError;

#[derive(Debug, Error)]
pub enum NodeError {
    #[error("{0}")]
    /// An error has occurred within Chitchat.
    ChitChatError(String),

    #[error("Failed to complete operation due to consistency level failure: {0}")]
    /// The operation succeeded on the local node but failed to meet the required
    /// consistency level within the timeout period. (2 seconds)
    ConsistencyError(ConsistencyError),

    #[error("Failed to initialised cluster extension: {0}")]
    ExtensionError(anyhow::Error),
}
