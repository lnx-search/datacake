use std::io;
use glommio::GlommioError;

pub type Result<T> = core::result::Result<T, SegmentError>;

#[derive(Debug, thiserror::Error)]
pub enum SegmentError {
    #[error("Failed to complete operation due to error: {0}")]
    IoError(#[from] io::Error),

    #[error("Failed to create new segment writer: {0}")]
    SegmentCreationError(String),

    #[error("Failed to open immutable segment writer: {0}")]
    SegmentOpenError(String),

    #[error("Failed to complete operation due to error from the glommio executor: {0}")]
    GlommioError(String),

    #[error("Failed to serialize document: {0}")]
    SerializationError(String),

    #[error("Failed to deserialize data: {0}")]
    DeserializationError(String),

    #[error("Failed to read segment block: {0}")]
    BlockReadError(String),
}

impl From<GlommioError<()>> for SegmentError {
    fn from(e: GlommioError<()>) -> Self {
        match e {
            GlommioError::IoError(e) => Self::IoError(e),
            other => Self::GlommioError(other.to_string())
        }
    }
}