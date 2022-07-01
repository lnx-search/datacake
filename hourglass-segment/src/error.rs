use std::io;

pub type Result<T> = core::result::Result<T, SegmentError>;

#[derive(Debug, thiserror::Error)]
pub enum SegmentError {
    #[error("Failed to complete operation due to error: {0}")]
    IoError(#[from] io::Error),

    #[error("Failed to create new segment writer: {0}")]
    SegmentCreationError(String),

    #[error("Failed to open immutable segment writer: {0}")]
    SegmentOpenError(String),

    #[error("Failed to serialize document: {0}")]
    SerializationError(String),
}
