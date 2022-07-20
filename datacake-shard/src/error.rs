use std::io;
use glommio::GlommioError;

pub type Result<T> = core::result::Result<T, ShardError>;

#[derive(Debug, thiserror::Error)]
pub enum ShardError {
    #[error("Failed to complete operation due to segment error: {0}")]
    SegmentError(#[from] datacake_segment::error::SegmentError),

    #[error("Failed to complete operation due to an error occurring during the WAL commit phase: {0}")]
    WALError(#[from] WALError),
}

#[derive(Debug, thiserror::Error)]
pub enum WALError {
    #[error("{0}")]
    SerializationError(String),

    #[error("{0}")]
    IoError(#[from] io::Error),

    #[error("{0}")]
    GlommioError(GlommioError<()>),
    
    #[error("Cannot read data contained in the wal, the data appears to be corrupted.")]
    Corrupted,
}

impl From<GlommioError<()>> for WALError {
    fn from(e: GlommioError<()>) -> Self {
        match e {
            GlommioError::IoError(e) => Self::IoError(e),
            other => Self::GlommioError(other)
        }
    }
}