pub type Result<T> = core::result::Result<T, ShardError>;

#[derive(Debug, thiserror::Error)]
pub enum ShardError {
    #[error("Failed to complete operation due to segment error: {0}")]
    SegmentError(#[from] datacake_segment::error::SegmentError),
}
