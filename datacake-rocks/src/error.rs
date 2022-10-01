use thiserror::Error;

pub(crate) type Result<T> = core::result::Result<T, RocksStoreError>;

#[derive(Debug, Error)]
pub enum RocksStoreError {
    #[error("An unexpected error occurred from rocks: {0}")]
    RocksDbError(#[from] rocksdb::Error),

    #[error("Failed to spawn a worker thread.")]
    ThreadSpawnError,

    #[error("Failed to deserialize entry: {0}")]
    DeserializationError(String),

    #[error("Failed to serialize entry")]
    SerializationError,

    #[error("The targeted shard has died unexpectedly.")]
    DeadShard,
}
