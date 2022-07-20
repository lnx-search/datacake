use std::collections::VecDeque;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::Duration;
use rkyv::{Serialize, Deserialize, Archive, AlignedVec};
use bytecheck::CheckBytes;
use futures_lite::{AsyncReadExt, AsyncWriteExt};

use glommio::io::{DmaFile, DmaStreamReaderBuilder, DmaStreamWriter, DmaStreamWriterBuilder};
use uuid::Uuid;

use crate::error::{ShardError, WALError};

type LogEntries = Vec<LogEntry>;

pub struct CommitLog {
    log: LogEntries,
    create_new_file: bool,
    shard_id: Uuid,
    base_path: PathBuf,
}


impl CommitLog{
    /// Creates a new commit log writer with a given log id.
    pub async fn open(shard_id: Uuid, base_path: &Path) -> crate::error::Result<Self> {
        let maybe_log = read_persisted_log(shard_id, base_path)
            .await?;

        let create_new_file = maybe_log.is_none();
        let log = maybe_log.unwrap_or_default();

        Ok(Self {
            log,
            create_new_file,
            shard_id,
            base_path: base_path.to_path_buf(),
        })
    }

    async fn persist_log(&mut self) -> crate::error::Result<()> {
        let raw_wal = rkyv::to_bytes::<_, 2048>(&self.log)
            .map_err(|e| WALError::SerializationError(e.to_string()))?;

        let path = get_file_path(self.shard_id, &self.base_path);
        let file = if self.create_new_file {
            DmaFile::create(path)
                .await
                .map_err(WALError::from)?
        } else {
            DmaFile::open(path)
                .await
                .map_err(WALError::from)?
        };

        let mut writer = DmaStreamWriterBuilder::new(file)
            .with_buffer_size(64 << 10)
            .with_write_behind(1)
            .build();

        writer
            .write_all(&raw_wal)
            .await
            .map_err(WALError::from)?;


        Ok(())
    }

    // /// Appends the a set of active segments to the end of the commit log.
    // pub async fn append(&mut self, active_segments: Vec<Uuid>) -> crate::error::Result<()> {
    //     let log = LogEntry {
    //         time: now(),
    //         active_segments,
    //     };
    //
    //     self.append_to_file(&log).await
    // }
    //
    // async fn append_to_file(&mut self, entry: &LogEntry) -> crate::error::Result<()> {
    //     let active_segments = rkyv::to_bytes::<_, 1024>(entry)
    //         .map_err(|e| WALError::SerializationError(e.to_string()))?;
    //
    //     let serialized_length = rkyv::to_bytes::<_, 8>(&(active_segments.len() as u32))
    //         .map_err(|e| WALError::SerializationError(e.to_string()))?;
    //
    //     self.writer
    //         .write_all(&serialized_length)
    //         .await
    //         .map_err(WALError::IoError)?;
    //
    //     self.writer
    //         .write_all(&active_segments)
    //         .await
    //         .map_err(WALError::IoError)?;
    //
    //     self.writer
    //         .sync()
    //         .await
    //         .map_err(WALError::GlommioError)?;
    //
    //     Ok(())
    // }
}



#[derive(Archive, Debug, Deserialize, Serialize, Clone)]
#[archive_attr(derive(CheckBytes, Debug))]
pub struct LogEntry {
    time: Duration,
    active_segments: Vec<Uuid>,
}

mod time {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    pub fn now() -> Duration {
        SystemTime::now().duration_since(UNIX_EPOCH).expect("get time")
    }
}


#[inline]
fn get_file_path(shard_id: Uuid, base_path: &Path) -> PathBuf {
    base_path.join(shard_id.to_string())
}


async fn read_persisted_log(
    shard_id: Uuid,
    base_path: &Path,
) -> Result<Option<LogEntries>, WALError> {
    let file = match DmaFile::open(get_file_path(shard_id, base_path)).await {
        Err(glommio::GlommioError::IoError(e)) if e.kind() == ErrorKind::NotFound =>
            return Ok(None),
        Err(e) => return Err(WALError::from(e)),
        Ok(file) => file,
    };

    if file.file_size().await? == 0 {
        return Ok(None)
    }

    let mut reader = DmaStreamReaderBuilder::new(file)
        .with_buffer_size(64 << 10)
        .with_read_ahead(4)
        .build();

    let mut data = vec![];
    reader.read_to_end(&mut data).await?;

    let mut aligned = AlignedVec::with_capacity(data.len());
    aligned.extend_from_slice(&data);

    let entries = rkyv::check_archived_root::<LogEntries>(&aligned)
        .map_err(|_| WALError::Corrupted)?;

    let log: LogEntries = entries
        .deserialize(&mut rkyv::Infallible)
        .map_err(|_| WALError::Corrupted)?;

    Ok(Some(log))
}