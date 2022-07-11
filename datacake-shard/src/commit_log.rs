use std::time::Duration;
use rkyv::{Serialize, Deserialize, Archive};
use bytecheck::CheckBytes;

use glommio::io::DmaStreamWriter;
use uuid::Uuid;

pub struct CommitLogWriter {
    writer: DmaStreamWriter,
}

impl CommitLogWriter {
    pub async fn append(&mut self, active_segments: Vec<Uuid>) -> crate::error::Result<()> {
        todo!()
    }
}

#[derive(Archive, Debug, Deserialize, Serialize, Clone)]
#[archive_attr(derive(CheckBytes, Debug))]
pub struct LogEntry {
    time: Duration,
    active_segments: Vec<Uuid>,
}

