use std::path::Path;
use futures_lite::AsyncWriteExt;

use glommio::io::{DmaFile, DmaStreamWriter, DmaStreamWriterBuilder, ImmutableFile, ImmutableFileBuilder};
use hourglass_data::block::{BlockReader, BlockWriter};
use hourglass_data::blocking::BlockingExecutor;
use hourglass_data::Id;
use hourglass_data::segment_footer::{MAX_SEGMENT_SIZE, SegmentFooterWriter};
use hourglass_data::value::Document;

use crate::error::{Result, SegmentError};

pub struct SegmentWriter {
    /// The total number of bytes written to disk that the process is aware of.
    num_bytes_written: usize,

    /// A threadpool executor for running CPU intensive operations.
    executor: BlockingExecutor,

    footer: SegmentFooterWriter,
    writer: DmaStreamWriter,
    active_block: BlockWriter,
}

impl SegmentWriter {
    /// Creates a new segment writer.
    ///
    /// This intern creates a new file and empty block writer.
    pub async fn create(executor: BlockingExecutor, path: &Path) -> Result<Self> {
        let file = DmaFile::create(path)
            .await
            .map_err(|v| SegmentError::SegmentCreationError(v.to_string()))?;

        let file = DmaStreamWriterBuilder::new(file)
            .with_write_behind(10)
            .with_buffer_size(512 << 10)
            .build();

        Ok(Self {
            num_bytes_written: 0,
            executor,
            footer: SegmentFooterWriter::default(),
            active_block: BlockWriter::default(),
            writer: file,
        })
    }

    /// Adds a document to the writer.
    ///
    /// This only asynchronously blocks once the given block writer is full.
    ///
    /// Returns `true` if the segment is at capacity.
    pub async fn add_document(&mut self, id: Id, doc: &Document) -> Result<bool> {
        let is_full = self.active_block
            .write_document(id, doc)
            .map_err(|e| SegmentError::SerializationError(e.to_string()))?;

        if !is_full {
            return Ok(false)
        }

        let n_bytes = self.write_block().await?;
        self.num_bytes_written += n_bytes;

        Ok(self.num_bytes_written >= MAX_SEGMENT_SIZE)
    }

    /// Writes the remaining data in the block writer to disk.
    pub async fn write_block(&mut self) -> Result<usize> {
        // No point trying to drain if we don't have any data to write.
        if self.active_block.num_docs() == 0 {
            return Ok(0)
        }

        let compressed = self
            .active_block
            .drain_and_compress(&self.executor)
            .await
            .map_err(|e| SegmentError::SerializationError(e.to_string()))?;

        self.writer
            .write_all(&compressed)
            .await?;

        Ok(compressed.len())
    }

    /// Flushes all pending data to disk and writes the segment footer, sealing the file.
    pub async fn seal_segment(mut self) -> Result<()> {
        self.write_block().await?;

        let footer = self
            .footer
            .to_bytes()
            .map_err(|e| SegmentError::SerializationError(e.to_string()))?;

        self.writer
            .write_all(&footer)
            .await?;

        self.writer
            .flush()
            .await?;

        Ok(())
    }
}


/// A immutable segment reader.
///
/// Once a segment is able to be read from, no other changes can be made
/// to that segment without causing corruption of data.
pub struct SegmentReader {
    /// A threadpool executor for running CPU intensive operations.
    executor: BlockingExecutor,
    file: ImmutableFile,
}

impl SegmentReader {
    /// Opens an existing segment as an immutable reader.
    ///
    /// This is undefined behaviour if the file is still being modified while
    /// the reader has the file open.
    pub async fn open(executor: BlockingExecutor, path: &Path) -> Result<Self> {
        let file = ImmutableFileBuilder::new(path)
            .with_sequential_concurrency(10)
            .with_buffer_size(512 << 10)
            .build_existing()
            .await
            .map_err(|v| SegmentError::SegmentOpenError(v.to_string()))?;

        Ok(Self {
            executor,
            file,
        })
    }

    pub async fn get_block_reader(&self) -> Result<BlockReader> {

        todo!()
    }
}