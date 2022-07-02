use std::path::Path;
use std::time::Instant;

use humansize::{FileSize, file_size_opts::CONVENTIONAL};
use futures_lite::AsyncWriteExt;
use glommio::io::{DmaFile, DmaStreamWriter, DmaStreamWriterBuilder};
use hourglass_data::block::BlockWriter;
use hourglass_data::blocking::BlockingExecutor;
use hourglass_data::segment_footer::{SegmentFooterWriter, MAX_SEGMENT_SIZE};
use hourglass_data::value::Document;
use hourglass_data::Id;

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
        debug!("Creating mutable segment @ {:?}", path);

        let file = DmaFile::create(path)
            .await
            .map_err(|v| SegmentError::SegmentCreationError(v.to_string()))?;

        let file = DmaStreamWriterBuilder::new(file)
            .with_write_behind(10)
            .with_buffer_size(512 << 10)
            .build();

        debug!("Created mutable segment @ {:?}", path);

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
        let is_full = self
            .active_block
            .write_document(id, doc)
            .map_err(|e| SegmentError::SerializationError(e.to_string()))?;

        if !is_full {
            return Ok(false);
        }

        self.write_block().await?;

        Ok(self.num_bytes_written >= MAX_SEGMENT_SIZE)
    }

    /// Flushes all pending data to disk and writes the segment footer, sealing the file.
    pub async fn seal_segment(mut self) -> Result<()> {
        self.write_block().await?;

        let start = Instant::now();
        let footer = self
            .footer
            .to_bytes()
            .map_err(|e| SegmentError::SerializationError(e.to_string()))?;

        self.writer.write_all(&footer).await?;

        self.writer.flush().await?;
        self.writer.close().await?;

        info!(
            "Segment containing {} docs, totalling {} on disk has been sealed in {:?}.",
            self.footer.num_docs(),
            self.num_bytes_written
                .file_size(CONVENTIONAL)
                .unwrap_or_else(|_| "an unknown number of bytes.".to_string()),
            start.elapsed(),
        );

        Ok(())
    }

    /// Writes the remaining data in the block writer to disk.
    async fn write_block(&mut self) -> Result<usize> {
        // No point trying to drain if we don't have any data to write.
        if self.active_block.num_docs() == 0 {
            return Ok(0);
        }

        let start = Instant::now();

        let (compressed, docset) = self
            .active_block
            .drain_and_compress(&self.executor)
            .await
            .map_err(|e| SegmentError::SerializationError(e.to_string()))?;

        let block_start = self.num_bytes_written as u32;
        let len = compressed.len() as u32;

        self.footer
            .add_block((block_start, len), docset.keys().copied());
        self.writer.write_all(&compressed).await?;
        self.num_bytes_written += compressed.len();

        debug!(
            "Segment wrote out {} to disk. Segment size is now {}, totalling {} docs in {:?}.",
            compressed.len()
                .file_size(CONVENTIONAL)
                .unwrap_or_else(|_| "an unknown number of bytes.".to_string()),
            self.num_bytes_written
                .file_size(CONVENTIONAL)
                .unwrap_or_else(|_| "an unknown number of bytes.".to_string()),
            self.footer.num_docs(),
            start.elapsed(),
        );

        Ok(compressed.len())
    }

    #[inline]
    pub fn num_docs(&self) -> usize {
        self.footer.num_docs()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::*;

    macro_rules! run {
        ($fut:expr) => {{
            glommio::LocalExecutorBuilder::default()
                .spawn($fut)
                .unwrap()
                .join()
                .expect("Successful run")
        }};
    }

    #[test]
    fn test_segment_writer_create() {
        let file = std::env::temp_dir().join("segment-create-test");
        let executor = BlockingExecutor::with_n_threads(1).expect("Create executor");

        let fut = || async move {
            SegmentWriter::create(executor, &file)
                .await
                .expect("Successful segment creation");
        };

        run!(fut);
    }

    #[test]
    fn test_segment_writer_write_doc() {
        let file = std::env::temp_dir().join("segment-write-doc-test");
        let executor = BlockingExecutor::with_n_threads(1).expect("Create executor");

        let doc = get_random_doc();

        let fut = || async move {
            let mut writer = SegmentWriter::create(executor, &file)
                .await
                .expect("Successful segment creation");

            writer
                .add_document(1, &doc)
                .await
                .expect("Successful doc addition");
        };

        run!(fut);
    }

    #[test]
    fn test_segment_writer_write_doc_until_flush() {
        let file = std::env::temp_dir().join("segment-write-doc-test");
        let executor = BlockingExecutor::with_n_threads(1).expect("Create executor");

        let doc = get_random_doc();

        let fut = || async move {
            let mut writer = SegmentWriter::create(executor, &file)
                .await
                .expect("Successful segment creation");

            let mut last_num_bytes = 0;
            // Just a completely amount of documents, just ensuring we add enough to cause
            // a flush and block drain.
            for i in 0..4096 {
                writer
                    .add_document(i, &doc)
                    .await
                    .expect("Successful doc addition");

                // We want to make sure we're actually writing to disk.
                if (i != 0) && (writer.active_block.num_docs() == 0) {
                    assert!(writer.num_bytes_written > last_num_bytes);
                    last_num_bytes = writer.num_bytes_written;
                }
            }
        };

        run!(fut);
    }

    #[test]
    fn test_segment_writer_seal() {
        let file = std::env::temp_dir().join("segment-seal-test");
        let executor = BlockingExecutor::with_n_threads(1).expect("Create executor");

        let fut = || async move {
            let writer = get_populated_segment_writer(executor, &file).await;

            writer
                .seal_segment()
                .await
                .expect("Successful sealing of segment");
        };

        run!(fut);
    }
}
