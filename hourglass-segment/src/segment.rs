use std::cmp;
use std::path::Path;

use futures_lite::AsyncWriteExt;
use glommio::io::{
    DmaFile,
    DmaStreamReader,
    DmaStreamWriter,
    DmaStreamWriterBuilder,
    ImmutableFile,
    ImmutableFileBuilder,
};
use hourglass_data::block::{BlockReader, BlockWriter};
use hourglass_data::blocking::BlockingExecutor;
use hourglass_data::segment_footer::{
    SegmentFooterReader,
    SegmentFooterWriter,
    FOOTER_OFFSET_LEN,
    MAX_SEGMENT_SIZE,
};
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

    /// Writes the remaining data in the block writer to disk.
    pub async fn write_block(&mut self) -> Result<usize> {
        // No point trying to drain if we don't have any data to write.
        if self.active_block.num_docs() == 0 {
            return Ok(0);
        }

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

        Ok(compressed.len())
    }

    /// Flushes all pending data to disk and writes the segment footer, sealing the file.
    pub async fn seal_segment(mut self) -> Result<()> {
        self.write_block().await?;

        let footer = self
            .footer
            .to_bytes()
            .map_err(|e| SegmentError::SerializationError(e.to_string()))?;

        self.writer.write_all(&footer).await?;

        self.writer.flush().await?;
        self.writer.close().await?;

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
    footer: SegmentFooterReader,
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
            .map_err(|e| SegmentError::SegmentOpenError(e.to_string()))?;

        let footer = get_footer_info(&file).await?;

        Ok(Self {
            executor,
            file,
            footer,
        })
    }

    /// Gets a given doc's block reader.
    ///
    /// This may seem a little weird from a high level view however, this is a necessary
    /// behaviour to return the entire block reader vs just the doc, as we cache blocks
    /// rather than individual documents, and each cache is shard-local rather than
    /// segment local. This means our cache could potentially be handling blocks from
    /// multiple segments, and we want to avoid maintaining and managing a cache per-segment.
    pub async fn get_doc_block(&self, doc_id: Id) -> Result<Option<BlockReader>> {
        let (start, len) = match self.footer.get_block_offsets_for_doc(doc_id) {
            None => return Ok(None),
            Some(offsets) => offsets,
        };

        let data = self
            .file
            .read_at(start as u64, len as usize)
            .await
            .map_err(|e| SegmentError::BlockReadError(e.to_string()))?;

        let block = BlockReader::from_compressed(&data, &self.executor)
            .await
            .map_err(|e| SegmentError::DeserializationError(e.to_string()))?;

        Ok(Some(block))
    }

    /// Creates an iterator that walks through the segment file sequentially.
    ///
    /// The order of blocks are guarantee to be in the order they appear in the file.
    pub fn iter_blocks(&self) -> SegmentBlocksIterator {
        let block_ids = self.footer.iter_blocks();

        let stream = self
            .file
            .stream_reader()
            .with_buffer_size(512 << 10)
            .with_read_ahead(10)
            .build();

        // We want to sort our blocks so we're not jumping back and forth through
        // our file.
        let mut owned_ids = block_ids
            .map(|(_, offsets)| offsets)
            .copied()
            .collect::<Vec<_>>();

        // We want smallest at the end.
        owned_ids.sort_by_key(|(start, _)| cmp::Reverse(*start));

        SegmentBlocksIterator {
            ids: owned_ids,
            executor: self.executor.clone(),
            stream,
        }
    }
}

pub struct SegmentBlocksIterator {
    ids: Vec<(u32, u32)>,
    stream: DmaStreamReader,
    executor: BlockingExecutor,
}

impl SegmentBlocksIterator {
    pub async fn next(&mut self) -> Option<Result<BlockReader>> {
        let len = self.seek_to_next_block()?;

        let mut data = vec![];
        let mut to_get = len;
        while to_get > 0 {
            let maybe_buff = self
                .stream
                .get_buffer_aligned(to_get)
                .await
                .map_err(|e| SegmentError::BlockReadError(e.to_string()));

            let buff = match maybe_buff {
                Ok(b) => b,
                Err(other) => return Some(Err(other)),
            };

            // Small optimisation to avoid the additional allocation of
            // the vec if we get all the data at once.
            if buff.len() as u64 == len {
                return BlockReader::from_compressed(&buff, &self.executor)
                    .await
                    .map_err(|e| SegmentError::DeserializationError(e.to_string()))
                    .map(Some)
                    .transpose();
            }

            data.extend_from_slice(&buff);
            to_get -= buff.len() as u64;
        }

        BlockReader::from_compressed(&data, &self.executor)
            .await
            .map_err(|e| SegmentError::DeserializationError(e.to_string()))
            .map(Some)
            .transpose()
    }

    fn seek_to_next_block(&mut self) -> Option<u64> {
        // We pop off the end of the vec as our vec is reversed.
        let (start, len) = match self.ids.pop() {
            None => return None,
            Some(v) => v,
        };

        let current = self.stream.current_pos();
        let skip_n = start as u64 - current;

        if skip_n != 0 {
            self.stream.skip(skip_n);
        }

        Some(len as u64)
    }
}

async fn get_footer_info(file: &ImmutableFile) -> Result<SegmentFooterReader> {
    let length = file.file_size();
    let footer_suffix = file
        .read_at(length - FOOTER_OFFSET_LEN as u64, FOOTER_OFFSET_LEN)
        .await
        .map_err(|e| {
            SegmentError::SegmentOpenError(format!(
                "Failed to read segment footer suffix. {}",
                e
            ))
        })?;

    let data_len = SegmentFooterReader::get_data_len(&footer_suffix)
        .map_err(|e| SegmentError::DeserializationError(e.to_string()))?;

    let footer_data = file
        .read_at(
            length - FOOTER_OFFSET_LEN as u64 - data_len as u64,
            data_len,
        )
        .await
        .map_err(|e| {
            SegmentError::SegmentOpenError(format!(
                "Failed to read segment footer. {}",
                e
            ))
        })?;

    SegmentFooterReader::from_buffer(&footer_data, 0, data_len)
        .map_err(|e| SegmentError::DeserializationError(e.to_string()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use hourglass_data::value::{FixedStructureValue, JsonNumber, JsonValue};

    use super::*;

    macro_rules! run {
        ($fut:expr) => {{
            glommio::LocalExecutorBuilder::default()
                .spawn($fut)
                .unwrap()
                .join()
                .expect("Successful run")
        }};
    }

    // TODO: Move to dedicated test utils crate.
    fn get_random_doc() -> Document {
        use rand::random;

        let mut big_object = BTreeMap::new();
        big_object.insert("name".to_string(), JsonValue::String("Jeremy".to_string()));
        big_object.insert(
            "age".to_string(),
            JsonValue::Number(JsonNumber::Float(random())),
        );
        big_object.insert(
            "entries".to_string(),
            JsonValue::Array(vec![
                JsonValue::String("Hello, world!".to_string()),
                JsonValue::Number(JsonNumber::Float(random())),
                JsonValue::Bool(random()),
                JsonValue::Number(JsonNumber::PosInt(random())),
                JsonValue::Null,
                JsonValue::Number(JsonNumber::NegInt(random())),
            ]),
        );

        let mut inner = BTreeMap::new();

        if random() {
            inner.insert(
                "data".to_string(),
                FixedStructureValue::MultiU64(vec![12, 1234, 23778235723, 823572875]),
            );
        }

        if random() {
            inner.insert(
                "names".to_string(),
                FixedStructureValue::MultiString(vec![
                    "bob".to_string(),
                    "jerry".to_string(),
                    "julian".to_string(),
                ]),
            );
        }

        if random() {
            inner.insert(
                "json-data".to_string(),
                FixedStructureValue::Dynamic(big_object),
            );
        }

        Document::from(inner)
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

    async fn get_populated_segment_writer(
        executor: BlockingExecutor,
        file: &Path,
    ) -> SegmentWriter {
        let doc = get_random_doc();

        let mut writer = SegmentWriter::create(executor, file)
            .await
            .expect("Successful segment creation");

        writer
            .add_document(1, &doc)
            .await
            .expect("Successful doc addition");

        writer
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

    #[test]
    fn test_segment_reader_open() {
        let file = std::env::temp_dir().join("segment-reader-test");
        let executor = BlockingExecutor::with_n_threads(1).expect("Create executor");

        let fut = || async move {
            {
                let writer = get_populated_segment_writer(executor.clone(), &file).await;

                writer
                    .seal_segment()
                    .await
                    .expect("Successful sealing of segment");
            }

            SegmentReader::open(executor, &file)
                .await
                .expect("Successful segment read");
        };

        run!(fut);
    }

    #[test]
    fn test_segment_reader_get_block() {
        let file = std::env::temp_dir().join("segment-reader-get-block-test");
        let executor = BlockingExecutor::with_n_threads(1).expect("Create executor");

        let fut = || async move {
            {
                let writer = get_populated_segment_writer(executor.clone(), &file).await;

                writer
                    .seal_segment()
                    .await
                    .expect("Successful sealing of segment");
            }

            let reader = SegmentReader::open(executor, &file)
                .await
                .expect("Successful segment read");

            let block = reader
                .get_doc_block(1)
                .await
                .expect("Successfully check doc block");

            assert!(
                block.is_some(),
                "Expected doc block to exist when doc id = 1"
            );

            let block = reader
                .get_doc_block(1241241241)
                .await
                .expect("Successfully check doc block");

            assert!(block.is_none(), "Expected no block to be found");
        };

        run!(fut);
    }

    #[test]
    fn test_segment_reader_block_iter() {
        let file = std::env::temp_dir().join("segment-reader-block-iter-test");
        let executor = BlockingExecutor::with_n_threads(1).expect("Create executor");

        let fut = || async move {
            {
                let mut writer =
                    get_populated_segment_writer(executor.clone(), &file).await;

                // Add a bunch of docs to test iterating through.
                for i in 0..4096 {
                    let doc = get_random_doc();
                    writer
                        .add_document(i, &doc)
                        .await
                        .expect("Successful doc addition");
                }

                writer
                    .seal_segment()
                    .await
                    .expect("Successful sealing of segment");
            }

            let reader = SegmentReader::open(executor, &file)
                .await
                .expect("Successful segment read");

            let mut blocks = reader.iter_blocks();

            let mut num_blocks = 0;
            let mut total = 0;
            while let Some(block) = blocks.next().await {
                let block = block.expect("read block correctly");
                total += block.document_ids().count();
                num_blocks += 1;
            }

            // No +1 for the original doc as we override it.
            assert_eq!(total, 4096, "Expected 4096 documents to be stored and retrieved correctly, got {} blocks: {}", total, num_blocks);
        };

        run!(fut);
    }
}
