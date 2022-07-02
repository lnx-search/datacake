use std::cmp;
use std::path::Path;

use glommio::io::{DmaStreamReader, ImmutableFile, ImmutableFileBuilder};
use hourglass_data::block::BlockReader;
use hourglass_data::blocking::BlockingExecutor;
use hourglass_data::segment_footer::{SegmentFooterReader, FOOTER_OFFSET_LEN};
use hourglass_data::Id;
use humansize::file_size_opts::CONVENTIONAL;
use humansize::FileSize;

use crate::error::{Result, SegmentError};

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
        debug!("Opening immutable segment @ {:?}", path);

        let file = ImmutableFileBuilder::new(path)
            .with_sequential_concurrency(10)
            .with_buffer_size(512 << 10)
            .build_existing()
            .await
            .map_err(|e| SegmentError::SegmentOpenError(e.to_string()))?;

        let footer = get_footer_info(&file).await?;

        debug!(
            "Opened immutable segment with {} documents, totalling {} @ {:?}",
            footer.num_docs(),
            file.file_size()
                .file_size(CONVENTIONAL)
                .unwrap_or_else(|_| "an unknown number of bytes.".to_string()),
            path,
        );

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
