use std::collections::{HashMap, HashSet};
use std::mem;
use std::path::{Path, PathBuf};
use std::time::Instant;

use datacake_data::block::ReadGuard;
use datacake_data::blocking::BlockingExecutor;
use datacake_data::cache::ShardCache;
use datacake_data::value::Document;
use datacake_data::DocId;
use datacake_segment::{SegmentReader, SegmentWriter};
use humansize::file_size_opts::CONVENTIONAL;
use humansize::FileSize;
use uuid::Uuid;
use datacake_segment::error::SegmentError;

use crate::error::{Result, ShardError};

/// An abstraction around multiple segments that the shard manages.
///
/// This only handles document addition, document fetching and marking docs as dead.
/// It does not handle actual deletions or merging of similar sized segments.
pub struct StoreShard {
    block_cache: ShardCache,
    index: Index<usize>,
    uncommitted_index: Index<Uuid>,

    active_writer: SegmentWriter,
    segment_readers: Vec<SegmentReader>,

    executor: BlockingExecutor,
    base_path: PathBuf,

    prepared_commit: PreparedCommit,
    bad_state: bool,
}

impl StoreShard {
    /// Retries a document from the shard with a given id.
    ///
    /// If the document is not already present in cache, the document may be cached afterwards
    /// (although not parented.)
    pub async fn get_document(&mut self, id: DocId) -> Result<Option<ReadGuard>> {
        // SAFETY:
        //  There is potentially really dangerous, and any changes made to this block should be
        //  through checked (preferably by trying to remove the unsafe!) The only reason why unsafe
        //  is being used here is due to the the current borrow checker limitations which do not
        //  allow two mutable borrows even if the first borrow is able to be safely dropped due to
        //  the return.
        //
        //  See: https://github.com/rust-lang/rust/issues/54663
        //  TODO: Remove if polonius ever gets into stable -_-
        let segment_reader = {
            let fake_mutable_self = self as *mut Self;

            // This first deref is safe because it's the first time that anything actually
            // uses the pointer or self.
            if let Some(doc) = unsafe { (*fake_mutable_self).block_cache.get_doc(id) } {
                return Ok(Some(doc));
            }

            // This second deref is safe because any borrowing of the first deref is dropped due
            // to the early return. As such, we can guarantee that this is the only borrow currently
            // occurring.
            match unsafe { (*fake_mutable_self).get_segment_reader(id) } {
                None => return Ok(None),
                Some(reader) => reader,
            }
        };

        let block = match segment_reader.get_doc_block(id).await? {
            None => return Ok(None),
            Some(block) => block,
        };

        self.block_cache.store_block(block.clone());

        Ok(block.get_document(id))
    }

    /// Adds a document to the shard.
    ///
    /// This is generally buffered in memory so is not guaranteed to be written
    /// out to disk or offer any sort of persistence guarantee.
    pub async fn add_document(&mut self, id: DocId, doc: &Document) -> Result<()> {
        let is_full = self.active_writer.add_document(id, doc).await?;
        self.register_doc_insert_locally(id);

        if !is_full {
            return Ok(());
        }

        let start = Instant::now();
        let old_writer_id = self.active_writer.id();

        let new_segment = self.create_new_segment().await?;
        let old_segment = mem::replace(&mut self.active_writer, new_segment);
        let num_bytes = old_segment.seal_segment().await?;

        info!(
            "Replaced and sealed segment {} containing {} of data in {:?}.",
            old_writer_id,
            num_bytes.file_size(CONVENTIONAL).unwrap_or_else(|e| e),
            start.elapsed(),
        );

        Ok(())
    }

    /// Marks a document for removal from the shard.
    ///
    /// Due to the immutable nature of segments, this is only a in-memory operation
    /// that then waits for a GC cycle to purge the documents from the segments themselves.
    pub async fn remove_document(&mut self, id: DocId) {
        self.uncommitted_index.dead_documents.insert(id);
    }

    /// Cleans up and resets the segment state back to the last commit.
    ///
    /// This purges and intermediate buffers and written segments.
    ///
    /// Returns a list of segments that failed to be removed as part of the
    /// immediate cleanup operation.
    pub async fn rollback(&mut self) -> Result<Vec<(Uuid, SegmentError)>> {
        // Assume something has gone wrong to start with.
        self.bad_state = true;

        let mut removed_segments = HashSet::new();

        let mut failed_segments = vec![];
        for segment_id in self.uncommitted_index.alive_documents.values() {
            if removed_segments.contains(segment_id) {
                continue
            }

            if let Err(e) = datacake_segment::remove_segment(*segment_id, &self.base_path).await {
                error!("Shard encountered an error during rollback: Failed to remove segment {} due to error: {:?}", segment_id, e);
                failed_segments.push((*segment_id, e));
            }

            removed_segments.insert(*segment_id);
        }

        let segment_id = self.active_writer.id();

        let new_segment = self.create_new_segment().await?;
        let mut old_segment = mem::replace(&mut self.active_writer, new_segment);
        let _ = old_segment.close().await;

        if let Err(e) = datacake_segment::remove_segment(segment_id, &self.base_path).await {
            error!("Shard encountered an error during rollback: Failed to remove segment {} due to error: {:?}", segment_id, e);
            failed_segments.push((segment_id, e));
        }

        // Our rollback was successful in terms of resetting our shard to a safe state.
        self.bad_state = false;
        Ok(failed_segments)
    }

    /// Begins the commit process.
    ///
    /// This is potentially fallible as it requires opening
    /// several segment readers and performing some IO operations.
    ///
    /// This does not finalise the commit / reflect any pending changes made.
    pub async fn prepare_commit(&mut self) -> Result<()> {


        Ok(())
    }

    pub fn finalise_commit(&mut self) {
        todo!(

        )
    }

    #[inline]
    fn get_segment_reader(&self, id: DocId) -> Option<&SegmentReader> {
        self.index
            .alive_documents
            .get(&id)
            .and_then(|id| self.segment_readers.get(*id))
    }

    #[inline]
    fn register_doc_insert_locally(&mut self, id: DocId) {
        self.uncommitted_index
            .dead_documents
            .remove(&id);

        self.uncommitted_index
            .alive_documents
            .insert(id, self.active_writer.id());
    }

    #[inline]
    fn reset_uncommitted_index(&mut self) {
        self.uncommitted_index = Index::default();
    }

    #[inline]
    async fn create_new_segment(&self) -> Result<SegmentWriter> {
        let new_writer = SegmentWriter::create(
            self.executor.clone(),
            &self.base_path,
        ).await?;

        Ok(new_writer)
    }
}

#[derive(Clone, Default)]
pub struct Index<SegmentId> {
    /// A index mapping documents to their given segment location/
    alive_documents: HashMap<DocId, SegmentId>,

    /// A set of dead documents which no technically still exist on disk,
    /// but are marked as removed.
    dead_documents: HashSet<DocId>,
}


#[derive(Default)]
pub struct PreparedCommit {
    prepared_index: Index<usize>,
    opened_readers: Vec<SegmentReader>,
}

impl PreparedCommit {
    /// Closes all prepared segments.
    pub async fn close_segments(self) -> Result<()> {
        for segment in self.opened_readers {
            if let Err(e) = segment.close().await {
                warn!("Failed to close prepared segment reader with error: {:?}", e);
            };
        }

        Ok(())
    }
}