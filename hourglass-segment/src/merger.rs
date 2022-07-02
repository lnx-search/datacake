use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use hourglass_data::Id;

use crate::error::Result;
use crate::{SegmentReader, SegmentWriter};

/// Merges a segment into another writer segment.
///
/// This can be repeated for multiple segments however, the merger does
/// not check if the segment is full or not, it assumes that merging the
/// given reader will not cause the writer to go beyond an allowed limit.
pub async fn merge_segment_into_writer(
    writer: &mut SegmentWriter,
    to_merge: &SegmentReader,
    tombstones: &HashSet<Id>,
) -> Result<()> {
    let mut blocks = to_merge.iter_blocks();

    let start = Instant::now();
    let mut num_docs_processed = 0;
    let mut num_docs_removed = 0;
    let mut processed_docs = HashMap::<Id, Duration>::new();
    while let Some(block) = blocks.next().await {
        let block = block?;

        // TODO: This is probably quite expensive, do we really need to deserialize every time?
        //  - We could make this just copy over the specific bytes, but maybe that's a little
        //    too specialised. Need to test.
        for (id, doc) in block.iter_documents_owned() {
            if tombstones.contains(&id) {
                num_docs_removed += 1;
                continue;
            }

            if let Some(newest_ts) = processed_docs.get(&id) {
                // If a document in the block has the same id, and is newer we should use that.
                if newest_ts >= &doc.created {
                    num_docs_removed += 1;
                    continue;
                }
            }

            writer.add_document(id, &doc).await?;
            num_docs_processed += 1;

            processed_docs.insert(id, doc.created);
        }
    }

    info!(
        "Merged {} docs into new segment totalling {} docs, {} dead docs removed in {:?}.",
        num_docs_processed,
        writer.num_docs(),
        num_docs_removed,
        start.elapsed(),
    );

    Ok(())
}
