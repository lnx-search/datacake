use std::collections::{HashMap, HashSet};
use std::time::Duration;

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

    let mut processed_docs = HashMap::<Id, Duration>::new();
    while let Some(block) = blocks.next().await {
        let block = block?;

        // TODO: This is probably quite expensive, do we really need to deserialize every time?
        //  - We could make this just copy over the specific bytes, but maybe that's a little
        //    too specialised. Need to test.
        for (id, doc) in block.iter_documents_owned() {
            if tombstones.contains(&id) {
                continue;
            }

            if let Some(newest_ts) = processed_docs.get(&id) {
                // If a document in the block has the same id, and is newer we should use that.
                if newest_ts >= &doc.created {
                    continue;
                }
            }

            writer.add_document(id, &doc).await?;

            processed_docs.insert(id, doc.created);
        }
    }

    Ok(())
}
