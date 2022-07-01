use std::collections::BTreeMap;
use rkyv::{Archive, Deserialize, Serialize};
use bytecheck::CheckBytes;
use anyhow::{anyhow, Result};

use crate::Id;

/// Maximum segment size of 10GB
pub const MAX_SEGMENT_SIZE: u64 = 10 << 30;

type SegmentLocalBlockId = u16;


#[derive(Default)]
/// A writer for a segment metadata.
///
/// This is data stored at the start of the file which contains data about the
/// blocks contained within the segment.
pub struct SegmentHeaderWriter {
    meta: SegmentMetadata,
}

impl SegmentHeaderWriter {
    /// Adds a block to the segment header data in memory.
    pub fn add_block(
        &mut self,
        block: (u32, u32),
        contained_docs: impl Iterator<Item = Id>,
    ) {
        self.meta.block_id_counter += 1;
        let block_id = self.meta.block_id_counter;

        self.meta.blocks.insert(block_id, block);
        self.meta.docset.extend(contained_docs.map(|id| (id, block_id)));
    }
}



/// A writer for a segment metadata.
///
/// This is data stored at the start of the file which contains data about the
/// blocks contained within the segment.
pub struct SegmentHeaderReader {
    meta: SegmentMetadata,
}

impl SegmentHeaderReader {
    /// Gets the header metadata from a given buffer.
    ///
    /// This assumes that the buffer starts with the correct header value.
    pub fn from_buffer(buff: &[u8]) -> Result<Self> {
        let inner = SegmentMetadata::from_buffer(buff)?;

        Ok(Self {
            meta: inner,
        })
    }

    /// Gets the block offsets associated with the given doc id.
    pub fn get_block_offsets_for_doc(&self, id: Id) -> Option<(u32, u32)> {
        let block = self.meta.docset.get(&id)?;
        self.meta.blocks.get(block).copied()
    }
}


#[derive(Default, Archive, Debug, Deserialize, Serialize, Clone)]
#[archive_attr(derive(CheckBytes, Debug))]
pub(crate) struct SegmentMetadata {
    block_id_counter: SegmentLocalBlockId,
    pub docset: BTreeMap<Id, SegmentLocalBlockId>,
    pub blocks: BTreeMap<SegmentLocalBlockId, (u32, u32)>,
}

impl SegmentMetadata {
    /// Gets the header metadata from a given buffer.
    ///
    /// This assumes that the buffer starts with the correct header value.
    pub fn from_buffer(buff: &[u8]) -> Result<Self> {
        let data_len = rkyv::check_archived_root::<u32>(&buff[..4])?;

        // Linter doesn't like this, but this is completely fine.
        let archived: Self = rkyv::check_archived_root::<Self>(&buff[4..*data_len as usize])
            .map_err(|_| anyhow!("Failed to get archived segment metadata header. Is the segment corrupted?"))?
            .deserialize(&mut rkyv::Infallible)
            .map_err(|_| anyhow!("Failed to deserialize segment metadata header. Is the segment corrupted?"))?;

        Ok(archived)
    }
}