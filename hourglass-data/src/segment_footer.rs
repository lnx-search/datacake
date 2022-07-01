use std::collections::BTreeMap;

use anyhow::{anyhow, Result};
use bytecheck::CheckBytes;
use rkyv::{AlignedVec, Archive, Deserialize, Serialize};

use crate::blocking::BlockingExecutor;
use crate::Id;

/// Maximum segment size of 10GB
pub const MAX_SEGMENT_SIZE: usize = 10 << 30;

type SegmentLocalBlockId = u16;

#[derive(Default)]
/// A writer for a segment metadata.
///
/// This is data stored at the start of the file which contains data about the
/// blocks contained within the segment.
pub struct SegmentFooterWriter {
    meta: SegmentMetadata,
}

impl SegmentFooterWriter {
    /// Adds a block to the segment footer data in memory.
    pub fn add_block(
        &mut self,
        block: (u32, u32),
        contained_docs: impl Iterator<Item = Id>,
    ) {
        self.meta.block_id_counter += 1;
        let block_id = self.meta.block_id_counter;

        self.meta.blocks.insert(block_id, block);
        self.meta
            .docset
            .extend(contained_docs.map(|id| (id, block_id)));
    }

    /// Serializes and compresses the segment footer.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        rkyv::to_bytes::<_, 1024>(&self.meta)
            .map_err(anyhow::Error::from)
            .map(|v| v.into_vec())
    }
}

/// A writer for a segment metadata.
///
/// This is data stored at the start of the file which contains data about the
/// blocks contained within the segment.
pub struct SegmentFooterReader {
    meta: SegmentMetadata,
}

impl SegmentFooterReader {
    /// Gets the footer metadata from a given buffer.
    ///
    /// This assumes that the buffer starts with the correct footer value.
    pub async fn from_buffer(buff: &[u8]) -> Result<Self> {
        let inner = SegmentMetadata::from_buffer(buff)?;

        Ok(Self { meta: inner })
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
    /// Gets the footer metadata from a given buffer.
    ///
    /// This assumes that the buffer starts with the correct footer value.
    pub fn from_buffer(buff: &[u8]) -> Result<Self> {
        let offset_start = buff.len() - std::mem::size_of::<u32>();
        let data_len = rkyv::check_archived_root::<u32>(&buff[offset_start..])?;
        let data_start = offset_start - *data_len as usize;

        // Linter doesn't like this, but this is completely fine.
        let slf: Self = rkyv::check_archived_root::<Self>(&buff[data_start..offset_start])
            .map_err(|_| anyhow!("Failed to get archived segment metadata footer. Is the segment corrupted?"))?
            .deserialize(&mut rkyv::Infallible)
            .map_err(|_| anyhow!("Failed to deserialize segment metadata footer. Is the segment corrupted?"))?;

        Ok(slf)
    }
}
