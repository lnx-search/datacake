use std::collections::BTreeMap;

use anyhow::{anyhow, Result};
use bytecheck::CheckBytes;
use rkyv::{AlignedVec, Archive, Deserialize, Serialize};

use crate::Id;

/// Maximum segment size of 10GB
pub const MAX_SEGMENT_SIZE: usize = 10 << 30;
pub const FOOTER_OFFSET_LEN: usize = std::mem::size_of::<u32>();

pub type SegmentLocalBlockId = u16;

#[derive(Default, Debug)]
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
        let mut buffer =
            rkyv::to_bytes::<_, 1024>(&self.meta).map_err(anyhow::Error::from)?;

        let offset = buffer.len() as u32;
        let offset = rkyv::to_bytes::<_, 8>(&offset).map_err(anyhow::Error::from)?;

        buffer.extend_from_slice(&offset);

        Ok(buffer.into_vec())
    }
}

#[derive(Debug)]
/// A writer for a segment metadata.
///
/// This is data stored at the start of the file which contains data about the
/// blocks contained within the segment.
pub struct SegmentFooterReader {
    meta: SegmentMetadata,
}

impl SegmentFooterReader {
    /// Gets the length of the footer via the file suffix.
    pub fn get_data_len(buff: &[u8]) -> Result<usize> {
        SegmentMetadata::get_data_len(buff)
    }

    /// Gets the footer metadata from a given buffer.
    ///
    /// This assumes that the buffer starts with the correct footer value.
    pub fn from_buffer(buff: &[u8], data_start: usize, len: usize) -> Result<Self> {
        let inner = SegmentMetadata::from_buffer(buff, data_start, len)?;

        Ok(Self { meta: inner })
    }

    #[inline]
    /// Produces an interator of all the block ids and their offsets
    /// contained within the segment.
    pub fn iter_blocks(
        &self,
    ) -> std::collections::btree_map::Iter<SegmentLocalBlockId, (u32, u32)> {
        self.meta.iter_blocks()
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
    pub fn get_data_len(buff: &[u8]) -> Result<usize> {
        // The alignment coming from the file can change quite a bit. So we want a solid alignment.
        let mut aligned = AlignedVec::with_capacity(buff.len());
        aligned.extend_from_slice(buff);

        let offset_start = buff.len() - std::mem::size_of::<u32>();
        let data_len = rkyv::check_archived_root::<u32>(&aligned[offset_start..])?;
        Ok(*data_len as usize)
    }

    /// Produces an interator of all the block ids and their offsets
    /// contained within the segment.
    pub fn iter_blocks(
        &self,
    ) -> std::collections::btree_map::Iter<SegmentLocalBlockId, (u32, u32)> {
        self.blocks.iter()
    }

    /// Gets the footer metadata from a given buffer.
    ///
    /// This assumes that the buffer starts with the correct footer value.
    pub fn from_buffer(buff: &[u8], data_start: usize, len: usize) -> Result<Self> {
        let mut aligned = AlignedVec::with_capacity(buff.len());
        aligned.extend_from_slice(buff);

        // Linter doesn't like this, but this is completely fine.
        let slf: Self = rkyv::check_archived_root::<Self>(&aligned[data_start..data_start + len])
            .map_err(|e| anyhow!("Failed to get archived segment metadata footer. Is the segment corrupted? {}", e))?
            .deserialize(&mut rkyv::Infallible)
            .map_err(|_| anyhow!("Failed to deserialize segment metadata footer. Is the segment corrupted?"))?;

        Ok(slf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_footer_writer() {
        let mut footer = SegmentFooterWriter::default();
        footer.add_block((0, 324), 0..3);
        footer.to_bytes().expect("Successful serialize");
    }

    #[test]
    fn test_footer_reader() {
        let mut footer = SegmentFooterWriter::default();
        footer.add_block((0, 324), 0..3);
        let buffer = footer.to_bytes().expect("Successful serialize");

        // Try load it from bytes again.
        SegmentFooterReader::from_buffer(&buffer, 0, buffer.len() - FOOTER_OFFSET_LEN)
            .expect("Successful deserialize");
    }
}
