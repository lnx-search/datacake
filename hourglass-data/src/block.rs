use std::collections::BTreeMap;

use anyhow::Result;
use humansize::{FileSize, file_size_opts::CONVENTIONAL};
use rkyv::{AlignedVec, Deserialize};

use crate::blocking::BlockingExecutor;
use crate::value::Document;
use crate::Id;

pub const OFFSET_HEADER_SIZE: usize = std::mem::size_of::<(u32, u32)>();
pub const BLOCK_SIZE: usize = 512 << 10;

type Offsets = BTreeMap<Id, (u32, u32)>;

#[derive(Default)]
pub struct BlockWriter {
    doc_offsets: Offsets,
    inner_buffer: Vec<u8>,
}

impl BlockWriter {
    /// Serializes and appends the document to the buffer.
    pub fn write_document(&mut self, id: Id, doc: &Document) -> Result<()> {
        let data = rkyv::to_bytes::<_, 2048>(doc)?;

        let len = data.len() as u32;
        let start = self.inner_buffer.len() as u32;

        self.inner_buffer.extend_from_slice(data.as_slice());
        self.doc_offsets.insert(id, (start, len));

        Ok(())
    }

    /// The approximate total memory usage of the block writer.
    pub fn used_memory(&self) -> usize {
        self.inner_buffer.len()
            + std::mem::size_of::<Vec<u8>>()
            + self.doc_offsets.len()
                * (std::mem::size_of::<Id>() + std::mem::size_of::<(u32, u32)>())
            + std::mem::size_of::<Offsets>()
    }

    /// Drains all data in the writer and compresses it into a single block.
    ///
    /// Blocks are produced in the following format:
    ///
    /// | <-------- compressed data --------> | offset_length | uncompressed_length |
    /// | <--- documents ---> | <- offsets -> | offset_length | uncompressed_length |
    pub async fn drain_and_compress(
        &mut self,
        executor: &BlockingExecutor,
    ) -> Result<Vec<u8>> {
        let start = std::time::Instant::now();
        let num_documents = self.doc_offsets.len();

        let offsets = rkyv::to_bytes::<_, 2048>(&self.doc_offsets)?;

        let offsets_len = offsets.len() as u32;
        self.inner_buffer.extend_from_slice(&offsets);

        let uncompressed_length = self.inner_buffer.len() as u32;

        // SAFETY:
        //  This is safe as we guarantee that the buffer will outlive the executor function.
        let to_compress: &'static [u8] =
            unsafe { std::mem::transmute(self.inner_buffer.as_slice()) };
        let mut compressed = executor
            .execute(move || lz4_flex::compress(to_compress))
            .await?;

        // We do a hard reset here as we probably dont want to be
        // holding onto the left over memory all the time.
        self.inner_buffer = vec![];
        self.doc_offsets.clear();

        let offset_markers =
            rkyv::to_bytes::<_, 8>(&(offsets_len, uncompressed_length))?;

        // Add the uncompressed length to the end of compressed buffer.
        compressed.extend_from_slice(&offset_markers);

        debug!(
            "Block compression with {} documents totalling {} ({} compressed) completed in {:?}.",
            num_documents,
            uncompressed_length.file_size(CONVENTIONAL).unwrap(),
            compressed.len().file_size(CONVENTIONAL).unwrap(),
            start.elapsed(),
        );

        Ok(compressed)
    }
}

#[derive(Clone)]
/// A zero-copy block reader.
///
/// Only the original de-compressed vector is allocated, everything else
/// is a reference to the data within the vector.
pub struct BlockReader {
    /// The raw decompressed block.
    raw_block: AlignedVec,

    /// A reference into the raw block.
    ///
    /// WARNING:
    ///  This is not technically `'static`, it lives for as long as
    ///  the `raw_block`, anything beyond that is UB.
    ///  This should not be publicly exposed in anyway.
    doc_offsets: &'static rkyv::Archived<Offsets>,

    /// A reference to the document buffer in the raw block.
    ///
    /// WARNING:
    ///  This is not technically `'static`, it lives for as long as
    ///  the `raw_block`, anything beyond that is UB.
    ///  This should not be publicly exposed in anyway.
    inner_buffer: &'static [u8],
}

impl BlockReader {
    /// Deserializes a compressed block of bytes into a BlockReader.
    ///
    /// Blocks are produced in the following format:
    ///
    /// | <-------- compressed data --------> | offset_length | uncompressed_length |
    /// | <--- documents ---> | <- offsets -> | offset_length | uncompressed_length |
    pub async fn from_compressed(
        buffer: &[u8],
        executor: &BlockingExecutor,
    ) -> Result<Self> {
        // SAFETY:
        //  This is safe as we guarantee that the slice will live for as long as the executed closure.
        let data_buffer: &'static [u8] = unsafe { std::mem::transmute(buffer) };

        let block = executor
            .execute(move || {
                let start = std::time::Instant::now();
                let meta_start = data_buffer.len() - OFFSET_HEADER_SIZE;

                let (offsets_len, uncompressed_length): (u32, u32) = {
                    let mut aligned = AlignedVec::new();
                    aligned.extend_from_slice(&data_buffer[meta_start..]);
                    rkyv::check_archived_root::<(u32, u32)>(&aligned)?
                        .deserialize(&mut rkyv::Infallible)?
                };

                let to_decompress = &data_buffer[..meta_start];

                let data =
                    lz4_flex::decompress(to_decompress, uncompressed_length as usize)?;
                let mut raw_block =
                    AlignedVec::with_capacity(uncompressed_length as usize);
                raw_block.extend_from_slice(&data);

                let offsets_start = raw_block.len() - offsets_len as usize;

                let doc_offsets =
                    rkyv::check_archived_root::<Offsets>(&raw_block[offsets_start..])?;
                let doc_offsets: &'static rkyv::Archived<Offsets> =
                    unsafe { std::mem::transmute(doc_offsets) };

                let inner_buffer = &raw_block[..offsets_start];
                let inner_buffer: &'static [u8] =
                    unsafe { std::mem::transmute(inner_buffer) };

                debug!(
                    "Block decompression with {} documents totalling {} took {:?}.",
                    doc_offsets.len(),
                    raw_block.len().file_size(CONVENTIONAL).unwrap(),
                    start.elapsed(),
                );

                let slf = BlockReader {
                    raw_block,
                    doc_offsets,
                    inner_buffer,
                };

                Ok::<_, anyhow::Error>(slf)
            })
            .await??;

        Ok(block)
    }

    /// The approximate total memory usage of the block reader.
    pub fn used_memory(&self) -> usize {
        self.raw_block.len() + std::mem::size_of::<Vec<u8>>()
    }

    pub fn get_document(&self, id: Id) -> Option<&rkyv::Archived<Document>> {
        let (start, len) = self.doc_offsets.get(&id)?;
        let slice = &self.inner_buffer[*start as usize..*start as usize + *len as usize];

        // TODO:
        //  Make this safe via check bytes.
        let doc = unsafe { rkyv::archived_root::<Document>(slice) };
        Some(doc)
    }

    pub fn document_ids(&self) -> impl Iterator<Item = &rkyv::Archived<Id>> {
        self.doc_offsets.keys()
    }
}

#[cfg(test)]
pub(crate) mod test_utils {

    use rand::random;

    use super::*;
    use crate::value::{FixedStructureValue, JsonNumber, JsonValue};

    pub(crate) fn get_random_document() -> Document {
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

    pub(crate) async fn create_temporary_block_reader(doc: &Document, id: Id) -> BlockReader {
        let mut writer = BlockWriter::default();

        writer
            .write_document(id, &doc)
            .unwrap_or_else(|_| panic!("Serialize OK, failed on doc: {:?}", doc));

        let executor = BlockingExecutor::with_n_threads(1).expect("Build executor");

        let data = writer
            .drain_and_compress(&executor)
            .await
            .expect("Drain and compress");

        let reader = BlockReader::from_compressed(&data, &executor)
            .await
            .expect("Decompress");

        println!("Got reader with offsets: {:?}", reader.doc_offsets);

        reader
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_write() {
        let mut writer = BlockWriter::default();

        for _ in 0..3 {
            let doc = test_utils::get_random_document();
            writer
                .write_document(1, &doc)
                .unwrap_or_else(|_| panic!("Serialize OK, failed on doc: {:?}", doc));
        }
    }

    #[tokio::test]
    async fn test_block_compress() {
        let mut writer = BlockWriter::default();

        for _ in 0..3 {
            let doc = test_utils::get_random_document();
            writer
                .write_document(1, &doc)
                .unwrap_or_else(|_| panic!("Serialize OK, failed on doc: {:?}", doc));
        }

        let executor = BlockingExecutor::with_n_threads(1).expect("Build executor");

        writer
            .drain_and_compress(&executor)
            .await
            .expect("Drain and compress");
    }

    #[tokio::test]
    async fn test_block_decompress() {
        let mut writer = BlockWriter::default();

        let doc = test_utils::get_random_document();
        writer
            .write_document(1, &doc)
            .unwrap_or_else(|_| panic!("Serialize OK, failed on doc: {:?}", doc));

        let executor = BlockingExecutor::with_n_threads(1).expect("Build executor");

        let data = writer
            .drain_and_compress(&executor)
            .await
            .expect("Drain and compress");

        let reader = BlockReader::from_compressed(&data, &executor)
            .await
            .expect("Decompress");

        let _doc = reader.get_document(1).expect("Get document");
    }
}
