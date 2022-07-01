use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::Deref;
use std::rc::Rc;

use moka::unsync::Cache;
use uuid::Uuid;

use crate::block::BlockReader;
use crate::value::Document;
use crate::Id;

/// A shard-local cache for retrieving documents backed by the individual buffers.
pub struct ShardCache {
    /// A lookup table for mapping document ids, to their given block id.
    lookup: Rc<RefCell<HashMap<Id, Uuid>>>,
    block_cache: Cache<Uuid, BlockReaderWrapper>,
}

impl ShardCache {
    /// Creates a new LRU cache with a maximum capacity of n bytes.
    pub fn with_capacity(n_bytes: u64) -> Self {
        let block_cache = Cache::builder()
            .weigher(|_k, v: &BlockReaderWrapper| v.used_memory() as u32)
            .max_capacity(n_bytes)
            .build();

        Self {
            lookup: Default::default(),
            block_cache,
        }
    }

    /// Gets some general metrics around the cache state.
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            num_docs: self.lookup.borrow().len(),
            num_bytes_used: self.block_cache.weighted_size() as usize,
            num_blocks_cached: self.block_cache.entry_count() as usize,
            avg_block_size_bytes: (self.block_cache.entry_count()
                / self.block_cache.weighted_size())
                as usize,
        }
    }

    /// Attempts to get the document from the cache if it exists.
    pub fn get_doc(&mut self, id: Id) -> Option<&rkyv::Archived<Document>> {
        let lookup = self.lookup.borrow();
        let block = lookup.get(&id)?;

        let reader = self.block_cache.get(block)?;
        reader.get_document(id)
    }

    /// Stores a block reader into the cache.
    ///
    /// Note on eviction:
    /// If a old block must be evicted, the system does not purge the doc id's
    /// immediately. Instead this can be manually cleanup via the `run_cache_gc()` method
    /// or when the document is next accessed.
    pub fn store_block(&mut self, block: BlockReader) {
        let block_id = Uuid::new_v4();
        let doc_ids = block
            .document_ids()
            .map(|v| (*v as Id, block_id))
            .collect::<Vec<_>>();

        let wrapped = BlockReaderWrapper {
            inner: block,
            lookup: self.lookup.clone(),
        };

        // Required to avoid a double mutable borrow attempt from the RefCell.
        {
            self.lookup.borrow_mut().extend(doc_ids);
        }

        self.block_cache.insert(block_id, wrapped);
    }
}

pub struct CacheStats {
    /// The total number of docs contained within the cache.
    pub num_docs: usize,

    /// The total number of bytes used by the cache.
    pub num_bytes_used: usize,

    /// The total number of blocks cached.
    pub num_blocks_cached: usize,

    /// The average amount of bytes contained within each block.
    pub avg_block_size_bytes: usize,
}

pub struct BlockReaderWrapper {
    inner: BlockReader,
    lookup: Rc<RefCell<HashMap<Id, Uuid>>>,
}

impl Deref for BlockReaderWrapper {
    type Target = BlockReader;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Drop for BlockReaderWrapper {
    fn drop(&mut self) {
        let mut lookup = self.lookup.borrow_mut();

        for id in self.inner.document_ids() {
            lookup.remove(id);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use crate::value::{FixedStructureValue, JsonNumber, JsonValue};

    use super::*;

    #[tokio::test]
    async fn test_cache_eviction() {
        let mut cache = ShardCache::with_capacity(1500);

        let mut big_object = BTreeMap::new();
        big_object.insert("name".to_string(), JsonValue::String("Jeremy".to_string()));
        big_object.insert(
            "age".to_string(),
            JsonValue::Number(JsonNumber::Float(12.322)),
        );
        big_object.insert(
            "entries".to_string(),
            JsonValue::Array(vec![
                JsonValue::String("Hello, world!".to_string()),
                JsonValue::Number(JsonNumber::Float(123.23)),
                JsonValue::Bool(false),
                JsonValue::Number(JsonNumber::PosInt(12234)),
                JsonValue::Null,
                JsonValue::Number(JsonNumber::NegInt(-123)),
            ]),
        );

        let mut inner = BTreeMap::new();
         inner.insert(
            "json-data".to_string(),
            FixedStructureValue::Dynamic(big_object),
        );
        let doc = Document::from(inner);

        // We expect 5 to be cached as 6 will push us over the limit.
        for i in 1..2 {
            let block_reader =
                crate::block::test_utils::create_temporary_block_reader(&doc,i).await;
            cache.store_block(block_reader);
        }

        cache.get_doc(1);

        for i in 4..6 {
            let block_reader =
                crate::block::test_utils::create_temporary_block_reader(&doc,i).await;
            cache.store_block(block_reader);
        }
        cache.get_doc(4);

        assert_eq!(
            cache.lookup.borrow().len(),
            3,
            "Expected 3 documents in lookup table."
        );
        assert_eq!(
            cache.block_cache.entry_count(),
            3,
            "Expected 3 entries in inner cache."
        );
        assert!(
            cache.get_doc(1).is_some(),
            "Expected cached document id: 1."
        );
        assert!(
            cache.get_doc(4).is_some(),
            "Expected cached document id: 1."
        );
    }
}
