# Datacake Rocks

Datacake Rocks is a pre-made implementation of the Datacake Cluster `Docstore` and `Metastore` traits, built upon
a sharded storage design and RocksDB allowing you get setup and start using Datacake without worrying about implementing
the required traits.

### Basic Example

```rust
use anyhow::Result;
use datacake_rocks::open_store_with_options;

#[tokio::main]
async fn main() -> Result<()> {
    let mut options = rocksdb::Options::default();
    
    // We can now use this store to be our backend in a DatacakeCluster.
    let my_store = open_store_with_options(12, options, "/my-data/path-here")
        .await
        .expect("Open our database.");    
}
```

