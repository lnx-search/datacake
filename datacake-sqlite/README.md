# Datacake SQLite

A pre-built implementation of the datacake-eventual-consistency `Storage` trait, this allows you to set up
a persistent cluster immediately without any hassle of implementing a correct store.

For more info see https://github.com/lnx-search/datacake

## Setup
It's important to note that this crate does bundle SQLite with it but it can be disabled by passing
`default-features = false`.

## Example

```rust
use anyhow::Result;
use datacake_eventual_consistency::EventuallyConsistentStoreExtension;
use datacake_node::{
    ConnectionConfig,
    Consistency,
    DCAwareSelector,
    DatacakeNodeBuilder,
};
use datacake_sqlite::SqliteStorage;

static KEYSPACE: &str = "sqlite-store";

#[tokio::test]
async fn test_basic_sqlite_cluster() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let store = SqliteStorage::open_in_memory().await?;

    let addr = test_helper::get_unused_addr();
    let connection_cfg = ConnectionConfig::new(addr, addr, Vec::<String>::new());

    let node = DatacakeNodeBuilder::<DCAwareSelector>::new(1, connection_cfg)
        .connect()
        .await?;
    let store = node
        .add_extension(EventuallyConsistentStoreExtension::new(store))
        .await?;

    let handle = store.handle();

    handle
        .put(KEYSPACE, 1, b"Hello, world".to_vec(), Consistency::All)
        .await
        .expect("Put value.");

    let doc = handle
        .get(KEYSPACE, 1)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id(), 1);
    assert_eq!(doc.data(), b"Hello, world");

    handle
        .del(KEYSPACE, 1, Consistency::All)
        .await
        .expect("Del value.");
    let doc = handle.get(KEYSPACE, 1).await.expect("Get value.");
    assert!(doc.is_none(), "No document should not exist!");

    handle
        .del(KEYSPACE, 2, Consistency::All)
        .await
        .expect("Del value which doesnt exist locally.");
    let doc = handle.get(KEYSPACE, 2).await.expect("Get value.");
    assert!(doc.is_none(), "No document should not exist!");

    node.shutdown().await;

    Ok(())
}
```