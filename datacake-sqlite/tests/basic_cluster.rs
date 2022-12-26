use std::net::SocketAddr;

use anyhow::Result;
use datacake_cluster::EventuallyConsistentStoreExtension;
use datacake_node::{ConnectionConfig, Consistency, DatacakeNodeBuilder, DCAwareSelector};
use datacake_sqlite::SqliteStorage;

static KEYSPACE: &str = "sqlite-store";

#[tokio::test]
async fn test_basic_sqlite_cluster() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let store = SqliteStorage::open_in_memory().await?;

    let addr = "127.0.0.1:9000".parse::<SocketAddr>().unwrap();
    let connection_cfg = ConnectionConfig::new(addr, addr, Vec::<String>::new());

    let node = DatacakeNodeBuilder::<DCAwareSelector>::new("node-1", connection_cfg).connect().await?;
    let store = node.add_extension(EventuallyConsistentStoreExtension::new(store)).await?;

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
