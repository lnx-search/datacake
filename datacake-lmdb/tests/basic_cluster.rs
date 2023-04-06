use std::env::temp_dir;

use anyhow::Result;
use datacake_eventual_consistency::EventuallyConsistentStoreExtension;
use datacake_lmdb::LmdbStorage;
use datacake_node::{
    ConnectionConfig,
    Consistency,
    DCAwareSelector,
    DatacakeNodeBuilder,
};
use uuid::Uuid;

static KEYSPACE: &str = "lmdb-store";

#[tokio::test]
async fn test_basic_lmdb_cluster() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let temp_dir = temp_dir().join(Uuid::new_v4().to_string());
    std::fs::create_dir_all(&temp_dir)?;

    let store = LmdbStorage::open(temp_dir).await?;

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
