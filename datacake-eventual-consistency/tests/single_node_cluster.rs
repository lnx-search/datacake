use std::net::SocketAddr;

use datacake_eventual_consistency::test_utils::MemStore;
use datacake_eventual_consistency::{
    EventuallyConsistentStore,
    EventuallyConsistentStoreExtension,
};
use datacake_node::{
    ConnectionConfig,
    Consistency,
    DCAwareSelector,
    DatacakeNodeBuilder,
};

static KEYSPACE: &str = "my-keyspace";

#[tokio::test]
async fn test_single_node_cluster() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let store = create_store("127.0.0.1:8001").await;
    let handle = store.handle();

    // Test reading
    let doc = handle.get(KEYSPACE, 1).await.expect("Get value.");
    assert!(doc.is_none(), "No document should not exist!");

    // Test writing
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

    Ok(())
}

#[tokio::test]
async fn test_single_node_cluster_with_keyspace_handle() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let store = create_store("127.0.0.1:8002").await;
    let handle = store.handle_with_keyspace(KEYSPACE);

    // Test reading
    let doc = handle.get(1).await.expect("Get value.");
    assert!(doc.is_none(), "No document should not exist!");

    // Test writing
    handle
        .put(1, b"Hello, world".to_vec(), Consistency::All)
        .await
        .expect("Put value.");

    let doc = handle
        .get(1)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id(), 1);
    assert_eq!(doc.data(), b"Hello, world");

    handle.del(1, Consistency::All).await.expect("Del value.");
    let doc = handle.get(1).await.expect("Get value.");
    assert!(doc.is_none(), "No document should not exist!");

    handle
        .del(2, Consistency::All)
        .await
        .expect("Del value which doesnt exist locally.");
    let doc = handle.get(2).await.expect("Get value.");
    assert!(doc.is_none(), "No document should not exist!");

    Ok(())
}

#[tokio::test]
async fn test_single_node_cluster_bulk_op() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let store = create_store("127.0.0.1:8003").await;
    let handle = store.handle();

    // Test reading
    let num_docs = handle
        .get_many(KEYSPACE, [1])
        .await
        .expect("Get value.")
        .count();
    assert_eq!(num_docs, 0, "No document should not exist!");

    // Test writing
    handle
        .put_many(KEYSPACE, [(1, b"Hello, world".to_vec())], Consistency::All)
        .await
        .expect("Put value.");

    let docs = handle
        .get_many(KEYSPACE, [1])
        .await
        .expect("Get value.")
        .collect::<Vec<_>>();
    assert_eq!(docs[0].id(), 1);
    assert_eq!(docs[0].data(), b"Hello, world");

    handle
        .del_many(KEYSPACE, [1], Consistency::All)
        .await
        .expect("Del value.");
    let num_docs = handle
        .get_many(KEYSPACE, [1])
        .await
        .expect("Get value.")
        .count();
    assert_eq!(num_docs, 0, "No document should not exist!");

    handle
        .del_many(KEYSPACE, [2, 3, 1, 5], Consistency::All)
        .await
        .expect("Del value which doesnt exist locally.");
    let num_docs = handle
        .get_many(KEYSPACE, [2, 3, 5, 1])
        .await
        .expect("Get value.")
        .count();
    assert_eq!(num_docs, 0, "No document should not exist!");

    Ok(())
}

#[tokio::test]
async fn test_single_node_cluster_bulk_op_with_keyspace_handle() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let store = create_store("127.0.0.1:8004").await;
    let handle = store.handle_with_keyspace(KEYSPACE);

    // Test reading
    let num_docs = handle.get_many([1]).await.expect("Get value.").count();
    assert_eq!(num_docs, 0, "No document should not exist!");

    // Test writing
    handle
        .put_many([(1, b"Hello, world".to_vec())], Consistency::All)
        .await
        .expect("Put value.");

    let docs = handle
        .get_many([1])
        .await
        .expect("Get value.")
        .collect::<Vec<_>>();
    assert_eq!(docs[0].id(), 1);
    assert_eq!(docs[0].data(), b"Hello, world");

    handle
        .del_many([1], Consistency::All)
        .await
        .expect("Del value.");
    let num_docs = handle.get_many([1]).await.expect("Get value.").count();
    assert_eq!(num_docs, 0, "No document should not exist!");

    handle
        .del_many([2, 3, 1, 5], Consistency::All)
        .await
        .expect("Del value which doesnt exist locally.");
    let num_docs = handle
        .get_many([2, 3, 5, 1])
        .await
        .expect("Get value.")
        .count();
    assert_eq!(num_docs, 0, "No document should not exist!");

    Ok(())
}

async fn create_store(addr: &str) -> EventuallyConsistentStore<MemStore> {
    let addr = addr.parse::<SocketAddr>().unwrap();
    let connection_cfg = ConnectionConfig::new(addr, addr, Vec::<String>::new());
    let node = DatacakeNodeBuilder::<DCAwareSelector>::new(1, connection_cfg)
        .connect()
        .await
        .expect("Connect node.");

    let store = node
        .add_extension(EventuallyConsistentStoreExtension::new(MemStore::default()))
        .await
        .expect("Create store.");

    store
}
