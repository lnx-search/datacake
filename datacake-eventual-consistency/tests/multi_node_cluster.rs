use std::time::Duration;

use datacake_eventual_consistency::test_utils::MemStore;
use datacake_eventual_consistency::EventuallyConsistentStoreExtension;
use datacake_node::{
    ConnectionConfig,
    Consistency,
    DCAwareSelector,
    DatacakeNode,
    DatacakeNodeBuilder,
};

#[tokio::test]
async fn test_consistency_all() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let [node_1, node_2, node_3] = connect_cluster().await;

    let store_1 = node_1
        .add_extension(EventuallyConsistentStoreExtension::new(MemStore::default()))
        .await?;
    let store_2 = node_2
        .add_extension(EventuallyConsistentStoreExtension::new(MemStore::default()))
        .await?;
    let store_3 = node_3
        .add_extension(EventuallyConsistentStoreExtension::new(MemStore::default()))
        .await?;

    let node_1_handle = store_1.handle_with_keyspace("my-keyspace");
    let node_2_handle = store_2.handle_with_keyspace("my-keyspace");
    let node_3_handle = store_3.handle_with_keyspace("my-keyspace");

    // Test reading
    let doc = node_1_handle.get(1).await.expect("Get value.");
    assert!(doc.is_none(), "No document should not exist!");

    // Test writing
    node_1_handle
        .put(1, b"Hello, world".to_vec(), Consistency::All)
        .await
        .expect("Put value.");

    // Node 1 should have the value as it's just written locally.
    let doc = node_1_handle
        .get(1)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id(), 1);
    assert_eq!(doc.data(), b"Hello, world");

    // Nodes 2 and 3 should also have the value immediately due to the consistency level.
    let doc = node_2_handle
        .get(1)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id(), 1);
    assert_eq!(doc.data(), b"Hello, world");
    let doc = node_3_handle
        .get(1)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id(), 1);
    assert_eq!(doc.data(), b"Hello, world");

    // Delete a key from the cluster
    node_3_handle
        .del(1, Consistency::All)
        .await
        .expect("Del value.");

    // Node 3 should have the value as it's just written locally.
    let doc = node_3_handle.get(1).await.expect("Get value.");
    assert!(doc.is_none(), "No document should not exist!");

    // Nodes 2 and 1 should also have the value immediately due to the consistency level.
    let doc = node_2_handle.get(1).await.expect("Get value.");
    assert!(doc.is_none());
    let doc = node_1_handle.get(1).await.expect("Get value.");
    assert!(doc.is_none());

    // Delete a non-existent key from the cluster
    node_3_handle
        .del(1, Consistency::All)
        .await
        .expect("Del value.");

    // All of the nodes should register the delete.
    let doc = node_3_handle.get(1).await.expect("Get value.");
    assert!(doc.is_none(), "No document should not exist!");
    let doc = node_2_handle.get(1).await.expect("Get value.");
    assert!(doc.is_none());
    let doc = node_1_handle.get(1).await.expect("Get value.");
    assert!(doc.is_none());

    node_1.shutdown().await;
    node_2.shutdown().await;
    node_3.shutdown().await;

    Ok(())
}

#[tokio::test]
async fn test_consistency_none() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let [node_1, node_2, node_3] = connect_cluster().await;

    let store_1 = node_1
        .add_extension(EventuallyConsistentStoreExtension::new(MemStore::default()))
        .await?;
    let store_2 = node_2
        .add_extension(EventuallyConsistentStoreExtension::new(MemStore::default()))
        .await?;
    let store_3 = node_3
        .add_extension(EventuallyConsistentStoreExtension::new(MemStore::default()))
        .await?;

    let node_1_handle = store_1.handle_with_keyspace("my-keyspace");
    let node_2_handle = store_2.handle_with_keyspace("my-keyspace");
    let node_3_handle = store_3.handle_with_keyspace("my-keyspace");

    // Test reading
    let doc = node_1_handle.get(1).await.expect("Get value.");
    assert!(doc.is_none(), "No document should not exist!");

    // Test writing
    node_1_handle
        .put(1, b"Hello, world".to_vec(), Consistency::None)
        .await
        .expect("Put value.");

    // Node 1 should have the value as it's just written locally.
    let doc = node_1_handle
        .get(1)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id(), 1);
    assert_eq!(doc.data(), b"Hello, world");

    // Nodes 2 and 3 will not have the value yet as syncing has not taken place.
    let doc = node_2_handle.get(1).await.expect("Get value.");
    assert!(doc.is_none(), "No document should not exist!");
    let doc = node_3_handle.get(1).await.expect("Get value.");
    assert!(doc.is_none(), "No document should not exist!");

    // 10 seconds should be enough for this test to propagate state without becoming flaky.
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Nodes 2 and 3 should now see the updated value.
    let doc = node_2_handle
        .get(1)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id(), 1);
    assert_eq!(doc.data(), b"Hello, world");
    let doc = node_3_handle
        .get(1)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id(), 1);
    assert_eq!(doc.data(), b"Hello, world");

    // Delete a key from the cluster
    node_3_handle
        .del(1, Consistency::None)
        .await
        .expect("Del value.");

    // Node 3 should have the value as it's just written locally.
    let doc = node_3_handle.get(1).await.expect("Get value.");
    assert!(doc.is_none(), "No document should not exist!");

    tokio::time::sleep(Duration::from_secs(10)).await;

    // Nodes should be caught up now.
    let doc = node_2_handle.get(1).await.expect("Get value.");
    assert!(doc.is_none());
    let doc = node_1_handle.get(1).await.expect("Get value.");
    assert!(doc.is_none());

    // Delete a non-existent key from the cluster
    node_3_handle
        .del(1, Consistency::None)
        .await
        .expect("Del value.");

    // All of the nodes should register the delete.
    let doc = node_3_handle.get(1).await.expect("Get value.");
    assert!(doc.is_none(), "No document should not exist!");
    let doc = node_2_handle.get(1).await.expect("Get value.");
    assert!(doc.is_none());
    let doc = node_1_handle.get(1).await.expect("Get value.");
    assert!(doc.is_none());

    node_1.shutdown().await;
    node_2.shutdown().await;
    node_3.shutdown().await;

    Ok(())
}

#[tokio::test]
async fn test_async_operations() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let [node_1, node_2, node_3] = connect_cluster().await;

    let store_1 = node_1
        .add_extension(EventuallyConsistentStoreExtension::new(MemStore::default()))
        .await?;
    let store_2 = node_2
        .add_extension(EventuallyConsistentStoreExtension::new(MemStore::default()))
        .await?;
    let store_3 = node_3
        .add_extension(EventuallyConsistentStoreExtension::new(MemStore::default()))
        .await?;

    let node_1_handle = store_1.handle_with_keyspace("my-keyspace");
    let node_2_handle = store_2.handle_with_keyspace("my-keyspace");
    let node_3_handle = store_3.handle_with_keyspace("my-keyspace");

    // These operations all happen at the exact same time. But they will always be applied in the
    // same deterministic order. So we know node-3 will win.
    node_1_handle
        .put(1, b"Hello, world from node-1".to_vec(), Consistency::None)
        .await
        .expect("Put value.");
    tokio::time::sleep(Duration::from_millis(2)).await;
    node_2_handle
        .put(1, b"Hello, world from node-2".to_vec(), Consistency::None)
        .await
        .expect("Put value.");
    tokio::time::sleep(Duration::from_millis(2)).await;
    node_3_handle
        .put(1, b"Hello, world from node-3".to_vec(), Consistency::None)
        .await
        .expect("Put value.");

    // *sigh* no consistency in sight! - This is because we haven't given any time to sync yet.
    let doc = node_1_handle
        .get(1)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id(), 1);
    assert_eq!(doc.data(), b"Hello, world from node-1");
    let doc = node_2_handle
        .get(1)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id(), 1);
    assert_eq!(doc.data(), b"Hello, world from node-2");
    let doc = node_3_handle
        .get(1)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id(), 1);
    assert_eq!(doc.data(), b"Hello, world from node-3");

    tokio::time::sleep(Duration::from_secs(10)).await;

    // Man I love CRDTs, look at how easy this was! They're all the same now.
    let doc = node_1_handle
        .get(1)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id(), 1);
    assert_eq!(doc.data(), b"Hello, world from node-3"); // TODO: This fails if the logical clock isn't correct??
    let doc = node_2_handle
        .get(1)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id(), 1);
    assert_eq!(doc.data(), b"Hello, world from node-3");
    let doc = node_3_handle
        .get(1)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id(), 1);
    assert_eq!(doc.data(), b"Hello, world from node-3");

    // This goes for all operations.
    // Node 2 will win, even though they're technically happening at the exact same time.
    node_1_handle
        .put(
            1,
            b"Hello, world from node-1 but updated".to_vec(),
            Consistency::None,
        )
        .await
        .expect("Put value.");
    tokio::time::sleep(Duration::from_millis(2)).await;
    node_2_handle
        .del(1, Consistency::None)
        .await
        .expect("Delete value.");

    // Node 1 has only seen it's put so far, so it assumes it's correct.
    let doc = node_1_handle
        .get(1)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id(), 1);
    assert_eq!(doc.data(), b"Hello, world from node-1 but updated");

    // Node 2 has only seen it's delete so far, so it assumes it's correct.
    let doc = node_2_handle.get(1).await.expect("Get value.");
    assert!(doc.is_none(), "Document should be deleted.");

    tokio::time::sleep(Duration::from_secs(10)).await;

    // And now everything is consistent.
    let doc = node_1_handle.get(1).await.expect("Get value.");
    assert!(doc.is_none(), "Document should be deleted.");
    let doc = node_2_handle.get(1).await.expect("Get value.");
    assert!(doc.is_none(), "Document should be deleted.");
    let doc = node_3_handle.get(1).await.expect("Get value.");
    assert!(doc.is_none(), "Document should be deleted.");

    node_1.shutdown().await;
    node_2.shutdown().await;
    node_3.shutdown().await;

    Ok(())
}

async fn connect_cluster() -> [DatacakeNode; 3] {
    let node_1_addr = test_helper::get_unused_addr();
    let node_2_addr = test_helper::get_unused_addr();
    let node_3_addr = test_helper::get_unused_addr();

    let node_1_connection_cfg = ConnectionConfig::new(
        node_1_addr,
        node_1_addr,
        [node_2_addr.to_string(), node_3_addr.to_string()],
    );
    let node_2_connection_cfg = ConnectionConfig::new(
        node_2_addr,
        node_2_addr,
        [node_1_addr.to_string(), node_3_addr.to_string()],
    );
    let node_3_connection_cfg = ConnectionConfig::new(
        node_3_addr,
        node_3_addr,
        [node_1_addr.to_string(), node_2_addr.to_string()],
    );

    let node_1 = DatacakeNodeBuilder::<DCAwareSelector>::new(1, node_1_connection_cfg)
        .connect()
        .await
        .unwrap();
    let node_2 = DatacakeNodeBuilder::<DCAwareSelector>::new(2, node_2_connection_cfg)
        .connect()
        .await
        .unwrap();
    let node_3 = DatacakeNodeBuilder::<DCAwareSelector>::new(3, node_3_connection_cfg)
        .connect()
        .await
        .unwrap();

    node_1
        .wait_for_nodes(&[2, 3], Duration::from_secs(60))
        .await
        .expect("Nodes should connect within timeout.");
    node_2
        .wait_for_nodes(&[3, 1], Duration::from_secs(60))
        .await
        .expect("Nodes should connect within timeout.");
    node_3
        .wait_for_nodes(&[2, 1], Duration::from_secs(60))
        .await
        .expect("Nodes should connect within timeout.");

    let stats = node_1.statistics();
    assert_eq!(stats.num_data_centers(), 1);
    assert_eq!(stats.num_live_members(), 3);
    assert_eq!(stats.num_dead_members(), 0);

    let stats = node_2.statistics();
    assert_eq!(stats.num_data_centers(), 1);
    assert_eq!(stats.num_live_members(), 3);
    assert_eq!(stats.num_dead_members(), 0);

    let stats = node_3.statistics();
    assert_eq!(stats.num_data_centers(), 1);
    assert_eq!(stats.num_live_members(), 3);
    assert_eq!(stats.num_dead_members(), 0);

    [node_1, node_2, node_3]
}
