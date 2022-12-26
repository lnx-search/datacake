use std::net::SocketAddr;
use std::time::Duration;

use datacake_cluster::test_utils::MemStore;
use datacake_cluster::EventuallyConsistentStoreExtension;
use datacake_node::{Consistency, ConnectionConfig, DatacakeNodeBuilder, DCAwareSelector};

#[tokio::test]
pub async fn test_member_join() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let node_1_addr = "127.0.0.1:8018".parse::<SocketAddr>().unwrap();
    let node_2_addr = "127.0.0.1:8019".parse::<SocketAddr>().unwrap();
    let node_3_addr = "127.0.0.1:8020".parse::<SocketAddr>().unwrap();
    let node_1_connection_cfg = ConnectionConfig::new(
        node_1_addr,
        node_1_addr,
        &[node_2_addr.to_string(), node_3_addr.to_string()],
    );
    let node_2_connection_cfg = ConnectionConfig::new(
        node_2_addr,
        node_2_addr,
        &[node_1_addr.to_string(), node_3_addr.to_string()],
    );
    let node_3_connection_cfg = ConnectionConfig::new(
        node_3_addr,
        node_3_addr,
        &[node_1_addr.to_string(), node_2_addr.to_string()],
    );

    let node_1 = DatacakeNodeBuilder::<DCAwareSelector>::new("node-1", node_1_connection_cfg).connect().await?;
    let node_2 = DatacakeNodeBuilder::<DCAwareSelector>::new("node-2", node_2_connection_cfg).connect().await?;

    node_1
        .wait_for_nodes(&["node-2", "node-3"], Duration::from_secs(30))
        .await
        .expect("Nodes should connect within timeout.");
    node_2
        .wait_for_nodes(&["node-1", "node-3"], Duration::from_secs(30))
        .await
        .expect("Nodes should connect within timeout.");
    let store_1 = node_1
        .add_extension(EventuallyConsistentStoreExtension::new(MemStore::default()))
        .await?;
    let store_2 = node_2
        .add_extension(EventuallyConsistentStoreExtension::new(MemStore::default()))
        .await?;

    let node_1_handle = store_1.handle_with_keyspace("my-keyspace");
    let node_2_handle = store_2.handle_with_keyspace("my-keyspace");

    node_1_handle
        .put(1, b"Hello, world from node-1".to_vec(), Consistency::All)
        .await
        .expect("Put value.");
    node_2_handle
        .put(2, b"Hello, world from node-2".to_vec(), Consistency::All)
        .await
        .expect("Put value.");

    let doc = node_1_handle
        .get(1)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id(), 1);
    assert_eq!(doc.data(), b"Hello, world from node-1");
    let doc = node_1_handle
        .get(2)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id(), 2);
    assert_eq!(doc.data(), b"Hello, world from node-2");

    let doc = node_2_handle
        .get(1)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id(), 1);
    assert_eq!(doc.data(), b"Hello, world from node-1");
    let doc = node_2_handle
        .get(2)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id(), 2);
    assert_eq!(doc.data(), b"Hello, world from node-2");

    // Node-3 joins the cluster.
    let node_3 = DatacakeNodeBuilder::<DCAwareSelector>::new("node-3", node_3_connection_cfg).connect().await?;
    node_3
        .wait_for_nodes(&["node-2", "node-1"], Duration::from_secs(30))
        .await
        .expect("Nodes should connect within timeout.");
    let store_3 = node_3
        .add_extension(EventuallyConsistentStoreExtension::new(MemStore::default()))
        .await?;
    let node_3_handle = store_3.handle_with_keyspace("my-keyspace");

    node_3_handle
        .put(3, b"Hello, world from node-3".to_vec(), Consistency::All)
        .await
        .expect("Put value.");

    let doc = node_3_handle.get(1).await.expect("Get value.");
    assert!(doc.is_none());
    let doc = node_3_handle.get(2).await.expect("Get value.");
    assert!(doc.is_none());

    node_3
        .wait_for_nodes(&["node-1", "node-2"], Duration::from_secs(30))
        .await
        .expect("Nodes should connect within timeout.");

    // Let state propagate
    tokio::time::sleep(Duration::from_secs(10)).await;

    let doc = node_3_handle
        .get(1)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id(), 1);
    assert_eq!(doc.data(), b"Hello, world from node-1");
    let doc = node_3_handle
        .get(2)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id(), 2);
    assert_eq!(doc.data(), b"Hello, world from node-2");

    let doc = node_1_handle
        .get(3)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id(), 3);
    assert_eq!(doc.data(), b"Hello, world from node-3");
    let doc = node_2_handle
        .get(3)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id(), 3);
    assert_eq!(doc.data(), b"Hello, world from node-3");

    Ok(())
}

