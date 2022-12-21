use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use datacake_cluster::test_utils::{InstrumentedStorage, MemStore};
use datacake_cluster::{
    ClusterOptions,
    ConnectionConfig,
    Consistency,
    DCAwareSelector,
    EventuallyConsistentStore,
};

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

    let node_1 = EventuallyConsistentStore::connect(
        "node-1",
        node_1_connection_cfg,
        InstrumentedStorage(MemStore::default()),
        DCAwareSelector::default(),
        ClusterOptions::default(),
    )
    .await
    .expect("Connect node.");
    let node_2 = EventuallyConsistentStore::connect(
        "node-2",
        node_2_connection_cfg,
        InstrumentedStorage(MemStore::default()),
        DCAwareSelector::default(),
        ClusterOptions::default(),
    )
    .await
    .expect("Connect node.");

    node_1
        .wait_for_nodes(&["node-2"], Duration::from_secs(30))
        .await
        .expect("Nodes should connect within timeout.");
    node_2
        .wait_for_nodes(&["node-1"], Duration::from_secs(30))
        .await
        .expect("Nodes should connect within timeout.");

    let node_1_handle = node_1.handle_with_keyspace("my-keyspace");
    let node_2_handle = node_2.handle_with_keyspace("my-keyspace");

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
    assert_eq!(doc.id, 1);
    assert_eq!(doc.data, Bytes::from_static(b"Hello, world from node-1"));
    let doc = node_1_handle
        .get(2)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id, 2);
    assert_eq!(doc.data, Bytes::from_static(b"Hello, world from node-2"));

    let doc = node_2_handle
        .get(1)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id, 1);
    assert_eq!(doc.data, Bytes::from_static(b"Hello, world from node-1"));
    let doc = node_2_handle
        .get(2)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id, 2);
    assert_eq!(doc.data, Bytes::from_static(b"Hello, world from node-2"));

    let node_3 = EventuallyConsistentStore::connect(
        "node-3",
        node_3_connection_cfg,
        InstrumentedStorage(MemStore::default()),
        DCAwareSelector::default(),
        ClusterOptions::default(),
    )
    .await
    .expect("Connect node.");

    let node_3_handle = node_3.handle_with_keyspace("my-keyspace");

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
    assert_eq!(doc.id, 1);
    assert_eq!(doc.data, Bytes::from_static(b"Hello, world from node-1"));
    let doc = node_3_handle
        .get(2)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id, 2);
    assert_eq!(doc.data, Bytes::from_static(b"Hello, world from node-2"));

    let doc = node_1_handle
        .get(3)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id, 3);
    assert_eq!(doc.data, Bytes::from_static(b"Hello, world from node-3"));
    let doc = node_2_handle
        .get(3)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id, 3);
    assert_eq!(doc.data, Bytes::from_static(b"Hello, world from node-3"));

    Ok(())
}

#[tokio::test]
// TODO: Improve this test so it's not really flaky
pub async fn test_member_leave() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let node_1_addr = "127.0.0.1:8021".parse::<SocketAddr>().unwrap();
    let node_2_addr = "127.0.0.1:8022".parse::<SocketAddr>().unwrap();
    let node_3_addr = "127.0.0.1:8023".parse::<SocketAddr>().unwrap();
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

    let node_1 = EventuallyConsistentStore::connect(
        "node-1",
        node_1_connection_cfg,
        InstrumentedStorage(MemStore::default()),
        DCAwareSelector::default(),
        ClusterOptions::default(),
    )
    .await
    .expect("Connect node.");
    let node_2 = EventuallyConsistentStore::connect(
        "node-2",
        node_2_connection_cfg,
        InstrumentedStorage(MemStore::default()),
        DCAwareSelector::default(),
        ClusterOptions::default(),
    )
    .await
    .expect("Connect node.");
    let node_3 = EventuallyConsistentStore::connect(
        "node-3",
        node_3_connection_cfg.clone(),
        InstrumentedStorage(MemStore::default()),
        DCAwareSelector::default(),
        ClusterOptions::default(),
    )
    .await
    .expect("Connect node.");

    node_1
        .wait_for_nodes(&["node-2", "node-3"], Duration::from_secs(30))
        .await
        .expect("Nodes should connect within timeout.");
    node_2
        .wait_for_nodes(&["node-3", "node-1"], Duration::from_secs(30))
        .await
        .expect("Nodes should connect within timeout.");
    node_3
        .wait_for_nodes(&["node-2", "node-1"], Duration::from_secs(30))
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

    node_3.shutdown().await;

    // Let the cluster sort itself out.
    // It's a long time because the system tries to give the node time to become apart of the system again.
    tokio::time::sleep(Duration::from_secs(90)).await;

    let stats = node_1.statistics();
    assert_eq!(stats.num_data_centers(), 1);
    assert_eq!(stats.num_live_members(), 2);

    let stats = node_2.statistics();
    assert_eq!(stats.num_data_centers(), 1);
    assert_eq!(stats.num_live_members(), 2);

    Ok(())
}
