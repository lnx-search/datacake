use std::net::SocketAddr;
use std::time::Duration;
use datacake_cluster::{ClusterOptions, ConnectionConfig, Consistency, DatacakeCluster, DCAwareSelector};
use datacake_cluster::mem_store::MemStore;
use datacake_cluster::test_suite::InstrumentedStorage;

static KEYSPACE_1: &str = "my-first-keyspace";
static KEYSPACE_2: &str = "my-second-keyspace";
static KEYSPACE_3: &str = "my-third-keyspace";

#[tokio::test]
async fn test_single_node() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let node_addr = "127.0.0.1:8014".parse::<SocketAddr>().unwrap();
    let connection_cfg = ConnectionConfig::new(
        node_addr,
        node_addr,
        Vec::<String>::new(),
    );
    let node = DatacakeCluster::connect(
        "node-1",
        connection_cfg,
        InstrumentedStorage(MemStore::default()),
        DCAwareSelector::default(),
        ClusterOptions::default(),
    )
    .await
    .expect("Connect node.");

    let handle = node.handle();

    handle
        .put(KEYSPACE_1, 1, b"Hello, world! From keyspace 1.".to_vec(), Consistency::All)
        .await
        .expect("Put doc.");

    handle
        .get(KEYSPACE_1, 1)
        .await
        .expect("Get doc.")
        .expect("Get document just stored.");
    let doc = handle
        .get(KEYSPACE_2, 1)
        .await
        .expect("Get doc.");
    assert!(doc.is_none());
    let doc = handle
        .get(KEYSPACE_3, 1)
        .await
        .expect("Get doc.");
    assert!(doc.is_none());

    handle
        .del(KEYSPACE_1, 1, Consistency::All)
        .await
        .expect("Put doc.");

    let doc = handle
        .get(KEYSPACE_1, 1)
        .await
        .expect("Get doc.");
    assert!(doc.is_none());
    let doc = handle
        .get(KEYSPACE_2, 1)
        .await
        .expect("Get doc.");
    assert!(doc.is_none());
    let doc = handle
        .get(KEYSPACE_3, 1)
        .await
        .expect("Get doc.");
    assert!(doc.is_none());

    Ok(())
}

#[tokio::test]
async fn test_multi_node() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let node_1_addr = "127.0.0.1:8015".parse::<SocketAddr>().unwrap();
    let node_2_addr = "127.0.0.1:8016".parse::<SocketAddr>().unwrap();
    let node_3_addr = "127.0.0.1:8017".parse::<SocketAddr>().unwrap();
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

    let node_1 = DatacakeCluster::connect(
        "node-1",
        node_1_connection_cfg,
        InstrumentedStorage(MemStore::default()),
        DCAwareSelector::default(),
        ClusterOptions::default(),
    )
    .await
    .expect("Connect node.");
    let node_2 = DatacakeCluster::connect(
        "node-2",
        node_2_connection_cfg,
        InstrumentedStorage(MemStore::default()),
        DCAwareSelector::default(),
        ClusterOptions::default(),
    )
    .await
    .expect("Connect node.");
    let node_3 = DatacakeCluster::connect(
        "node-3",
        node_3_connection_cfg,
        InstrumentedStorage(MemStore::default()),
        DCAwareSelector::default(),
        ClusterOptions::default(),
    )
    .await
    .expect("Connect node.");

    node_1
        .wait_for_nodes(&["node-2", "node-3"], Duration::from_secs(5))
        .await
        .expect("Nodes should connect within timeout.");
    node_2
        .wait_for_nodes(&["node-3", "node-1"], Duration::from_secs(5))
        .await
        .expect("Nodes should connect within timeout.");
    node_3
        .wait_for_nodes(&["node-2", "node-1"], Duration::from_secs(5))
        .await
        .expect("Nodes should connect within timeout.");

    let node_1_handle = node_1.handle();
    let node_2_handle = node_2.handle();
    let node_3_handle = node_3.handle();

    node_1_handle
        .put(KEYSPACE_1, 1, b"Hello, world! From keyspace 1.".to_vec(), Consistency::All)
        .await
        .expect("Put doc.");

    node_2_handle
        .get(KEYSPACE_1, 1)
        .await
        .expect("Get doc.")
        .expect("Get document just stored.");
    let doc = node_3_handle
        .get(KEYSPACE_2, 1)
        .await
        .expect("Get doc.");
    assert!(doc.is_none());
    let doc = node_1_handle
        .get(KEYSPACE_3, 1)
        .await
        .expect("Get doc.");
    assert!(doc.is_none());

    node_2_handle
        .del(KEYSPACE_1, 1, Consistency::All)
        .await
        .expect("Put doc.");

    let doc = node_2_handle
        .get(KEYSPACE_1, 1)
        .await
        .expect("Get doc.");
    assert!(doc.is_none());
    let doc = node_3_handle
        .get(KEYSPACE_2, 1)
        .await
        .expect("Get doc.");
    assert!(doc.is_none());
    let doc = node_3_handle
        .get(KEYSPACE_3, 1)
        .await
        .expect("Get doc.");
    assert!(doc.is_none());

    Ok(())
}
