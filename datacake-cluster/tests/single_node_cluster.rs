use std::net::SocketAddr;
use std::time::Duration;
use tracing::info;
use datacake_cluster::{ClusterOptions, ConnectionConfig, Consistency, DatacakeCluster, DCAwareSelector};
use datacake_cluster::mem_store::MemStore;

static KEYSPACE: &str = "my-keyspace";

#[tokio::test]
async fn test_single_node_cluster() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let addr = "127.0.0.1:8001".parse::<SocketAddr>().unwrap();
    let connection_cfg = ConnectionConfig::new(addr, addr, Vec::<String>::new());

    let cluster = DatacakeCluster::connect(
        "node-1",
        connection_cfg,
        MemStore::default(),
        DCAwareSelector::default(),
        ClusterOptions::default(),
    ).await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let stats = cluster.statistics();
    assert_eq!(stats.num_data_centers(), 1);
    assert_eq!(stats.num_live_members(), 1);
    assert_eq!(stats.num_dead_members(), 0);
    assert_eq!(stats.num_slow_sync_tasks(), 0);
    assert_eq!(stats.num_failed_sync_tasks(), 0);
    assert_eq!(stats.num_ongoing_sync_tasks(), 0);

    let handle = cluster.handle();

    // Test reading
    let doc = handle
        .get(KEYSPACE, 1)
        .await
        .expect("Get value.");
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
    assert_eq!(doc.id, 1);
    assert_eq!(doc.data.as_ref(), b"Hello, world");

    handle
        .del(KEYSPACE, 1, Consistency::All)
        .await
        .expect("Del value.");
    let doc = handle
        .get(KEYSPACE, 1)
        .await
        .expect("Get value.");
    assert!(doc.is_none(), "No document should not exist!");

    handle
        .del(KEYSPACE, 2, Consistency::All)
        .await
        .expect("Del value which doesnt exist locally.");
    let doc = handle
        .get(KEYSPACE, 2)
        .await
        .expect("Get value.");
    assert!(doc.is_none(), "No document should not exist!");

    // We don't poll ourselves.
    let stats = cluster.statistics();
    assert_eq!(stats.num_data_centers(), 1);
    assert_eq!(stats.num_live_members(), 1);
    assert_eq!(stats.num_dead_members(), 0);
    assert_eq!(stats.num_slow_sync_tasks(), 0);
    assert_eq!(stats.num_failed_sync_tasks(), 0);
    assert_eq!(stats.num_ongoing_sync_tasks(), 0);

    info!("Shutting down cluster");
    cluster.shutdown().await;

    Ok(())
}


#[tokio::test]
async fn test_single_node_cluster_with_keyspace_handle() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let addr = "127.0.0.1:8002".parse::<SocketAddr>().unwrap();
    let connection_cfg = ConnectionConfig::new(addr, addr, Vec::<String>::new());

    let cluster = DatacakeCluster::connect(
        "node-1",
        connection_cfg,
        MemStore::default(),
        DCAwareSelector::default(),
        ClusterOptions::default(),
    ).await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let stats = cluster.statistics();
    assert_eq!(stats.num_data_centers(), 1);
    assert_eq!(stats.num_live_members(), 1);
    assert_eq!(stats.num_dead_members(), 0);
    assert_eq!(stats.num_slow_sync_tasks(), 0);
    assert_eq!(stats.num_failed_sync_tasks(), 0);
    assert_eq!(stats.num_ongoing_sync_tasks(), 0);

    let handle = cluster.handle_with_keyspace(KEYSPACE);

    // Test reading
    let doc = handle
        .get(1)
        .await
        .expect("Get value.");
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
    assert_eq!(doc.id, 1);
    assert_eq!(doc.data.as_ref(), b"Hello, world");

    handle
        .del(1, Consistency::All)
        .await
        .expect("Del value.");
    let doc = handle
        .get(1)
        .await
        .expect("Get value.");
    assert!(doc.is_none(), "No document should not exist!");

    handle
        .del(2, Consistency::All)
        .await
        .expect("Del value which doesnt exist locally.");
    let doc = handle
        .get( 2)
        .await
        .expect("Get value.");
    assert!(doc.is_none(), "No document should not exist!");

    // We don't poll ourselves.
    let stats = cluster.statistics();
    assert_eq!(stats.num_data_centers(), 1);
    assert_eq!(stats.num_live_members(), 1);
    assert_eq!(stats.num_dead_members(), 0);
    assert_eq!(stats.num_slow_sync_tasks(), 0);
    assert_eq!(stats.num_failed_sync_tasks(), 0);
    assert_eq!(stats.num_ongoing_sync_tasks(), 0);

    cluster.shutdown().await;

    Ok(())
}

#[tokio::test]
async fn test_single_node_cluster_bulk_op() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let addr = "127.0.0.1:8003".parse::<SocketAddr>().unwrap();
    let connection_cfg = ConnectionConfig::new(addr, addr, Vec::<String>::new());

    let cluster = DatacakeCluster::connect(
        "node-1",
        connection_cfg,
        MemStore::default(),
        DCAwareSelector::default(),
        ClusterOptions::default(),
    ).await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let stats = cluster.statistics();
    assert_eq!(stats.num_data_centers(), 1);
    assert_eq!(stats.num_live_members(), 1);
    assert_eq!(stats.num_dead_members(), 0);
    assert_eq!(stats.num_slow_sync_tasks(), 0);
    assert_eq!(stats.num_failed_sync_tasks(), 0);
    assert_eq!(stats.num_ongoing_sync_tasks(), 0);

    let handle = cluster.handle();

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
    assert_eq!(docs[0].id, 1);
    assert_eq!(docs[0].data.as_ref(), b"Hello, world");

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
        .get_many(KEYSPACE,  [2, 3, 5, 1])
        .await
        .expect("Get value.")
        .count();
    assert_eq!(num_docs, 0, "No document should not exist!");

    // We don't poll ourselves.
    let stats = cluster.statistics();
    assert_eq!(stats.num_data_centers(), 1);
    assert_eq!(stats.num_live_members(), 1);
    assert_eq!(stats.num_dead_members(), 0);
    assert_eq!(stats.num_slow_sync_tasks(), 0);
    assert_eq!(stats.num_failed_sync_tasks(), 0);
    assert_eq!(stats.num_ongoing_sync_tasks(), 0);

    cluster.shutdown().await;

    Ok(())
}


#[tokio::test]
async fn test_single_node_cluster_bulk_op_with_keyspace_handle() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let addr = "127.0.0.1:8004".parse::<SocketAddr>().unwrap();
    let connection_cfg = ConnectionConfig::new(addr, addr, Vec::<String>::new());

    let cluster = DatacakeCluster::connect(
        "node-1",
        connection_cfg,
        MemStore::default(),
        DCAwareSelector::default(),
        ClusterOptions::default(),
    ).await?;

    tokio::time::sleep(Duration::from_millis(5000)).await;

    let stats = cluster.statistics();
    assert_eq!(stats.num_data_centers(), 1);
    assert_eq!(stats.num_live_members(), 1);
    assert_eq!(stats.num_dead_members(), 0);
    assert_eq!(stats.num_slow_sync_tasks(), 0);
    assert_eq!(stats.num_failed_sync_tasks(), 0);
    assert_eq!(stats.num_ongoing_sync_tasks(), 0);

    let handle = cluster.handle_with_keyspace(KEYSPACE);

    // Test reading
    let num_docs = handle
        .get_many([1])
        .await
        .expect("Get value.")
        .count();
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
    assert_eq!(docs[0].id, 1);
    assert_eq!(docs[0].data.as_ref(), b"Hello, world");

    handle
        .del_many([1], Consistency::All)
        .await
        .expect("Del value.");
    let num_docs = handle
        .get_many([1])
        .await
        .expect("Get value.")
        .count();
    assert_eq!(num_docs, 0, "No document should not exist!");

    handle
        .del_many([2, 3, 1, 5], Consistency::All)
        .await
        .expect("Del value which doesnt exist locally.");
    let num_docs = handle
        .get_many( [2, 3, 5, 1])
        .await
        .expect("Get value.")
        .count();
    assert_eq!(num_docs, 0, "No document should not exist!");

    // We don't poll ourselves.
    let stats = cluster.statistics();
    assert_eq!(stats.num_data_centers(), 1);
    assert_eq!(stats.num_live_members(), 1);
    assert_eq!(stats.num_dead_members(), 0);
    assert_eq!(stats.num_slow_sync_tasks(), 0);
    assert_eq!(stats.num_failed_sync_tasks(), 0);
    assert_eq!(stats.num_ongoing_sync_tasks(), 0);

    cluster.shutdown().await;

    Ok(())
}