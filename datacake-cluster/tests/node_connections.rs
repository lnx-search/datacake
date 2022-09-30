use std::time::Duration;

use anyhow::Result;
use datacake_cluster::test_utils;

#[tokio::test]
async fn test_node_single_connect() -> Result<()> {
    let node_1 =
        test_utils::make_test_node("node-1", "127.0.0.1:8010", "127.0.0.1:8110", vec![])
            .await?;

    // Our cluster should be fully connected very quickly on localhost.
    tokio::time::sleep(Duration::from_secs(3)).await;

    assert_eq!(
        node_1.handle().live_nodes_count(),
        0,
        "Expected node-1 to have 0 peer nodes."
    );

    Ok(())
}

#[tokio::test]
async fn test_node_single_connect_dead_peer() -> Result<()> {
    let node_1 = test_utils::make_test_node(
        "node-1",
        "127.0.0.1:8020",
        "127.0.0.1:8120",
        vec!["127.0.0.1:8030"],
    )
    .await?;

    // Our cluster should be fully connected very quickly on localhost.
    tokio::time::sleep(Duration::from_secs(3)).await;

    assert_eq!(
        node_1.handle().live_nodes_count(),
        0,
        "Expected node-1 to have 0 peer nodes."
    );

    Ok(())
}

#[tokio::test]
async fn test_node_single_connect_dead_peer_recovery() -> Result<()> {
    let node_1 = test_utils::make_test_node(
        "node-1",
        "127.0.0.1:8030",
        "127.0.0.1:8130",
        vec!["127.0.0.1:8031"],
    )
    .await?;

    // Our cluster should be fully connected very quickly on localhost.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let _node_2 = test_utils::make_test_node(
        "node-2",
        "127.0.0.1:8031",
        "127.0.0.1:8131",
        vec!["127.0.0.1:8030"],
    )
    .await?;

    // Our cluster should be fully connected very quickly on localhost.
    tokio::time::sleep(Duration::from_secs(3)).await;

    assert_eq!(
        node_1.handle().live_nodes_count(),
        1,
        "Expected node-1 to have 1 peer nodes."
    );

    Ok(())
}

#[tokio::test]
async fn test_node_quorum_connect() -> Result<()> {
    let node_1 = test_utils::make_test_node(
        "node-1",
        "127.0.0.1:8000",
        "127.0.0.1:8100",
        vec!["127.0.0.1:8001", "127.0.0.1:8002"],
    )
    .await?;
    let node_2 = test_utils::make_test_node(
        "node-2",
        "127.0.0.1:8001",
        "127.0.0.1:8101",
        vec!["127.0.0.1:8000", "127.0.0.1:8002"],
    )
    .await?;
    let node_3 = test_utils::make_test_node(
        "node-3",
        "127.0.0.1:8002",
        "127.0.0.1:8102",
        vec!["127.0.0.1:8001", "127.0.0.1:8000"],
    )
    .await?;

    // Our cluster should be fully connected very quickly on localhost.
    tokio::time::sleep(Duration::from_secs(10)).await;

    assert_eq!(
        node_1.handle().live_nodes_count(),
        2,
        "Expected node-1 to have 2 peer nodes within 3 seconds."
    );
    assert_eq!(
        node_2.handle().live_nodes_count(),
        2,
        "Expected node-2 to have 2 peer nodes within 3 seconds."
    );
    assert_eq!(
        node_3.handle().live_nodes_count(),
        2,
        "Expected node-3 to have 2 peer nodes within 3 seconds."
    );

    Ok(())
}
