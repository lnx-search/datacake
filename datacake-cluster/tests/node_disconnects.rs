use std::time::Duration;

use anyhow::Result;
use datacake_cluster::test_utils;

#[tokio::test]
async fn test_node_failure() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

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
    tokio::time::sleep(Duration::from_secs(2)).await;

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

    node_1.shutdown().await?;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let handle = node_2.handle();
    handle.insert(123, vec![123]).await?;

    assert_eq!(
        node_2.handle().live_nodes_count(),
        1,
        "Expected node-2 to have 1 peer nodes within 3 seconds."
    );
    assert_eq!(
        node_3.handle().live_nodes_count(),
        1,
        "Expected node-3 to have 1 peer nodes within 3 seconds."
    );

    Ok(())
}

#[tokio::test]
async fn test_node_failure_and_recover() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let node_1 = test_utils::make_test_node(
        "node-1",
        "127.0.0.1:8010",
        "127.0.0.1:8110",
        vec!["127.0.0.1:8011", "127.0.0.1:8012"],
    )
    .await?;
    let node_2 = test_utils::make_test_node(
        "node-2",
        "127.0.0.1:8011",
        "127.0.0.1:8111",
        vec!["127.0.0.1:8010", "127.0.0.1:8012"],
    )
    .await?;
    let node_3 = test_utils::make_test_node(
        "node-3",
        "127.0.0.1:8012",
        "127.0.0.1:8112",
        vec!["127.0.0.1:8011", "127.0.0.1:8010"],
    )
    .await?;

    // Our cluster should be fully connected very quickly on localhost.
    tokio::time::sleep(Duration::from_secs(3)).await;

    node_1.shutdown().await?;
    tokio::time::sleep(Duration::from_secs(3)).await;

    let handle = node_2.handle();
    handle.insert(123, vec![123]).await?;

    assert_eq!(
        node_2.handle().live_nodes_count(),
        1,
        "Expected node-2 to have 1 peer nodes within 3 seconds."
    );
    assert_eq!(
        node_3.handle().live_nodes_count(),
        1,
        "Expected node-3 to have 1 peer nodes within 3 seconds."
    );

    let node_1 = test_utils::make_test_node(
        "node-1",
        "127.0.0.1:8010",
        "127.0.0.1:8110",
        vec!["127.0.0.1:8011", "127.0.0.1:8012"],
    )
    .await?;
    tokio::time::sleep(Duration::from_secs(3)).await;

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
