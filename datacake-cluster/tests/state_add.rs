use std::time::Duration;

use anyhow::Result;
use datacake_cluster::test_utils;

#[tokio::test]
async fn test_document_addition_propagation() -> Result<()> {
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

    tokio::time::sleep(Duration::from_secs(1)).await;

    let node_1_handle = node_1.handle();
    let node_2_handle = node_2.handle();
    let node_3_handle = node_3.handle();

    node_1_handle.insert(1, b"Hello, World!".to_vec()).await?;

    tokio::time::sleep(Duration::from_secs(5)).await;

    let node_2_doc = node_2_handle.get(1).await?;
    assert!(
        node_2_doc.is_some(),
        "Node-2: Expected document state to be propagated correctly."
    );

    let node_2_doc = node_2_doc.unwrap();
    assert_eq!(node_2_doc.id, 1, "Node-2: Document ids do not match.");
    assert_eq!(
        node_2_doc.data.as_slice(),
        b"Hello, World!",
        "Node-2: Document data does not match."
    );

    let node_3_doc = node_3_handle.get(1).await?;
    assert!(
        node_3_doc.is_some(),
        "Node-3: Expected document state to be propagated correctly."
    );

    let node_3_doc = node_3_doc.unwrap();
    assert_eq!(node_3_doc.id, 1, "Node-3: Document ids do not match.");
    assert_eq!(
        node_3_doc.data.as_slice(),
        b"Hello, World!",
        "Node-3: Document data does not match."
    );

    Ok(())
}


#[tokio::test]
async fn test_document_addition_data_race() -> Result<()> {
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

    tokio::time::sleep(Duration::from_secs(1)).await;

    let node_1_handle = node_1.handle();
    let node_2_handle = node_2.handle();
    let node_3_handle = node_3.handle();

    // These will technically happen at the same time.
    // So we want to make sure that all the data becomes consistent and all nodes
    // see the same document by the end of it.
    node_1_handle.insert(1, b"Hello, World! From node 1.".to_vec()).await?;
    node_2_handle.insert(1, b"Hello, World! From node 2.".to_vec()).await?;

    tokio::time::sleep(Duration::from_secs(5)).await;

    let node_1_doc = node_1_handle.get(1).await?;
    let node_2_doc = node_2_handle.get(1).await?;
    let node_3_doc = node_3_handle.get(1).await?;

    assert_eq!(node_1_doc, node_2_doc, "Expected documents to be the same.");
    assert_eq!(node_1_doc, node_3_doc, "Expected documents to be the same.");
    assert_eq!(node_2_doc, node_3_doc, "Expected documents to be the same.");

    Ok(())
}