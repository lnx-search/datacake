use std::time::Duration;

use anyhow::Result;
use datacake_cluster::test_utils;

#[tokio::test]
async fn test_document_removal_propagation() -> Result<()> {
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

    // Our state should be propagated.
    node_1_handle.insert(1, b"Hello, World!".to_vec()).await?;
    tokio::time::sleep(Duration::from_secs(5)).await;

    let node_2_doc = node_2_handle.get(1).await?;
    assert!(
        node_2_doc.is_some(),
        "Node-2: Expected document state to be propagated correctly."
    );

    let node_3_doc = node_3_handle.get(1).await?;
    assert!(
        node_3_doc.is_some(),
        "Node-3: Expected document state to be propagated correctly."
    );

    // Delete the document from a different node.
    node_3_handle.delete(1).await?;
    tokio::time::sleep(Duration::from_secs(5)).await;

    let node_1_doc = node_1_handle.get(1).await?;
    assert!(
        node_1_doc.is_none(),
        "Node-1: Expected document to no longer exist."
    );
    let node_2_doc = node_2_handle.get(1).await?;
    assert!(
        node_2_doc.is_some(),
        "Node-2: Expected document to no longer exist."
    );
    let node_3_doc = node_3_handle.get(1).await?;
    assert!(
        node_3_doc.is_some(),
        "Node-3: Expected document to no longer exist."
    );

    Ok(())
}

