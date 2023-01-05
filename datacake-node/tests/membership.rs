use std::time::Duration;

use datacake_node::{ConnectionConfig, DCAwareSelector, DatacakeNodeBuilder};

#[tokio::test]
// TODO: Improve this test so it's not really flaky
pub async fn test_member_join() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let node_1_addr = test_helper::get_unused_addr();
    let node_2_addr = test_helper::get_unused_addr();
    let node_3_addr = test_helper::get_unused_addr();
    let node_1_connection_cfg =
        ConnectionConfig::new(node_1_addr, node_1_addr, [node_2_addr.to_string()]);
    let node_2_connection_cfg =
        ConnectionConfig::new(node_2_addr, node_2_addr, [node_1_addr.to_string()]);
    let node_3_connection_cfg = ConnectionConfig::new(
        node_3_addr,
        node_3_addr,
        [node_1_addr.to_string(), node_2_addr.to_string()],
    );

    let node_1 = DatacakeNodeBuilder::<DCAwareSelector>::new(1, node_1_connection_cfg)
        .connect()
        .await?;
    let node_2 = DatacakeNodeBuilder::<DCAwareSelector>::new(2, node_2_connection_cfg)
        .connect()
        .await?;

    node_1
        .wait_for_nodes(&[2], Duration::from_secs(30))
        .await
        .expect("Nodes should connect within timeout.");
    node_2
        .wait_for_nodes(&[1], Duration::from_secs(30))
        .await
        .expect("Nodes should connect within timeout.");

    let stats = node_1.statistics();
    assert_eq!(stats.num_data_centers(), 1);
    assert_eq!(stats.num_live_members(), 2);
    assert_eq!(stats.num_dead_members(), 0);

    let stats = node_2.statistics();
    assert_eq!(stats.num_data_centers(), 1);
    assert_eq!(stats.num_live_members(), 2);
    assert_eq!(stats.num_dead_members(), 0);

    let node_3 = DatacakeNodeBuilder::<DCAwareSelector>::new(3, node_3_connection_cfg)
        .connect()
        .await?;

    node_3
        .wait_for_nodes(&[2, 1], Duration::from_secs(30))
        .await
        .expect("Nodes should connect within timeout.");

    tokio::time::sleep(Duration::from_secs(5)).await;

    let stats = node_3.statistics();
    assert_eq!(stats.num_data_centers(), 1);
    assert_eq!(stats.num_live_members(), 3);

    let stats = node_1.statistics();
    assert_eq!(stats.num_data_centers(), 1);
    assert_eq!(stats.num_live_members(), 3);

    let stats = node_2.statistics();
    assert_eq!(stats.num_data_centers(), 1);
    assert_eq!(stats.num_live_members(), 3);

    Ok(())
}

#[tokio::test]
// TODO: Improve this test so it's not really flaky
pub async fn test_member_leave() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

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
        .await?;
    let node_2 = DatacakeNodeBuilder::<DCAwareSelector>::new(2, node_2_connection_cfg)
        .connect()
        .await?;
    let node_3 = DatacakeNodeBuilder::<DCAwareSelector>::new(3, node_3_connection_cfg)
        .connect()
        .await?;

    node_1
        .wait_for_nodes(&[2, 3], Duration::from_secs(30))
        .await
        .expect("Nodes should connect within timeout.");
    node_2
        .wait_for_nodes(&[1, 3], Duration::from_secs(30))
        .await
        .expect("Nodes should connect within timeout.");
    node_3
        .wait_for_nodes(&[2, 1], Duration::from_secs(30))
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
