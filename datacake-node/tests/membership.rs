use std::net::SocketAddr;
use std::time::Duration;
use datacake_node::{ConnectionConfig, DatacakeNodeBuilder, DCAwareSelector};

#[tokio::test]
// TODO: Improve this test so it's not really flaky
pub async fn test_member_join() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let node_1_addr = "127.0.0.1:8018".parse::<SocketAddr>().unwrap();
    let node_2_addr = "127.0.0.1:8019".parse::<SocketAddr>().unwrap();
    let node_3_addr = "127.0.0.1:8020".parse::<SocketAddr>().unwrap();
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

    let stats = node_1.statistics();
    assert_eq!(stats.num_data_centers(), 1);
    assert_eq!(stats.num_live_members(), 2);
    assert_eq!(stats.num_dead_members(), 0);

    let stats = node_2.statistics();
    assert_eq!(stats.num_data_centers(), 1);
    assert_eq!(stats.num_live_members(), 2);
    assert_eq!(stats.num_dead_members(), 0);


    let node_3 = DatacakeNodeBuilder::<DCAwareSelector>::new("node-3", node_3_connection_cfg).connect().await?;

    node_3
        .wait_for_nodes(&["node-2", "node-1"], Duration::from_secs(30))
        .await
        .expect("Nodes should connect within timeout.");

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

    let node_1_addr = "127.0.0.1:8021".parse::<SocketAddr>().unwrap();
    let node_2_addr = "127.0.0.1:8022".parse::<SocketAddr>().unwrap();
    let node_3_addr = "127.0.0.1:8023".parse::<SocketAddr>().unwrap();
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

    let node_1 = DatacakeNodeBuilder::<DCAwareSelector>::new("node-1", node_1_connection_cfg).connect().await?;
    let node_2 = DatacakeNodeBuilder::<DCAwareSelector>::new("node-2", node_2_connection_cfg).connect().await?;
    let node_3 = DatacakeNodeBuilder::<DCAwareSelector>::new("node-3", node_3_connection_cfg).connect().await?;

    node_1
        .wait_for_nodes(&["node-2", "node-3"], Duration::from_secs(30))
        .await
        .expect("Nodes should connect within timeout.");
    node_2
        .wait_for_nodes(&["node-1", "node-3"], Duration::from_secs(30))
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
