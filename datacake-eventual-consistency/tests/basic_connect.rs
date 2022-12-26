use std::net::SocketAddr;
use std::time::Duration;

use datacake_eventual_consistency::test_utils::MemStore;
use datacake_eventual_consistency::EventuallyConsistentStoreExtension;
use datacake_node::{ConnectionConfig, DCAwareSelector, DatacakeNodeBuilder};

#[tokio::test]
async fn test_basic_connect() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let addr = "127.0.0.1:8000".parse::<SocketAddr>().unwrap();
    let connection_cfg = ConnectionConfig::new(addr, addr, Vec::<String>::new());

    let node = DatacakeNodeBuilder::<DCAwareSelector>::new("node-1", connection_cfg)
        .connect()
        .await?;
    let _store = node
        .add_extension(EventuallyConsistentStoreExtension::new(MemStore::default()))
        .await?;

    tokio::time::sleep(Duration::from_secs(1)).await;

    node.shutdown().await;

    Ok(())
}
