use std::time::Duration;

use datacake_eventual_consistency::test_utils::MemStore;
use datacake_eventual_consistency::EventuallyConsistentStoreExtension;
use datacake_node::{ConnectionConfig, DCAwareSelector, DatacakeNodeBuilder};

#[tokio::test]
async fn test_basic_connect() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let addr = test_helper::get_unused_addr();
    let connection_cfg = ConnectionConfig::new(addr, addr, Vec::<String>::new());

    let node = DatacakeNodeBuilder::<DCAwareSelector>::new(1, connection_cfg)
        .connect()
        .await?;
    let _store = node
        .add_extension(EventuallyConsistentStoreExtension::new(MemStore::default()))
        .await?;

    tokio::time::sleep(Duration::from_secs(1)).await;

    node.shutdown().await;

    Ok(())
}
