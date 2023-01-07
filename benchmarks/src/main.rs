#[macro_use]
extern crate tracing;

mod replication;
mod stores;

use std::time::Instant;
use anyhow::Result;
use datacake::node::Consistency;
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "info,datacake_node=error,datacake_eventual_consistency=error");
    tracing_subscriber::fmt::init();

    info!("Beginning eventually consistent replication benchmark...");
    let start = Instant::now();

    replication::run_datacake(1, Consistency::All).await?;
    replication::run_datacake(3, Consistency::All).await?;
    replication::run_datacake(5, Consistency::All).await?;

    replication::run_datacake(1, Consistency::None).await?;
    replication::run_datacake(3, Consistency::None).await?;
    replication::run_datacake(5, Consistency::None).await?;

    // 1 node cluster cannot use Consistency::One - replication::run_datacake(1, Consistency::One).await?;
    replication::run_datacake(3, Consistency::One).await?;
    replication::run_datacake(5, Consistency::One).await?;

    replication::run_datacake(1, Consistency::EachQuorum).await?;
    replication::run_datacake(3, Consistency::EachQuorum).await?;
    replication::run_datacake(5, Consistency::EachQuorum).await?;

    replication::run_datacake(1, Consistency::Quorum).await?;
    replication::run_datacake(3, Consistency::Quorum).await?;
    replication::run_datacake(5, Consistency::Quorum).await?;

    replication::run_datacake(1, Consistency::LocalQuorum).await?;
    replication::run_datacake(3, Consistency::LocalQuorum).await?;
    replication::run_datacake(5, Consistency::LocalQuorum).await?;

    info!("Benchmark Took: {}", humantime::format_duration(start.elapsed()));

    Ok(())
}
