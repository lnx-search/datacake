mod replication;

use std::time::Instant;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let start = Instant::now();
    replication::run_raft();
    println!("Took: {:?}", start.elapsed());

    Ok(())
}
