use std::ops::Range;
use std::time::{Duration, Instant};

use anyhow::Result;
use datacake::eventual_consistency::{
    EventuallyConsistentStore,
    EventuallyConsistentStoreExtension,
    ReplicatedStoreHandle,
};
use datacake::node::{
    ConnectionConfig,
    Consistency,
    DCAwareSelector,
    DatacakeNode,
    DatacakeNodeBuilder,
};

use crate::stores::memstore::MemStore;

#[instrument(name = "datacake-ec-benchmark")]
pub async fn run_datacake(n_nodes: u8, consistency: Consistency) -> Result<()> {
    let nodes = connect_nodes(n_nodes).await?;
    let first_node = &nodes[0];
    let handle = first_node.store.handle();

    let start = Instant::now();
    insert_n_docs(handle.clone(), 0..1_000, consistency).await?;
    info!(
        "Inserting 1000 docs serially took {}",
        humantime::format_duration(start.elapsed())
    );

    for concurrency in [10, 125, 256, 400, 512, 1024, 2048] {
        let start = Instant::now();
        run_insert_concurrently(handle.clone(), concurrency, 0..1_000, consistency)
            .await?;
        info!(
            "Inserting 1000 docs @ {concurrency} took {}",
            humantime::format_duration(start.elapsed())
        );
    }

    let start = Instant::now();
    remove_n_docs(handle.clone(), 0..1_000, consistency).await?;
    info!(
        "Removing 1000 docs serially took {}",
        humantime::format_duration(start.elapsed())
    );

    for concurrency in [10, 125, 256, 400, 512, 1024, 2048] {
        let start = Instant::now();
        run_remove_concurrently(handle.clone(), concurrency, 0..1_000, consistency)
            .await?;
        info!(
            "Removing 1000 docs @ {concurrency} took {}",
            humantime::format_duration(start.elapsed())
        );
    }

    Ok(())
}

async fn insert_n_docs(
    handle: ReplicatedStoreHandle<MemStore>,
    range: Range<u64>,
    consistency: Consistency,
) -> Result<()> {
    for id in range {
        handle
            .put(
                "my-keyspace",
                id,
                b"Hello, world! From keyspace 1.".to_vec(),
                consistency,
            )
            .await?;
    }
    Ok(())
}

async fn remove_n_docs(
    handle: ReplicatedStoreHandle<MemStore>,
    range: Range<u64>,
    consistency: Consistency,
) -> Result<()> {
    for id in range {
        handle.del("my-keyspace", id, consistency).await?;
    }
    Ok(())
}

async fn run_insert_concurrently(
    handle: ReplicatedStoreHandle<MemStore>,
    concurrency: usize,
    range: Range<u64>,
    consistency: Consistency,
) -> Result<()> {
    let mut handles = Vec::new();
    for _ in 0..concurrency {
        let handle = handle.clone();
        handles.push(tokio::spawn(insert_n_docs(
            handle,
            range.clone(),
            consistency,
        )));
    }

    for handle in handles {
        handle.await??;
    }

    Ok(())
}

async fn run_remove_concurrently(
    handle: ReplicatedStoreHandle<MemStore>,
    concurrency: usize,
    range: Range<u64>,
    consistency: Consistency,
) -> Result<()> {
    let mut handles = Vec::new();
    for _ in 0..concurrency {
        let handle = handle.clone();
        handles.push(tokio::spawn(remove_n_docs(
            handle,
            range.clone(),
            consistency,
        )));
    }

    for handle in handles {
        handle.await??;
    }

    Ok(())
}

async fn connect_nodes(n: u8) -> Result<Vec<DatacakeSystem>> {
    let mut nodes = Vec::new();
    let mut previous_seeds = Vec::new();
    let mut previous_node_ids = Vec::new();
    for id in 0..n {
        let addr = test_helper::get_unused_addr();

        let connection_cfg = ConnectionConfig::new(addr, addr, &previous_seeds);
        let node = DatacakeNodeBuilder::<DCAwareSelector>::new(id, connection_cfg)
            .connect()
            .await
            .expect("Connect node.");

        node.wait_for_nodes(&previous_node_ids, Duration::from_secs(30))
            .await?;

        previous_node_ids.push(id);
        if previous_seeds.len() >= 2 {
            previous_seeds.pop();
        }
        previous_seeds.push(addr.to_string());

        let store = node
            .add_extension(EventuallyConsistentStoreExtension::new(MemStore::default()))
            .await
            .expect("Create store.");

        nodes.push(DatacakeSystem { _node: node, store });
    }

    Ok(nodes)
}

struct DatacakeSystem {
    _node: DatacakeNode,
    store: EventuallyConsistentStore<MemStore>,
}
