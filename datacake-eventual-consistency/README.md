# Datacake Cluster
A batteries included library for building your own distributed data stores or replicated state.

This library is largely based on the same concepts as Riak and Cassandra. Consensus, membership and failure 
detection are managed by [Quickwit's Chitchat](https://github.com/quickwit-oss/chitchat) while state alignment
and replication is managed by [Datacake CRDT](https://github.com/lnx-search/datacake/tree/main/datacake-crdt).

RPC is provided and managed entirely within Datacake using [Tonic](https://crates.io/crates/tonic) and GRPC.

This library is focused around providing a simple and easy to build framework for your distributed apps without
being overwhelming. In fact, you can be up and running just by implementing 2 async traits.

## Basic Example

```rust
use std::net::SocketAddr;
use datacake_node::{Consistency, ConnectionConfig, DCAwareSelector, DatacakeNodeBuilder};
use datacake_eventual_consistency::test_utils::MemStore;
use datacake_eventual_consistency::EventuallyConsistentStoreExtension;

async fn main() -> anyhow::Result<()> {
    let addr = "127.0.0.1:8080".parse::<SocketAddr>().unwrap();
    let connection_cfg = ConnectionConfig::new(addr, addr, Vec::<String>::new());
    let node = DatacakeNodeBuilder::<DCAwareSelector>::new(1, connection_cfg)
        .connect()
        .await
        .expect("Connect node.");

    let store = node
        .add_extension(EventuallyConsistentStoreExtension::new(MemStore::default()))
        .await
        .expect("Create store.");
    
    let handle = store.handle();

    handle
        .put(
            "my-keyspace",
            1,
            b"Hello, world! From keyspace 1.".to_vec(),
            Consistency::All,
        )
        .await
        .expect("Put doc.");
    
    Ok(())
}
```

## Complete Examples
Indepth examples [can be found here](https://github.com/lnx-search/datacake/tree/main/examples).
