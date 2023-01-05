# Datacake Node

The core membership system used within Datacake.

This system allows you to build cluster extensions on top of this core functionality giving you access to
the live membership watchers, node selectors, cluster clock, etc...

A good example of this is the `datacake-eventual-consistency` crate, it simply implements the `ClusterExtension` crate
which lets it be added at runtime without issue.

## Features
- Zero-copy RPC framework which allows for runtime adding and removing of services.
- Changeable node selector used for picking nodes out of a live membership to handle tasks.
- Pre-built data-center aware node selector for prioritisation of nodes in other availability zones.
- Distributed clock used for keeping an effective wall clock which respects causality.

## Getting Started

To get started we'll begin by creating our cluster:

```rust

use std::net::SocketAddr;

use datacake_node::{ConnectionConfig, DCAwareSelector, DatacakeNodeBuilder};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let bind_addr = "127.0.0.1:8000".parse::<SocketAddr>().unwrap();
    
    // We setup our connection config for the node passing in the bind address, public address and seed nodes.
    // Here we're just using the bind address as our public address with no seed, but in the real world
    // this will be a different value when deployed across several servers with seeds to contact.
    let connection_cfg = ConnectionConfig::new(bind_addr, bind_addr, Vec::<String>::new());
    
    // Our builder lets us configure the node. 
    // 
    // We can configure the node selector, data center of the node, cluster ID, etc...
    let my_node = DatacakeNodeBuilder::<DCAwareSelector>::new(1, connection_cfg).connect().await?;
        
    // Now we're connected we can add any extensions at runtime, our RPC server will already be
    // running and setup.
    //
    // Check out the `datacake-eventual-consistency` implementation for a demo.
    
    Ok(())
}
```

#### Creating A Extension

Creating a cluster extension is really simple, it's one trait and it can do just about anything:

```rust
use datacake_node::{ClusterExtension, DatacakeNode};
use async_trait::async_trait;

pub struct MyExtension;

#[async_trait]
impl ClusterExtension for MyExtension {
    type Output = ();
    type Error = MyError;

    async fn init_extension(
        self,
        node: &DatacakeNode,
    ) -> Result<Self::Output, Self::Error> {
        // In here we can setup our system using the live node.
        // This gives us things like the cluster clock and RPC server:
        
        println!("Creating my extension!");
        
        let timestamp = node.clock().get_time().await;
        println!("My timestamp: {timestamp}");
        
        Ok(())
    }  
}

pub struct MyError;
```
