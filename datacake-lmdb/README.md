# Datacake LMDB

A pre-built implementation of the datacake-eventual-consistency `Storage` trait, this allows you to set up
a persistent cluster immediately without any hassle of implementing a correct store.

For more info see https://github.com/lnx-search/datacake

## Example

```rust
use std::env::temp_dir;                                                            
use anyhow::Result;                                                                
use uuid::Uuid;                                                                    
use datacake_eventual_consistency::EventuallyConsistentStoreExtension;             
use datacake_node::{                                                               
    ConnectionConfig,                                                              
    Consistency,                                                                   
    DCAwareSelector,                                                               
    DatacakeNodeBuilder,                                                           
};                                                                                 
use datacake_lmdb::LmdbStorage;                                                    
                                                                                   
static KEYSPACE: &str = "lmdb-store";                                              
                                                                                   
#[tokio::main]                                                                     
async fn main() -> Result<()> {                                                    
    tracing_subscriber::fmt::init();                                               
                                                                                   
    let temp_dir = temp_dir().join(Uuid::new_v4().to_string());                    
    std::fs::create_dir_all(&temp_dir)?;                                           
                                                                                   
    let store = LmdbStorage::open(temp_dir).await?;                                
                                                                                   
    let addr = test_helper::get_unused_addr();                                     
    let connection_cfg = ConnectionConfig::new(addr, addr, Vec::<String>::new());  
                                                                                   
    let node = DatacakeNodeBuilder::<DCAwareSelector>::new(1, connection_cfg)      
        .connect()                                                                 
        .await?;                                                                   
    let store = node                                                               
        .add_extension(EventuallyConsistentStoreExtension::new(store))             
        .await?;                                                                   
                                                                                   
    let handle = store.handle();                                                   
                                                                                   
    handle.put(KEYSPACE, 1, b"Hello, world".to_vec(), Consistency::All).await?;    
                                                                                   
    let doc = handle                                                               
        .get(KEYSPACE, 1)                                                          
        .await?                                                                    
        .expect("Document should not be none");                                    
    assert_eq!(doc.id(), 1);                                                       
    assert_eq!(doc.data(), b"Hello, world");                                       
                                                                                   
    handle.del(KEYSPACE, 1, Consistency::All).await?;                              
    let doc = handle.get(KEYSPACE, 1).await?;                                      
    assert!(doc.is_none(), "No document should not exist!");                       
                                                                                   
    handle.del(KEYSPACE, 2, Consistency::All).await?;                              
    let doc = handle.get(KEYSPACE, 2).await?;                                      
    assert!(doc.is_none(), "No document should not exist!");                       
                                                                                   
    node.shutdown().await;                                                         
                                                                                   
    Ok(())                                                                         
}                                                                                  






```