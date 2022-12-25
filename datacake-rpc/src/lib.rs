//! An actor-like RPC framework built for true zero-copy message handling.
//!
//! This framework is inspired by tonic but is *not* a GRPC framework. Instead,
//! it makes use of the incredible [rkyv] (de)serialization framework which provides
//! us with lightning fast (de)serialization and also lets us perform true zero-copy
//! deserialization which can lead to massive performance improvements when processing
//! lots of big messages at once.
//!
//! ### Features
//! - Fast (de)serialization of owned types.
//! - True zero-copy deserialization avoiding heavy allocations.
//! - Dynamic adding and removing of message handlers/services.
//!
//! ### Basic example
//! ```rust
//! use std::net::SocketAddr;
//!
//! use bytecheck::CheckBytes;
//! use datacake_rpc::{
//!     Channel,
//!     Handler,
//!     Request,
//!     RpcClient,
//!     RpcService,
//!     Server,
//!     ServiceRegistry,
//!     Status,
//! };
//! use rkyv::{Archive, Deserialize, Serialize};
//!
//! // The framework accepts any messages which implement `Archive` and `Serialize` along
//! // with the archived values implementing `CheckBytes` from the `bytecheck` crate.
//! // This is to ensure safe, validated deserialization of the values.
//! //
//! // Checkout rkyv for more information!
//! #[repr(C)]
//! #[derive(Serialize, Deserialize, Archive, PartialEq, Debug)]
//! #[archive(compare(PartialEq))]
//! #[archive_attr(derive(CheckBytes, PartialEq, Debug))]
//! pub struct MyMessage {
//!     name: String,
//!     age: u32,
//! }
//!
//! pub struct MyService;
//!
//! impl RpcService for MyService {
//!     // The `register_handlers` is used to mark messages as something
//!     // the given service can handle and process.
//!     //
//!     // Messages which are not registered will not be dispatched to the handler.
//!     fn register_handlers(registry: &mut ServiceRegistry<Self>) {
//!         registry.add_handler::<MyMessage>();
//!     }
//! }
//!
//! #[datacake_rpc::async_trait]
//! impl Handler<MyMessage> for MyService {
//!     type Reply = String;
//!
//!     // Our `Request` gives us a zero-copy view to our message, this doesn't actually
//!     // allocate the message type.
//!     async fn on_message(&self, msg: Request<MyMessage>) -> Result<Self::Reply, Status> {
//!         Ok(msg.to_owned().unwrap().name)
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let address = "127.0.0.1:8000".parse::<SocketAddr>()?;
//!
//!     let server = Server::listen(address).await?;
//!     // Services can be added and removed at runtime once the server is started.
//!     server.add_service(MyService);
//!     println!("Listening to address {}!", address);
//!
//!     // Channels are cheap to clone similar to tonic.
//!     let client = Channel::connect(address).await?;
//!     println!("Connected to address {}!", address);
//!
//!     let mut rpc_client = RpcClient::<MyService>::new(client);
//!
//!     let msg1 = MyMessage {
//!         name: "Bobby".to_string(),
//!         age: 12,
//!     };
//!
//!     // Clients only need references to the message which helps
//!     // reduce allocations.
//!     let resp = rpc_client.send(&msg1).await?;
//!     assert_eq!(resp, msg1);
//!     Ok(())
//! }
//! ```

#[macro_use]
extern crate tracing;

mod client;
mod handler;
mod net;
mod request;
mod server;
mod view;

pub(crate) const SCRATCH_SPACE: usize = 4096;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// A re-export of the async-trait macro.
pub use async_trait::async_trait;
pub use client::{MessageReply, RpcClient};
pub use handler::{Handler, RpcService, ServiceRegistry};
pub use net::{ArchivedErrorCode, ArchivedStatus, Channel, ErrorCode, Status};
pub use request::Request;
pub use server::Server;
pub use view::{DataView, InvalidView};

pub(crate) fn hash<H: Hash + ?Sized>(v: &H) -> u64 {
    let mut hasher = DefaultHasher::new();
    v.hash(&mut hasher);
    hasher.finish()
}

pub(crate) fn to_uri_path(service: &str, path: &str) -> String {
    format!("/{}/{}", service, path)
}
