#[macro_use]
extern crate tracing;

mod client;
mod handler;
mod net;
mod replay;
mod request;
mod server;
mod view;

pub const SCRATCH_SPACE: usize = 4096;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub use async_trait::async_trait;
pub use client::{Client, MessageReply, RpcClient};
pub use net::{
    ArchivedErrorCode,
    ArchivedStatus,
    ClientConnectError,
    ErrorCode,
    ServerBindError,
    Status,
};
pub use request::Request;
pub use server::Server;
pub use view::{DataView, InvalidView};
pub use handler::{ServiceRegistry, RpcService, Handler};

pub(crate) fn hash<H: Hash + ?Sized>(v: &H) -> u64 {
    let mut hasher = DefaultHasher::new();
    v.hash(&mut hasher);
    hasher.finish()
}
