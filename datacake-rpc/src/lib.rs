#[macro_use]
extern crate tracing;

mod client;
mod handler;
mod net;
mod request;
mod server;
mod view;

pub const SCRATCH_SPACE: usize = 4096;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

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
