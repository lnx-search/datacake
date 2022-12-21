mod client;
#[allow(clippy::all)]
#[rustfmt::skip]
pub(crate) mod datacake_api;
mod server;

pub use client::{ConsistencyClient, ReplicationClient};
