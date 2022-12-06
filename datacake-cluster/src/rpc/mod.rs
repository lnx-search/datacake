#[allow(clippy::all)]
mod chitchat_transport;
mod client;
#[allow(clippy::all)]
#[rustfmt::skip]
pub(crate) mod datacake_api;
mod network;
mod server;

pub use chitchat_transport::GrpcTransport;
pub use client::{ConsistencyClient, ReplicationClient};
pub use network::{RpcNetwork, CONNECT_TIMEOUT_LIMIT, TIMEOUT_LIMIT};
pub use server::{Context, DefaultRegistry, ServiceRegistry};
