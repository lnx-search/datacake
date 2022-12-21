use datacake_crdt::HLCTimestamp;

pub(crate) mod server;
pub(crate) mod chitchat_transport;
#[allow(clippy::all)]
#[rustfmt::skip]
mod datacake_api;
pub(crate) mod network;

pub use server::{ServiceRegistry, DefaultRegistry};

impl From<datacake_api::Timestamp> for HLCTimestamp {
    fn from(value: datacake_api::Timestamp) -> Self {
        Self::new(value.millis, value.counter as u16, value.node_id)
    }
}

impl From<HLCTimestamp> for datacake_api::Timestamp {
    fn from(value: HLCTimestamp) -> Self {
        Self {
            millis: value.millis(),
            counter: value.counter() as u32,
            node_id: value.node(),
        }
    }
}