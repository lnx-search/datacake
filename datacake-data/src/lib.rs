#[macro_use]
extern crate tracing;

pub mod block;
pub mod blocking;
pub mod cache;
pub mod segment_footer;
pub mod value;

pub type DocId = u64;
