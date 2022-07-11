#[macro_use]
extern crate tracing;

pub mod error;
mod merger;
mod reader;
mod writer;

mod shared;
#[cfg(test)]
mod test_utils;
mod removal;

pub use merger::merge_segment_into_writer;
pub use reader::{SegmentBlocksIterator, SegmentReader};
pub use writer::SegmentWriter;
pub use removal::remove_segment;
