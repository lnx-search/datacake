extern crate core;

pub mod error;
mod merger;
mod reader;
mod writer;

#[cfg(test)]
mod test_utils;

pub use merger::merge_segment_into_writer;
pub use reader::{SegmentBlocksIterator, SegmentReader};
pub use writer::SegmentWriter;
