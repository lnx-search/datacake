extern crate core;

pub mod error;
mod reader;
mod writer;

#[cfg(test)]
mod test_utils;

pub use reader::{SegmentBlocksIterator, SegmentReader};
pub use writer::SegmentWriter;
