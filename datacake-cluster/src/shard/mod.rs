mod actor;
mod base;
mod group;
pub mod state;

pub use actor::{DeadShard, ShardHandle, StateChangeTs};
use datacake_crdt::Key;
pub use group::{create_shard_group, ShardGroupHandle, NUMBER_OF_SHARDS};

pub(crate) fn get_shard_id(key: Key) -> usize {
    (key % NUMBER_OF_SHARDS as u64) as usize
}
