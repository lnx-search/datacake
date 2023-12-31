mod actor;
mod group;
mod messages;

pub use actor::{spawn_keyspace, KeyspaceActor};
pub use group::{KeyspaceGroup, KeyspaceInfo, KeyspaceTimestamps};
pub use messages::{
    Del,
    Diff,
    LastUpdated,
    MultiDel,
    MultiSet,
    Serialize,
    Set,
    NUM_SOURCES,
};

pub const CONSISTENCY_SOURCE_ID: usize = 0;
pub const READ_REPAIR_SOURCE_ID: usize = 1;
