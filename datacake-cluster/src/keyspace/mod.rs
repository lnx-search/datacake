mod actor;
mod group;
mod messages;

pub use actor::{spawn_keyspace, KeyspaceActor};
pub use group::{KeyspaceGroup, KeyspaceTimestamps};
pub use messages::{
    CorruptedState,
    Del,
    Diff,
    LastUpdated,
    MultiDel,
    MultiSet,
    Serialize,
    Set,
    SymDiff,
    NUM_SOURCES,
};

pub const CONSISTENCY_SOURCE_ID: usize = 0;
pub const READ_REPAIR_SOURCE_ID: usize = 1;
