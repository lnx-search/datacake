mod actor;
mod messages;
mod group;

pub use messages::{
    Set,
    MultiSet,
    Del,
    MultiDel,
    SymDiff,
    Diff,
    Serialize,
    LastUpdated,
    CorruptedState,
    NUM_SOURCES,
};
pub use actor::{spawn_keyspace, KeyspaceActor};
pub use group::{KeyspaceGroup, KeyspaceTimestamps};

pub const CONSISTENCY_SOURCE_ID: usize = 0;
pub const READ_REPAIR_SOURCE_ID: usize = 1;
