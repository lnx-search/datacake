use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub type Counter = AtomicU64;

#[derive(Debug, Clone, Default)]
/// Live metrics around the cluster system.
pub struct SystemStatistics(Arc<SystemStatisticsInner>);

impl Deref for SystemStatistics {
    type Target = SystemStatisticsInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Default)]
pub struct SystemStatisticsInner {
    /// The number of synchronisation tasks that are currently running concurrently.
    pub(crate) num_ongoing_sync_tasks: Counter,
    /// The number of synchronisation tasks that took longer than the selected timeout.
    pub(crate) num_slow_sync_tasks: Counter,
    /// The number of sync tasks that failed to complete due to an error.
    pub(crate) num_failed_sync_tasks: Counter,
    /// The number of times the node has observed a remote keyspace change.
    pub(crate) num_keyspace_changes: Counter,
}

impl SystemStatisticsInner {
    /// The number of synchronisation tasks that are currently running concurrently.
    pub fn num_ongoing_sync_tasks(&self) -> u64 {
        self.num_ongoing_sync_tasks.load(Ordering::Relaxed)
    }

    /// The number of synchronisation tasks that took longer than the selected timeout.
    pub fn num_slow_sync_tasks(&self) -> u64 {
        self.num_slow_sync_tasks.load(Ordering::Relaxed)
    }

    /// The number of sync tasks that failed to complete due to an error.
    pub fn num_failed_sync_tasks(&self) -> u64 {
        self.num_failed_sync_tasks.load(Ordering::Relaxed)
    }

    /// The number of times the node has observed a remote keyspace change.
    pub fn num_keyspace_changes(&self) -> u64 {
        self.num_keyspace_changes.load(Ordering::Relaxed)
    }
}
