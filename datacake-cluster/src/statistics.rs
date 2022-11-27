use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub type Counter = AtomicU64;

#[derive(Debug, Clone, Default)]
/// Live metrics around the cluster system.
pub struct ClusterStatistics(Arc<ClusterStatisticsInner>);

impl Deref for ClusterStatistics {
    type Target = ClusterStatisticsInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Default)]
pub struct ClusterStatisticsInner {
    /// The number of currently alive members the node is aware of.
    pub(crate) num_live_members: Counter,
    /// The number of members the node currently believes is dead.
    pub(crate) num_dead_members: Counter,
    /// The number of synchronisation tasks that are currently running concurrently.
    pub(crate) num_ongoing_sync_tasks: Counter,
    /// The number of synchronisation tasks that took longer than the selected timeout.
    pub(crate) num_slow_sync_tasks: Counter,
    /// The number of sync tasks that failed to complete due to an error.
    pub(crate) num_failed_sync_tasks: Counter,
    /// The number of times the node has observed a remote keyspace change.
    pub(crate) num_keyspace_changes: Counter,
    /// The number of data centers/availability zones the cluster belongs to.
    pub(crate) num_data_centers: Counter,
}

impl ClusterStatisticsInner {
    /// The number of currently alive members the node is aware of.
    pub fn num_live_members(&self) -> u64 {
        self.num_live_members.load(Ordering::Relaxed)
    }

    /// The number of members the node currently believes is dead.
    pub fn num_dead_members(&self) -> u64 {
        self.num_dead_members.load(Ordering::Relaxed)
    }

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

    /// The number of data centers/availability zones the cluster belongs to.
    pub fn num_data_centers(&self) -> u64 {
        self.num_data_centers.load(Ordering::Relaxed)
    }
}
