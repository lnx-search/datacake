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

    /// The number of data centers/availability zones the cluster belongs to.
    pub fn num_data_centers(&self) -> u64 {
        self.num_data_centers.load(Ordering::Relaxed)
    }
}
