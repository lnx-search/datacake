mod distributor;
mod poller;

use std::borrow::Cow;
use std::net::SocketAddr;

pub const MAX_CONCURRENT_REQUESTS: usize = 10;

#[derive(Debug, Default, Clone)]
/// Represents a set of changes to the membership of the cluster.
pub(crate) struct MembershipChanges {
    /// A set of nodes which have joined the cluster.
    pub(crate) joined: Vec<(Cow<'static, str>, SocketAddr)>,
    /// A set of nodes which have left the cluster.
    pub(crate) left: Vec<Cow<'static, str>>,
}

pub(crate) use distributor::{
    start_task_distributor_service,
    TaskDistributor,
    TaskServiceContext,
};
pub(crate) use poller::{
    start_replication_cycle,
    ReplicationCycleContext,
    ReplicationHandle,
};
