
#[derive(Debug)]
/// The type of membership behaviour the cluster should exhibit.
pub enum MembershipMode {
    /// Uses a fixed subset of nodes within the system to create the raft cluster.
    Fixed(Vec<String>),
    /// Similar to [MembershipMode::Fixed] however, this allows for dynamic adding
    /// and removal of nodes from the cluster.
    Dynamic(Vec<String>),
    /// Automatically include all nodes in the system and add nodes which join the
    /// cluster.
    Auto,
}