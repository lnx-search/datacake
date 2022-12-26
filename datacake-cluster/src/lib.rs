#[macro_use]
extern crate tracing;

mod core;
pub mod error;
mod keyspace;
mod replication;
mod rpc;
mod statistics;
mod storage;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

use std::borrow::Cow;
use std::future::Future;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datacake_crdt::Key;
use datacake_node::{
    ClusterExtension,
    Consistency,
    ConsistencyError,
    DatacakeHandle,
    DatacakeNode,
};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
pub use statistics::SystemStatistics;
#[cfg(feature = "test-utils")]
pub use storage::test_suite;
pub use storage::{BulkMutationError, ProgressTracker, PutContext, Storage};

pub use self::core::{Document, DocumentMetadata};
use crate::keyspace::{
    Del,
    KeyspaceGroup,
    MultiDel,
    MultiSet,
    Set,
    CONSISTENCY_SOURCE_ID,
};
use crate::replication::{
    Mutation,
    ReplicationCycleContext,
    ReplicationHandle,
    TaskDistributor,
    TaskServiceContext,
};
use crate::rpc::ConsistencyClient;
use crate::rpc::services::consistency_impl::ConsistencyService;
use crate::rpc::services::replication_impl::ReplicationService;

const TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_REPAIR_INTERVAL: Duration = if cfg!(any(test, feature = "test-utils")) {
    Duration::from_secs(1)
} else {
    Duration::from_secs(60 * 60) // 1 Hour
};

/// A fully managed eventually consistent state controller.
///
/// The [EventuallyConsistentStore] manages all RPC and state propagation for
/// a given application, where the only setup required is the
/// RPC based configuration and the required handler traits
/// which wrap the application itself.
///
/// Datacake essentially acts as a frontend wrapper around a datastore
/// to make is distributed.
pub struct EventuallyConsistentStoreExtension<S>
where
    S: Storage + Send + Sync + 'static,
{
    datastore: S,
    repair_interval: Duration,
}

impl<S> EventuallyConsistentStoreExtension<S>
where
    S: Storage + Send + Sync + 'static,
{
    /// Creates a new extension with a given data store, using the default repair
    /// interval.
    pub fn new(store: S) -> Self {
        Self {
            datastore: store,
            repair_interval: DEFAULT_REPAIR_INTERVAL,
        }
    }

    /// Set a custom repair interval rather than the default (1 hour.)
    pub fn with_repair_interval(mut self, dur: Duration) -> Self {
        self.repair_interval = dur;
        self
    }
}

#[async_trait]
impl<S> ClusterExtension for EventuallyConsistentStoreExtension<S>
where
    S: Storage + Send + Sync + 'static,
{
    type Output = EventuallyConsistentStore<S>;
    type Error = error::StoreError<S::Error>;

    async fn init_extension(
        self,
        node: &DatacakeNode,
    ) -> Result<Self::Output, Self::Error> {
        EventuallyConsistentStore::create(self.datastore, self.repair_interval, node).await
    }
}

/// A fully managed eventually consistent state controller.
///
/// The [EventuallyConsistentStore] manages all RPC and state propagation for
/// a given application, where the only setup required is the
/// RPC based configuration and the required handler traits
/// which wrap the application itself.
///
/// Datacake essentially acts as a frontend wrapper around a datastore
/// to make is distributed.
pub struct EventuallyConsistentStore<S>
where
    S: Storage + Send + Sync + 'static,
{
    node: DatacakeHandle,
    group: KeyspaceGroup<S>,
    task_service: TaskDistributor,
    repair_service: ReplicationHandle,
    statistics: SystemStatistics,
}

impl<S> EventuallyConsistentStore<S>
where
    S: Storage + Send + Sync + 'static,
{
    async fn create(
        datastore: S,
        repair_interval: Duration,
        node: &DatacakeNode,
    ) -> Result<Self, error::StoreError<S::Error>> {
        let storage = Arc::new(datastore);

        let group = KeyspaceGroup::new(storage.clone(), node.clock().clone()).await;
        let statistics = SystemStatistics::default();

        // Load the keyspace states.
        group.load_states_from_storage().await?;

        let task_ctx = TaskServiceContext {
            clock: node.clock().clone(),
            network: node.network().clone(),
            local_node_id: Cow::Owned(node.me().node_id.clone()),
            public_node_addr: node.me().public_addr,
        };
        let replication_ctx = ReplicationCycleContext {
            repair_interval,
            group: group.clone(),
            network: node.network().clone(),
        };
        let task_service = replication::start_task_distributor_service::<S>(task_ctx).await;
        let repair_service = replication::start_replication_cycle(replication_ctx).await;

        tokio::spawn(watch_membership_changes(
            task_service.clone(),
            repair_service.clone(),
            node.handle(),
        ));

        node.add_rpc_service(ConsistencyService::new(group.clone(), node.network().clone()));
        node.add_rpc_service(ReplicationService::new(group.clone()));

        Ok(Self {
            node: node.handle(),
            group,
            statistics,
            task_service,
            repair_service,
        })
    }

    #[inline]
    /// Gets the live cluster statistics.
    pub fn statistics(&self) -> &SystemStatistics {
        &self.statistics
    }

    /// Creates a new handle to the underlying storage system.
    ///
    /// Changes applied to the handle are distributed across the cluster.
    pub fn handle(&self) -> ReplicatedStoreHandle<S> {
        ReplicatedStoreHandle {
            node: self.node.clone(),
            task_service: self.task_service.clone(),
            statistics: self.statistics.clone(),
            group: self.group.clone(),
        }
    }

    /// Creates a new handle to the underlying storage system with a preset keyspace.
    ///
    /// Changes applied to the handle are distributed across the cluster.
    pub fn handle_with_keyspace(
        &self,
        keyspace: impl Into<String>,
    ) -> ReplicatorKeyspaceHandle<S> {
        ReplicatorKeyspaceHandle {
            inner: self.handle(),
            keyspace: Cow::Owned(keyspace.into()),
        }
    }
}

impl<S> Drop for EventuallyConsistentStore<S>
where
    S: Storage + Send + Sync + 'static,
{
    fn drop(&mut self) {
        self.task_service.kill();
        self.repair_service.kill();
    }
}

/// A cheaply cloneable handle to control the data store.
pub struct ReplicatedStoreHandle<S>
where
    S: Storage + Send + Sync + 'static,
{
    node: DatacakeHandle,
    group: KeyspaceGroup<S>,
    task_service: TaskDistributor,
    statistics: SystemStatistics,
}

impl<S> Clone for ReplicatedStoreHandle<S>
where
    S: Storage + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            group: self.group.clone(),
            task_service: self.task_service.clone(),
            statistics: self.statistics.clone(),
        }
    }
}

impl<S> ReplicatedStoreHandle<S>
where
    S: Storage + Send + Sync + 'static,
{
    #[inline]
    /// Gets the live cluster statistics.
    pub fn statistics(&self) -> &SystemStatistics {
        &self.statistics
    }

    /// Creates a new handle to the underlying storage system with a preset keyspace.
    ///
    /// Changes applied to the handle are distributed across the cluster.
    pub fn with_keyspace(
        &self,
        keyspace: impl Into<String>,
    ) -> ReplicatorKeyspaceHandle<S> {
        ReplicatorKeyspaceHandle {
            inner: self.clone(),
            keyspace: Cow::Owned(keyspace.into()),
        }
    }

    /// Retrieves a document from the underlying storage.
    pub async fn get(
        &self,
        keyspace: &str,
        doc_id: Key,
    ) -> Result<Option<Document>, S::Error> {
        let storage = self.group.storage();
        storage.get(keyspace, doc_id).await
    }

    /// Retrieves a set of documents from the underlying storage.
    ///
    /// If a document does not exist with the given ID, it is simply not part
    /// of the returned iterator.
    pub async fn get_many<I, T>(
        &self,
        keyspace: &str,
        doc_ids: I,
    ) -> Result<S::DocsIter, S::Error>
    where
        T: Iterator<Item = Key> + Send,
        I: IntoIterator<IntoIter = T> + Send,
    {
        let storage = self.group.storage();
        storage.multi_get(keyspace, doc_ids.into_iter()).await
    }

    /// Insert or update a single document into the datastore.
    pub async fn put<D>(
        &self,
        keyspace: &str,
        doc_id: Key,
        data: D,
        consistency: Consistency,
    ) -> Result<(), error::StoreError<S::Error>>
    where
        D: Into<Vec<u8>>,
    {
        let nodes = self
            .node
            .select_nodes(consistency)
            .await
            .map_err(error::StoreError::ConsistencyError)?;

        let last_updated = self.node.clock().get_time().await;
        let document = Document::new(doc_id, last_updated, data);

        let keyspace = self.group.get_or_create_keyspace(keyspace).await;
        let msg = Set {
            source: CONSISTENCY_SOURCE_ID,
            doc: document.clone(),
            ctx: None,
            _marker: PhantomData::<S>::default(),
        };
        keyspace.send(msg).await?;

        // Register mutation with the distributor service.
        self.task_service.mutation(Mutation::Put {
            keyspace: Cow::Owned(keyspace.name().to_string()),
            doc: document.clone(),
        });

        let factory = |node| {
            let clock = self.node.clock().clone();
            let keyspace = keyspace.name().to_string();
            let document = document.clone();
            async move {
                let channel = self
                    .node
                    .network()
                    .get_or_connect(node)
                    .map_err(|e| error::StoreError::TransportError(node, e))?;

                let mut client = ConsistencyClient::<S>::new(clock, channel);

                client
                    .put(
                        keyspace,
                        document,
                        &self.node.me().node_id,
                        self.node.me().public_addr,
                    )
                    .await
                    .map_err(|e| error::StoreError::RpcError(node, e))?;

                Ok::<_, error::StoreError<S::Error>>(())
            }
        };

        handle_consistency_distribution::<S, _, _>(nodes, factory).await
    }

    /// Insert or update multiple documents into the datastore at once.
    pub async fn put_many<I, T, D>(
        &self,
        keyspace: &str,
        documents: I,
        consistency: Consistency,
    ) -> Result<(), error::StoreError<S::Error>>
    where
        D: Into<Vec<u8>>,
        T: Iterator<Item = (Key, D)> + Send,
        I: IntoIterator<IntoIter = T> + Send,
    {
        let nodes = self
            .node
            .select_nodes(consistency)
            .await
            .map_err(error::StoreError::ConsistencyError)?;

        let last_updated = self.node.clock().get_time().await;
        let docs = documents
            .into_iter()
            .map(|(id, data)| Document::new(id, last_updated, data))
            .collect::<Vec<_>>();

        let keyspace = self.group.get_or_create_keyspace(keyspace).await;
        let msg = MultiSet {
            source: CONSISTENCY_SOURCE_ID,
            docs: docs.clone(),
            ctx: None,
            _marker: PhantomData::<S>::default(),
        };
        keyspace.send(msg).await?;

        // Register mutation with the distributor service.
        self.task_service.mutation(Mutation::MultiPut {
            keyspace: Cow::Owned(keyspace.name().to_string()),
            docs: docs.clone(),
        });

        let factory = |node| {
            let clock = self.node.clock().clone();
            let keyspace = keyspace.name().to_string();
            let documents = docs.clone();
            let self_member = self.node.me().clone();
            async move {
                let channel = self
                    .node
                    .network()
                    .get_or_connect(node)
                    .map_err(|e| error::StoreError::TransportError(node, e))?;

                let mut client = ConsistencyClient::<S>::new(clock, channel);

                client
                    .multi_put(
                        keyspace,
                        documents.into_iter(),
                        &self_member.node_id,
                        self_member.public_addr,
                    )
                    .await
                    .map_err(|e| error::StoreError::RpcError(node, e))?;

                Ok::<_, error::StoreError<S::Error>>(())
            }
        };

        handle_consistency_distribution::<S, _, _>(nodes, factory).await
    }

    /// Delete a document from the datastore with a given doc ID.
    pub async fn del(
        &self,
        keyspace: &str,
        doc_id: Key,
        consistency: Consistency,
    ) -> Result<(), error::StoreError<S::Error>> {
        let nodes = self
            .node
            .select_nodes(consistency)
            .await
            .map_err(error::StoreError::ConsistencyError)?;

        let last_updated = self.node.clock().get_time().await;

        let keyspace = self.group.get_or_create_keyspace(keyspace).await;
        let doc = DocumentMetadata {
            id: doc_id,
            last_updated,
        };
        let msg = Del {
            source: CONSISTENCY_SOURCE_ID,
            doc,
            _marker: PhantomData::<S>::default(),
        };
        keyspace.send(msg).await?;

        // Register mutation with the distributor service.
        self.task_service.mutation(Mutation::Del {
            keyspace: Cow::Owned(keyspace.name().to_string()),
            doc,
        });

        let factory = |node| {
            let clock = self.node.clock().clone();
            let keyspace = keyspace.name().to_string();
            async move {
                let channel = self
                    .node
                    .network()
                    .get_or_connect(node)
                    .map_err(|e| error::StoreError::TransportError(node, e))?;

                let mut client = ConsistencyClient::<S>::new(clock, channel);

                client
                    .del(keyspace, doc_id, last_updated)
                    .await
                    .map_err(|e| error::StoreError::RpcError(node, e))?;

                Ok::<_, error::StoreError<S::Error>>(())
            }
        };

        handle_consistency_distribution::<S, _, _>(nodes, factory).await
    }

    /// Delete multiple documents from the datastore from the set of doc IDs.
    pub async fn del_many<I, T>(
        &self,
        keyspace: &str,
        doc_ids: I,
        consistency: Consistency,
    ) -> Result<(), error::StoreError<S::Error>>
    where
        T: Iterator<Item = Key> + Send,
        I: IntoIterator<IntoIter = T> + Send,
    {
        let nodes = self
            .node
            .select_nodes(consistency)
            .await
            .map_err(error::StoreError::ConsistencyError)?;

        let last_updated = self.node.clock().get_time().await;
        let docs = doc_ids
            .into_iter()
            .map(|id|  {
                DocumentMetadata {
                    id,
                    last_updated,
                }
            })
            .collect::<Vec<_>>();

        let keyspace = self.group.get_or_create_keyspace(keyspace).await;
        let msg = MultiDel {
            source: CONSISTENCY_SOURCE_ID,
            docs: docs.clone(),
            _marker: PhantomData::<S>::default(),
        };
        keyspace.send(msg).await?;

        // Register mutation with the distributor service.
        self.task_service.mutation(Mutation::MultiDel {
            keyspace: Cow::Owned(keyspace.name().to_string()),
            docs: docs.clone(),
        });

        let factory = |node| {
            let clock = self.node.clock().clone();
            let keyspace = keyspace.name().to_string();
            let docs = docs.clone();
            async move {
                let channel = self
                    .node
                    .network()
                    .get_or_connect(node)
                    .map_err(|e| error::StoreError::TransportError(node, e))?;

                let mut client = ConsistencyClient::<S>::new(clock, channel);

                client
                    .multi_del(keyspace, docs)
                    .await
                    .map_err(|e| error::StoreError::RpcError(node, e))?;

                Ok::<_, error::StoreError<S::Error>>(())
            }
        };

        handle_consistency_distribution::<S, _, _>(nodes, factory).await
    }
}

/// A convenience wrapper which creates a new handle with a preset keyspace.
pub struct ReplicatorKeyspaceHandle<S>
where
    S: Storage + Send + Sync + 'static,
{
    inner: ReplicatedStoreHandle<S>,
    keyspace: Cow<'static, str>,
}

impl<S> Clone for ReplicatorKeyspaceHandle<S>
where
    S: Storage + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            keyspace: self.keyspace.clone(),
        }
    }
}

impl<S> ReplicatorKeyspaceHandle<S>
where
    S: Storage + Send + Sync + 'static,
{
    /// Retrieves a document from the underlying storage.
    pub async fn get(&self, doc_id: Key) -> Result<Option<Document>, S::Error> {
        self.inner.get(self.keyspace.as_ref(), doc_id).await
    }

    /// Retrieves a set of documents from the underlying storage.
    ///
    /// If a document does not exist with the given ID, it is simply not part
    /// of the returned iterator.
    pub async fn get_many<I, T>(&self, doc_ids: I) -> Result<S::DocsIter, S::Error>
    where
        T: Iterator<Item = Key> + Send,
        I: IntoIterator<IntoIter = T> + Send,
    {
        self.inner.get_many(self.keyspace.as_ref(), doc_ids).await
    }

    /// Insert or update a single document into the datastore.
    pub async fn put(
        &self,
        doc_id: Key,
        data: Vec<u8>,
        consistency: Consistency,
    ) -> Result<(), error::StoreError<S::Error>> {
        self.inner
            .put(self.keyspace.as_ref(), doc_id, data, consistency)
            .await
    }

    /// Insert or update multiple documents into the datastore at once.
    pub async fn put_many<I, T>(
        &self,
        documents: I,
        consistency: Consistency,
    ) -> Result<(), error::StoreError<S::Error>>
    where
        T: Iterator<Item = (Key, Vec<u8>)> + Send,
        I: IntoIterator<IntoIter = T> + Send,
    {
        self.inner
            .put_many(self.keyspace.as_ref(), documents, consistency)
            .await
    }

    /// Delete a document from the datastore with a given doc ID.
    pub async fn del(
        &self,
        doc_id: Key,
        consistency: Consistency,
    ) -> Result<(), error::StoreError<S::Error>> {
        self.inner
            .del(self.keyspace.as_ref(), doc_id, consistency)
            .await
    }

    /// Delete multiple documents from the datastore from the set of doc IDs.
    pub async fn del_many<I, T>(
        &self,
        doc_ids: I,
        consistency: Consistency,
    ) -> Result<(), error::StoreError<S::Error>>
    where
        T: Iterator<Item = Key> + Send,
        I: IntoIterator<IntoIter = T> + Send,
    {
        self.inner
            .del_many(self.keyspace.as_ref(), doc_ids, consistency)
            .await
    }
}

/// Watches for changes in the cluster membership.
///
/// When nodes leave and join, pollers are stopped and started as required.
async fn watch_membership_changes(
    task_service: TaskDistributor,
    repair_service: ReplicationHandle,
    node_handle: DatacakeHandle,
) {
    let mut changes = node_handle.membership_changes();
    while let Some(members) = changes.next().await {
        task_service.membership_change(members.clone());
        repair_service.membership_change(members.clone());
    }
}

async fn handle_consistency_distribution<S, CB, F>(
    nodes: Vec<SocketAddr>,
    factory: CB,
) -> Result<(), error::StoreError<S::Error>>
where
    S: Storage,
    CB: FnMut(SocketAddr) -> F,
    F: Future<Output = Result<(), error::StoreError<S::Error>>>,
{
    let mut num_success = 0;
    let num_required = nodes.len();

    let mut requests = nodes
        .into_iter()
        .map(factory)
        .collect::<FuturesUnordered<_>>();

    while let Some(res) = requests.next().await {
        match res {
            Ok(()) => {
                num_success += 1;
            },
            Err(error::StoreError::RpcError(node, error)) => {
                error!(
                    error = ?error,
                    target_node = %node,
                    "Replica failed to acknowledge change to meet consistency level requirement."
                );
            },
            Err(error::StoreError::TransportError(node, error)) => {
                error!(
                    error = ?error,
                    target_node = %node,
                    "Replica failed to acknowledge change to meet consistency level requirement."
                );
            },
            Err(other) => {
                error!(
                    error = ?other,
                    "Failed to send action to replica due to unknown error.",
                );
            },
        }
    }

    if num_success != num_required {
        Err(error::StoreError::ConsistencyError(
            ConsistencyError::ConsistencyFailure {
                responses: num_success,
                required: num_required,
                timeout: TIMEOUT,
            },
        ))
    } else {
        Ok(())
    }
}
