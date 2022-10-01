use std::fmt::{Debug, Display};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Semaphore;
use tokio::time::{interval, timeout, MissedTickBehavior};

const CHANGES_POLLING_DURATION: Duration = Duration::from_secs(1);

#[cfg(feature = "test-utils")]
const TOMBSTONE_PURGE_DELAY: Duration = Duration::from_secs(1);
#[cfg(not(feature = "test-utils"))]
const TOMBSTONE_PURGE_DELAY: Duration = Duration::from_secs(600);

use crate::node::ClusterMember;
use crate::rpc::{Client, RpcError};
use crate::shard::{ShardGroupHandle, StateChangeTs};
use crate::{ClientCluster, DatacakeError, DataHandler, NUMBER_OF_SHARDS};

/// A background task for the node which purges any documents marked as tombstones
/// once it is safe to completely remove any trace of them.
pub(crate) async fn tombstone_purge_task<E>(
    node_id: String,
    shards: ShardGroupHandle,
    handler: Arc<dyn DataHandler<Error = E>>,
)
where
    E: Display + Debug + Send + Sync + 'static
{
    let mut interval = interval(TOMBSTONE_PURGE_DELAY);

    loop {
        interval.tick().await;

        let mut keys = vec![];
        for shard_id in 0..NUMBER_OF_SHARDS {
            let purged = match shards.purge(shard_id).await {
                Ok(purged) => purged,
                Err(e) => {
                    error!(node_id = %node_id, error = ?e, "Failed to purge shard tombstones.");
                    continue;
                },
            };

            keys.extend(purged);
        }

        if keys.is_empty() {
            continue;
        }

        let num_cleared = keys.len();
        let start = Instant::now();

        if let Err(e) = handler.clear_tombstone_documents(keys).await {
            error!(node_id = %node_id, error = ?e, "Data handler failed to clear tombstone documents.");
        }

        info!(
            node_id = %node_id,
            num_cleared = num_cleared,
            time_taken = ?start.elapsed(),
            "Purged tombstones from store."
        );
    }
}

/// A polling task that check the remote node's shard changes
/// every given period of time.
///
/// If the connection to the remote node fails, this task ends as it assumes the node
/// is dead and this task will be restarted once it reconnects.
pub(crate) async fn shard_state_poller_task<E>(member: ClusterMember, node: NodeInfo<E>)
where
    E: Display + Debug + Send + Sync + 'static
{
    let mut interval = interval(CHANGES_POLLING_DURATION);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let stop = Arc::new(AtomicBool::new(false));
    let mut previous_state = vec![0; NUMBER_OF_SHARDS];
    while !stop.load(Ordering::Relaxed) {
        interval.tick().await;

        let fut = timeout(Duration::from_secs(1), node.rpc.sync.get_shard_changes());

        let state = match fut.await {
            Err(_) => {
                warn!(
                    target_node_id = %member.node_id,
                    "Node timeout exceeded while requesting shard changes. \
                    This could mean the node is down or behind.",
                );
                continue;
            },
            Ok(Err(e)) => {
                if matches!(e, RpcError::Disconnected) {
                    warn!(target_node_id = %member.node_id, "Node has lost connection to remote.");
                    node.client_cluster.disconnect_node(&member.node_id);
                    break;
                }

                error!(target_node_id = %member.node_id, error = %e, "Poller shutting down for node.");
                break;
            },

            Ok(Ok(state)) => state,
        };

        handle_node_state_change(
            &member,
            state,
            &mut previous_state,
            node.clone(),
            stop.clone(),
        )
        .await;
    }
}

pub(crate) struct NodeInfo<E>
where
    E: Display + Debug + Send + 'static
{
    pub(crate) rpc: Client,
    pub(crate) client_cluster: ClientCluster,
    pub(crate) data_handler: Arc<dyn DataHandler<Error = E>>,
    pub(crate) shard_group: ShardGroupHandle,
}

impl<E> Clone for NodeInfo<E>
where
    E: Display + Debug + Send + 'static
{
    fn clone(&self) -> Self {
        Self {
            rpc: self.rpc.clone(),
            client_cluster: self.client_cluster.clone(),
            data_handler: self.data_handler.clone(),
            shard_group: self.shard_group.clone(),
        }
    }
}

/// Calculated what shards have changed for a given node's state
/// and spawns a handler task for each shard that has changed.
///
/// In the case that the shard's change timestamp is `0` (Initial startup state)
/// then the shard is always marked as changed and follows the synchronisation
/// process, regardless of if the local node's state is aligned already.
async fn handle_node_state_change<E>(
    member: &ClusterMember,
    new_state: Vec<StateChangeTs>,
    previous_state: &mut [StateChangeTs],
    node: NodeInfo<E>,
    stop: Arc<AtomicBool>,
)
where
    E: Display + Debug + Send + 'static
{
    let completed = spawn_handlers(member, new_state, previous_state, node, stop).await;

    while let Ok((shard_id, aligned_ts)) = completed.recv_async().await {
        previous_state[shard_id] = aligned_ts;
    }
}

async fn spawn_handlers<E>(
    member: &ClusterMember,
    new_state: Vec<StateChangeTs>,
    previous_state: &mut [StateChangeTs],
    node: NodeInfo<E>,
    stop: Arc<AtomicBool>,
) -> flume::Receiver<(usize, StateChangeTs)>
where
    E: Display + Debug + Send + 'static
{
    let shard_changes = new_state.iter().zip(previous_state.iter()).enumerate();

    let (tx, rx) = flume::bounded(2);
    let concurrency_limiter = Arc::new(Semaphore::new(2));
    for (shard_id, (&new, &old)) in shard_changes {
        // If the shard state hasn't changed don't bother trying to sync it.
        // `0` is reserved just for initial states. If a state is `0` then we must
        // request a re-sync to make sure we haven't missed updates.
        if new == old && !(new == 0 || old == 0) {
            continue;
        }

        debug!(
            target_node_id = %member.node_id,
            shard_id = shard_id,
            "Shard is behind for remote node. Fetching updates.",
        );

        let tx = tx.clone();
        let concurrency_limiter = concurrency_limiter.clone();
        let node_id = member.node_id.clone();
        let node = node.clone();
        let stop = stop.clone();

        tokio::spawn(async move {
            if stop.load(Ordering::Relaxed) {
                return;
            }

            let _permit = concurrency_limiter.acquire().await;

            let fut = handle_shard_change(&node_id, shard_id, node.clone());

            let err = match fut.await {
                Err(e) => e,
                Ok(()) => {
                    let _ = tx.send_async((shard_id, new)).await;
                    return;
                },
            };

            match err {
                DatacakeError::DatastoreError(e) => {
                    error!(
                        node_id = %node_id,
                        target_shard_id = %shard_id,
                        error = ?e,
                        "Failed to handle shard state change due to an error occurring within the datastore.",
                    );
                },
                DatacakeError::RpcError(ref e) if matches!(e, RpcError::Disconnected) => {
                    warn!(node_id = %node_id, "Node has lost connection to remote.");
                    stop.store(true, Ordering::Relaxed);
                    node.client_cluster.disconnect_node(&node_id);
                },
                e => {
                    error!(
                        node_id = %node_id,
                        target_shard_id = %shard_id,
                        error = ?e,
                        "Failed to handle shard state changes due to an RPC error.",
                    );
                }
            }
        });
    }

    rx
}

/// Handles a given node's state shard changing.
///
/// * This works by first getting the shard's doc set which can then have the
///   deterministic difference calculated between the two sets.
///
/// * The node spawns a task to mark the deleted documents as tombstones.
///
/// * The node fetches and streams the updated/inserted documents from the remote node,
///   feeding them into the local node's data handler.
///
/// * The remote set is merged into the current set and any observed deletes are purged
///   from the set.
///
/// * Purged deletes are then cleared completely including removing the tombstone markers
///   for that given document.
async fn handle_shard_change<E>(
    node_id: &str,
    shard_id: usize,
    node: NodeInfo<E>,
) -> Result<(), DatacakeError<E>>
where
    E: Display + Debug + Send + 'static
{
    let state = node.rpc.sync.get_doc_set(shard_id).await?;

    let (updated, removed) = node.shard_group.diff(shard_id, state.clone()).await?;

    if updated.is_empty() && removed.is_empty() {
        return Ok(());
    }

    let num_updates = updated.len();
    let num_removed = removed.len();

    let start = Instant::now();
    let handler = node.data_handler.clone();
    let delete_task = tokio::spawn(async move {
        if removed.is_empty() {
            Ok(())
        } else {
            handler.mark_tombstone_documents(removed).await
        }
    });

    if !updated.is_empty() {
        let mut stream = node
            .rpc
            .sync
            .fetch_docs(updated.iter().map(|v| v.0).collect())
            .await?;

        while let Some(docs) = stream.next().await {
            node.data_handler
                .upsert_documents(Vec::from_iter(docs?))
                .await?;
        }
    }

    delete_task.await.expect("Join background task.")?;
    debug!(
        target_node_id = %node_id,
        target_shard_id = %shard_id,
        num_updated = num_updates,
        num_removed = num_removed,
        processing_time = ?start.elapsed(),
        "Deleted documents and updates synchronised successfully.",
    );

    node.shard_group.merge(shard_id, state).await?;

    Ok(())
}
