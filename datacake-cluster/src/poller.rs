use std::borrow::Cow;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use datacake_crdt::StateChanges;
use tokio::task::JoinHandle;
use tokio::time::{interval, Interval};
use tonic::transport::Channel;

use crate::keyspace::{CounterKey, KeyspaceGroup, KeyspaceState, KeyspaceTimestamps, ReplicationSource, StateSource};
use crate::rpc::ReplicationClient;
use crate::Storage;

const KEYSPACE_SYNC_TIMEOUT: Duration = Duration::from_micros(10);
const MAX_NUMBER_OF_DOCS_PER_FETCH: usize = 100_000;

/// A actor responsible for continuously polling a node's keyspace state
/// and triggering the required callbacks.
pub(crate) struct NodePollerState<S>
where
    S: Storage + Send + Sync + 'static,
{
    /// The ID of the node being polled.
    target_node_id: Cow<'static, str>,

    /// The target node's RPC address.
    target_rpc_addr: SocketAddr,

    /// The keyspace group for the current node.
    group: KeyspaceGroup<S>,

    /// The last recorded keyspace timestamps.
    ///
    /// This is used to work out the difference between the old and new states.
    last_keyspace_timestamps: KeyspaceTimestamps,

    /// The remote node's RPC channel.
    channel: Channel,

    /// A shutdown signal to tell the actor to shut down.
    shutdown: Arc<AtomicBool>,

    /// The time interval to elapse between polling the state.
    interval: Interval,

    /// A map of keyspace names to their relative poll handle.
    ///
    /// If a handle exists and is not done, this means the keyspace
    /// is currently already in the process of being synchronised.
    handles: HashMap<Cow<'static, str>, KeyspacePollHandle>,
}

impl<S> NodePollerState<S>
where
    S: Storage + Send + Sync + 'static,
{
    /// Creates a new poller state.
    pub(crate) fn new(
        target_node_id: Cow<'static, str>,
        target_rpc_addr: SocketAddr,
        keyspace_group: KeyspaceGroup<S>,
        rpc_channel: Channel,
        interval_duration: Duration,
    ) -> Self {
        Self {
            target_node_id,
            target_rpc_addr,
            group: keyspace_group,
            last_keyspace_timestamps: Default::default(),
            channel: rpc_channel,
            shutdown: Arc::new(AtomicBool::new(false)),
            interval: interval(interval_duration),
            handles: Default::default(),
        }
    }

    /// Creates a new handle to the poller state's shutdown flag.
    pub(crate) fn shutdown_handle(&self) -> ShutdownHandle {
        ShutdownHandle(self.shutdown.clone())
    }

    /// Checks if the poller should shutdown or not, then waits until the given
    /// polling interval has elapsed, where the shutdown flag is re-checked.
    ///
    /// Returns if the poller should exit or not.
    async fn tick(&mut self) -> bool {
        if self.shutdown.load(Ordering::Relaxed) {
            return true;
        }

        self.interval.tick().await;

        self.shutdown.load(Ordering::Relaxed)
    }

    /// Creates the state that a given synchronisation task requires in order
    /// to operate.
    ///
    /// This is effectively a copy of the main state but with some adjustments.
    fn create_task_state(&self, keyspace: Cow<'static, str>) -> TaskState<S> {
        TaskState {
            target_node_id: self.target_node_id.clone(),
            target_rpc_addr: self.target_rpc_addr,
            group: self.group.clone(),
            keyspace: keyspace.clone(),
            timestamp: self
                .last_keyspace_timestamps
                .get(&CounterKey(keyspace))
                .cloned()
                .unwrap_or_default(),
            done: Arc::new(AtomicBool::new(false)),
            client: ReplicationClient::from(self.channel.clone()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ShutdownHandle(Arc<AtomicBool>);
impl ShutdownHandle {
    /// Kill's the poller which this handle belongs to.
    pub fn kill(&self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

struct TaskState<S>
where
    S: Storage + Send + Sync + 'static,
{
    /// The ID of the node being polled.
    target_node_id: Cow<'static, str>,

    /// The target node's RPC address.
    target_rpc_addr: SocketAddr,

    /// The keyspace group for the current node.
    group: KeyspaceGroup<S>,

    /// The target keyspace.
    keyspace: Cow<'static, str>,

    /// The shared timestamp counter for the given keyspace.
    timestamp: Arc<AtomicU64>,

    /// A flag signalling if the task is complete or not.
    done: Arc<AtomicBool>,

    /// The client the task can use for RPC.
    client: ReplicationClient,
}

impl<S> TaskState<S>
where
    S: Storage + Send + Sync + 'static,
{
    /// Set's the keyspace last updated timestamp and marks
    /// itself as completed.
    fn set_done(&self, ts: u64) {
        self.timestamp.store(ts, Ordering::SeqCst);
        self.done.store(true, Ordering::SeqCst);
    }
}

#[instrument(name = "node-poller", skip_all)]
/// A polling task which continuously checks if the remote node's
/// state has changed.
///
/// If the state has changed it spawns handlers to synchronise the keyspace
/// with the new changes.
pub(crate) async fn node_poller<S>(mut state: NodePollerState<S>)
where
    S: Storage + Send + Sync + 'static,
{
    info!(
        target_node_id = %state.target_node_id,
        target_rpc_addr = %state.target_rpc_addr,
        "Polling for state changes..."
    );

    let mut client = ReplicationClient::from(state.channel.clone());

    loop {
        if state.tick().await {
            break;
        }

        if let Err(e) = poll_node(&mut state, &mut client).await {
            error!(
                error = ?e,
                target_node_id = %state.target_node_id,
                target_rpc_addr = %state.target_rpc_addr,
                "Unable to poll node due to error.",
            );
        }
    }

    info!(
        target_node_id = %state.target_node_id,
        target_rpc_addr = %state.target_rpc_addr,
        "Poller has received shutdown signal, Closing."
    );
}

/// Polls the remote node's keyspace timestamps.
///
/// If any timestamps are different to when the node was last polled, a task is created
/// for each keyspace which has changed.
///
/// If a task already exists for a given keyspace, it is checked to see if the task is complete
/// or not, if the task is complete then a new task is created, otherwise, if the task is *not*
/// complete but has taken longer than the allowed timeout period, the existing task is cancelled
/// and restarted.
async fn poll_node<S>(
    state: &mut NodePollerState<S>,
    client: &mut ReplicationClient,
) -> Result<(), anyhow::Error>
where
    S: Storage + Send + Sync + 'static,
{
    let keyspace_timestamps = client.poll_keyspace().await?;
    let diff = keyspace_timestamps.diff(&state.last_keyspace_timestamps);

    for keyspace in diff {
        if let Some(handle) = state.handles.remove(&keyspace) {
            if handle.is_timeout() {
                handle.handle.abort();
                continue;
            }

            if !handle.is_done() {
                state.handles.insert(keyspace.clone(), handle);
                continue;
            }
        }

        let mut task_state = state.create_task_state(keyspace.clone());
        let done = task_state.done.clone();
        let inner = tokio::spawn(async move {
            let start = Instant::now();

            match begin_keyspace_sync(&mut task_state).await {
                Err(e) => {
                    error!(
                        error = ?e,
                        keyspace = %task_state.keyspace,
                        target_node_id = %task_state.target_node_id,
                        target_rpc_addr = %task_state.target_rpc_addr,
                        "Failed to synchronise keyspace due to error.",
                    );
                },
                Ok(ts) => {
                    debug!(
                        elapsed = ?start.elapsed(),
                        keyspace = %task_state.keyspace,
                        target_node_id = %task_state.target_node_id,
                        target_rpc_addr = %task_state.target_rpc_addr,
                        "Synchronisation complete.",
                    );
                    task_state.set_done(ts);
                },
            }
        });

        state
            .handles
            .insert(keyspace, KeyspacePollHandle::new(inner, done));
    }

    Ok(())
}

/// Starts the synchronisation process of syncing the remote node's keyspace
/// to the current node's keyspace.
///
/// The system begins by requesting the keyspace CRDT and gets the diff between
/// the current CRDT and the remote CRDT.
async fn begin_keyspace_sync<S>(state: &mut TaskState<S>) -> Result<u64, anyhow::Error>
where
    S: Storage + Send + Sync + 'static,
{
    let keyspace = state.group.get_or_create_keyspace(&state.keyspace).await;
    let (last_updated, set) = state.client.get_state(keyspace.name()).await?;

    let (modified, removed) = keyspace.diff(set).await;

    // The removal task can operate interdependently of the modified handler.
    // If, in the process of handling removals, the modified handler errors,
    // we simply let the removal task continue on as normal.
    let removal_task = tokio::spawn(handle_removals::<ReplicationSource, _>(keyspace.clone(), removed));

    let res = handle_modified(&mut state.client, keyspace, modified).await;
    removal_task.await.expect("join task")?;

    res?;

    Ok(last_updated)
}

/// Fetches all the documents which have changed since the last state fetch.
///
/// These documents are then persisted and the metadata marked accordingly.
async fn handle_modified<S>(
    client: &mut ReplicationClient,
    keyspace: KeyspaceState<S>,
    modified: StateChanges,
) -> Result<(), anyhow::Error>
where
    S: Storage + Send + Sync + 'static,
{
    let doc_id_chunks = modified
        .chunks(MAX_NUMBER_OF_DOCS_PER_FETCH)
        .map(|entries| entries.iter().map(|(k, _)| *k).collect::<Vec<_>>());

    let storage = keyspace.storage();
    for doc_ids in doc_id_chunks {
        let mut doc_timestamps = Vec::new();
        let docs = client
            .fetch_docs(keyspace.name(), doc_ids)
            .await?
            .map(|doc| {
                doc_timestamps.push((doc.id, doc.last_updated));
                doc
            });

        storage.multi_put(keyspace.name(), docs).await?;

        // We only update the metadata if the persistence has passed.
        // If we were to do this the other way around, we could potentially
        // end up being in a situation where the state *thinks* it's up to date
        // but in reality it's not.
        keyspace.multi_put::<ReplicationSource>(doc_timestamps).await?;
    }

    Ok(())
}

/// Removes the marked documents from the persisted storage and then
/// marks the document metadata as a tombstone.
///
/// This does not remove the metadata of the document entirely, instead the document is marked
/// as deleted along with the main data itself, but we keep a history of the deletes we've made.
async fn handle_removals<SS, S>(
    keyspace: KeyspaceState<S>,
    mut removed: StateChanges,
) -> Result<(), S::Error>
where
    SS: StateSource + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    let storage = keyspace.storage();

    if removed.len() == 1 {
        let (key, ts) = removed.pop().expect("get element");

        storage.del(keyspace.name(), key).await?;
        keyspace.del::<SS>(key, ts).await?;
        return Ok::<_, S::Error>(());
    }

    storage
        .multi_del(keyspace.name(), removed.iter().map(|(k, _)| *k))
        .await?;

    keyspace.multi_del::<SS>(removed).await
}

pub struct KeyspacePollHandle {
    handle: JoinHandle<()>,
    done: Arc<AtomicBool>,
    started_at: Instant,
}

impl KeyspacePollHandle {
    fn new(handle: JoinHandle<()>, done: Arc<AtomicBool>) -> Self {
        Self {
            handle,
            done,
            started_at: Instant::now(),
        }
    }

    fn is_done(&self) -> bool {
        self.done.load(Ordering::Relaxed)
    }

    fn is_timeout(&self) -> bool {
        self.started_at.elapsed() >= KEYSPACE_SYNC_TIMEOUT
    }
}
