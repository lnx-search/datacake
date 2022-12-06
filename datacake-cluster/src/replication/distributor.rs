use std::borrow::Cow;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crossbeam_channel::{unbounded, Receiver, Sender};
use datacake_crdt::{HLCTimestamp, Key, StateChanges};
use tokio::sync::Semaphore;
use tokio::time::{interval, MissedTickBehavior};

use crate::replication::{MembershipChanges, MAX_CONCURRENT_REQUESTS};
use crate::rpc::datacake_api;
use crate::rpc::datacake_api::Context;
use crate::{Clock, ConsistencyClient, Document, RpcNetwork};

const BATCHING_INTERVAL: Duration = Duration::from_secs(1);

pub struct TaskServiceContext {
    /// The global cluster clock.
    pub(crate) clock: Clock,
    /// The network handle which contains all RPC connections.
    pub(crate) network: RpcNetwork,
    /// The unique ID of the node running.
    pub(crate) local_node_id: Cow<'static, str>,
    /// The public RPC address of the node running.
    pub(crate) public_node_addr: SocketAddr,
}

#[derive(Clone)]
/// A handle to the task distributor service.
///
/// This handle is cheap to clone.
pub(crate) struct TaskDistributor {
    tx: Sender<Op>,
    kill_switch: Arc<AtomicBool>,
}

impl TaskDistributor {
    /// Marks that the cluster has had a membership change.
    pub(crate) fn membership_change(&self, changes: MembershipChanges) {
        let _ = self.tx.send(Op::MembershipChange(changes));
    }

    /// Marks that the cluster has mutated some data.
    pub(crate) fn mutation(&self, mutation: Mutation) {
        let _ = self.tx.send(Op::Mutation(mutation));
    }

    /// Kills the distributor service.
    pub(crate) fn kill(&self) {
        self.kill_switch.store(true, Ordering::Relaxed);
    }
}

/// A enqueued event/operation for the distributor to handle next tick.
enum Op {
    MembershipChange(MembershipChanges),
    Mutation(Mutation),
}

/// Represents an operation on the store, mutating the data.
pub enum Mutation {
    Put {
        keyspace: Cow<'static, str>,
        doc: Document,
    },
    MultiPut {
        keyspace: Cow<'static, str>,
        docs: Vec<Document>,
    },
    Del {
        keyspace: Cow<'static, str>,
        doc_id: Key,
        ts: HLCTimestamp,
    },
    MultiDel {
        keyspace: Cow<'static, str>,
        docs: StateChanges,
    },
}

/// Starts the task distributor service.
///
/// The distributor service is responsible for batching mutation requests
/// which are not part of the node broadcast when mutating data.
///
/// This service will send the events to the remaining nodes in a single batch.
pub(crate) async fn start_task_distributor_service(
    ctx: TaskServiceContext,
) -> TaskDistributor {
    let kill_switch = Arc::new(AtomicBool::new(false));
    let (tx, rx) = unbounded();

    tokio::spawn(task_distributor_service(ctx, rx, kill_switch.clone()));

    TaskDistributor { tx, kill_switch }
}

async fn task_distributor_service(
    ctx: TaskServiceContext,
    rx: Receiver<Op>,
    kill_switch: Arc<AtomicBool>,
) {
    info!("Task distributor service is running.");

    let mut live_members = BTreeMap::new();
    let mut interval = interval(BATCHING_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        interval.tick().await;

        if kill_switch.load(Ordering::Relaxed) {
            break;
        }

        let mut put_payloads = BTreeMap::new();
        let mut del_payloads = BTreeMap::new();
        while let Ok(task) = rx.try_recv() {
            match task {
                Op::MembershipChange(changes) => {
                    for node_id in changes.left {
                        live_members.remove(node_id.as_ref());
                    }

                    for (node_id, addr) in changes.joined {
                        live_members.insert(node_id, addr);
                    }
                },
                Op::Mutation(mutation) => {
                    register_mutation(&mut put_payloads, &mut del_payloads, mutation);
                },
            }
        }

        if !put_payloads.is_empty() || !del_payloads.is_empty() {
            let ts = ctx.clock.get_time().await;
            let batch = datacake_api::BatchPayload {
                timestamp: Some(ts.into()),
                modified: put_payloads
                    .into_iter()
                    .map(|(keyspace, payloads)| datacake_api::MultiPutPayload {
                        keyspace: keyspace.to_string(),
                        ctx: Some(Context {
                            node_id: ctx.local_node_id.to_string(),
                            node_addr: ctx.public_node_addr.to_string(),
                        }),
                        documents: payloads,
                    })
                    .collect(),
                removed: del_payloads
                    .into_iter()
                    .map(|(keyspace, payloads)| datacake_api::MultiRemovePayload {
                        keyspace: keyspace.to_string(),
                        documents: payloads,
                    })
                    .collect(),
            };

            if let Err(e) = execute_batch(&ctx, &live_members, batch).await {
                error!(error = ?e, "Failed to execute synchronisation batch.");
            }
        }
    }
}

fn register_mutation(
    put_payloads: &mut BTreeMap<Cow<'static, str>, Vec<datacake_api::Document>>,
    del_payloads: &mut BTreeMap<Cow<'static, str>, Vec<datacake_api::DocumentMetadata>>,
    mutation: Mutation,
) {
    match mutation {
        Mutation::Put { keyspace, doc } => {
            put_payloads.entry(keyspace).or_default().push(doc.into());
        },
        Mutation::MultiPut { keyspace, docs } => {
            put_payloads
                .entry(keyspace)
                .or_default()
                .extend(docs.into_iter().map(datacake_api::Document::from));
        },
        Mutation::Del {
            keyspace,
            doc_id,
            ts,
        } => {
            del_payloads.entry(keyspace).or_default().push(
                datacake_api::DocumentMetadata {
                    id: doc_id,
                    last_updated: Some(ts.into()),
                },
            );
        },
        Mutation::MultiDel { keyspace, docs } => {
            let iter =
                docs.into_iter()
                    .map(|(doc_id, ts)| datacake_api::DocumentMetadata {
                        id: doc_id,
                        last_updated: Some(ts.into()),
                    });

            del_payloads.entry(keyspace).or_default().extend(iter);
        },
    }
}

async fn execute_batch(
    ctx: &TaskServiceContext,
    live_members: &BTreeMap<Cow<'static, str>, SocketAddr>,
    batch: datacake_api::BatchPayload,
) -> anyhow::Result<()> {
    let limiter = Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS));
    let mut tasks = Vec::with_capacity(live_members.len());
    for (node_id, &addr) in live_members {
        let node_id = node_id.clone();
        let limiter = limiter.clone();
        let batch = batch.clone();
        let channel = ctx.network.get_or_connect_lazy(addr);
        let mut client = ConsistencyClient::new(ctx.clock.clone(), channel);

        let task = tokio::spawn(async move {
            let _permit = limiter.acquire().await;
            let resp = client.apply_batch(batch).await;
            (node_id, addr, resp)
        });
        tasks.push(task);
    }

    for task in tasks {
        let (node_id, addr, res) = task.await.expect("Join task.");
        if let Err(e) = res {
            error!(
                error = ?e,
                target_node_id = %node_id,
                target_addr = %addr,
                "Failed to synchronise node with batch events. This will resolved when the next replication cycle occurs.",
            );
        }
    }

    Ok(())
}
