use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use datacake_crdt::{HLCTimestamp, Key, OrSWotSet, StateChanges};
use futures::StreamExt;
use tokio::sync::oneshot;
use tonic::transport::{Channel, Endpoint, Uri};
use tonic::{Status, Streaming};

use super::cluster_rpc_models::document_sync_client::DocumentSyncClient;
use super::cluster_rpc_models::general_rpc_client::GeneralRpcClient;
use super::{DocsBlock, RpcError};
use crate::rpc::cluster_rpc_models::{
    Blank,
    DataFetchRequest,
    DataFetchResponse,
    DeletePayload,
    ShardState,
    SyncRequest,
    SyncResponse,
    Timestamp,
    UpsertPayload,
};
use crate::shard::StateChangeTs;

/// Connects to a remote RPC server.
pub async fn connect_client(
    public_addr: SocketAddr,
) -> Result<(SynchronisationClient, DataClient), tonic::transport::Error> {
    let addr = format!("http://{}", public_addr)
        .parse::<Uri>()
        .expect("Parse public address.");
    let channel = Endpoint::from(addr).connect().await?;

    let sync_client = SynchronisationClient::new(channel.clone());
    let data_client = DataClient::new(channel);

    Ok((sync_client, data_client))
}

#[derive(Clone)]
/// The synchronisation rpc client which manages distributing the
/// document states across nodes.
///
/// This just manages aligning the state rather than managing event
/// distribution for things like updates and deletes with varying
/// consistency levels.
pub struct SynchronisationClient {
    tx: flume::Sender<SynchronisationEvent>,
}

impl SynchronisationClient {
    /// Creates a new [SynchronisationClient] which itself is a wrapper over
    /// an actor that handles each event sequentially.
    ///
    /// This assumes that it is being created within a tokio runtime context.
    pub fn new(channel: Channel) -> Self {
        let (tx, rx) = flume::bounded(10);

        let actor = SynchronisationClientActor {
            rx,
            inner: DocumentSyncClient::new(channel),
        };

        tokio::spawn(actor.run_actor());

        Self { tx }
    }

    /// Requests the node's current shard changes state.
    pub async fn get_shard_changes(&self) -> Result<Vec<StateChangeTs>, RpcError> {
        let (tx, resp) = oneshot::channel();

        self.tx
            .send_async(SynchronisationEvent::GetShardStates { tx })
            .await
            .map_err(|_| RpcError::DeadActor)?;

        let set = resp.await.map_err(|_| RpcError::DeadActor)??;
        Ok(set.shards)
    }

    /// Requests the node's doc set for the given shard.
    pub async fn get_doc_set(&self, shard_id: usize) -> Result<OrSWotSet, RpcError> {
        let (tx, resp) = oneshot::channel();

        self.tx
            .send_async(SynchronisationEvent::Sync {
                shard_id: shard_id as u32,
                tx,
            })
            .await
            .map_err(|_| RpcError::DeadActor)?;

        let set = resp.await.map_err(|_| RpcError::DeadActor)??;

        crate::shared::decompress_set(&set.doc_set, set.uncompressed_size as usize)
            .await
            .map_err(|_| RpcError::DeserializeError)
    }

    /// Fetches a set of documents.
    ///
    /// The returned set of documents are returned as a stream of block iterators.
    pub async fn fetch_docs(&self, docs: Vec<Key>) -> Result<DocFetchStream, RpcError> {
        let (tx, resp) = oneshot::channel();

        self.tx
            .send_async(SynchronisationEvent::FetchDocs { docs, tx })
            .await
            .map_err(|_| RpcError::DeadActor)?;

        resp.await
            .map_err(|_| RpcError::DeadActor)?
            .map_err(RpcError::from)
            .map(DocFetchStream::from)
    }
}

/// A stream of document blocks.
pub struct DocFetchStream {
    response_stream: Streaming<DataFetchResponse>,
}

impl From<Streaming<DataFetchResponse>> for DocFetchStream {
    fn from(response_stream: Streaming<DataFetchResponse>) -> Self {
        Self { response_stream }
    }
}

impl DocFetchStream {
    /// Waits and accepts the next block of documents from the network stream.
    pub async fn next(&mut self) -> Option<Result<DocsBlock, RpcError>> {
        let res = self.response_stream.next().await?;

        let data = match res {
            Ok(res) => res,
            Err(status) => return Some(Err(status.into())),
        };

        let res =
            crate::shared::decompress_docs(&data.docs, data.uncompressed_size as usize)
                .await
                .map_err(|_| RpcError::DeserializeError)
                .map(|docs| DocsBlock {
                    doc_ids: data.doc_ids,
                    offsets: data.offsets,
                    timestamps: data
                        .timestamps
                        .into_iter()
                        .map(|ts| {
                            HLCTimestamp::new(ts.millis, ts.counter as u16, ts.node_id)
                        })
                        .collect(),
                    docs_buffer: Bytes::from(docs),
                });

        Some(res)
    }
}

enum SynchronisationEvent {
    /// Get the current shard state.
    GetShardStates {
        tx: oneshot::Sender<Result<ShardState, Status>>,
    },
    /// Begin the synchronisation phase by requesting the remote node's doc set for the given shard.
    Sync {
        shard_id: u32,
        tx: oneshot::Sender<Result<SyncResponse, Status>>,
    },
    /// Fetch a set of documents returning a stream of small payloads.
    FetchDocs {
        docs: Vec<u64>,
        tx: oneshot::Sender<Result<Streaming<DataFetchResponse>, Status>>,
    },
}

struct SynchronisationClientActor {
    rx: flume::Receiver<SynchronisationEvent>,
    inner: DocumentSyncClient<Channel>,
}

impl SynchronisationClientActor {
    async fn run_actor(mut self) {
        while let Ok(event) = self.rx.recv_async().await {
            match event {
                SynchronisationEvent::Sync { shard_id, tx } => {
                    self.sync(shard_id, tx).await
                },
                SynchronisationEvent::FetchDocs { docs, tx } => {
                    self.fetch_docs(docs, tx).await
                },
                SynchronisationEvent::GetShardStates { tx } => {
                    self.get_shard_state(tx).await
                },
            }
        }
    }

    async fn get_shard_state(
        &mut self,
        tx: oneshot::Sender<Result<ShardState, Status>>,
    ) {
        let res = self.inner.get_shard_state(Blank {}).await;
        let _ = tx.send(res.map(|r| r.into_inner()));
    }

    async fn sync(
        &mut self,
        shard_id: u32,
        tx: oneshot::Sender<Result<SyncResponse, Status>>,
    ) {
        let res = self.inner.sync(SyncRequest { shard_id }).await;

        let _ = tx.send(res.map(|r| r.into_inner()));
    }

    async fn fetch_docs(
        &mut self,
        requested_docs: Vec<Key>,
        tx: oneshot::Sender<Result<Streaming<DataFetchResponse>, Status>>,
    ) {
        let res = self
            .inner
            .fetch_docs(DataFetchRequest { requested_docs })
            .await;

        let _ = tx.send(res.map(|r| r.into_inner()));
    }
}

#[derive(Clone)]
/// A general RPC client that manages node interactions that are
/// not directly related to state propagation and synchronisation.
///
/// Instead these RPC calls are used to fast track state proliferation
/// to remote nodes.
pub struct DataClient {
    tx: flume::Sender<DataEvent>,
}

impl DataClient {
    /// Creates a new [DataClient] which itself is a wrapper over
    /// an actor that handles each event sequentially.
    ///
    /// This assumes that it is being created within a tokio runtime context.
    pub fn new(channel: Channel) -> DataClient {
        let (tx, rx) = flume::bounded(10);

        let actor = DataClientActor {
            rx,
            inner: GeneralRpcClient::new(channel),
        };

        tokio::spawn(actor.run_actor());

        Self { tx }
    }

    /// Sends a set of documents to be updated on the remote node.
    pub async fn upsert(
        &self,
        docs: Arc<Vec<(Key, HLCTimestamp, Bytes)>>,
    ) -> Result<(), RpcError> {
        let (doc_ids, offsets, timestamps, docs, uncompressed_len) =
            super::build_docs_buffer(docs.iter().map(|v| (v.0, v.1, v.2.clone()))).await;

        let (tx, resp) = oneshot::channel();

        self.tx
            .send_async(DataEvent::Upsert {
                docs,
                doc_ids,
                offsets,
                timestamps,
                tx,
                uncompressed_len,
            })
            .await
            .map_err(|_| RpcError::DeadActor)?;

        resp.await
            .map_err(|_| RpcError::DeadActor)?
            .map_err(RpcError::from)
    }

    /// Sends a set of documents to be removed from the remote node.
    pub async fn delete(
        &self,
        doc_key_ts_pairs: Arc<StateChanges>,
    ) -> Result<(), RpcError> {
        let (tx, resp) = oneshot::channel();

        let mut doc_ids = vec![];
        let mut timestamps = vec![];
        for (doc_id, timestamp) in doc_key_ts_pairs.iter() {
            doc_ids.push(*doc_id);
            timestamps.push(Timestamp {
                millis: timestamp.millis(),
                counter: timestamp.counter() as u32,
                node_id: timestamp.node(),
            })
        }

        self.tx
            .send_async(DataEvent::Delete {
                doc_ids,
                timestamps,
                tx,
            })
            .await
            .map_err(|_| RpcError::DeadActor)?;

        resp.await
            .map_err(|_| RpcError::DeadActor)?
            .map_err(RpcError::from)
    }
}

enum DataEvent {
    /// Perform an upsert for the given set of documents.
    Upsert {
        docs: Bytes,
        uncompressed_len: u32,
        doc_ids: Vec<Key>,
        offsets: Vec<u32>,
        timestamps: Vec<Timestamp>,
        tx: oneshot::Sender<Result<(), Status>>,
    },

    /// Deletes a set of documents.
    Delete {
        doc_ids: Vec<Key>,
        timestamps: Vec<Timestamp>,
        tx: oneshot::Sender<Result<(), Status>>,
    },
}

struct DataClientActor {
    rx: flume::Receiver<DataEvent>,
    inner: GeneralRpcClient<Channel>,
}

impl DataClientActor {
    async fn run_actor(mut self) {
        while let Ok(event) = self.rx.recv_async().await {
            match event {
                DataEvent::Upsert {
                    docs,
                    uncompressed_len,
                    doc_ids,
                    offsets,
                    timestamps,
                    tx,
                } => {
                    self.upsert(docs, uncompressed_len, doc_ids, timestamps, offsets, tx)
                        .await
                },
                DataEvent::Delete {
                    doc_ids,
                    timestamps,
                    tx,
                } => self.delete(doc_ids, timestamps, tx).await,
            }
        }
    }

    async fn upsert(
        &mut self,
        doc_data: Bytes,
        uncompressed_size: u32,
        doc_ids: Vec<Key>,
        timestamps: Vec<Timestamp>,
        offsets: Vec<u32>,
        tx: oneshot::Sender<Result<(), Status>>,
    ) {
        let res = self
            .inner
            .upsert_docs(UpsertPayload {
                doc_data,
                uncompressed_size,
                doc_ids,
                offsets,
                timestamps,
            })
            .await;

        let _ = tx.send(res.map(|_| ()));
    }

    async fn delete(
        &mut self,
        doc_ids: Vec<Key>,
        timestamps: Vec<Timestamp>,
        tx: oneshot::Sender<Result<(), Status>>,
    ) {
        let res = self
            .inner
            .delete_docs(DeletePayload {
                doc_ids,
                timestamps,
            })
            .await;

        let _ = tx.send(res.map(|_| ()));
    }
}
