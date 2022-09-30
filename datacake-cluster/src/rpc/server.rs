use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use datacake_crdt::{HLCTimestamp, Key};
use futures::channel::oneshot;
use futures::Stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use super::DocsBlock;
use crate::rpc::cluster_rpc_models::document_sync_server::{
    DocumentSync,
    DocumentSyncServer,
};
use crate::rpc::cluster_rpc_models::general_rpc_server::{GeneralRpc, GeneralRpcServer};
use crate::rpc::cluster_rpc_models::{
    Blank,
    DataFetchRequest,
    DataFetchResponse,
    DeletePayload,
    ShardState,
    SyncRequest,
    SyncResponse,
    UpsertPayload,
};
use crate::rpc::DataHandler;
use crate::shard::state::StateWatcherHandle;
use crate::shard::ShardGroupHandle;

type ResponseStream =
    Pin<Box<dyn Stream<Item = Result<DataFetchResponse, Status>> + Send>>;

pub async fn start_rpc_server(
    shards: ShardGroupHandle,
    shard_changes_watcher: StateWatcherHandle,
    handler: Arc<dyn DataHandler>,
    bind: SocketAddr,
) -> oneshot::Sender<()> {
    let server = RpcServer {
        shards,
        shard_changes_watcher,
        handler,
    };

    let (tx, rx) = oneshot::channel();

    tokio::spawn(async move {
        let server = Server::builder()
            .add_service(DocumentSyncServer::new(server.clone()))
            .add_service(GeneralRpcServer::new(server))
            .serve_with_shutdown(bind, async {
                let _ = rx.await;
            });

        if let Err(e) = server.await {
            error!(bind = %bind, error = ?e, "Failed to run RPC server due to error: {}", e);
        }
    });

    tx
}

#[derive(Clone)]
pub struct RpcServer {
    shards: ShardGroupHandle,
    shard_changes_watcher: StateWatcherHandle,
    handler: Arc<dyn DataHandler>,
}

#[tonic::async_trait]
impl DocumentSync for RpcServer {
    async fn get_shard_state(
        &self,
        _request: Request<Blank>,
    ) -> Result<Response<ShardState>, Status> {
        let shards = self.shard_changes_watcher.get().await;
        Ok(Response::new(ShardState { shards }))
    }

    async fn sync(
        &self,
        request: Request<SyncRequest>,
    ) -> Result<Response<SyncResponse>, Status> {
        let req = request.into_inner();

        let (set, len) = self.shards
            .get_serialized_set(req.shard_id as usize)
            .await
            .map_err(|_| Status::internal("The requested shard has experienced an unrecoverable shutdown."))?
            .map_err(|_| Status::internal("The shard state has been corrupted and was unable to serialize it's state."))?;

        Ok(Response::new(SyncResponse {
            uncompressed_size: len as u32,
            doc_set: set,
        }))
    }

    type FetchDocsStream = ResponseStream;

    async fn fetch_docs(
        &self,
        request: Request<DataFetchRequest>,
    ) -> Result<Response<Self::FetchDocsStream>, Status> {
        let req = request.into_inner();
        let data_handler = self.handler.clone();

        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(async move {
            for docs_block in req.requested_docs.chunks(10_000) {
                let resp = fetch_documents(&data_handler, docs_block)
                    .await
                    .map_err(|e| Status::internal(e.to_string()));

                if let Err(_) = tx.send(resp).await {
                    break;
                }
            }
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::FetchDocsStream
        ))
    }
}

async fn fetch_documents(
    handler: &Arc<dyn DataHandler>,
    docs: &[Key],
) -> anyhow::Result<DataFetchResponse> {
    let docs = handler
        .get_documents(docs)
        .await?
        .into_iter()
        .map(|d| (d.id, d.last_modified, d.data));

    let (doc_ids, offsets, timestamps, docs, uncompressed_size) =
        super::build_docs_buffer(docs).await;

    Ok(DataFetchResponse {
        doc_ids,
        offsets,
        timestamps,
        uncompressed_size,
        docs,
    })
}

#[tonic::async_trait]
impl GeneralRpc for RpcServer {
    async fn upsert_docs(
        &self,
        request: Request<UpsertPayload>,
    ) -> Result<Response<Blank>, Status> {
        let req = request.into_inner();

        let docs = crate::shared::decompress_docs(
            &req.doc_data,
            req.uncompressed_size as usize,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        let iterator = DocsBlock {
            doc_ids: req.doc_ids,
            offsets: req.offsets,
            timestamps: req
                .timestamps
                .into_iter()
                .map(|ts| HLCTimestamp::new(ts.millis, ts.counter as u16, ts.node_id))
                .collect(),
            docs_buffer: Bytes::from(docs),
        };

        self.handler
            .upsert_documents(Vec::from_iter(iterator))
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(Blank {}))
    }

    async fn delete_docs(
        &self,
        request: Request<DeletePayload>,
    ) -> Result<Response<Blank>, Status> {
        let req = request.into_inner();

        let ts_converter = req
            .timestamps
            .into_iter()
            .map(|ts| HLCTimestamp::new(ts.millis, ts.counter as u16, ts.node_id));

        let doc_id_pairs = req.doc_ids.into_iter().zip(ts_converter).collect();

        self.handler
            .mark_tombstone_documents(doc_id_pairs)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(Blank {}))
    }
}
