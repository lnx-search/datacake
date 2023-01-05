mod storage;

#[macro_use]
extern crate tracing;

use std::net::SocketAddr;

use anyhow::Result;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use clap::Parser;
use datacake::eventual_consistency::{
    EventuallyConsistentStoreExtension,
    ReplicatedStoreHandle,
};
use datacake::node::{
    ConnectionConfig,
    Consistency,
    DCAwareSelector,
    DatacakeNodeBuilder,
};
use serde_json::json;

use crate::storage::ShardedStorage;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args: Args = Args::parse();

    let storage = ShardedStorage::open_in_dir(&args.data_dir).await?;
    let connection_cfg = ConnectionConfig::new(
        args.cluster_listen_addr,
        args.public_addr.unwrap_or(args.cluster_listen_addr),
        args.seeds.into_iter(),
    );

    let node = DatacakeNodeBuilder::<DCAwareSelector>::new(1, connection_cfg)
        .connect()
        .await?;
    let store = node
        .add_extension(EventuallyConsistentStoreExtension::new(storage))
        .await?;

    let handle = store.handle();

    let app = Router::new()
        .route("/:keyspace/:key", get(get_value).post(set_value))
        .with_state(handle);

    info!("listening on {}", args.rest_listen_addr);
    let _ = axum::Server::bind(&args.rest_listen_addr)
        .serve(app.into_make_service())
        .await;

    node.shutdown().await;

    Ok(())
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[arg(long)]
    /// The unique ID of the node.
    node_id: String,

    #[arg(long = "seed")]
    /// The set of seed nodes.
    ///
    /// This is used to kick start the auto-discovery of nodes within the cluster.
    seeds: Vec<String>,

    #[arg(long, default_value = "127.0.0.1:8000")]
    /// The address for the REST server to listen on.
    ///
    /// This is what will serve the API.
    rest_listen_addr: SocketAddr,

    #[arg(long, default_value = "127.0.0.1:8001")]
    /// The address for the cluster RPC system to listen on.
    cluster_listen_addr: SocketAddr,

    #[arg(long)]
    /// The public address for the node to broadcast to other nodes.
    ///
    /// If not provided the `cluster_listen_addr` is used which will only
    /// work when running a cluster on the same local network.
    public_addr: Option<SocketAddr>,

    #[arg(long)]
    /// The path to store the data.
    data_dir: String,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct Params {
    keyspace: String,
    key: u64,
}

async fn get_value(
    Path(params): Path<Params>,
    State(handle): State<ReplicatedStoreHandle<ShardedStorage>>,
) -> Result<Bytes, StatusCode> {
    info!(
        doc_id = params.key,
        keyspace = params.keyspace,
        "Getting document!"
    );

    let doc = handle
        .get(&params.keyspace, params.key)
        .await
        .map_err(|e| {
            error!(error = ?e, doc_id = params.key, "Failed to fetch doc.");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    match doc {
        None => Err(StatusCode::NOT_FOUND),
        Some(doc) => Ok(Bytes::copy_from_slice(doc.data())),
    }
}

async fn set_value(
    Path(params): Path<Params>,
    State(handle): State<ReplicatedStoreHandle<ShardedStorage>>,
    data: Bytes,
) -> Result<Json<serde_json::Value>, StatusCode> {
    info!(
        doc_id = params.key,
        keyspace = params.keyspace,
        "Storing document!"
    );

    handle
        .put(&params.keyspace, params.key, data, Consistency::EachQuorum)
        .await
        .map_err(|e| {
            error!(error = ?e, doc_id = params.key, "Failed to fetch doc.");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(json!({
        "key": params.key,
        "keyspace": params.keyspace,
    })))
}
