mod storage;

#[macro_use]
extern crate tracing;

use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Result;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
use clap::Parser;
use datacake::cluster::{
    ClusterOptions,
    ConnectionConfig,
    Consistency,
    DCAwareSelector,
    DatacakeCluster,
    DatacakeHandle,
};
use notify::{Event, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::storage::{get_path_buf, FileStorage, Files, KEYSPACE};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args: Args = Args::parse();

    let storage = FileStorage::open_in_dir(&args.data_dir).await?;
    let files = storage.files();

    // Setup the cluster.
    let connection_cfg = ConnectionConfig::new(
        args.cluster_listen_addr,
        args.public_addr.unwrap_or(args.cluster_listen_addr),
        args.seeds.into_iter(),
    );
    let cluster = DatacakeCluster::connect(
        args.node_id,
        connection_cfg,
        storage,
        DCAwareSelector::default(),
        ClusterOptions::default(),
    )
    .await?;

    let handle = cluster.handle();

    // Setup the file watcher to look for changes.
    load_and_watch_files(files, handle.clone(), args.data_dir.as_ref()).await?;

    // Run the API which serves the primary file content.
    let app = Router::new()
        .route("/:file_id", get(get_doc))
        .with_state(handle.clone());

    let mut data_listen_addr = args.cluster_listen_addr;
    data_listen_addr.set_port(data_listen_addr.port() + 1);
    let _ = axum::Server::bind(&data_listen_addr)
        .serve(app.into_make_service())
        .await;

    cluster.shutdown().await;

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
    file_id: u64,
}

async fn get_doc(
    Path(params): Path<Params>,
    State(handle): State<DatacakeHandle<FileStorage>>,
) -> Result<Bytes, StatusCode> {
    // Technically, this should never be None, as we only expect this to be called
    // when the system is requesting a file it knows the node has.
    let doc = handle
        .get(KEYSPACE, params.file_id)
        .await
        .map_err(|e| {
            error!(error = ?e, file_id = params.file_id, "Failed to fetch file.");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let file_path = get_path_buf(&doc.data);
    let data = tokio::fs::read(file_path).await.map_err(|e| {
        error!(error = ?e, file_id = params.file_id, "Failed to fetch file.");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Bytes::from(data))
}

async fn load_and_watch_files(
    files: Files,
    handle: DatacakeHandle<FileStorage>,
    base_path: &std::path::Path,
) -> Result<()> {
    let dirs = std::fs::read_dir(base_path)?;

    for file in dirs {
        let file = file?;

        let path = file.path();
        if path.is_dir() {
            continue;
        }

        files.insert(hash_path(&path), path);
    }

    let (file_changes_tx, file_changes_rx) = mpsc::unbounded_channel();
    let mut watcher =
        notify::recommended_watcher(move |res: notify::Result<Event>| match res {
            Ok(event) => {
                let _ = file_changes_tx.send(event.paths);
            },
            Err(e) => error!(error = ?e, "File watcher error."),
        })?;
    watcher.watch(base_path, RecursiveMode::NonRecursive)?;

    tokio::spawn(watch_files(
        files,
        handle,
        file_changes_rx,
        base_path.to_path_buf(),
    ));

    Ok(())
}

async fn watch_files(
    files: Files,
    handle: DatacakeHandle<FileStorage>,
    mut changes: UnboundedReceiver<Vec<PathBuf>>,
    base_path: PathBuf,
) {
    while let Some(changes) = changes.recv().await {
        let mut docs = Vec::new();
        for path in changes {
            let doc_id = hash_path(&path);
            files.insert(doc_id, path.clone());

            let export_path = path
                .strip_prefix(&base_path)
                .expect("Strip path prefix.")
                .to_string_lossy()
                .as_bytes()
                .to_vec();
            docs.push((doc_id, export_path))
        }

        if let Err(e) = handle
            .put_many(KEYSPACE, docs, Consistency::LocalQuorum)
            .await
        {
            error!(error = ?e, "Failed to store local changes.");
        }
    }
}

/// Gets the hash of the path.
///
/// In the real world you should *not* use the CRC32 hashing system.
/// This is just being used for convenience.
fn hash_path(path: &std::path::Path) -> u64 {
    let mut hasher = crc32fast::Hasher::new();
    path.hash(&mut hasher);
    hasher.finish()
}
