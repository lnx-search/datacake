use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::async_trait;
use dashmap::DashMap;
use datacake::cluster::{BulkMutationError, Document, PutContext, Storage};
use datacake::crdt::{HLCTimestamp, Key};
use datacake::sqlite::SqliteStorage;
use futures::StreamExt;
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;

pub static DB_NAME: &str = "metadata.db";
pub static KEYSPACE: &str = "files";

pub type Files = Arc<DashMap<Key, PathBuf>>;

#[derive(Debug, thiserror::Error)]
pub enum FileStoreError {
    #[error("{0}")]
    SQLite(#[from] rusqlite::Error),

    #[error("IO Error: {0}")]
    Io(#[from] io::Error),

    #[error("Logical Error: {0}")]
    Logical(String),

    #[error("Reqwest Error: {0}")]
    Reqwest(#[from] reqwest::Error),
}

pub struct FileStorage {
    metadata: SqliteStorage,
    client: reqwest::Client,
    files: Files,
    base_path: PathBuf,
}

impl FileStorage {
    /// Opens a storage instances in a given directory.
    pub async fn open_in_dir(dir: impl AsRef<Path>) -> Result<Self> {
        let path = dir.as_ref();
        let client = reqwest::ClientBuilder::new()
            .timeout(Duration::from_secs(2))
            .build()?;

        let metadata = SqliteStorage::open(path.join(DB_NAME)).await?;

        Ok(Self {
            metadata,
            client,
            base_path: path.to_path_buf(),
            files: Arc::new(DashMap::new()),
        })
    }

    /// Creates a new handle to the live files mapping.
    pub fn files(&self) -> Files {
        self.files.clone()
    }
}

#[async_trait]
impl Storage for FileStorage {
    type Error = FileStoreError;
    type DocsIter = <SqliteStorage as Storage>::DocsIter;
    type MetadataIter = <SqliteStorage as Storage>::MetadataIter;

    async fn get_keyspace_list(&self) -> std::result::Result<Vec<String>, Self::Error> {
        self.metadata
            .get_keyspace_list()
            .await
            .map_err(Self::Error::from)
    }

    async fn iter_metadata(
        &self,
        keyspace: &str,
    ) -> std::result::Result<Self::MetadataIter, Self::Error> {
        self.metadata
            .iter_metadata(keyspace)
            .await
            .map_err(Self::Error::from)
    }

    async fn remove_tombstones(
        &self,
        keyspace: &str,
        keys: impl Iterator<Item = Key> + Send,
    ) -> std::result::Result<(), BulkMutationError<Self::Error>> {
        self.metadata
            .remove_tombstones(keyspace, keys)
            .await
            .map_err(|e| {
                let doc_ids = e.successful_doc_ids().to_vec();
                BulkMutationError::new(FileStoreError::SQLite(e.into_inner()), doc_ids)
            })
    }

    async fn put_with_ctx(
        &self,
        keyspace: &str,
        document: Document,
        ctx: Option<&PutContext>,
    ) -> std::result::Result<(), Self::Error> {
        assert_eq!(
            keyspace, KEYSPACE,
            "We only use one keyspace, you shouldn't do this normally."
        );

        if let Some(ctx) = ctx {
            let addr = get_file_api(ctx.remote_addr());
            let export_path = self.base_path.join(get_path_buf(&document.data));

            let tmp_file = NamedTempFile::new()?;
            let tmp_path = tmp_file.path().to_path_buf();

            download_file(
                format!("http://{}/{}", addr, document.id),
                &self.client,
                tmp_file,
            )
            .await?;

            tokio::fs::copy(&tmp_path, export_path).await?;
            let _ = tokio::fs::remove_file(tmp_path).await;
        }

        self.metadata.put_with_ctx(keyspace, document, ctx).await?;

        Ok(())
    }

    async fn put(
        &self,
        keyspace: &str,
        _document: Document,
    ) -> std::result::Result<(), Self::Error> {
        assert_eq!(
            keyspace, KEYSPACE,
            "We only use one keyspace, you shouldn't do this normally."
        );
        Err(FileStoreError::Logical(
            "Datacake guarantees that this will not be called under normal \
            operations over the `with_ctx` method."
                .to_string(),
        ))
    }

    async fn multi_put_with_ctx(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = Document> + Send,
        ctx: Option<&PutContext>,
    ) -> std::result::Result<(), BulkMutationError<Self::Error>> {
        let mut successful_ids = Vec::new();
        for doc in documents {
            let doc_id = doc.id;
            self.put_with_ctx(keyspace, doc, ctx)
                .await
                .map_err(Self::Error::from)
                .map_err(|e| BulkMutationError::new(e, successful_ids.clone()))?;

            successful_ids.push(doc_id);
        }

        Ok(())
    }

    async fn multi_put(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = Document> + Send,
    ) -> std::result::Result<(), BulkMutationError<Self::Error>> {
        let mut successful_ids = Vec::new();
        for doc in documents {
            let doc_id = doc.id;
            self.put(keyspace, doc)
                .await
                .map_err(Self::Error::from)
                .map_err(|e| BulkMutationError::new(e, successful_ids.clone()))?;

            successful_ids.push(doc_id);
        }

        Ok(())
    }

    async fn mark_as_tombstone(
        &self,
        keyspace: &str,
        doc_id: Key,
        timestamp: HLCTimestamp,
    ) -> std::result::Result<(), Self::Error> {
        assert_eq!(
            keyspace, KEYSPACE,
            "We only use one keyspace, you shouldn't do this normally."
        );

        if let Some(fp) = self.files.get(&doc_id) {
            tokio::fs::remove_dir_all(fp.as_path()).await?;
        }

        self.metadata
            .mark_as_tombstone(keyspace, doc_id, timestamp)
            .await?;

        Ok(())
    }

    async fn mark_many_as_tombstone(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = (Key, HLCTimestamp)> + Send,
    ) -> std::result::Result<(), BulkMutationError<Self::Error>> {
        let mut successful_ids = Vec::new();
        for (doc_id, ts) in documents {
            self.mark_as_tombstone(keyspace, doc_id, ts)
                .await
                .map_err(Self::Error::from)
                .map_err(|e| BulkMutationError::new(e, successful_ids.clone()))?;

            successful_ids.push(doc_id);
        }

        Ok(())
    }

    async fn get(
        &self,
        keyspace: &str,
        doc_id: Key,
    ) -> std::result::Result<Option<Document>, Self::Error> {
        self.metadata
            .get(keyspace, doc_id)
            .await
            .map_err(Self::Error::from)
    }

    async fn multi_get(
        &self,
        keyspace: &str,
        doc_ids: impl Iterator<Item = Key> + Send,
    ) -> std::result::Result<Self::DocsIter, Self::Error> {
        self.metadata
            .multi_get(keyspace, doc_ids)
            .await
            .map_err(Self::Error::from)
    }
}

async fn download_file(
    url: String,
    client: &reqwest::Client,
    file: NamedTempFile,
) -> Result<(), FileStoreError> {
    let mut stream = client.get(url).send().await?.bytes_stream();

    let mut file = tokio::fs::File::from_std(file.into_file());
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk).await?;
    }

    Ok(())
}

/// Takes the current socket address and returns the same +1 on the port.
pub fn get_file_api(mut addr: SocketAddr) -> SocketAddr {
    addr.set_port(addr.port() + 1);
    addr
}

pub fn get_path_buf(doc: &[u8]) -> PathBuf {
    let path = String::from_utf8(doc.to_vec()).expect("Path should be UTF-8");
    PathBuf::from_str(&path).expect("Path should be valid")
}
