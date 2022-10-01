use std::path::Path;

use datacake_cluster::Document;
use futures::channel::oneshot;
use rocksdb::OptimisticTransactionDB;

use crate::error::{Result, RocksStoreError};

pub fn start_shard(
    shard_id: usize,
    options: &rocksdb::Options,
    path: &Path,
) -> Result<ShardHandle> {
    let (tx, rx) = flume::bounded(10);
    let db = OptimisticTransactionDB::open(options, path)?;

    let actor = StoreShard {
        shard_id,
        db,
        ops: rx,
    };

    std::thread::Builder::new()
        .name(format!("rocksdb-shard-{}", shard_id))
        .spawn(move || actor.run())
        .map_err(|_| RocksStoreError::ThreadSpawnError)?;

    Ok(ShardHandle { tx })
}

#[derive(Clone)]
pub struct ShardHandle {
    tx: flume::Sender<Op>,
}

impl ShardHandle {
    pub async fn get_documents(&self, doc_ids: Vec<RawKey>) -> Result<Vec<Document>> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::GetDocuments { doc_ids, tx })
            .await
            .map_err(|_| RocksStoreError::DeadShard)?;

        rx.await.map_err(|_| RocksStoreError::DeadShard)?
    }

    pub async fn get_document(&self, doc_id: RawKey) -> Result<Option<Document>> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::GetDocument { doc_id, tx })
            .await
            .map_err(|_| RocksStoreError::DeadShard)?;

        rx.await.map_err(|_| RocksStoreError::DeadShard)?
    }

    pub async fn upsert_documents(
        &self,
        documents: Vec<(RawKey, Vec<u8>)>,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::UpsertDocuments { documents, tx })
            .await
            .map_err(|_| RocksStoreError::DeadShard)?;

        rx.await.map_err(|_| RocksStoreError::DeadShard)?
    }

    pub async fn upsert_document(
        &self,
        doc_id: RawKey,
        document: Vec<u8>,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::UpsertDocument {
                doc_id,
                document,
                tx,
            })
            .await
            .map_err(|_| RocksStoreError::DeadShard)?;

        rx.await.map_err(|_| RocksStoreError::DeadShard)?
    }

    pub async fn delete_documents(&self, doc_ids: Vec<RawKey>) -> Result<()> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::DeleteDocuments { doc_ids, tx })
            .await
            .map_err(|_| RocksStoreError::DeadShard)?;

        rx.await.map_err(|_| RocksStoreError::DeadShard)?
    }

    pub async fn delete_document(&self, doc_id: RawKey) -> Result<()> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::DeleteDocument { doc_id, tx })
            .await
            .map_err(|_| RocksStoreError::DeadShard)?;

        rx.await.map_err(|_| RocksStoreError::DeadShard)?
    }
}

pub struct StoreShard {
    shard_id: usize,
    ops: flume::Receiver<Op>,
    db: OptimisticTransactionDB,
}

impl StoreShard {
    pub fn run(mut self) {
        while let Ok(op) = self.ops.recv() {
            self.handle_op(op);
        }
    }

    fn handle_op(&mut self, op: Op) {
        match op {
            Op::GetDocuments { doc_ids, tx } => {
                let _ = tx.send(self.get_documents(doc_ids));
            },
            Op::GetDocument { doc_id, tx } => {
                let _ = tx.send(self.get_document(doc_id));
            },
            Op::UpsertDocuments { documents, tx } => {
                let _ = tx.send(self.upsert_documents(documents));
            },
            Op::UpsertDocument {
                doc_id,
                document,
                tx,
            } => {
                let _ = tx.send(self.upsert_document(doc_id, document));
            },
            Op::DeleteDocuments { doc_ids, tx } => {
                let _ = tx.send(self.delete_documents(doc_ids));
            },
            Op::DeleteDocument { doc_id, tx } => {
                let _ = tx.send(self.delete_document(doc_id));
            },
        }
    }

    fn get_documents(&self, doc_ids: Vec<RawKey>) -> Result<Vec<Document>> {
        let mut documents = vec![];

        for id in doc_ids {
            if let Some(doc) = self.db.get_pinned(&id)? {
                let doc = rkyv::from_bytes::<Document>(doc.as_ref())
                    .map_err(|e| RocksStoreError::DeserializationError(e.to_string()))?;

                documents.push(doc);
            }
        }

        Ok(documents)
    }

    fn get_document(&self, doc_id: RawKey) -> Result<Option<Document>> {
        self.db
            .get_pinned(&doc_id)?
            .map(|doc| {
                rkyv::from_bytes::<Document>(doc.as_ref())
                    .map_err(|e| RocksStoreError::DeserializationError(e.to_string()))
            })
            .transpose()
    }

    fn upsert_documents(&self, documents: Vec<(RawKey, Vec<u8>)>) -> Result<()> {
        let transaction = self.db.transaction();

        for (key, data) in documents {
            if let Err(e) = transaction.put(key, data) {
                if let Err(e) = transaction.rollback() {
                    error!(
                        shard_id = self.shard_id,
                        error = ?e,
                        "Rollback attempt failed during operation.",
                    );
                }

                return Err(e.into());
            }
        }

        transaction.commit()?;

        Ok(())
    }

    fn upsert_document(&self, doc_id: RawKey, document: Vec<u8>) -> Result<()> {
        self.db
            .put(doc_id, document)
            .map_err(RocksStoreError::RocksDbError)
    }

    fn delete_documents(&self, doc_ids: Vec<RawKey>) -> Result<()> {
        let transaction = self.db.transaction();

        for key in doc_ids {
            if let Err(e) = transaction.delete(key) {
                if let Err(e) = transaction.rollback() {
                    error!(
                        shard_id = self.shard_id,
                        error = ?e,
                        "Rollback attempt failed during operation.",
                    );
                }

                return Err(e.into());
            }
        }

        transaction.commit()?;

        Ok(())
    }

    fn delete_document(&self, doc_id: RawKey) -> Result<()> {
        self.db
            .delete(doc_id)
            .map_err(RocksStoreError::RocksDbError)
    }
}

pub(crate) type RawKey = Vec<u8>;

enum Op {
    GetDocuments {
        doc_ids: Vec<RawKey>,
        tx: oneshot::Sender<Result<Vec<Document>>>,
    },
    GetDocument {
        doc_id: RawKey,
        tx: oneshot::Sender<Result<Option<Document>>>,
    },
    UpsertDocuments {
        documents: Vec<(RawKey, Vec<u8>)>,
        tx: oneshot::Sender<Result<()>>,
    },
    UpsertDocument {
        doc_id: RawKey,
        document: Vec<u8>,
        tx: oneshot::Sender<Result<()>>,
    },
    DeleteDocuments {
        doc_ids: Vec<RawKey>,
        tx: oneshot::Sender<Result<()>>,
    },
    DeleteDocument {
        doc_id: RawKey,
        tx: oneshot::Sender<Result<()>>,
    },
}
