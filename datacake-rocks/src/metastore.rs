use std::path::Path;
use futures::channel::oneshot;
use rocksdb::{IteratorMode, OptimisticTransactionDB};

use crate::error::{Result, RocksStoreError};
use crate::{HLCTimestamp, Key, StateChanges};

pub fn start_metastore(
    options: rocksdb::Options,
    path: &Path
) -> Result<MetastoreHandle> {
    let (tx, rx) = flume::bounded(100);
    let db = OptimisticTransactionDB::open(&options, path)?;

    let actor = MetaStore {
        db,
        ops: rx,
        options,
    };

    std::thread::Builder::new()
        .name(format!("rocksdb-metastore"))
        .spawn(move || actor.run())
        .map_err(|_| RocksStoreError::ThreadSpawnError)?;

    Ok(MetastoreHandle { tx })
}

#[derive(Clone)]
pub struct MetastoreHandle {
    tx: flume::Sender<Op>,
}

impl MetastoreHandle {
    pub async fn get_keys(
        &self,
        is_tombstone: bool,
        shard_id: usize,
    ) -> Result<StateChanges> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::GetKeys { is_tombstone, shard_id, tx })
            .await
            .map_err(|_| RocksStoreError::DeadShard)?;

        rx.await.map_err(|_| RocksStoreError::DeadShard)?
    }

    pub async fn update_keys(
        &self,
        is_tombstone: bool,
        shard_id: usize,
        changes: StateChanges,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::UpdateKeys { is_tombstone, shard_id, changes, tx })
            .await
            .map_err(|_| RocksStoreError::DeadShard)?;

        rx.await.map_err(|_| RocksStoreError::DeadShard)?
    }

    pub async fn delete_keys(
        &self,
        is_tombstone: bool,
        shard_id: usize,
        keys: Vec<Key>,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::DeleteKeys { is_tombstone, shard_id, keys, tx })
            .await
            .map_err(|_| RocksStoreError::DeadShard)?;

        rx.await.map_err(|_| RocksStoreError::DeadShard)?
    }
}


pub struct MetaStore {
    ops: flume::Receiver<Op>,
    db: OptimisticTransactionDB,
    options: rocksdb::Options,
}

impl MetaStore {
    pub fn run(mut self)  {
        while let Ok(op) = self.ops.recv() {
            self.handle_op(op);
        }
    }

    fn handle_op(&mut self, op: Op) {
        match op {
            Op::GetKeys { is_tombstone, shard_id, tx } => {
                let res = self.get_keys(is_tombstone, shard_id);
                let _ = tx.send(res);
            }
            Op::UpdateKeys { is_tombstone, shard_id, changes, tx } => {
                let res = self.update_keys(is_tombstone, shard_id, changes);
                let _ = tx.send(res);
            }
            Op::DeleteKeys { is_tombstone, shard_id, keys, tx } => {
                let res = self.delete_keys(is_tombstone, shard_id, keys);
                let _ = tx.send(res);
            }
        }
    }

    fn get_keys(
        &mut self,
        is_tombstone: bool,
        shard_id: usize,
    ) -> Result<StateChanges> {
        let family = get_cf(is_tombstone, shard_id);

        // this is un-ideal, but rust's current borrow checker rules
        // don't allow us to easily make this a helper method.
        let cf = match self.db.cf_handle(&family) {
            Some(family) => family,
            None => {
                self.db.create_cf(&family, &self.options)?;
                self.db.cf_handle(&family)
                    .expect("Just created handle does not exist.")
            }
        };

        let iterator = self.db
            .full_iterator_cf(cf, IteratorMode::Start);

        let mut changes = vec![];
        for row in iterator {
            let (key, value) = row?;

            let key = rkyv::from_bytes::<Key>(&key)
                .map_err(|e| RocksStoreError::DeserializationError(e.to_string()))?;
            let ts = rkyv::from_bytes::<HLCTimestamp>(&value)
                .map_err(|e| RocksStoreError::DeserializationError(e.to_string()))?;

            changes.push((key, ts));
        }

        Ok(changes)
    }

    fn update_keys(
        &mut self,
        is_tombstone: bool,
        shard_id: usize,
        changes: StateChanges,
    ) -> Result<()> {
        let family = get_cf(is_tombstone, shard_id);

        // this is un-ideal, but rust's current borrow checker rules
        // don't allow us to easily make this a helper method.
        let cf = match self.db.cf_handle(&family) {
            Some(family) => family,
            None => {
                self.db.create_cf(&family, &self.options)?;
                self.db.cf_handle(&family)
                    .expect("Just created handle does not exist.")
            }
        };

        let transaction = self.db.transaction();

        for (key, ts) in changes {
            let raw_key = rkyv::to_bytes::<_, 8>(&key)
                .map_err(|_| RocksStoreError::SerializationError)?;
            let raw_ts = rkyv::to_bytes::<_, 24>(&ts)
                .map_err(|_| RocksStoreError::SerializationError)?;

            if let Err(e) = transaction.put_cf(cf, raw_key, raw_ts) {
                if let Err(e) = transaction.rollback() {
                    error!(error = ?e, "Failed to complete rollback while handling error.");
                }

                return Err(e.into())
            }
        }

        transaction.commit()?;

        Ok(())
    }

    fn delete_keys(
        &mut self,
        is_tombstone: bool,
        shard_id: usize,
        keys: Vec<Key>,
    ) -> Result<()> {
        let family = get_cf(is_tombstone, shard_id);

        // this is un-ideal, but rust's current borrow checker rules
        // don't allow us to easily make this a helper method.
        let cf = match self.db.cf_handle(&family) {
            Some(family) => family,
            None => {
                self.db.create_cf(&family, &self.options)?;
                self.db.cf_handle(&family)
                    .expect("Just created handle does not exist.")
            }
        };

        let transaction = self.db.transaction();

        for key in keys {
            let raw_key = rkyv::to_bytes::<_, 8>(&key)
                .map_err(|_| RocksStoreError::SerializationError)?;

            if let Err(e) = transaction.delete_cf(cf, raw_key) {
                if let Err(e) = transaction.rollback() {
                    error!(error = ?e, "Failed to complete rollback while handling error.");
                }

                return Err(e.into())
            }
        }

        transaction.commit()?;

        Ok(())
    }
}


enum Op {
    GetKeys {
        is_tombstone: bool,
        shard_id: usize,
        tx: oneshot::Sender<Result<StateChanges>>,
    },
    UpdateKeys {
        is_tombstone: bool,
        shard_id: usize,
        changes: StateChanges,
        tx: oneshot::Sender<Result<()>>,
    },
    DeleteKeys {
        is_tombstone: bool,
        shard_id: usize,
        keys: Vec<Key>,
        tx: oneshot::Sender<Result<()>>,
    },
}


fn get_cf(is_tombstone: bool, shard_id: usize) -> String {
    if is_tombstone {
        format!("tombstone-{}", shard_id)
    } else {
        shard_id.to_string()
    }
}