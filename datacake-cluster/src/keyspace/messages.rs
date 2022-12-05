use std::marker::PhantomData;
use datacake_crdt::{HLCTimestamp, Key, OrSWotSet, StateChanges};
use puppet::{derive_message, Message};

use crate::{Document, PutContext, Storage};
use crate::storage::BulkMutationError;

#[derive(Debug, thiserror::Error)]
#[error("Failed to (de)serialize state.")]
pub struct CorruptedState;

pub const NUM_SOURCES: usize = 2;

#[derive(Clone)]
pub struct Set<S> {
    pub source: usize,
    pub doc: Document,
    pub ctx: Option<PutContext>,
    pub _marker: PhantomData<S>,
}
impl<S: Storage> Message for Set<S> {
    type Output = Result<(), S::Error>;
}

#[derive(Clone)]
pub struct MultiSet<S> {
    pub source: usize,
    pub docs: Vec<Document>,
    pub ctx: Option<PutContext>,
    pub _marker: PhantomData<S>,
}
impl<S: Storage> Message for MultiSet<S> {
    type Output = Result<(), BulkMutationError<S::Error>>;
}

#[derive(Clone)]
pub struct Del<S> {
    pub source: usize,
    pub doc_id: Key,
    pub ts: HLCTimestamp,
    pub _marker: PhantomData<S>,
}
impl<S: Storage> Message for Del<S> {
    type Output = Result<(), S::Error>;
}

#[derive(Clone)]
pub struct MultiDel<S> {
    pub source: usize,
    pub key_ts_pairs: StateChanges,
    pub _marker: PhantomData<S>,
}
impl<S: Storage> Message for MultiDel<S> {
    type Output = Result<(), BulkMutationError<S::Error>>;
}

#[derive(Debug, Copy, Clone)]
pub struct Serialize;
derive_message!(Serialize, Result<Vec<u8>, CorruptedState>);

#[derive(Debug, Copy, Clone)]
pub struct LastUpdated;
derive_message!(LastUpdated, HLCTimestamp);

#[derive(Debug, Clone)]
pub struct Diff(pub OrSWotSet<NUM_SOURCES>);
derive_message!(Diff, (StateChanges, StateChanges));

#[derive(Debug, Clone)]
pub struct SymDiff(pub OrSWotSet<NUM_SOURCES>);
derive_message!(SymDiff, (StateChanges, StateChanges));

#[derive(Debug, Copy, Clone)]
pub struct PurgeDeletes<S>(pub PhantomData<S>);
impl<S: Storage> Message for PurgeDeletes<S> {
    type Output = Result<(), S::Error>;
}

