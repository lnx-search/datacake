use std::marker::PhantomData;

use datacake_crdt::{HLCTimestamp, OrSWotSet, StateChanges};
use puppet::{derive_message, Message};

use crate::core::DocumentMetadata;
use crate::storage::BulkMutationError;
use crate::{DocVec, Document, PutContext, Storage};

#[derive(Debug, thiserror::Error)]
#[error("Failed to (de)serialize state.")]
pub struct CorruptedState;

pub const NUM_SOURCES: usize = 2;

pub struct Set<S> {
    pub source: usize,
    pub doc: Document,
    pub ctx: Option<PutContext>,
    pub _marker: PhantomData<S>,
}
impl<S: Storage> Message for Set<S> {
    type Output = Result<(), S::Error>;
}

pub struct MultiSet<S> {
    pub source: usize,
    pub docs: DocVec<Document>,
    pub ctx: Option<PutContext>,
    pub _marker: PhantomData<S>,
}
impl<S: Storage> Message for MultiSet<S> {
    type Output = Result<(), BulkMutationError<S::Error>>;
}

pub struct Del<S> {
    pub source: usize,
    pub doc: DocumentMetadata,
    pub _marker: PhantomData<S>,
}
impl<S: Storage> Message for Del<S> {
    type Output = Result<(), S::Error>;
}

pub struct MultiDel<S> {
    pub source: usize,
    pub docs: DocVec<DocumentMetadata>,
    pub _marker: PhantomData<S>,
}
impl<S: Storage> Message for MultiDel<S> {
    type Output = Result<(), BulkMutationError<S::Error>>;
}

#[derive(Copy, Clone)]
pub struct Serialize;
derive_message!(Serialize, Result<Vec<u8>, CorruptedState>);

#[derive(Copy, Clone)]
pub struct LastUpdated;
derive_message!(LastUpdated, HLCTimestamp);

#[derive(Clone)]
pub struct Diff(pub OrSWotSet<NUM_SOURCES>);
derive_message!(Diff, (StateChanges, StateChanges));

#[derive(Clone)]
pub struct SymDiff(pub OrSWotSet<NUM_SOURCES>);
derive_message!(SymDiff, (StateChanges, StateChanges));

#[derive(Copy, Clone)]
pub struct PurgeDeletes<S>(pub PhantomData<S>);
impl<S: Storage> Message for PurgeDeletes<S> {
    type Output = Result<(), S::Error>;
}
