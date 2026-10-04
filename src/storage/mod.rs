//! Where DBMesh keeps *its own* state.
//!
//! Synchronization state (identity, peers, cursors, the change log) is stored
//! apart from application data, in a place the host application chooses: it
//! may be a file, a separate database, or the application's own database
//! behind an adapter it wrote on purpose. DBMesh never creates bookkeeping
//! tables inside the application database by itself.
//!
//! The write-ahead rule that makes crash recovery work: a received
//! transaction is appended to the log **before** it is applied, and the
//! `applied` cursor moves only **after**. Whatever sits between the two is
//! what [`Store::unapplied`] returns on restart.

mod memory;

use std::future::Future;

pub use memory::MemoryStore;
use serde::{Deserialize, Serialize};

use crate::core::{
    CursorSet, Mutation, NodeId, Origin, Peer, PeerId, PeerState, PeerStatus, Provenance, Sequence,
    SourcePosition, Transaction,
};
use crate::error::Result;

/// One slot of an origin's history in the log.
///
/// The log has no gaps: every sequence from 1 up to the processed cursor has
/// an entry. An entry without a transaction records a range this node
/// handled without keeping the content (policy dropped it entirely).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogEntry {
    /// Whose history.
    pub origin: Origin,
    /// The slot's sequence.
    pub sequence: Sequence,
    /// What was kept, if anything. May be [`partial`](Transaction::partial).
    pub transaction: Option<Transaction>,
    /// How it reached this node.
    pub provenance: Provenance,
}

impl LogEntry {
    /// Whether this entry can be relayed to others as the origin's own transaction.
    ///
    /// A placeholder or a policy-trimmed transaction cannot: forwarding it
    /// would let the next node believe it saw everything.
    #[must_use]
    pub fn is_relayable(&self) -> bool {
        self.transaction.as_ref().is_some_and(|tx| !tx.partial)
    }
}

/// The answer to a log read.
#[derive(Clone, Debug, PartialEq)]
pub enum LogRead {
    /// The requested entries, in sequence order, contiguous from the first one requested.
    Entries(Vec<LogEntry>),
    /// The history the requester needs was discarded; a snapshot is required.
    ///
    /// This is how a *stale peer* shows up: its cursor is older than what the
    /// log still retains.
    Compacted {
        /// The oldest sequence still available.
        oldest: Sequence,
    },
}

/// Durable storage for DBMesh state.
///
/// Every method must be atomic on its own and durable when it returns. The
/// methods that combine several effects ([`append_local`](Self::append_local))
/// exist because splitting them would open a crash window.
pub trait Store: Send + Sync {
    /// The persisted node identity, if one was ever stored.
    fn identity(&self) -> impl Future<Output = Result<Option<NodeId>>> + Send;

    /// Persists the node identity.
    fn set_identity(&self, node: &NodeId) -> impl Future<Output = Result<()>> + Send;

    /// Creates a peer, or updates its configuration while keeping its checkpoint.
    fn put_peer(&self, peer: Peer) -> impl Future<Output = Result<()>> + Send;

    /// A peer's persisted state.
    fn peer_state(&self, peer: &PeerId) -> impl Future<Output = Result<Option<PeerState>>> + Send;

    /// Every known peer's state, in peer-id order.
    fn peer_states(&self) -> impl Future<Output = Result<Vec<PeerState>>> + Send;

    /// Records how the last session with a peer ended.
    fn set_status(
        &self,
        peer: &PeerId,
        status: PeerStatus,
    ) -> impl Future<Output = Result<()>> + Send;

    /// The processed cursors of this node: its version vector.
    fn cursors(&self) -> impl Future<Output = Result<CursorSet>> + Send;

    /// Where the adapter's change feed was after the last captured transaction.
    fn source_position(&self) -> impl Future<Output = Result<Option<SourcePosition>>> + Send;

    /// Assigns the next sequence to a locally committed transaction, appends it
    /// to the log, advances the local cursor and records the source position,
    /// all atomically.
    ///
    /// Atomicity is the point: a crash must never leave the log ahead of the
    /// position (the transaction would be captured twice) or behind it (it
    /// would be lost).
    fn append_local(
        &self,
        origin: &Origin,
        mutations: Vec<Mutation>,
        position: Option<SourcePosition>,
    ) -> impl Future<Output = Result<Transaction>> + Send;

    /// Appends received entries to the log without applying them.
    ///
    /// Idempotent: an entry whose slot is already filled is left untouched, so
    /// a retransmitted batch cannot overwrite or duplicate anything.
    fn append_remote(&self, entries: Vec<LogEntry>) -> impl Future<Output = Result<()>> + Send;

    /// Moves the processed cursor of `origin` forward, never back.
    fn mark_applied(
        &self,
        origin: &Origin,
        through: Sequence,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Entries logged but not yet marked applied, in (origin, sequence) order.
    /// What a restart must finish before synchronizing again.
    fn unapplied(&self) -> impl Future<Output = Result<Vec<LogEntry>>> + Send;

    /// Reads the log for `origin` after `after`, at most `limit` entries, none beyond `up_to`.
    fn read(
        &self,
        origin: &Origin,
        after: Sequence,
        up_to: Sequence,
        limit: usize,
    ) -> impl Future<Output = Result<LogRead>> + Send;

    /// Records that `peer` acknowledged `origin` through `through`.
    fn record_ack(
        &self,
        peer: &PeerId,
        origin: &Origin,
        through: Sequence,
    ) -> impl Future<Output = Result<()>> + Send;
}
