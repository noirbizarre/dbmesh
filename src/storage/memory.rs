//! An in-memory [`Store`], for tests and for applications that do not need
//! synchronization state to survive a restart.
//!
//! It is a cheap handle: cloning shares the same state, which is how tests
//! simulate a process restart (drop the engine, build a new one on the same
//! store).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use super::{LogEntry, LogRead, Store};
use crate::core::{
    CursorSet, Mutation, NodeId, Origin, Peer, PeerId, PeerState, PeerStatus, Provenance, Sequence,
    SourcePosition, Transaction, TransactionId,
};
use crate::error::Result;

#[derive(Default)]
struct Inner {
    identity: Option<NodeId>,
    peers: BTreeMap<PeerId, PeerState>,
    applied: CursorSet,
    position: Option<SourcePosition>,
    log: BTreeMap<Origin, BTreeMap<Sequence, LogEntry>>,
    // Everything at or below this sequence has been discarded, per origin.
    compacted: BTreeMap<Origin, Sequence>,
}

/// A [`Store`] that lives in memory.
#[derive(Clone, Default)]
pub struct MemoryStore {
    inner: Arc<Mutex<Inner>>,
}

impl MemoryStore {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        // A poisoned lock only means another test thread panicked mid-write;
        // the data is plain and still consistent per operation.
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Discards `origin`'s log entries up to and including `through`, as a
    /// retention policy would. Peers behind that point then need a snapshot.
    pub fn compact(&self, origin: &Origin, through: Sequence) {
        let mut inner = self.lock();
        if let Some(log) = inner.log.get_mut(origin) {
            log.retain(|sequence, _| *sequence > through);
        }
        let slot = inner.compacted.entry(origin.clone()).or_default();
        *slot = (*slot).max(through);
    }
}

impl Store for MemoryStore {
    async fn identity(&self) -> Result<Option<NodeId>> {
        Ok(self.lock().identity.clone())
    }

    async fn set_identity(&self, node: &NodeId) -> Result<()> {
        self.lock().identity = Some(node.clone());
        Ok(())
    }

    async fn put_peer(&self, peer: Peer) -> Result<()> {
        let mut inner = self.lock();
        match inner.peers.get_mut(&peer.id) {
            Some(state) => state.peer = peer,
            None => {
                inner.peers.insert(peer.id.clone(), PeerState::new(peer));
            }
        }
        Ok(())
    }

    async fn peer_state(&self, peer: &PeerId) -> Result<Option<PeerState>> {
        Ok(self.lock().peers.get(peer).cloned())
    }

    async fn peer_states(&self) -> Result<Vec<PeerState>> {
        Ok(self.lock().peers.values().cloned().collect())
    }

    async fn set_status(&self, peer: &PeerId, status: PeerStatus) -> Result<()> {
        if let Some(state) = self.lock().peers.get_mut(peer) {
            state.status = status;
        }
        Ok(())
    }

    async fn cursors(&self) -> Result<CursorSet> {
        Ok(self.lock().applied.clone())
    }

    async fn source_position(&self) -> Result<Option<SourcePosition>> {
        Ok(self.lock().position)
    }

    async fn append_local(
        &self,
        origin: &Origin,
        mutations: Vec<Mutation>,
        position: Option<SourcePosition>,
    ) -> Result<Transaction> {
        // One lock held across all three effects is this implementation's atomicity.
        let mut inner = self.lock();
        let sequence = inner.applied.get(origin).next();
        let transaction = Transaction::new(
            TransactionId::generate(),
            origin.clone(),
            sequence,
            mutations,
        );
        inner.log.entry(origin.clone()).or_default().insert(
            sequence,
            LogEntry {
                origin: origin.clone(),
                sequence,
                transaction: Some(transaction.clone()),
                provenance: Provenance::default(),
            },
        );
        inner.applied.advance(origin, sequence);
        if position.is_some() {
            inner.position = position;
        }
        Ok(transaction)
    }

    async fn append_remote(&self, entries: Vec<LogEntry>) -> Result<()> {
        let mut inner = self.lock();
        for entry in entries {
            inner
                .log
                .entry(entry.origin.clone())
                .or_default()
                .entry(entry.sequence)
                .or_insert(entry);
        }
        Ok(())
    }

    async fn mark_applied(&self, origin: &Origin, through: Sequence) -> Result<()> {
        self.lock().applied.advance(origin, through);
        Ok(())
    }

    async fn unapplied(&self) -> Result<Vec<LogEntry>> {
        let inner = self.lock();
        Ok(inner
            .log
            .iter()
            .flat_map(|(origin, log)| {
                let applied = inner.applied.get(origin);
                log.range(applied.next()..).map(|(_, entry)| entry.clone())
            })
            .collect())
    }

    async fn read(
        &self,
        origin: &Origin,
        after: Sequence,
        up_to: Sequence,
        limit: usize,
    ) -> Result<LogRead> {
        let inner = self.lock();
        if let Some(compacted) = inner.compacted.get(origin)
            && after < *compacted
        {
            return Ok(LogRead::Compacted {
                oldest: compacted.next(),
            });
        }
        let mut entries = Vec::new();
        let mut expected = after.next();
        if let Some(log) = inner.log.get(origin) {
            for (sequence, entry) in log.range(expected..=up_to).take(limit) {
                // Stop at the first hole rather than skipping it: a contiguous
                // read is what lets a batch claim `(from, through]`.
                if *sequence != expected {
                    break;
                }
                entries.push(entry.clone());
                expected = expected.next();
            }
        }
        Ok(LogRead::Entries(entries))
    }

    async fn record_ack(&self, peer: &PeerId, origin: &Origin, through: Sequence) -> Result<()> {
        if let Some(state) = self.lock().peers.get_mut(peer) {
            state.checkpoint.acked.advance(origin, through);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Collection, Operation, RecordId};

    fn mutation() -> Mutation {
        Mutation::new(
            RecordId::new(Collection::new("memories").unwrap(), "foo"),
            Operation::Create,
            None,
        )
    }

    fn origin() -> Origin {
        Origin::new("a").unwrap()
    }

    #[tokio::test]
    async fn local_sequences_are_assigned_without_gaps_starting_at_one() {
        let store = MemoryStore::new();
        let first = store
            .append_local(&origin(), vec![mutation()], None)
            .await
            .unwrap();
        let second = store
            .append_local(&origin(), vec![mutation()], None)
            .await
            .unwrap();
        assert_eq!(
            (first.sequence, second.sequence),
            (Sequence::new(1), Sequence::new(2))
        );
    }

    #[tokio::test]
    async fn appending_a_local_transaction_records_the_source_position_with_it() {
        let store = MemoryStore::new();
        store
            .append_local(&origin(), vec![mutation()], Some(SourcePosition(42)))
            .await
            .unwrap();
        assert_eq!(
            store.source_position().await.unwrap(),
            Some(SourcePosition(42))
        );
    }

    #[tokio::test]
    async fn appending_the_same_remote_entry_twice_keeps_the_first() {
        let store = MemoryStore::new();
        let entry = |provenance_hops| LogEntry {
            origin: origin(),
            sequence: Sequence::new(1),
            transaction: None,
            provenance: Provenance {
                sender: None,
                hops: provenance_hops,
            },
        };
        store.append_remote(vec![entry(1)]).await.unwrap();
        store.append_remote(vec![entry(9)]).await.unwrap();
        let LogRead::Entries(entries) = store
            .read(&origin(), Sequence::ZERO, Sequence::new(1), 10)
            .await
            .unwrap()
        else {
            panic!("expected entries");
        };
        assert_eq!(entries[0].provenance.hops, 1);
    }

    #[tokio::test]
    async fn entries_logged_but_not_marked_applied_are_reported_for_recovery() {
        let store = MemoryStore::new();
        let entry = |sequence| LogEntry {
            origin: origin(),
            sequence: Sequence::new(sequence),
            transaction: None,
            provenance: Provenance::default(),
        };
        store.append_remote(vec![entry(1), entry(2)]).await.unwrap();
        store
            .mark_applied(&origin(), Sequence::new(1))
            .await
            .unwrap();
        let pending = store.unapplied().await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].sequence, Sequence::new(2));
    }

    #[tokio::test]
    async fn reading_behind_the_compaction_point_demands_a_snapshot() {
        let store = MemoryStore::new();
        for _ in 0..3 {
            store
                .append_local(&origin(), vec![mutation()], None)
                .await
                .unwrap();
        }
        store.compact(&origin(), Sequence::new(2));
        let read = store
            .read(&origin(), Sequence::ZERO, Sequence::new(3), 10)
            .await
            .unwrap();
        assert_eq!(
            read,
            LogRead::Compacted {
                oldest: Sequence::new(3)
            }
        );
    }

    #[tokio::test]
    async fn a_read_stops_at_the_first_hole_in_the_log() {
        let store = MemoryStore::new();
        let entry = |sequence| LogEntry {
            origin: origin(),
            sequence: Sequence::new(sequence),
            transaction: None,
            provenance: Provenance::default(),
        };
        store.append_remote(vec![entry(1), entry(3)]).await.unwrap();
        let LogRead::Entries(entries) = store
            .read(&origin(), Sequence::ZERO, Sequence::new(3), 10)
            .await
            .unwrap()
        else {
            panic!("expected entries");
        };
        assert_eq!(entries.len(), 1);
    }
}
