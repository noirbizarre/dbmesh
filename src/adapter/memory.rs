//! A reference adapter over an in-memory record map.
//!
//! It exists to prove the adapter contracts and to let the engine be tested
//! without a real database. Conflict detection is optimistic: a mutation
//! carries the revision its author saw, and a mismatch with the local revision
//! is a conflict. That is deliberately simple; causality tracking is a
//! follow-up, and this adapter is not a model for production detection.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use super::{ApplyOutcome, CapturedTransaction, ChangeApplier, ChangeSource, Resolutions};
use crate::core::{
    Conflict, Mutation, Operation, Origin, Payload, RecordId, Resolution, Revision, Sequence,
    SourcePosition, Transaction,
};
use crate::error::Result;

#[derive(Clone)]
struct Record {
    payload: Payload,
    revision: Revision,
}

#[derive(Default)]
struct Inner {
    records: BTreeMap<RecordId, Record>,
    feed: Vec<CapturedTransaction>,
    // Which remote transactions were installed, so a replay is recognized.
    applied: BTreeSet<(Origin, Sequence)>,
}

/// An in-memory database that implements [`DatabaseAdapter`](super::DatabaseAdapter).
#[derive(Clone, Default)]
pub struct MemoryDatabase {
    inner: Arc<Mutex<Inner>>,
}

impl MemoryDatabase {
    /// An empty database.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Commits a local transaction and appends it to the change feed.
    ///
    /// The caller supplies operations and payloads; revisions are assigned here.
    pub fn commit(&self, mutations: Vec<Mutation>) -> SourcePosition {
        let mut inner = self.lock();
        let mut stamped = Vec::with_capacity(mutations.len());
        for mut mutation in mutations {
            let current = inner
                .records
                .get(&mutation.record)
                .map(|record| record.revision);
            mutation.base = current;
            match mutation.operation {
                Operation::Delete => {
                    inner.records.remove(&mutation.record);
                    mutation.revision = None;
                }
                _ => {
                    let revision = Revision(current.map_or(1, |r| r.0 + 1));
                    mutation.revision = Some(revision);
                    inner.records.insert(
                        mutation.record.clone(),
                        Record {
                            payload: mutation
                                .payload
                                .clone()
                                .unwrap_or(Payload(serde_json::Value::Null)),
                            revision,
                        },
                    );
                }
            }
            stamped.push(mutation);
        }
        let position = SourcePosition(inner.feed.len() as u64 + 1);
        inner.feed.push(CapturedTransaction {
            position,
            mutations: stamped,
        });
        position
    }

    /// The current content of a record.
    #[must_use]
    pub fn get(&self, record: &RecordId) -> Option<Payload> {
        self.lock().records.get(record).map(|r| r.payload.clone())
    }

    /// How many records exist.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().records.len()
    }

    /// Whether the database holds no record.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl ChangeSource for MemoryDatabase {
    async fn poll(
        &self,
        after: Option<SourcePosition>,
        limit: usize,
    ) -> Result<Vec<CapturedTransaction>> {
        let inner = self.lock();
        Ok(inner
            .feed
            .iter()
            .filter(|captured| after.is_none_or(|after| captured.position > after))
            .take(limit)
            .cloned()
            .collect())
    }
}

/// Whether `mutation` fits on top of `current`.
fn conflicts(mutation: &Mutation, current: Option<Revision>) -> bool {
    // No revision information at all: the author could not say what it saw,
    // so there is nothing to compare.
    if mutation.base.is_none() && mutation.revision.is_none() {
        return false;
    }
    // Deleting what is already gone reaches the author's intended state.
    if mutation.operation == Operation::Delete && current.is_none() {
        return false;
    }
    current != mutation.base
}

impl ChangeApplier for MemoryDatabase {
    async fn apply(
        &self,
        transaction: &Transaction,
        resolutions: &Resolutions,
    ) -> Result<ApplyOutcome> {
        // The lock is held from the conflict check to the last write: that is
        // this adapter's atomicity.
        let mut inner = self.lock();
        let key = (transaction.origin.clone(), transaction.sequence);
        if inner.applied.contains(&key) {
            return Ok(ApplyOutcome::AlreadyApplied);
        }

        // Phase one: look only, so a conflict leaves nothing half-installed.
        let mut found = Vec::new();
        for change in transaction.changes() {
            let record = &change.mutation.record;
            if resolutions.contains_key(record)
                || found.iter().any(|c: &Conflict| &c.record == record)
            {
                continue;
            }
            let current = inner.records.get(record);
            if conflicts(&change.mutation, current.map(|r| r.revision)) {
                // A conflict needs a local side; absence is described as a delete.
                let local = match current {
                    Some(r) => Mutation {
                        record: record.clone(),
                        operation: Operation::Update,
                        payload: Some(r.payload.clone()),
                        base: None,
                        revision: Some(r.revision),
                    },
                    None => Mutation::new(record.clone(), Operation::Delete, None),
                };
                found.push(Conflict {
                    record: record.clone(),
                    local,
                    remote: change,
                });
            }
        }
        if !found.is_empty() {
            return Ok(ApplyOutcome::Conflicted(found));
        }

        // Phase two: install.
        for mutation in &transaction.mutations {
            let current = inner.records.get(&mutation.record).map(|r| r.revision);
            let effective = match resolutions.get(&mutation.record) {
                None | Some(Resolution::AcceptRemote) => Some(mutation.clone()),
                Some(Resolution::Merge(merged)) => {
                    // The merged state supersedes both sides, so it must out-rank them.
                    let rank = current.max(mutation.revision).map_or(1, |r| r.0 + 1);
                    let mut merged = merged.clone();
                    merged.record = mutation.record.clone();
                    merged.revision = merged.revision.or(Some(Revision(rank)));
                    Some(merged)
                }
                // Keeping the local state, or a rejection the engine already handled.
                Some(_) => None,
            };
            let Some(effective) = effective else { continue };
            match effective.operation {
                Operation::Delete => {
                    inner.records.remove(&effective.record);
                }
                _ => {
                    let revision = effective
                        .revision
                        .unwrap_or_else(|| Revision(current.map_or(1, |r| r.0 + 1)));
                    inner.records.insert(
                        effective.record.clone(),
                        Record {
                            payload: effective
                                .payload
                                .unwrap_or(Payload(serde_json::Value::Null)),
                            revision,
                        },
                    );
                }
            }
        }
        // Deliberately not pushed to the feed: remote changes must not echo back.
        inner.applied.insert(key);
        Ok(ApplyOutcome::Applied)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::core::{Collection, TransactionId};

    fn record(key: &str) -> RecordId {
        RecordId::new(Collection::new("memories").unwrap(), key)
    }

    fn write(operation: Operation, key: &str, value: i64) -> Mutation {
        Mutation::new(record(key), operation, Some(Payload(json!({ "v": value }))))
    }

    /// Captures `source`'s feed as a transaction as `origin` would number it.
    async fn captured(source: &MemoryDatabase, origin: &str, sequence: u64) -> Transaction {
        let feed = source.poll(None, usize::MAX).await.unwrap();
        Transaction::new(
            TransactionId::generate(),
            Origin::new(origin).unwrap(),
            Sequence::new(sequence),
            feed[sequence as usize - 1].mutations.clone(),
        )
    }

    #[tokio::test]
    async fn applied_changes_do_not_reappear_in_the_change_feed() {
        let (a, b) = (MemoryDatabase::new(), MemoryDatabase::new());
        a.commit(vec![write(Operation::Create, "x", 1)]);
        b.apply(&captured(&a, "a", 1).await, &Resolutions::new())
            .await
            .unwrap();
        assert!(b.poll(None, 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn applying_a_transaction_twice_is_recognized_as_a_replay() {
        let (a, b) = (MemoryDatabase::new(), MemoryDatabase::new());
        a.commit(vec![write(Operation::Create, "x", 1)]);
        let tx = captured(&a, "a", 1).await;
        assert_eq!(
            b.apply(&tx, &Resolutions::new()).await.unwrap(),
            ApplyOutcome::Applied
        );
        assert_eq!(
            b.apply(&tx, &Resolutions::new()).await.unwrap(),
            ApplyOutcome::AlreadyApplied
        );
    }

    #[tokio::test]
    async fn a_transaction_is_applied_whole_or_not_at_all() {
        let (a, b) = (MemoryDatabase::new(), MemoryDatabase::new());
        a.commit(vec![
            write(Operation::Create, "x", 1),
            write(Operation::Create, "y", 1),
        ]);
        // b already holds a different `y`, so the second mutation conflicts.
        b.commit(vec![write(Operation::Create, "y", 9)]);
        let outcome = b
            .apply(&captured(&a, "a", 1).await, &Resolutions::new())
            .await
            .unwrap();
        assert!(matches!(outcome, ApplyOutcome::Conflicted(_)));
        assert_eq!(
            b.get(&record("x")),
            None,
            "the clean mutation must not leak in"
        );
    }

    #[tokio::test]
    async fn a_resolved_conflict_applies_on_the_second_attempt() {
        let (a, b) = (MemoryDatabase::new(), MemoryDatabase::new());
        a.commit(vec![write(Operation::Create, "x", 1)]);
        b.commit(vec![write(Operation::Create, "x", 2)]);
        let tx = captured(&a, "a", 1).await;
        let ApplyOutcome::Conflicted(conflicts) = b.apply(&tx, &Resolutions::new()).await.unwrap()
        else {
            panic!("expected a conflict");
        };
        let resolutions: Resolutions = conflicts
            .iter()
            .map(|c| (c.record.clone(), Resolution::AcceptRemote))
            .collect();
        assert_eq!(
            b.apply(&tx, &resolutions).await.unwrap(),
            ApplyOutcome::Applied
        );
        assert_eq!(b.get(&record("x")), Some(Payload(json!({ "v": 1 }))));
    }

    #[tokio::test]
    async fn deleting_a_record_that_is_already_gone_is_not_a_conflict() {
        let (a, b) = (MemoryDatabase::new(), MemoryDatabase::new());
        a.commit(vec![write(Operation::Create, "x", 1)]);
        a.commit(vec![Mutation::new(record("x"), Operation::Delete, None)]);
        // b never saw the record at all.
        let tx = captured(&a, "a", 2).await;
        assert_eq!(
            b.apply(&tx, &Resolutions::new()).await.unwrap(),
            ApplyOutcome::Applied
        );
    }
}
