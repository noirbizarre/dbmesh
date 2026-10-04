//! Transactions and batches: the units that cross the wire.

use serde::{Deserialize, Serialize};

use super::change::{Change, Mutation};
use super::ids::{Origin, PeerId, TransactionId};
use super::sequence::Sequence;
use crate::error::{Error, Result};

/// The widest range of sequences one batch may cover.
///
/// A peer states `through` itself; without a bound, a hostile one could make a
/// receiver allocate a log slot for every sequence up to `u64::MAX`.
pub const MAX_BATCH_SPAN: u64 = 100_000;

fn is_false(value: &bool) -> bool {
    !*value
}

/// An atomic group of mutations, committed together at one origin.
///
/// It is the smallest thing DBMesh transfers or applies. A remote peer must
/// never observe half of it, which is why batches carry whole transactions and
/// why a sequence number names a transaction rather than a mutation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Transaction {
    /// Stable identity, preserved across relays.
    pub id: TransactionId,
    /// The node that committed it.
    pub origin: Origin,
    /// Its position in the origin's history.
    pub sequence: Sequence,
    /// Set when policy removed some mutations on the way.
    ///
    /// A partial transaction is still applied atomically, but it is no longer
    /// the transaction the origin committed, so it must never be relayed as if
    /// it were: whoever holds only a part cannot vouch for the whole.
    #[serde(default, skip_serializing_if = "is_false")]
    pub partial: bool,
    /// The effects, in commit order.
    pub mutations: Vec<Mutation>,
}

impl Transaction {
    /// Builds a complete transaction.
    #[must_use]
    pub fn new(
        id: TransactionId,
        origin: Origin,
        sequence: Sequence,
        mutations: Vec<Mutation>,
    ) -> Self {
        Self {
            id,
            origin,
            sequence,
            partial: false,
            mutations,
        }
    }

    /// The mutations as [`Change`]s carrying their origin and transaction.
    pub fn changes(&self) -> impl Iterator<Item = Change> + '_ {
        self.mutations.iter().map(|mutation| Change {
            origin: self.origin.clone(),
            sequence: self.sequence,
            transaction: self.id.clone(),
            mutation: mutation.clone(),
        })
    }
}

/// A contiguous slice of one origin's history, in flight.
///
/// It covers the half-open range `(from, through]`. Transactions skipped by
/// policy are simply absent, yet `through` still moves past them: the
/// receiver learns that the range was *handled* without learning what was in
/// it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ChangeBatch {
    /// Whose history this is.
    pub origin: Origin,
    /// The receiver's cursor this batch builds on (exclusive).
    pub from: Sequence,
    /// The last sequence this batch accounts for (inclusive).
    pub through: Sequence,
    /// How many links the data travelled to reach the sender (0 when the sender is the origin).
    #[serde(default)]
    pub hops: u32,
    /// The transactions that survived policy, in sequence order.
    pub transactions: Vec<Transaction>,
}

impl ChangeBatch {
    /// Checks the structural rules a receiver relies on.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidBatch`] when the range is inverted, or a
    /// transaction is for another origin, outside the range, or out of order.
    pub fn validate(&self) -> Result<()> {
        let invalid = |reason: &str| Error::InvalidBatch {
            origin: self.origin.to_string(),
            reason: reason.to_owned(),
        };
        if self.through < self.from {
            return Err(invalid("`through` is before `from`"));
        }
        if self.through.get() - self.from.get() > MAX_BATCH_SPAN {
            return Err(invalid("it covers more sequences than a batch may"));
        }
        let mut previous = self.from;
        for tx in &self.transactions {
            if tx.origin != self.origin {
                return Err(invalid("it contains a transaction from another origin"));
            }
            if tx.sequence <= previous || tx.sequence > self.through {
                return Err(invalid(
                    "a transaction is out of order or outside the covered range",
                ));
            }
            previous = tx.sequence;
        }
        Ok(())
    }
}

/// How a transaction reached this node. Origin, sender and receiver are
/// distinct so propagation can be reasoned about.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Provenance {
    /// The immediate sender; `None` for transactions committed locally.
    pub sender: Option<PeerId>,
    /// Links travelled from the origin to this node.
    pub hops: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Collection, Operation, RecordId};

    fn tx(origin: &str, seq: u64) -> Transaction {
        Transaction::new(
            TransactionId::generate(),
            Origin::new(origin).unwrap(),
            Sequence::new(seq),
            vec![Mutation::new(
                RecordId::new(Collection::new("memories").unwrap(), "foo"),
                Operation::Create,
                None,
            )],
        )
    }

    fn batch(from: u64, through: u64, transactions: Vec<Transaction>) -> ChangeBatch {
        ChangeBatch {
            origin: Origin::new("a").unwrap(),
            from: Sequence::new(from),
            through: Sequence::new(through),
            hops: 0,
            transactions,
        }
    }

    #[test]
    fn a_batch_with_gaps_covered_by_through_is_valid() {
        assert!(batch(0, 5, vec![tx("a", 2), tx("a", 4)]).validate().is_ok());
    }

    #[test]
    fn an_inverted_range_is_rejected() {
        assert!(batch(5, 3, vec![]).validate().is_err());
    }

    #[test]
    fn a_transaction_beyond_through_is_rejected() {
        assert!(batch(0, 2, vec![tx("a", 3)]).validate().is_err());
    }

    #[test]
    fn a_transaction_at_or_before_from_is_rejected() {
        assert!(batch(2, 5, vec![tx("a", 2)]).validate().is_err());
    }

    #[test]
    fn transactions_out_of_order_are_rejected() {
        assert!(
            batch(0, 5, vec![tx("a", 4), tx("a", 2)])
                .validate()
                .is_err()
        );
    }

    #[test]
    fn a_foreign_transaction_is_rejected() {
        assert!(batch(0, 5, vec![tx("b", 1)]).validate().is_err());
    }

    #[test]
    fn changes_inherit_origin_sequence_and_transaction() {
        let t = tx("a", 7);
        let change = t.changes().next().unwrap();
        assert_eq!(change.sequence, Sequence::new(7));
        assert_eq!(change.transaction, t.id);
    }
}
