//! Applying policy to a transaction.
//!
//! One function, used in both directions, so what is allowed to leave and
//! what is allowed to enter are decided by the same rule.

use crate::core::{Decision, Direction, PeerId, SyncPolicy, Transaction};

/// Returns the part of `transaction` that policy lets through, or `None` if nothing survives.
///
/// If some mutations are removed the result is marked
/// [`partial`](Transaction::partial): it is still atomic, but it is no longer
/// what the origin committed.
pub(crate) fn through_policy<P: SyncPolicy + ?Sized>(
    policy: &P,
    direction: Direction,
    peer: &PeerId,
    transaction: &Transaction,
) -> Option<Transaction> {
    let mutations: Vec<_> = transaction
        .mutations
        .iter()
        .filter(|mutation| {
            policy.decide(direction, peer, &transaction.origin, mutation) == Decision::Allow
        })
        .cloned()
        .collect();
    if mutations.is_empty() {
        return None;
    }
    let trimmed = mutations.len() != transaction.mutations.len();
    Some(Transaction {
        partial: transaction.partial || trimmed,
        mutations,
        ..transaction.clone()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{
        Collection, Mutation, Operation, Origin, RecordId, Sequence, StaticPolicy, TransactionId,
    };

    fn tx(collections: &[&str]) -> Transaction {
        Transaction::new(
            TransactionId::generate(),
            Origin::new("laptop").unwrap(),
            Sequence::new(1),
            collections
                .iter()
                .map(|c| {
                    Mutation::new(
                        RecordId::new(Collection::new(*c).unwrap(), "k"),
                        Operation::Create,
                        None,
                    )
                })
                .collect(),
        )
    }

    fn policy() -> StaticPolicy {
        StaticPolicy::new().send(
            PeerId::new("phone").unwrap(),
            [Collection::new("memories").unwrap()],
        )
    }

    #[test]
    fn a_transaction_wholly_allowed_passes_unchanged_and_complete() {
        let kept = through_policy(
            &policy(),
            Direction::Outbound,
            &PeerId::new("phone").unwrap(),
            &tx(&["memories"]),
        )
        .unwrap();
        assert!(!kept.partial);
        assert_eq!(kept.mutations.len(), 1);
    }

    #[test]
    fn a_transaction_wholly_denied_disappears() {
        assert!(
            through_policy(
                &policy(),
                Direction::Outbound,
                &PeerId::new("phone").unwrap(),
                &tx(&["entities"])
            )
            .is_none()
        );
    }

    #[test]
    fn a_partly_allowed_transaction_is_trimmed_and_marked_partial() {
        let kept = through_policy(
            &policy(),
            Direction::Outbound,
            &PeerId::new("phone").unwrap(),
            &tx(&["memories", "entities"]),
        )
        .unwrap();
        assert!(kept.partial);
        assert_eq!(kept.mutations.len(), 1);
    }
}
