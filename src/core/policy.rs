//! Synchronization policy: what may go where.
//!
//! The host application owns this decision. DBMesh evaluates the policy
//! *before* a change is sent and again *before* a received change is applied,
//! so a misbehaving peer cannot push data the application never agreed to
//! take. The default is to allow nothing.

use std::collections::{BTreeMap, BTreeSet};

use super::change::Mutation;
use super::ids::{Collection, Origin, PeerId};

/// Which way a mutation is travelling, relative to this node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    /// Leaving this node towards the peer.
    Outbound,
    /// Arriving from the peer, about to be applied.
    Inbound,
}

/// What to do with one mutation.
///
/// Non-exhaustive so redaction or field-level filtering can be added without
/// breaking implementors that match on it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Decision {
    /// Let it through.
    Allow,
    /// Leave it out. The cursor still moves past it.
    Skip,
}

/// Decides, per peer and per mutation, what may be synchronized.
///
/// Evaluation is synchronous and must be a pure function of its arguments:
/// policy is consulted while building batches, and a policy that does I/O
/// would make session behaviour non-deterministic. The mutation is passed
/// whole so record-level and field-level rules remain expressible later.
pub trait SyncPolicy: Send + Sync {
    /// Decides the fate of `mutation`, authored at `origin`, with respect to `peer`.
    fn decide(
        &self,
        direction: Direction,
        peer: &PeerId,
        origin: &Origin,
        mutation: &Mutation,
    ) -> Decision;
}

/// The default: nothing is synchronized with anyone.
#[derive(Clone, Copy, Debug, Default)]
pub struct DenyAll;

impl SyncPolicy for DenyAll {
    fn decide(&self, _: Direction, _: &PeerId, _: &Origin, _: &Mutation) -> Decision {
        Decision::Skip
    }
}

/// A collection allow-list per peer and direction, with a global deny-list.
///
/// Peers that are not listed get nothing, and a collection on the deny-list
/// never leaves or enters the node whatever else is configured (migrations,
/// caches, DBMesh's own bookkeeping).
#[derive(Clone, Debug, Default)]
pub struct StaticPolicy {
    never: BTreeSet<Collection>,
    send: BTreeMap<PeerId, BTreeSet<Collection>>,
    receive: BTreeMap<PeerId, BTreeSet<Collection>>,
}

impl StaticPolicy {
    /// A policy that allows nothing yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Collections that are never synchronized, with anyone, in either direction.
    #[must_use]
    pub fn never(mut self, collections: impl IntoIterator<Item = Collection>) -> Self {
        self.never.extend(collections);
        self
    }

    /// Allows sending `collections` to `peer`.
    #[must_use]
    pub fn send(mut self, peer: PeerId, collections: impl IntoIterator<Item = Collection>) -> Self {
        self.send.entry(peer).or_default().extend(collections);
        self
    }

    /// Allows accepting `collections` from `peer`.
    #[must_use]
    pub fn receive(
        mut self,
        peer: PeerId,
        collections: impl IntoIterator<Item = Collection>,
    ) -> Self {
        self.receive.entry(peer).or_default().extend(collections);
        self
    }

    /// Allows `collections` to flow both ways with `peer`.
    #[must_use]
    pub fn exchange(
        self,
        peer: PeerId,
        collections: impl IntoIterator<Item = Collection> + Clone,
    ) -> Self {
        self.send(peer.clone(), collections.clone())
            .receive(peer, collections)
    }
}

impl SyncPolicy for StaticPolicy {
    fn decide(
        &self,
        direction: Direction,
        peer: &PeerId,
        _: &Origin,
        mutation: &Mutation,
    ) -> Decision {
        let collection = &mutation.record.collection;
        if self.never.contains(collection) {
            return Decision::Skip;
        }
        let allowed = match direction {
            Direction::Outbound => &self.send,
            Direction::Inbound => &self.receive,
        };
        match allowed.get(peer) {
            Some(collections) if collections.contains(collection) => Decision::Allow,
            _ => Decision::Skip,
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::core::{Operation, RecordId};

    fn mutation(collection: &str) -> Mutation {
        Mutation::new(
            RecordId::new(Collection::new(collection).unwrap(), "k"),
            Operation::Create,
            None,
        )
    }

    fn policy() -> StaticPolicy {
        let collections = |names: &[&str]| {
            names
                .iter()
                .map(|n| Collection::new(*n).unwrap())
                .collect::<Vec<_>>()
        };
        StaticPolicy::new()
            .never(collections(&["migrations"]))
            .exchange(
                PeerId::new("server").unwrap(),
                collections(&["memories", "entities", "migrations"]),
            )
            .exchange(PeerId::new("phone").unwrap(), collections(&["memories"]))
    }

    #[rstest]
    #[case("server", "memories", Direction::Outbound, Decision::Allow)]
    #[case("server", "entities", Direction::Inbound, Decision::Allow)]
    #[case("phone", "memories", Direction::Outbound, Decision::Allow)]
    #[case("phone", "entities", Direction::Outbound, Decision::Skip)]
    #[case("phone", "embeddings", Direction::Inbound, Decision::Skip)]
    #[case("stranger", "memories", Direction::Outbound, Decision::Skip)]
    #[case("server", "migrations", Direction::Outbound, Decision::Skip)]
    fn each_peer_gets_only_the_collections_it_was_granted(
        #[case] peer: &str,
        #[case] collection: &str,
        #[case] direction: Direction,
        #[case] expected: Decision,
    ) {
        let origin = Origin::new("laptop").unwrap();
        let got = policy().decide(
            direction,
            &PeerId::new(peer).unwrap(),
            &origin,
            &mutation(collection),
        );
        assert_eq!(got, expected);
    }

    #[test]
    fn sending_permission_does_not_imply_receiving_permission() {
        let peer = PeerId::new("server").unwrap();
        let policy = StaticPolicy::new().send(peer.clone(), [Collection::new("memories").unwrap()]);
        let origin = Origin::new("laptop").unwrap();
        assert_eq!(
            policy.decide(Direction::Inbound, &peer, &origin, &mutation("memories")),
            Decision::Skip
        );
    }

    #[test]
    fn the_default_policy_allows_nothing() {
        let peer = PeerId::new("server").unwrap();
        let origin = Origin::new("laptop").unwrap();
        assert_eq!(
            DenyAll.decide(Direction::Outbound, &peer, &origin, &mutation("memories")),
            Decision::Skip
        );
    }
}
