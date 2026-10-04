//! Sequence numbers, cursors and checkpoints: the durable measure of progress.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use super::ids::{Origin, PeerId};

/// A position in one origin's history.
///
/// Sequences are assigned per *committed transaction*, per origin, with no
/// gaps, starting at 1. [`Sequence::ZERO`] means "nothing yet". Because a
/// sequence names a whole transaction, a cursor can never point into the
/// middle of one.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Sequence(u64);

impl Sequence {
    /// "Nothing yet": the position before the first transaction.
    pub const ZERO: Self = Self(0);

    /// Wraps a raw number.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// The raw number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The sequence right after this one.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl fmt::Display for Sequence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// "Everything from `origin` up to and including `through` has been processed."
///
/// Processed means applied, or deliberately skipped by policy: progress is
/// about what the holder has dealt with, not about what it kept.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    /// Whose history.
    pub origin: Origin,
    /// The last processed transaction.
    pub through: Sequence,
}

/// One cursor per origin: a version vector.
///
/// This is the whole of what a node needs to say to ask "send me what I am
/// missing". It only ever moves forward.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CursorSet(BTreeMap<Origin, Sequence>);

impl CursorSet {
    /// An empty set: nothing processed from anyone.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The position for `origin`, [`Sequence::ZERO`] when it has never been seen.
    #[must_use]
    pub fn get(&self, origin: &Origin) -> Sequence {
        self.0.get(origin).copied().unwrap_or(Sequence::ZERO)
    }

    /// Moves `origin` forward to `through`, never backwards.
    ///
    /// Returns whether anything changed. Refusing to regress is what makes
    /// replaying an old acknowledgement harmless.
    pub fn advance(&mut self, origin: &Origin, through: Sequence) -> bool {
        if through <= self.get(origin) {
            return false;
        }
        self.0.insert(origin.clone(), through);
        true
    }

    /// Iterates cursors in origin order, which keeps behaviour deterministic.
    pub fn iter(&self) -> impl Iterator<Item = (&Origin, Sequence)> {
        self.0.iter().map(|(origin, seq)| (origin, *seq))
    }

    /// Whether no origin has been seen.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl FromIterator<(Origin, Sequence)> for CursorSet {
    fn from_iter<T: IntoIterator<Item = (Origin, Sequence)>>(iter: T) -> Self {
        let mut set = Self::new();
        for (origin, seq) in iter {
            set.advance(&origin, seq);
        }
        set
    }
}

/// What a peer has confirmed it holds, as last acknowledged to us.
///
/// A checkpoint is persisted. It is *not* what drives a session (the peer's
/// own `Have` message does, so a lost checkpoint is never fatal); it records
/// observed progress for recovery, diagnostics and log compaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    /// The peer that acknowledged.
    pub peer: PeerId,
    /// What it acknowledged, per origin.
    pub acked: CursorSet,
}

impl Checkpoint {
    /// A checkpoint with no acknowledgements yet.
    #[must_use]
    pub fn new(peer: PeerId) -> Self {
        Self {
            peer,
            acked: CursorSet::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn origin(name: &str) -> Origin {
        Origin::new(name).unwrap()
    }

    #[test]
    fn a_cursor_never_moves_backwards() {
        let mut cursors = CursorSet::new();
        assert!(cursors.advance(&origin("a"), Sequence::new(5)));
        assert!(!cursors.advance(&origin("a"), Sequence::new(3)));
        assert_eq!(cursors.get(&origin("a")), Sequence::new(5));
    }

    #[test]
    fn replaying_the_same_acknowledgement_changes_nothing() {
        let mut cursors = CursorSet::new();
        cursors.advance(&origin("a"), Sequence::new(5));
        assert!(!cursors.advance(&origin("a"), Sequence::new(5)));
    }

    #[test]
    fn an_unseen_origin_is_at_zero() {
        assert_eq!(CursorSet::new().get(&origin("nobody")), Sequence::ZERO);
    }

    #[test]
    fn a_cursor_set_serializes_as_a_plain_map() {
        let set: CursorSet = [(origin("laptop"), Sequence::new(18427))]
            .into_iter()
            .collect();
        assert_eq!(serde_json::to_string(&set).unwrap(), r#"{"laptop":18427}"#);
    }
}
