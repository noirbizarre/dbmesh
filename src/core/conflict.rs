//! The conflict boundary.
//!
//! DBMesh detects nothing and decides nothing about conflicts on its own:
//! the database adapter reports that a remote change does not fit the local
//! state, and the host application's [`ConflictResolver`] says what happens.
//! There is deliberately no built-in last-write-wins; what is "right" is a
//! property of the application's data, not of the mesh.

use std::future::Future;

use super::change::{Change, Mutation, RecordId};

/// A remote change that cannot be applied on top of the local state as is.
#[derive(Clone, Debug, PartialEq)]
pub struct Conflict {
    /// The record both sides touched.
    pub record: RecordId,
    /// The local side, described as the mutation that would recreate its current state.
    pub local: Mutation,
    /// The incoming change.
    pub remote: Change,
}

/// What to do about a [`Conflict`].
///
/// Non-exhaustive: richer strategies (field-level merges, deferral to a human)
/// can be added without breaking resolvers written today.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum Resolution {
    /// Keep the local state; the remote mutation is dropped.
    AcceptLocal,
    /// Overwrite with the remote mutation.
    AcceptRemote,
    /// Apply this application-computed mutation instead of either side.
    Merge(Mutation),
    /// Refuse the whole remote transaction. It is never retried: the cursor
    /// moves past it and the divergence is the application's decision.
    Reject {
        /// Why, for the application's own logs.
        reason: String,
    },
}

/// Decides conflicts. The one place application-specific merge semantics live.
pub trait ConflictResolver: Send + Sync {
    /// Resolves a single conflict.
    fn resolve(&self, conflict: &Conflict) -> impl Future<Output = Resolution> + Send;
}

/// The default: refuse any transaction that conflicts.
///
/// Refusing is the only choice that loses no *local* data and invents no
/// semantics; an application that wants merging opts in explicitly.
#[derive(Clone, Copy, Debug, Default)]
pub struct RejectOnConflict;

impl ConflictResolver for RejectOnConflict {
    async fn resolve(&self, conflict: &Conflict) -> Resolution {
        Resolution::Reject {
            reason: format!("conflicting change on `{}`", conflict.record),
        }
    }
}
