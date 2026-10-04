//! The database adapter boundary.
//!
//! An adapter connects DBMesh's logical model to one database. It does two
//! jobs, kept as two traits so each can be implemented, tested and reasoned
//! about separately:
//!
//! * [`ChangeSource`] — *capture*: report committed local transactions, in
//!   order, each with a [`SourcePosition`] to resume from.
//! * [`ChangeApplier`] — *apply*: install a remote transaction atomically,
//!   idempotently, reporting conflicts rather than deciding them.
//!
//! Nothing database-specific crosses this boundary: only [`Mutation`]s,
//! [`Transaction`]s and positions. The engine never learns which database
//! sits behind it.
//!
//! # Contracts an adapter must honour
//!
//! 1. **Atomic apply.** Either every mutation of a transaction is installed or
//!    none is. Where the database has transactions, use one.
//! 2. **Idempotent apply.** Applying the same transaction twice leaves the same
//!    state and returns [`ApplyOutcome::AlreadyApplied`] or
//!    [`ApplyOutcome::Applied`]. The engine replays after a crash and cannot
//!    know whether the previous attempt reached the database.
//! 3. **No echo.** Changes installed by `apply` must not come back out of the
//!    [`ChangeSource`] as local changes. Otherwise every change would bounce
//!    between nodes forever.
//! 4. **Ordered capture.** `poll` returns transactions in commit order, and a
//!    transaction is reported whole or not at all.

mod memory;
#[cfg(feature = "surrealdb")]
pub mod surrealdb;

use std::collections::BTreeMap;
use std::future::Future;

pub use memory::MemoryDatabase;

use crate::core::{Conflict, Mutation, RecordId, Resolution, SourcePosition, Transaction};
use crate::error::Result;

/// A locally committed transaction, as seen by the change source.
#[derive(Clone, Debug, PartialEq)]
pub struct CapturedTransaction {
    /// The feed position right after this transaction.
    pub position: SourcePosition,
    /// Its effects, in commit order.
    pub mutations: Vec<Mutation>,
}

/// Reports local changes.
pub trait ChangeSource: Send + Sync {
    /// Returns up to `limit` transactions committed after `after`, oldest first.
    /// `None` means "from the beginning of the feed".
    fn poll(
        &self,
        after: Option<SourcePosition>,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<CapturedTransaction>>> + Send;
}

/// The resolutions chosen for conflicts, by record.
pub type Resolutions = BTreeMap<RecordId, Resolution>;

/// What happened when a remote transaction was offered to the database.
#[derive(Clone, Debug, PartialEq)]
pub enum ApplyOutcome {
    /// The transaction was installed.
    Applied,
    /// The database had already installed it; nothing changed.
    AlreadyApplied,
    /// It does not fit the local state and nothing was installed. Resolve each
    /// conflict, then offer the transaction again with the resolutions.
    Conflicted(Vec<Conflict>),
}

/// Installs remote changes.
pub trait ChangeApplier: Send + Sync {
    /// Installs `transaction`, honouring `resolutions` for records that conflicted before.
    ///
    /// On the first attempt `resolutions` is empty. If the adapter reports
    /// [`ApplyOutcome::Conflicted`], it must apply cleanly once every conflicting
    /// record has an entry.
    fn apply(
        &self,
        transaction: &Transaction,
        resolutions: &Resolutions,
    ) -> impl Future<Output = Result<ApplyOutcome>> + Send;
}

/// A database that can both be captured from and applied to.
pub trait DatabaseAdapter: ChangeSource + ChangeApplier {}

impl<T: ChangeSource + ChangeApplier> DatabaseAdapter for T {}
