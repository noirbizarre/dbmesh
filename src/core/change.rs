//! The logical change model.
//!
//! DBMesh moves *logical* changes ("record X of collection Y was created with
//! this payload"), never storage-engine pages or files. Nothing here knows
//! which database produced a change.

use std::fmt;

use serde::{Deserialize, Serialize};

use super::ids::{Collection, Origin, TransactionId};
use super::sequence::Sequence;

/// The identity of a record, independent of any database.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RecordId {
    /// The collection the record belongs to.
    pub collection: Collection,
    /// The record's key within its collection. Opaque: adapters decide what it means.
    pub key: String,
}

impl RecordId {
    /// Builds a record identity.
    #[must_use]
    pub fn new(collection: Collection, key: impl Into<String>) -> Self {
        Self {
            collection,
            key: key.into(),
        }
    }
}

impl fmt::Display for RecordId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.collection, self.key)
    }
}

/// What a change does to a record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Operation {
    /// The record came into existence.
    Create,
    /// The record's content changed.
    Update,
    /// The record was removed.
    Delete,
}

/// The content of a record, in a backend-neutral form.
///
/// A JSON value is the common denominator every adapter can map to and from.
/// It is a newtype so the representation can change without breaking callers.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Payload(pub serde_json::Value);

/// An adapter-defined version of a record.
///
/// DBMesh never interprets it, it only carries it so the receiving adapter can
/// tell whether a remote change was based on the state it holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Revision(pub u64);

/// One record-level effect inside a transaction.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Mutation {
    /// The record affected.
    pub record: RecordId,
    /// What happened to it.
    pub operation: Operation,
    /// The new content. Absent for deletions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Payload>,
    /// The version the author saw before this mutation (optimistic base).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base: Option<Revision>,
    /// The version this mutation produced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<Revision>,
}

impl Mutation {
    /// A mutation with no revision information.
    #[must_use]
    pub fn new(record: RecordId, operation: Operation, payload: Option<Payload>) -> Self {
        Self {
            record,
            operation,
            payload,
            base: None,
            revision: None,
        }
    }
}

/// A mutation together with where it came from.
///
/// Stored once per transaction (see [`Transaction`](super::Transaction)) and
/// materialized on demand, for example to describe a conflict.
#[derive(Clone, Debug, PartialEq)]
pub struct Change {
    /// The node that first committed it.
    pub origin: Origin,
    /// The transaction's position in the origin's history.
    pub sequence: Sequence,
    /// The transaction it belongs to.
    pub transaction: TransactionId,
    /// The effect itself.
    pub mutation: Mutation,
}

/// Where a database adapter's change feed stood when a transaction was captured.
///
/// Persisted next to the log so capture resumes exactly after the last
/// captured transaction. DBMesh never interprets it; for SurrealDB it is the
/// changefeed versionstamp.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SourcePosition(pub u64);
