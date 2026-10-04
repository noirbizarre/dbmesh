//! The error type, and the diagnostics it renders to.
//!
//! `thiserror` defines them, `miette` renders them. A diagnostic must carry the
//! two things the user does not already know: what specifically failed, and
//! what to do about it.

use miette::Diagnostic;
use thiserror::Error;

use crate::core::PeerId;

/// The crate's result type.
pub type Result<T> = std::result::Result<T, Error>;

/// A boxed error from a host-supplied implementation (store, transport, adapter).
///
/// DBMesh cannot know the concrete error of an implementation it does not own,
/// so it keeps the chain intact instead of flattening it to a string.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Everything that can go wrong.
///
/// Diagnostic codes are `dbmesh::<module>::<kind>`. A code is a public
/// identifier users grep for, so renaming one is a breaking change.
#[derive(Debug, Error, Diagnostic)]
#[non_exhaustive]
pub enum Error {
    /// A name-like value (node, peer, collection...) failed validation.
    #[error("invalid {kind} `{value}`: {reason}")]
    #[diagnostic(
        code(dbmesh::core::invalid_name),
        help("use 1 to 128 visible characters without whitespace or control characters")
    )]
    InvalidName {
        /// Which kind of name was being built (for example `node id`).
        kind: &'static str,
        /// The rejected value.
        value: String,
        /// Why it was rejected.
        reason: &'static str,
    },

    /// A batch violated the structural rules of the change model.
    #[error("invalid change batch from `{origin}`: {reason}")]
    #[diagnostic(
        code(dbmesh::core::invalid_batch),
        help(
            "a batch must cover (from, through] of one origin with strictly increasing transactions inside it"
        )
    )]
    InvalidBatch {
        /// The origin the batch claims to carry.
        origin: String,
        /// The rule that was broken.
        reason: String,
    },

    /// A frame could not be decoded.
    #[error("malformed protocol frame")]
    #[diagnostic(
        code(dbmesh::protocol::malformed),
        help("the peer sent bytes that are not a DBMesh frame; check it speaks the same protocol")
    )]
    Malformed(#[source] serde_json::Error),

    /// A frame could not be encoded.
    #[error("failed to encode a protocol frame")]
    #[diagnostic(code(dbmesh::protocol::encode))]
    Encode(#[source] serde_json::Error),

    /// The metadata store failed.
    #[error("the DBMesh metadata store failed")]
    #[diagnostic(
        code(dbmesh::storage::failed),
        help(
            "synchronization state is untouched; fix the store and retry, DBMesh resumes from its last checkpoint"
        )
    )]
    Storage(#[source] BoxError),

    /// The store holds an identity that contradicts the one configured.
    #[error("the store belongs to node `{stored}` but the mesh was configured as `{configured}`")]
    #[diagnostic(
        code(dbmesh::storage::identity_mismatch),
        help(
            "pointing a different node at this store would fork its sequence numbers; use a store per node"
        )
    )]
    IdentityMismatch {
        /// The identity persisted in the store.
        stored: String,
        /// The identity the builder was given.
        configured: String,
    },

    /// A transport failed to connect, send or receive.
    #[error("transport failure")]
    #[diagnostic(
        code(dbmesh::transport::failed),
        help(
            "the peer may be offline; synchronization resumes from the last checkpoint on the next session"
        )
    )]
    Transport(#[source] BoxError),

    /// The transport lacks a capability the protocol relies on.
    #[error("transport `{transport}` does not provide `{missing}`")]
    #[diagnostic(
        code(dbmesh::transport::unsuitable),
        help(
            "use a transport that delivers whole messages reliably, or wrap this one in a layer that does"
        )
    )]
    UnsuitableTransport {
        /// The transport kind.
        transport: String,
        /// The capability it lacks.
        missing: String,
    },

    /// The database adapter failed.
    #[error("the database adapter failed")]
    #[diagnostic(
        code(dbmesh::adapter::failed),
        help(
            "the transaction stays in the durable log and is replayed by `recover()`; fix the adapter and restart"
        )
    )]
    Adapter(#[source] BoxError),

    /// The adapter reported a conflict again after it had been resolved.
    #[error("record `{record}` still conflicts after resolution")]
    #[diagnostic(
        code(dbmesh::adapter::conflict_unresolved),
        help(
            "an adapter must apply a transaction without conflicts once every conflict carries a resolution"
        )
    )]
    ConflictUnresolved {
        /// The record that kept conflicting.
        record: String,
    },

    /// No such peer is known to this node.
    #[error("unknown peer `{0}`")]
    #[diagnostic(
        code(dbmesh::engine::unknown_peer),
        help("register the peer with `DbMesh::register_peer` before synchronizing with it")
    )]
    UnknownPeer(PeerId),
}
