//! The core layer: domain types and the boundaries the host application owns.
//!
//! Nothing in here knows about wire formats, transports, storage back-ends or
//! any particular database. That independence is what lets this module become
//! its own crate later without untangling anything, so it must not import
//! from its siblings.

mod change;
mod conflict;
mod ids;
mod membership;
mod peer;
mod policy;
mod security;
mod sequence;
mod transaction;

pub use change::{Change, Mutation, Operation, Payload, RecordId, Revision, SourcePosition};
pub use conflict::{Conflict, ConflictResolver, RejectOnConflict, Resolution};
pub use ids::{
    Capability, Collection, NodeId, Origin, PeerId, SessionId, TransactionId, TransportKind,
};
pub use membership::{Membership, PeerDiscovery, StaticDiscovery};
pub use peer::{Capabilities, Endpoint, Peer, PeerState, PeerStatus};
pub use policy::{Decision, DenyAll, Direction, StaticPolicy, SyncPolicy};
pub use security::{
    Authentication, Authenticator, Authorization, Authorizer, ConnectionInfo, Credential,
    DenyUnknown, StaticTrust, TransportCapability,
};
pub use sequence::{Checkpoint, Cursor, CursorSet, Sequence};
pub use transaction::{ChangeBatch, MAX_BATCH_SPAN, Provenance, Transaction};
