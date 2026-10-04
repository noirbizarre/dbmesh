//! The messages two nodes exchange.
//!
//! A session is: handshake (`Hello`/`Accept`), then each side states what it
//! already holds (`Have`) and is sent what is missing (`Batch`, answered by
//! `Ack`), then each side says it has nothing more (`Done`).
//!
//! Nothing in a message refers to a previous *connection*. Everything a peer
//! needs to resume is in its own durable cursors, which it states in `Have`.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::version::{ProtocolVersion, VersionRange};
use crate::core::{
    Capabilities, Capability, ChangeBatch, Credential, CursorSet, NodeId, Origin, Sequence,
};

/// Why a handshake was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RejectReason {
    /// No protocol version is common to both sides.
    IncompatibleVersion,
    /// A required capability is not available.
    UnsupportedCapability,
    /// The peer is not authenticated or not authorized.
    Unauthorized,
    /// The responder is not the node the initiator meant to reach.
    UnexpectedIdentity,
    /// A reason this build does not know. Kept so a newer peer's refusal still parses.
    #[serde(other)]
    Unknown,
}

/// Why a batch or message was refused mid-session.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum NackReason {
    /// The batch starts beyond what the receiver holds: something is missing between.
    Gap,
    /// The batch breaks a structural rule.
    InvalidBatch,
    /// The receiver could not apply it.
    ApplyFailed,
    /// The message does not belong at this point of the session.
    UnexpectedMessage,
    /// A reason this build does not know.
    #[serde(other)]
    Unknown,
}

/// One protocol message.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    /// The initiator's opening: who it is, what it speaks, what it insists on.
    Hello {
        /// The initiator's identity (a claim until authenticated).
        node: NodeId,
        /// The protocol versions it can speak.
        versions: VersionRange,
        /// The features it offers and requires.
        capabilities: Capabilities,
        /// Proof of identity, if the host application uses one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        credential: Option<Credential>,
    },
    /// The responder's answer: the settled terms of the session.
    Accept {
        /// The responder's identity (a claim until authenticated).
        node: NodeId,
        /// The version both sides will use.
        version: ProtocolVersion,
        /// The capabilities active for this session.
        capabilities: BTreeSet<Capability>,
        /// Proof of identity, if the host application uses one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        credential: Option<Credential>,
    },
    /// The handshake was refused; the session ends.
    Reject {
        /// Why, machine-readable.
        reason: RejectReason,
        /// Why, for humans.
        detail: String,
    },
    /// "This is what I have processed, per origin": the durable cursors.
    ///
    /// It doubles as the advertisement of the sender's heads: each side learns
    /// exactly what the other is missing, with no session state remembered
    /// from before.
    Have {
        /// The sender's cursors.
        cursors: CursorSet,
    },
    /// Changes the receiver is missing.
    Batch(ChangeBatch),
    /// "Everything from `origin` through `through` is durably processed."
    Ack {
        /// Whose history.
        origin: Origin,
        /// The last processed sequence.
        through: Sequence,
    },
    /// A batch or message was refused; the session ends and can be retried.
    Nack {
        /// Why, machine-readable.
        reason: NackReason,
        /// Why, for humans.
        detail: String,
        /// The receiver's current position for the batch's origin, when relevant.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        have: Option<Sequence>,
    },
    /// The sender has nothing more to send in this session.
    Done,
    /// The sender is leaving.
    Goodbye {
        /// Why, for humans.
        reason: String,
    },
    /// The sender received something it does not understand and says so.
    Unsupported {
        /// The message type or feature that was not understood.
        feature: String,
    },
}

impl Message {
    /// The message types of this protocol version, as they appear on the wire.
    ///
    /// Used to tell "a message from the future" (explicitly unsupported) from
    /// "a malformed message of a kind we know" (a protocol error).
    pub const KINDS: &'static [&'static str] = &[
        "hello",
        "accept",
        "reject",
        "have",
        "batch",
        "ack",
        "nack",
        "done",
        "goodbye",
        "unsupported",
    ];
}

/// A message plus the protocol version it was written in.
///
/// The version travels on every frame so a peer can tell which dialect it was
/// written in, and so a capture of a session is self-describing.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Frame {
    /// The version the sender speaks in this frame.
    pub version: ProtocolVersion,
    /// The payload.
    pub message: Message,
}
