//! The versioned node-to-node protocol.
//!
//! Transport-independent by construction: it is defined as [`Frame`]s that
//! the [`codec`] turns into bytes, and bytes are all a transport ever sees.

pub mod codec;
mod message;
mod version;

pub use codec::Decoded;
pub use message::{Frame, Message, NackReason, RejectReason};
pub use version::{ProtocolVersion, VersionRange};
