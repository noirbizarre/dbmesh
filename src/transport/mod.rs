//! The transport abstraction.
//!
//! A transport moves opaque, message-framed bytes between two nodes. It knows
//! nothing about sessions, cursors or changes, so HTTP, WebSocket, QUIC, Unix
//! sockets, a LAN-specific protocol or an application-defined channel can all
//! be dropped in without touching the engine.
//!
//! A node may use a different transport for each peer; see the architecture
//! document for how the engine selects one.
//!
//! Connections are expected to be short-lived. Nothing here implies that a
//! connection outlives a session, or that one exists at all between sessions.

use std::collections::BTreeSet;
use std::future::Future;

use crate::core::{ConnectionInfo, Peer, TransportCapability};
use crate::error::Result;

/// One established connection, carrying whole messages.
pub trait Connection: Send {
    /// What is known about this connection.
    fn info(&self) -> &ConnectionInfo;

    /// Sends one message.
    fn send(&mut self, message: Vec<u8>) -> impl Future<Output = Result<()>> + Send;

    /// Receives the next message, or `None` once the peer has closed cleanly.
    ///
    /// An `Err` means the connection broke: the session ends and is resumed
    /// later from durable cursors.
    fn recv(&mut self) -> impl Future<Output = Result<Option<Vec<u8>>>> + Send;

    /// Closes the connection. Closing twice is harmless.
    fn close(&mut self) -> impl Future<Output = Result<()>> + Send;
}

/// Opens outgoing connections.
pub trait Transport: Send + Sync {
    /// The connections this transport produces.
    type Connection: Connection;

    /// What this transport guarantees, so the engine can refuse unsuitable ones up front.
    fn capabilities(&self) -> BTreeSet<TransportCapability>;

    /// Connects to `peer`, trying whichever of its endpoints this transport understands.
    ///
    /// Failing is normal and expected: peers are often offline.
    fn connect(&self, peer: &Peer) -> impl Future<Output = Result<Self::Connection>> + Send;
}

/// Accepts incoming connections.
pub trait Acceptor: Send {
    /// The connections this acceptor produces.
    type Connection: Connection;

    /// Waits for the next incoming connection.
    fn accept(&mut self) -> impl Future<Output = Result<Self::Connection>> + Send;
}
