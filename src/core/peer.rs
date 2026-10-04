//! Peers, their configuration, and what they can do.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::ids::{Capability, PeerId, TransportKind};
use super::sequence::Checkpoint;

/// A way to reach a peer. A peer may have several, over different transports.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Endpoint {
    /// Which transport understands the address.
    pub transport: TransportKind,
    /// Where to connect. Opaque to DBMesh: only the transport reads it.
    pub address: String,
}

/// A node this one may synchronize with.
///
/// Knowing a peer says nothing about trusting it or reaching it right now:
/// membership, trust and connectivity are three separate questions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Peer {
    /// How we name it.
    pub id: PeerId,
    /// Ways to reach it, in order of preference. Empty for peers that only dial us.
    pub endpoints: Vec<Endpoint>,
}

impl Peer {
    /// A peer with no known endpoint.
    #[must_use]
    pub fn new(id: PeerId) -> Self {
        Self {
            id,
            endpoints: Vec::new(),
        }
    }

    /// Adds an endpoint.
    #[must_use]
    pub fn with_endpoint(mut self, transport: TransportKind, address: impl Into<String>) -> Self {
        self.endpoints.push(Endpoint {
            transport,
            address: address.into(),
        });
        self
    }
}

/// What the last session with a peer amounted to.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerStatus {
    /// No session has been attempted.
    #[default]
    Unknown,
    /// The last session completed.
    Synced,
    /// The last session did not complete.
    Failed {
        /// Why, for humans.
        reason: String,
    },
}

/// Everything persisted about a peer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerState {
    /// The peer's configuration.
    pub peer: Peer,
    /// What it has acknowledged so far.
    pub checkpoint: Checkpoint,
    /// How the last session ended.
    pub status: PeerStatus,
}

impl PeerState {
    /// A fresh state for a newly registered peer.
    #[must_use]
    pub fn new(peer: Peer) -> Self {
        let checkpoint = Checkpoint::new(peer.id.clone());
        Self {
            peer,
            checkpoint,
            status: PeerStatus::Unknown,
        }
    }
}

/// The protocol features a node offers and insists on.
///
/// `required` is a promise to refuse sessions without them; an unsupported
/// *optional* capability merely narrows what the session does.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    /// Features without which this node will not talk.
    pub required: BTreeSet<Capability>,
    /// Features this node can use. A superset of `required`.
    pub supported: BTreeSet<Capability>,
}

impl Capabilities {
    /// Offers `capability` without insisting on it.
    #[must_use]
    pub fn support(mut self, capability: Capability) -> Self {
        self.supported.insert(capability);
        self
    }

    /// Insists on `capability`, which implies supporting it.
    #[must_use]
    pub fn require(mut self, capability: Capability) -> Self {
        self.supported.insert(capability.clone());
        self.required.insert(capability);
        self
    }
}
