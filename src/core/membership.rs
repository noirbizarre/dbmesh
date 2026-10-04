//! Mesh membership and discovery.
//!
//! Three questions stay separate on purpose: *who is in the mesh*
//! ([`Membership`]), *how do we learn of peers* ([`PeerDiscovery`]), and
//! *who is reachable right now* (a transport concern that never appears here).

use std::collections::BTreeMap;
use std::future::Future;

use super::ids::PeerId;
use super::peer::Peer;
use crate::error::Result;

/// The peers this node knows about, reachable or not.
#[derive(Clone, Debug, Default)]
pub struct Membership {
    peers: BTreeMap<PeerId, Peer>,
}

impl Membership {
    /// An empty membership.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds or replaces a peer.
    pub fn insert(&mut self, peer: Peer) {
        self.peers.insert(peer.id.clone(), peer);
    }

    /// Looks a peer up.
    #[must_use]
    pub fn get(&self, id: &PeerId) -> Option<&Peer> {
        self.peers.get(id)
    }

    /// Iterates peers in id order.
    pub fn iter(&self) -> impl Iterator<Item = &Peer> {
        self.peers.values()
    }

    /// How many peers are known.
    #[must_use]
    pub fn len(&self) -> usize {
        self.peers.len()
    }

    /// Whether no peer is known.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }
}

/// A source of peers: static configuration, LAN discovery, a rendezvous service...
///
/// Discovery only *suggests* peers. Whether a discovered peer is trusted is
/// decided by the host application's authorizer, never by how it was found.
pub trait PeerDiscovery: Send + Sync {
    /// Returns the peers currently known to this source.
    fn discover(&self) -> impl Future<Output = Result<Vec<Peer>>> + Send;
}

/// The simplest discovery: a fixed list.
#[derive(Clone, Debug, Default)]
pub struct StaticDiscovery(pub Vec<Peer>);

impl PeerDiscovery for StaticDiscovery {
    async fn discover(&self) -> Result<Vec<Peer>> {
        Ok(self.0.clone())
    }
}
