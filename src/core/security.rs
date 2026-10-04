//! Security hooks.
//!
//! DBMesh does not ship an identity system or a PKI. It defines where trust
//! decisions are *asked* and leaves the *answer* to the host application:
//! the application stays authoritative over who is a peer and what that peer
//! may do. Transport security (encryption, certificates) belongs to the
//! transport and is reported through [`ConnectionInfo`].

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;

use serde::{Deserialize, Serialize};

use super::ids::{Capability, NodeId, PeerId, TransportKind};
use crate::error::Result;

/// An opaque token a node presents to prove who it is (a signed blob, a bearer token...).
///
/// DBMesh transports it in the handshake and never interprets it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Credential(pub String);

/// What the transport guarantees about a connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TransportCapability {
    /// Messages arrive intact, or the connection fails. Required by the engine.
    Reliable,
    /// Messages arrive in the order they were sent. Not assumed: gaps are detected anyway.
    Ordered,
    /// The channel is encrypted.
    Encrypted,
    /// The transport itself proved the peer's identity (mTLS, a Unix socket's peer credentials...).
    PeerAuthenticated,
}

/// Facts about one connection, handed to the security hooks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionInfo {
    /// The kind of transport carrying it.
    pub transport: TransportKind,
    /// What that transport guarantees.
    pub capabilities: BTreeSet<TransportCapability>,
    /// A transport-specific description of the remote end, for logs.
    pub remote: Option<String>,
}

/// The verdict of authentication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Authentication {
    /// The remote node is who it claims to be, and is known to us as this peer.
    Verified(PeerId),
    /// The claim could not be verified.
    Rejected(String),
}

/// The verdict of authorization.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Authorization {
    /// The peer may synchronize, using at most these capabilities.
    Granted(BTreeSet<Capability>),
    /// The peer may not synchronize at all.
    Denied(String),
}

/// Establishes who is on the other end of a connection.
pub trait Authenticator: Send + Sync {
    /// The credential to present to peers, if any.
    fn credential(&self) -> impl Future<Output = Result<Option<Credential>>> + Send;

    /// Verifies a remote node's claim. `Err` means the check could not run
    /// (an unreachable key server), not that the peer failed it.
    fn authenticate(
        &self,
        claimed: &NodeId,
        credential: Option<&Credential>,
        connection: &ConnectionInfo,
    ) -> impl Future<Output = Result<Authentication>> + Send;
}

/// Decides what an authenticated peer may do.
pub trait Authorizer: Send + Sync {
    /// Grants a subset of `requested` capabilities, or denies the peer.
    ///
    /// What the peer may *synchronize* is a separate question, answered per
    /// mutation by the [`SyncPolicy`](super::SyncPolicy).
    fn authorize(
        &self,
        peer: &PeerId,
        requested: &BTreeSet<Capability>,
    ) -> impl Future<Output = Result<Authorization>> + Send;
}

/// The default: trust nobody. A mesh built without security configuration
/// synchronizes with no one, rather than with everyone.
#[derive(Clone, Copy, Debug, Default)]
pub struct DenyUnknown;

impl Authenticator for DenyUnknown {
    async fn credential(&self) -> Result<Option<Credential>> {
        Ok(None)
    }

    async fn authenticate(
        &self,
        _: &NodeId,
        _: Option<&Credential>,
        _: &ConnectionInfo,
    ) -> Result<Authentication> {
        Ok(Authentication::Rejected(
            "no authenticator configured".to_owned(),
        ))
    }
}

impl Authorizer for DenyUnknown {
    async fn authorize(&self, _: &PeerId, _: &BTreeSet<Capability>) -> Result<Authorization> {
        Ok(Authorization::Denied("no authorizer configured".to_owned()))
    }
}

/// Trusts a fixed list of peers by the identity they claim.
///
/// This performs **no cryptographic check**: it is for tests, and for
/// transports that already authenticated the peer (see
/// [`TransportCapability::PeerAuthenticated`]). Anything else must supply
/// its own [`Authenticator`].
#[derive(Clone, Debug, Default)]
pub struct StaticTrust {
    credential: Option<Credential>,
    peers: BTreeMap<PeerId, BTreeSet<Capability>>,
}

impl StaticTrust {
    /// Trusts nobody yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Trusts `peer` with the given capabilities.
    #[must_use]
    pub fn trust(
        mut self,
        peer: PeerId,
        capabilities: impl IntoIterator<Item = Capability>,
    ) -> Self {
        self.peers.insert(peer, capabilities.into_iter().collect());
        self
    }

    /// Sets the credential presented to peers.
    #[must_use]
    pub fn presenting(mut self, credential: Credential) -> Self {
        self.credential = Some(credential);
        self
    }
}

impl Authenticator for StaticTrust {
    async fn credential(&self) -> Result<Option<Credential>> {
        Ok(self.credential.clone())
    }

    async fn authenticate(
        &self,
        claimed: &NodeId,
        _: Option<&Credential>,
        _: &ConnectionInfo,
    ) -> Result<Authentication> {
        let peer = PeerId::from(claimed);
        Ok(if self.peers.contains_key(&peer) {
            Authentication::Verified(peer)
        } else {
            Authentication::Rejected(format!("`{claimed}` is not a trusted peer"))
        })
    }
}

impl Authorizer for StaticTrust {
    async fn authorize(
        &self,
        peer: &PeerId,
        requested: &BTreeSet<Capability>,
    ) -> Result<Authorization> {
        Ok(match self.peers.get(peer) {
            Some(allowed) => {
                Authorization::Granted(requested.intersection(allowed).cloned().collect())
            }
            None => Authorization::Denied(format!("`{peer}` is not a trusted peer")),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info() -> ConnectionInfo {
        ConnectionInfo {
            transport: TransportKind::new("test").unwrap(),
            capabilities: BTreeSet::new(),
            remote: None,
        }
    }

    #[tokio::test]
    async fn an_unlisted_peer_is_not_authenticated() {
        let trust = StaticTrust::new().trust(PeerId::new("a").unwrap(), []);
        let verdict = trust
            .authenticate(&NodeId::new("c").unwrap(), None, &info())
            .await
            .unwrap();
        assert!(matches!(verdict, Authentication::Rejected(_)));
    }

    #[tokio::test]
    async fn granted_capabilities_never_exceed_what_the_host_allowed() {
        let trust = StaticTrust::new().trust(PeerId::new("a").unwrap(), []);
        let requested = BTreeSet::from([Capability::relay()]);
        let verdict = trust
            .authorize(&PeerId::new("a").unwrap(), &requested)
            .await
            .unwrap();
        assert_eq!(verdict, Authorization::Granted(BTreeSet::new()));
    }

    #[tokio::test]
    async fn the_default_security_denies_everyone() {
        let verdict = DenyUnknown
            .authenticate(&NodeId::new("a").unwrap(), None, &info())
            .await
            .unwrap();
        assert!(matches!(verdict, Authentication::Rejected(_)));
    }
}
