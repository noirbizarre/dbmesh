//! [`DbMesh`]: the handle an application embeds.

use super::driver::SessionReport;
use super::session::Role;
use crate::adapter::DatabaseAdapter;
use crate::core::{
    Authenticator, Authorizer, Capabilities, Capability, ConflictResolver, DenyAll, DenyUnknown,
    MAX_BATCH_SPAN, NodeId, Origin, Peer, PeerDiscovery, PeerId, PeerState, RejectOnConflict,
    SyncPolicy, TransportCapability,
};
use crate::error::{Error, Result};
use crate::protocol::VersionRange;
use crate::storage::Store;
use crate::transport::{Connection, Transport};

/// How many captured transactions are pulled from the database at once.
const CAPTURE_PAGE: usize = 256;

/// The default most transactions in one batch.
const DEFAULT_BATCH_LIMIT: usize = 128;

/// What [`DbMesh::start`] did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StartReport {
    /// Received transactions that had been logged but not applied before a crash, now applied.
    pub recovered: usize,
    /// Local transactions newly recorded for synchronization.
    pub captured: usize,
}

/// A long-lived participant in a mesh, embedded in an application.
///
/// It owns no thread and no socket. The host decides *when* to capture, serve
/// and synchronize and *how* peers are reached (it supplies the transport);
/// DBMesh guarantees that whatever it does is resumable from durable state.
///
/// The type parameters are the host's collaborators: the database `D`, the
/// metadata store `S`, the synchronization policy `P`, the conflict resolver
/// `R` and the security hooks `A`. The defaults are the safe ones: nothing is
/// synchronized, nobody is trusted, conflicts are refused.
pub struct DbMesh<D, S, P = DenyAll, R = RejectOnConflict, A = DenyUnknown> {
    pub(super) node: NodeId,
    pub(super) database: D,
    pub(super) store: S,
    pub(super) policy: P,
    pub(super) resolver: R,
    pub(super) security: A,
    pub(super) versions: VersionRange,
    pub(super) capabilities: Capabilities,
    pub(super) batch_limit: usize,
}

impl<D, S> DbMesh<D, S> {
    /// Starts configuring a mesh participant over `database`, keeping its state in `store`.
    #[must_use]
    pub fn builder(database: D, store: S) -> DbMeshBuilder<D, S> {
        DbMeshBuilder {
            database,
            store,
            identity: None,
            policy: DenyAll,
            resolver: RejectOnConflict,
            security: DenyUnknown,
            capabilities: Capabilities::default().support(Capability::relay()),
            versions: VersionRange::CURRENT,
            batch_limit: DEFAULT_BATCH_LIMIT,
        }
    }
}

/// Configures a [`DbMesh`].
pub struct DbMeshBuilder<D, S, P = DenyAll, R = RejectOnConflict, A = DenyUnknown> {
    database: D,
    store: S,
    identity: Option<NodeId>,
    policy: P,
    resolver: R,
    security: A,
    capabilities: Capabilities,
    versions: VersionRange,
    batch_limit: usize,
}

impl<D, S, P, R, A> DbMeshBuilder<D, S, P, R, A> {
    /// Pins the node identity.
    ///
    /// Optional: without it the identity stored with the metadata is used, or
    /// generated and stored on first run. If the store already holds a
    /// different identity, `build` fails rather than fork the node's history.
    #[must_use]
    pub fn identity(mut self, node: NodeId) -> Self {
        self.identity = Some(node);
        self
    }

    /// Sets what may be synchronized with whom.
    #[must_use]
    pub fn policy<P2: SyncPolicy>(self, policy: P2) -> DbMeshBuilder<D, S, P2, R, A> {
        DbMeshBuilder {
            database: self.database,
            store: self.store,
            identity: self.identity,
            policy,
            resolver: self.resolver,
            security: self.security,
            capabilities: self.capabilities,
            versions: self.versions,
            batch_limit: self.batch_limit,
        }
    }

    /// Sets how conflicts are decided.
    #[must_use]
    pub fn resolver<R2: ConflictResolver>(self, resolver: R2) -> DbMeshBuilder<D, S, P, R2, A> {
        DbMeshBuilder {
            database: self.database,
            store: self.store,
            identity: self.identity,
            policy: self.policy,
            resolver,
            security: self.security,
            capabilities: self.capabilities,
            versions: self.versions,
            batch_limit: self.batch_limit,
        }
    }

    /// Sets who is trusted.
    #[must_use]
    pub fn security<A2: Authenticator + Authorizer>(
        self,
        security: A2,
    ) -> DbMeshBuilder<D, S, P, R, A2> {
        DbMeshBuilder {
            database: self.database,
            store: self.store,
            identity: self.identity,
            policy: self.policy,
            resolver: self.resolver,
            security,
            capabilities: self.capabilities,
            versions: self.versions,
            batch_limit: self.batch_limit,
        }
    }

    /// Sets the protocol capabilities offered and required. Defaults to offering relay.
    #[must_use]
    pub fn capabilities(mut self, capabilities: Capabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    /// Narrows or widens the protocol versions this node speaks. Defaults to the current one only.
    #[must_use]
    pub fn versions(mut self, versions: VersionRange) -> Self {
        self.versions = versions;
        self
    }

    /// Sets how many transactions go in one batch (at least 1, at most [`MAX_BATCH_SPAN`]).
    #[must_use]
    pub fn batch_limit(mut self, limit: usize) -> Self {
        self.batch_limit = limit.clamp(1, usize::try_from(MAX_BATCH_SPAN).unwrap_or(usize::MAX));
        self
    }
}

impl<D, S, P, R, A> DbMeshBuilder<D, S, P, R, A>
where
    S: Store,
{
    /// Resolves the node identity against the store and builds the mesh.
    ///
    /// # Errors
    ///
    /// Returns [`Error::IdentityMismatch`] if the store belongs to another
    /// node, or the store's own error if it cannot be read or written.
    pub async fn build(self) -> Result<DbMesh<D, S, P, R, A>> {
        let node = match (self.store.identity().await?, self.identity) {
            (Some(stored), Some(configured)) if stored != configured => {
                return Err(Error::IdentityMismatch {
                    stored: stored.to_string(),
                    configured: configured.to_string(),
                });
            }
            (Some(stored), _) => stored,
            (None, configured) => {
                // First run: the identity is minted once and persisted before anything uses it.
                let node = configured.unwrap_or_else(NodeId::generate);
                self.store.set_identity(&node).await?;
                node
            }
        };
        Ok(DbMesh {
            node,
            database: self.database,
            store: self.store,
            policy: self.policy,
            resolver: self.resolver,
            security: self.security,
            capabilities: self.capabilities,
            versions: self.versions,
            batch_limit: self.batch_limit,
        })
    }
}

impl<D, S, P, R, A> DbMesh<D, S, P, R, A> {
    /// This node's identity.
    #[must_use]
    pub fn node(&self) -> &NodeId {
        &self.node
    }

    /// The metadata store, for inspection.
    #[must_use]
    pub fn store(&self) -> &S {
        &self.store
    }

    /// The database adapter.
    #[must_use]
    pub fn database(&self) -> &D {
        &self.database
    }
}

impl<D, S, P, R, A> DbMesh<D, S, P, R, A>
where
    D: DatabaseAdapter,
    S: Store,
    P: SyncPolicy,
    R: ConflictResolver,
    A: Authenticator + Authorizer,
{
    /// Brings the node to a consistent starting point.
    ///
    /// Finishes whatever a crash interrupted (received transactions that were
    /// logged but not applied), then records local changes made since the last
    /// run. It does not spawn anything or connect anywhere: scheduling
    /// sessions is the host's call, and every session starts with this anyway,
    /// so calling it is an optimization and a place to surface problems early.
    ///
    /// # Errors
    ///
    /// Returns the store's or the adapter's error.
    pub async fn start(&self) -> Result<StartReport> {
        // Recovery first: it completes work from *before* the crash, and capture
        // only ever adds new local history, so the order cannot matter for
        // correctness, but recovery failing should stop us before we grow the log.
        let recovered = self.recover().await?;
        let captured = self.capture().await?;
        Ok(StartReport {
            recovered,
            captured,
        })
    }

    /// Records local transactions committed since the last capture, in commit order.
    ///
    /// Returns how many were recorded. Safe to call at any time and as often
    /// as wanted.
    ///
    /// # Errors
    ///
    /// Returns the store's or the adapter's error.
    pub async fn capture(&self) -> Result<usize> {
        let origin = Origin::from(&self.node);
        let mut total = 0;
        loop {
            let after = self.store.source_position().await?;
            let page = self.database.poll(after, CAPTURE_PAGE).await?;
            if page.is_empty() {
                return Ok(total);
            }
            for captured in page {
                // Each append records the transaction and the feed position together,
                // so a crash here repeats no transaction and loses none.
                self.store
                    .append_local(&origin, captured.mutations, Some(captured.position))
                    .await?;
                total += 1;
            }
        }
    }

    /// Applies received transactions that were logged but not applied, e.g. before a crash.
    ///
    /// # Errors
    ///
    /// Returns the store's or the adapter's error.
    pub async fn recover(&self) -> Result<usize> {
        let mut recovered = 0;
        for entry in self.store.unapplied().await? {
            if let Some(transaction) = &entry.transaction {
                // Safe to repeat: the adapter contract makes apply idempotent.
                self.apply_transaction(transaction).await?;
                recovered += 1;
            }
            self.store
                .mark_applied(&entry.origin, entry.sequence)
                .await?;
        }
        Ok(recovered)
    }

    /// Adds a peer to the mesh, or updates its configuration. Its checkpoint is kept.
    ///
    /// # Errors
    ///
    /// Returns the store's error.
    pub async fn register_peer(&self, peer: Peer) -> Result<()> {
        self.store.put_peer(peer).await
    }

    /// Registers every peer a discovery source knows. Being discovered grants no trust.
    ///
    /// # Errors
    ///
    /// Returns the discovery source's or the store's error.
    pub async fn discover_with<Dis: PeerDiscovery>(&self, discovery: &Dis) -> Result<usize> {
        let peers = discovery.discover().await?;
        let count = peers.len();
        for peer in peers {
            self.store.put_peer(peer).await?;
        }
        Ok(count)
    }

    /// Everything persisted about the known peers.
    ///
    /// # Errors
    ///
    /// Returns the store's error.
    pub async fn peers(&self) -> Result<Vec<PeerState>> {
        self.store.peer_states().await
    }

    /// Opens a session with `peer` over `transport` and runs it to its end.
    ///
    /// An `Err` means *this node* failed (its store, its database). Anything
    /// that goes wrong with the peer or the connection mid-session is not an
    /// error: it is the [`outcome`](SessionReport::outcome) of the session,
    /// and the next session resumes from the last checkpoint.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnknownPeer`] if the peer was never registered,
    /// [`Error::UnsuitableTransport`] if the transport is not reliable,
    /// [`Error::Transport`] if the peer cannot be reached, or a local fault.
    pub async fn sync_with<T: Transport>(
        &self,
        transport: &T,
        peer: &PeerId,
    ) -> Result<SessionReport> {
        let state = self
            .store
            .peer_state(peer)
            .await?
            .ok_or_else(|| Error::UnknownPeer(peer.clone()))?;
        if !transport
            .capabilities()
            .contains(&TransportCapability::Reliable)
        {
            return Err(Error::UnsuitableTransport {
                transport: std::any::type_name::<T>().to_owned(),
                missing: "reliable message delivery".to_owned(),
            });
        }
        self.start().await?;
        let connection = match transport.connect(&state.peer).await {
            Ok(connection) => connection,
            Err(error) => {
                self.store
                    .set_status(
                        peer,
                        crate::core::PeerStatus::Failed {
                            reason: error.to_string(),
                        },
                    )
                    .await?;
                return Err(error);
            }
        };
        self.run(Role::Initiator, connection, Some(peer.clone()))
            .await
    }

    /// Serves one incoming connection, as the responder, to the end of its session.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnsuitableTransport`] if the connection is not reliable, or a local fault.
    pub async fn serve<C: Connection>(&self, connection: C) -> Result<SessionReport> {
        if !connection
            .info()
            .capabilities
            .contains(&TransportCapability::Reliable)
        {
            return Err(Error::UnsuitableTransport {
                transport: connection.info().transport.to_string(),
                missing: "reliable message delivery".to_owned(),
            });
        }
        self.start().await?;
        self.run(Role::Responder, connection, None).await
    }
}
