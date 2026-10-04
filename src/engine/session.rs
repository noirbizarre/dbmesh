//! The synchronization session as a pure state machine.
//!
//! [`Session`] performs no I/O, reads no clock and spawns nothing. It is fed
//! [`Input`]s and answers with [`Action`]s; the driver executes the actions
//! (send, load from the log, apply to the database...) and feeds the results
//! back. That split is what makes every failure scenario in the architecture
//! document reproducible as a plain, deterministic test: a dropped
//! connection is just `Input::Disconnected`.
//!
//! A session is cheap and disposable. All progress that matters lives in the
//! durable cursors the session is constructed with and the acknowledgements it
//! emits, never in the session itself, so a session that dies for any reason
//! is simply replaced by a new one that resumes from those cursors.
//!
//! # Shape
//!
//! ```text
//!   initiator                         responder
//!   Hello ───────────────────────────────▶
//!                                          (admit: authenticate, authorize)
//!   ◀──────────────────────────────── Accept
//!   (admit the responder too)
//!   Have ────────────────────────────────▶ ◀──────────────────────── Have
//!   ◀──────────── Batch / Ack, stop-and-wait, in both directions ────────▶
//!   Done ────────────────────────────────▶ ◀──────────────────────── Done
//! ```

use std::collections::{BTreeSet, VecDeque};

use crate::core::{
    Capabilities, Capability, ChangeBatch, Credential, CursorSet, NodeId, Origin, PeerId, Sequence,
};
use crate::protocol::{Message, NackReason, ProtocolVersion, RejectReason, VersionRange};

/// Which side opened the session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Dialed the connection and speaks first.
    Initiator,
    /// Accepted the connection and answers.
    Responder,
}

/// What a session needs to know before it starts.
#[derive(Clone, Debug)]
pub struct SessionConfig {
    /// This node's identity.
    pub node: NodeId,
    /// The protocol versions this node speaks.
    pub versions: VersionRange,
    /// The capabilities this node offers and requires.
    pub capabilities: Capabilities,
    /// The credential to present, if any.
    pub credential: Option<Credential>,
    /// For an initiator: the peer it meant to reach. A different identity answering is refused.
    pub expected_peer: Option<PeerId>,
    /// The node's durable processed cursors, as of the start of the session.
    pub cursors: CursorSet,
    /// The most transactions put in one batch.
    pub batch_limit: usize,
}

/// The result of the host's authentication and authorization of the remote node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Admission {
    /// Who the remote node is, as the host knows it.
    pub peer: PeerId,
    /// The capabilities the host grants it.
    pub capabilities: BTreeSet<Capability>,
}

/// The answer to a [`Action::Load`].
#[derive(Clone, Debug, PartialEq)]
pub enum Loaded {
    /// The next batch for the origin, with policy already applied.
    Batch(ChangeBatch),
    /// This node cannot serve the origin from where the peer is: the log holds
    /// only part of the content there (policy trimmed it on the way in). The
    /// peer must get that range from somewhere that holds it whole.
    Unavailable,
    /// The log no longer reaches back that far. The peer needs a snapshot.
    Compacted {
        /// The oldest sequence still available.
        oldest: Sequence,
    },
}

/// Something that happened, fed to the session by the driver.
#[derive(Clone, Debug)]
pub enum Input {
    /// The session begins. The initiator opens with `Hello`; the responder waits.
    Start,
    /// A message arrived.
    Received(Message),
    /// Bytes arrived that are not a frame this protocol can read.
    Malformed {
        /// What was wrong with them.
        detail: String,
    },
    /// A frame of a type this build does not know arrived.
    UnsupportedReceived {
        /// The unknown type.
        kind: String,
    },
    /// The host answered an [`Action::Admit`].
    Admitted(Result<Admission, String>),
    /// The log answered an [`Action::Load`].
    Loaded {
        /// The origin that was requested.
        origin: Origin,
        /// What was found.
        result: Loaded,
    },
    /// The database answered an [`Action::Apply`].
    Applied(Result<Sequence, String>),
    /// The connection broke.
    Disconnected,
    /// The host's patience ran out. The session itself never measures time.
    TimedOut,
}

/// Something the driver must do, produced by the session.
#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    /// Send a message.
    Send(Message),
    /// Authenticate and authorize the remote node, then answer with [`Input::Admitted`].
    Admit {
        /// The identity the remote claims.
        claimed: NodeId,
        /// The credential it presented.
        credential: Option<Credential>,
        /// The capabilities that would be active if the host agrees.
        requested: BTreeSet<Capability>,
    },
    /// Read the next batch for `origin` from the log, then answer with [`Input::Loaded`].
    Load {
        /// Whose history.
        origin: Origin,
        /// Start after this sequence.
        after: Sequence,
        /// Do not go past this sequence.
        up_to: Sequence,
        /// Take at most this many transactions.
        limit: usize,
    },
    /// Log, apply and mark a received batch, then answer with [`Input::Applied`].
    Apply(ChangeBatch),
    /// Persist that the peer acknowledged `origin` through `through`.
    RecordAck {
        /// Whose history.
        origin: Origin,
        /// The acknowledged sequence.
        through: Sequence,
    },
    /// The session is over. The driver closes the connection and reports the outcome.
    Close(Outcome),
}

/// How a session ended.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Outcome {
    /// Both sides sent everything the other was missing, and it was acknowledged.
    Completed,
    /// The handshake was refused, by us or by the peer.
    Rejected {
        /// Machine-readable reason.
        reason: RejectReason,
        /// Human-readable detail.
        detail: String,
    },
    /// The session broke a rule: a refused batch, a gap, an unexpected message, a failed apply.
    Failed {
        /// What happened.
        detail: String,
    },
    /// The peer is further behind than the log reaches and needs a snapshot first.
    SnapshotRequired {
        /// The origin whose history was discarded.
        origin: Origin,
    },
    /// One side sent something the other does not understand.
    Unsupported {
        /// The message type or feature.
        feature: String,
    },
    /// The peer ended the session on purpose.
    PeerLeft {
        /// What it said.
        reason: String,
    },
    /// The connection broke. Normal for an intermittently connected mesh.
    ConnectionLost,
    /// The host gave up waiting.
    TimedOut,
}

impl Outcome {
    /// Whether the session finished its work.
    #[must_use]
    pub fn is_completed(&self) -> bool {
        matches!(self, Self::Completed)
    }
}

/// What this side currently has in flight towards the peer.
#[derive(Debug)]
enum Flight {
    Idle,
    Loading(Origin),
    AwaitingAck { origin: Origin, through: Sequence },
}

/// What the handshake settled before the host's admission arrives.
#[derive(Debug)]
struct Terms {
    version: ProtocolVersion,
    // The capabilities the remote offered (responder) or accepted (initiator).
    remote: Capabilities,
}

/// A range still to be sent for one origin.
#[derive(Debug)]
struct Segment {
    origin: Origin,
    after: Sequence,
    up_to: Sequence,
}

#[derive(Debug)]
enum Phase {
    Created,
    AwaitingHello,
    AwaitingAccept,
    Admitting(Terms),
    Syncing,
    Closed,
}

/// One synchronization session, as a state machine.
#[derive(Debug)]
pub struct Session {
    role: Role,
    config: SessionConfig,
    phase: Phase,
    version: Option<ProtocolVersion>,
    peer: Option<PeerId>,
    active: BTreeSet<Capability>,
    // Live cursors: start from the durable ones and move as batches are applied.
    live: CursorSet,
    queue: VecDeque<Segment>,
    flight: Flight,
    applying: Option<Origin>,
    sent_done: bool,
    received_done: bool,
    received_have: bool,
}

impl Session {
    /// A session about to start.
    #[must_use]
    pub fn new(role: Role, config: SessionConfig) -> Self {
        Self {
            role,
            live: config.cursors.clone(),
            config,
            phase: Phase::Created,
            version: None,
            peer: None,
            active: BTreeSet::new(),
            queue: VecDeque::new(),
            flight: Flight::Idle,
            applying: None,
            sent_done: false,
            received_done: false,
            received_have: false,
        }
    }

    /// The version to stamp on outgoing frames: the negotiated one once known.
    #[must_use]
    pub fn frame_version(&self) -> ProtocolVersion {
        self.version.unwrap_or(self.config.versions.max)
    }

    /// The remote peer, once admitted.
    #[must_use]
    pub fn peer(&self) -> Option<&PeerId> {
        self.peer.as_ref()
    }

    /// Whether the session has ended.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        matches!(self.phase, Phase::Closed)
    }

    /// Feeds one input and returns what to do about it.
    pub fn handle(&mut self, input: Input) -> Vec<Action> {
        if self.is_closed() {
            // Late inputs (a reply to something requested before the close) are harmless.
            return Vec::new();
        }
        match input {
            Input::Start => self.start(),
            Input::Received(message) => self.received(message),
            Input::UnsupportedReceived { kind } => self.close_with(
                vec![Action::Send(Message::Unsupported {
                    feature: kind.clone(),
                })],
                Outcome::Unsupported { feature: kind },
            ),
            Input::Malformed { detail } => self.fail(NackReason::UnexpectedMessage, detail, None),
            Input::Admitted(result) => self.admitted(result),
            Input::Loaded { origin, result } => self.loaded(&origin, result),
            Input::Applied(result) => self.applied(result),
            Input::Disconnected => self.close_with(Vec::new(), Outcome::ConnectionLost),
            Input::TimedOut => self.close_with(Vec::new(), Outcome::TimedOut),
        }
    }

    fn close_with(&mut self, mut actions: Vec<Action>, outcome: Outcome) -> Vec<Action> {
        self.phase = Phase::Closed;
        actions.push(Action::Close(outcome));
        actions
    }

    /// Refuses the handshake and ends the session.
    fn reject(&mut self, reason: RejectReason, detail: impl Into<String>) -> Vec<Action> {
        let detail = detail.into();
        self.close_with(
            vec![Action::Send(Message::Reject {
                reason,
                detail: detail.clone(),
            })],
            Outcome::Rejected { reason, detail },
        )
    }

    /// Refuses a message mid-session and ends the session.
    fn fail(
        &mut self,
        reason: NackReason,
        detail: impl Into<String>,
        have: Option<Sequence>,
    ) -> Vec<Action> {
        let detail = detail.into();
        self.close_with(
            vec![Action::Send(Message::Nack {
                reason,
                detail: detail.clone(),
                have,
            })],
            Outcome::Failed { detail },
        )
    }

    fn start(&mut self) -> Vec<Action> {
        if !matches!(self.phase, Phase::Created) {
            return Vec::new();
        }
        match self.role {
            Role::Responder => {
                self.phase = Phase::AwaitingHello;
                Vec::new()
            }
            Role::Initiator => {
                self.phase = Phase::AwaitingAccept;
                vec![Action::Send(Message::Hello {
                    node: self.config.node.clone(),
                    versions: self.config.versions,
                    capabilities: self.config.capabilities.clone(),
                    credential: self.config.credential.clone(),
                })]
            }
        }
    }

    fn received(&mut self, message: Message) -> Vec<Action> {
        match (&self.phase, message) {
            // Whatever the phase, these end the session.
            (_, Message::Reject { reason, detail }) => {
                self.close_with(Vec::new(), Outcome::Rejected { reason, detail })
            }
            (_, Message::Goodbye { reason }) => {
                self.close_with(Vec::new(), Outcome::PeerLeft { reason })
            }
            (_, Message::Unsupported { feature }) => {
                self.close_with(Vec::new(), Outcome::Unsupported { feature })
            }
            (_, Message::Nack { reason, detail, .. }) => self.close_with(
                Vec::new(),
                Outcome::Failed {
                    detail: format!("the peer refused ({reason:?}): {detail}"),
                },
            ),
            (
                Phase::AwaitingHello,
                Message::Hello {
                    node,
                    versions,
                    capabilities,
                    credential,
                },
            ) => self.on_hello(node, &versions, capabilities, credential),
            (
                Phase::AwaitingAccept,
                Message::Accept {
                    node,
                    version,
                    capabilities,
                    credential,
                },
            ) => self.on_accept(node, version, capabilities, credential),
            (Phase::Syncing, Message::Have { cursors }) => self.on_have(&cursors),
            (Phase::Syncing, Message::Batch(batch)) => self.on_batch(batch),
            (Phase::Syncing, Message::Ack { origin, through }) => self.on_ack(&origin, through),
            (Phase::Syncing, Message::Done) => {
                self.received_done = true;
                self.check_complete()
            }
            (_, other) => self.fail(
                NackReason::UnexpectedMessage,
                format!("unexpected {} at this point of the session", kind(&other)),
                None,
            ),
        }
    }

    fn on_hello(
        &mut self,
        node: NodeId,
        versions: &VersionRange,
        remote: Capabilities,
        credential: Option<Credential>,
    ) -> Vec<Action> {
        let Some(version) = self.config.versions.negotiate(versions) else {
            return self.reject(
                RejectReason::IncompatibleVersion,
                format!(
                    "this node speaks {}..={}, the peer {}..={}",
                    self.config.versions.min, self.config.versions.max, versions.min, versions.max
                ),
            );
        };
        self.version = Some(version);
        let requested = self
            .config
            .capabilities
            .supported
            .intersection(&remote.supported)
            .cloned()
            .collect();
        self.phase = Phase::Admitting(Terms { version, remote });
        vec![Action::Admit {
            claimed: node,
            credential,
            requested,
        }]
    }

    fn on_accept(
        &mut self,
        node: NodeId,
        version: ProtocolVersion,
        offered: BTreeSet<Capability>,
        credential: Option<Credential>,
    ) -> Vec<Action> {
        if let Some(expected) = &self.config.expected_peer
            && PeerId::from(&node) != *expected
        {
            return self.reject(
                RejectReason::UnexpectedIdentity,
                format!("expected `{expected}` but `{node}` answered"),
            );
        }
        if version < self.config.versions.min || version > self.config.versions.max {
            return self.reject(
                RejectReason::IncompatibleVersion,
                format!("the peer chose {version}, outside what this node speaks"),
            );
        }
        self.version = Some(version);
        // `required` is checked against what the peer accepted, before spending an authentication on it.
        if let Some(missing) = self
            .config
            .capabilities
            .required
            .difference(&offered)
            .next()
        {
            return self.reject(
                RejectReason::UnsupportedCapability,
                format!("the peer does not provide the required capability `{missing}`"),
            );
        }
        let remote = Capabilities {
            required: BTreeSet::new(),
            supported: offered.clone(),
        };
        self.phase = Phase::Admitting(Terms { version, remote });
        vec![Action::Admit {
            claimed: node,
            credential,
            requested: offered,
        }]
    }

    fn admitted(&mut self, result: Result<Admission, String>) -> Vec<Action> {
        // An admission nobody asked for is ignored rather than acted on.
        if !matches!(self.phase, Phase::Admitting(_)) {
            return Vec::new();
        }
        let Phase::Admitting(terms) = std::mem::replace(&mut self.phase, Phase::Syncing) else {
            unreachable!("checked just above");
        };
        let admission = match result {
            Ok(admission) => admission,
            Err(detail) => return self.reject(RejectReason::Unauthorized, detail),
        };
        // What is active is what the host grants; the host may grant less than was requested.
        let granted = admission.capabilities;
        let local_missing = self
            .config
            .capabilities
            .required
            .difference(&granted)
            .next();
        let remote_missing = terms.remote.required.difference(&granted).next();
        if let Some(missing) = local_missing.or(remote_missing) {
            return self.reject(
                RejectReason::UnsupportedCapability,
                format!("the required capability `{missing}` is not available to this peer"),
            );
        }
        self.peer = Some(admission.peer);
        self.active = granted;
        let mut actions = Vec::new();
        if self.role == Role::Responder {
            actions.push(Action::Send(Message::Accept {
                node: self.config.node.clone(),
                version: terms.version,
                capabilities: self.active.clone(),
                credential: self.config.credential.clone(),
            }));
        }
        actions.push(Action::Send(Message::Have {
            cursors: self.config.cursors.clone(),
        }));
        actions
    }

    fn relay_active(&self) -> bool {
        self.active.contains(&Capability::relay())
    }

    fn on_have(&mut self, theirs: &CursorSet) -> Vec<Action> {
        if self.received_have {
            return self.fail(
                NackReason::UnexpectedMessage,
                "a second `have` in one session",
                None,
            );
        }
        self.received_have = true;
        let me = Origin::from(&self.config.node);
        // Serve from the cursors as they were when the session began: what arrives
        // during this session is not relayed until the next one, which keeps a
        // session's workload bounded.
        for (origin, held) in self.config.cursors.iter() {
            let after = theirs.get(origin);
            let relayable = *origin == me || self.relay_active();
            if held > after && relayable {
                self.queue.push_back(Segment {
                    origin: origin.clone(),
                    after,
                    up_to: held,
                });
            }
        }
        self.serve_next()
    }

    /// Asks for the next batch, or announces that there is nothing left to send.
    fn serve_next(&mut self) -> Vec<Action> {
        if !matches!(self.flight, Flight::Idle) || !self.received_have {
            return Vec::new();
        }
        if let Some(segment) = self.queue.front() {
            self.flight = Flight::Loading(segment.origin.clone());
            return vec![Action::Load {
                origin: segment.origin.clone(),
                after: segment.after,
                up_to: segment.up_to,
                limit: self.config.batch_limit,
            }];
        }
        if self.sent_done {
            return Vec::new();
        }
        self.sent_done = true;
        let mut actions = vec![Action::Send(Message::Done)];
        actions.extend(self.check_complete());
        actions
    }

    fn loaded(&mut self, origin: &Origin, result: Loaded) -> Vec<Action> {
        if !matches!(&self.flight, Flight::Loading(expected) if expected == origin) {
            return Vec::new();
        }
        self.flight = Flight::Idle;
        match result {
            Loaded::Batch(batch)
                if batch.through > self.queue.front().map_or(Sequence::ZERO, |s| s.after) =>
            {
                let through = batch.through;
                if let Some(segment) = self.queue.front_mut() {
                    segment.after = through;
                    if through >= segment.up_to {
                        self.queue.pop_front();
                    }
                }
                self.flight = Flight::AwaitingAck {
                    origin: origin.clone(),
                    through,
                };
                vec![Action::Send(Message::Batch(batch))]
            }
            // A batch that moves nothing would loop forever; treat it as nothing to serve.
            Loaded::Batch(_) | Loaded::Unavailable => {
                self.queue.pop_front();
                self.serve_next()
            }
            Loaded::Compacted { oldest } => self.close_with(
                vec![Action::Send(Message::Goodbye {
                    reason: format!(
                        "history of `{origin}` before {oldest} is gone: a snapshot is required"
                    ),
                })],
                Outcome::SnapshotRequired {
                    origin: origin.clone(),
                },
            ),
        }
    }

    fn on_ack(&mut self, origin: &Origin, through: Sequence) -> Vec<Action> {
        match &self.flight {
            Flight::AwaitingAck {
                origin: expected,
                through: wanted,
            } if expected == origin && *wanted == through => {
                self.flight = Flight::Idle;
                let mut actions = vec![Action::RecordAck {
                    origin: origin.clone(),
                    through,
                }];
                actions.extend(self.serve_next());
                actions
            }
            _ => self.fail(
                NackReason::UnexpectedMessage,
                format!("an acknowledgement of `{origin}` through {through} that was not awaited"),
                None,
            ),
        }
    }

    fn on_batch(&mut self, batch: ChangeBatch) -> Vec<Action> {
        if self.applying.is_some() {
            return self.fail(
                NackReason::UnexpectedMessage,
                "a batch arrived before the previous one was acknowledged",
                None,
            );
        }
        if let Err(error) = batch.validate() {
            return self.fail(NackReason::InvalidBatch, error.to_string(), None);
        }
        // Nobody may tell a node what its own history is.
        if batch.origin == Origin::from(&self.config.node) {
            return self.fail(
                NackReason::InvalidBatch,
                "a batch claims to carry this node's own history",
                None,
            );
        }
        let from_peer_itself = self
            .peer
            .as_ref()
            .is_some_and(|peer| Origin::from(peer) == batch.origin);
        if !from_peer_itself && !self.relay_active() {
            return self.fail(
                NackReason::UnexpectedMessage,
                "relayed history arrived but the relay capability is not active",
                None,
            );
        }
        let held = self.live.get(&batch.origin);
        if batch.from > held {
            return self.fail(
                NackReason::Gap,
                format!(
                    "`{}` batch starts after {} but only {held} is held",
                    batch.origin, batch.from
                ),
                Some(held),
            );
        }
        self.applying = Some(batch.origin.clone());
        vec![Action::Apply(batch)]
    }

    fn applied(&mut self, result: Result<Sequence, String>) -> Vec<Action> {
        let Some(origin) = self.applying.take() else {
            return Vec::new();
        };
        match result {
            Ok(through) => {
                self.live.advance(&origin, through);
                let mut actions = vec![Action::Send(Message::Ack { origin, through })];
                actions.extend(self.check_complete());
                actions
            }
            Err(detail) => self.fail(NackReason::ApplyFailed, detail, None),
        }
    }

    fn check_complete(&mut self) -> Vec<Action> {
        let finished = self.sent_done
            && self.received_done
            && self.applying.is_none()
            && matches!(self.flight, Flight::Idle);
        if finished && !self.is_closed() {
            return self.close_with(Vec::new(), Outcome::Completed);
        }
        Vec::new()
    }
}

/// The wire name of a message, for diagnostics.
fn kind(message: &Message) -> &'static str {
    match message {
        Message::Hello { .. } => "hello",
        Message::Accept { .. } => "accept",
        Message::Reject { .. } => "reject",
        Message::Have { .. } => "have",
        Message::Batch(_) => "batch",
        Message::Ack { .. } => "ack",
        Message::Nack { .. } => "nack",
        Message::Done => "done",
        Message::Goodbye { .. } => "goodbye",
        Message::Unsupported { .. } => "unsupported",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Collection, Mutation, Operation, RecordId, Transaction, TransactionId};

    fn node(name: &str) -> NodeId {
        NodeId::new(name).unwrap()
    }

    fn origin(name: &str) -> Origin {
        Origin::new(name).unwrap()
    }

    fn peer(name: &str) -> PeerId {
        PeerId::new(name).unwrap()
    }

    fn cursors(entries: &[(&str, u64)]) -> CursorSet {
        entries
            .iter()
            .map(|(o, s)| (origin(o), Sequence::new(*s)))
            .collect()
    }

    fn relay() -> BTreeSet<Capability> {
        BTreeSet::from([Capability::relay()])
    }

    fn config(name: &str, held: &[(&str, u64)]) -> SessionConfig {
        SessionConfig {
            node: node(name),
            versions: VersionRange::CURRENT,
            capabilities: Capabilities::default().support(Capability::relay()),
            credential: None,
            expected_peer: None,
            cursors: cursors(held),
            batch_limit: 10,
        }
    }

    fn admit(name: &str, capabilities: BTreeSet<Capability>) -> Input {
        Input::Admitted(Ok(Admission {
            peer: peer(name),
            capabilities,
        }))
    }

    fn tx(origin_name: &str, sequence: u64) -> Transaction {
        Transaction::new(
            TransactionId::generate(),
            origin(origin_name),
            Sequence::new(sequence),
            vec![Mutation::new(
                RecordId::new(Collection::new("memories").unwrap(), "foo"),
                Operation::Create,
                None,
            )],
        )
    }

    fn batch(origin_name: &str, from: u64, through: u64) -> ChangeBatch {
        ChangeBatch {
            origin: origin(origin_name),
            from: Sequence::new(from),
            through: Sequence::new(through),
            hops: 0,
            transactions: (from + 1..=through).map(|s| tx(origin_name, s)).collect(),
        }
    }

    fn hello(name: &str, versions: VersionRange, capabilities: Capabilities) -> Message {
        Message::Hello {
            node: node(name),
            versions,
            capabilities,
            credential: None,
        }
    }

    fn closed(actions: &[Action]) -> Option<&Outcome> {
        actions.iter().find_map(|a| match a {
            Action::Close(outcome) => Some(outcome),
            _ => None,
        })
    }

    fn sent(actions: &[Action]) -> Vec<&Message> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Send(m) => Some(m),
                _ => None,
            })
            .collect()
    }

    /// An initiator `me` that has completed the handshake with `other` and sent its `Have`.
    fn established(me: &[(&str, u64)], granted: BTreeSet<Capability>) -> Session {
        let mut session = Session::new(Role::Initiator, config("me", me));
        session.handle(Input::Start);
        session.handle(Input::Received(Message::Accept {
            node: node("other"),
            version: ProtocolVersion::CURRENT,
            capabilities: relay(),
            credential: None,
        }));
        session.handle(admit("other", granted));
        session
    }

    fn have(entries: &[(&str, u64)]) -> Input {
        Input::Received(Message::Have {
            cursors: cursors(entries),
        })
    }

    #[test]
    fn the_initiator_opens_with_its_identity_versions_and_capabilities() {
        let mut session = Session::new(Role::Initiator, config("me", &[]));
        let actions = session.handle(Input::Start);
        assert!(
            matches!(sent(&actions)[..], [Message::Hello { node: n, .. }] if n.as_str() == "me")
        );
    }

    #[test]
    fn the_responder_says_nothing_until_it_hears_hello() {
        let mut session = Session::new(Role::Responder, config("me", &[]));
        assert!(session.handle(Input::Start).is_empty());
    }

    #[test]
    fn a_hello_with_no_common_version_is_rejected_explicitly() {
        let mut session = Session::new(Role::Responder, config("me", &[]));
        session.handle(Input::Start);
        let future = VersionRange {
            min: ProtocolVersion::new(2, 0),
            max: ProtocolVersion::new(2, 1),
        };
        let actions = session.handle(Input::Received(hello(
            "other",
            future,
            Capabilities::default(),
        )));
        assert!(matches!(
            sent(&actions)[..],
            [Message::Reject {
                reason: RejectReason::IncompatibleVersion,
                ..
            }]
        ));
        assert!(matches!(
            closed(&actions),
            Some(Outcome::Rejected {
                reason: RejectReason::IncompatibleVersion,
                ..
            })
        ));
    }

    #[test]
    fn the_host_is_asked_to_admit_the_peer_before_anything_is_accepted() {
        let mut session = Session::new(Role::Responder, config("me", &[]));
        session.handle(Input::Start);
        let actions = session.handle(Input::Received(hello(
            "other",
            VersionRange::CURRENT,
            Capabilities::default().support(Capability::relay()),
        )));
        assert!(matches!(&actions[..], [Action::Admit { requested, .. }] if *requested == relay()));
    }

    #[test]
    fn an_unauthorized_peer_is_rejected_and_nothing_else_is_sent() {
        let mut session = Session::new(Role::Responder, config("me", &[]));
        session.handle(Input::Start);
        session.handle(Input::Received(hello(
            "other",
            VersionRange::CURRENT,
            Capabilities::default(),
        )));
        let actions = session.handle(Input::Admitted(Err("unknown peer".into())));
        assert!(matches!(
            sent(&actions)[..],
            [Message::Reject {
                reason: RejectReason::Unauthorized,
                ..
            }]
        ));
        assert!(closed(&actions).is_some());
    }

    #[test]
    fn a_capability_the_peer_requires_but_the_host_withholds_ends_the_handshake() {
        let mut session = Session::new(Role::Responder, config("me", &[]));
        session.handle(Input::Start);
        let demanding = Capabilities::default().require(Capability::relay());
        session.handle(Input::Received(hello(
            "other",
            VersionRange::CURRENT,
            demanding,
        )));
        let actions = session.handle(admit("other", BTreeSet::new()));
        assert!(matches!(
            closed(&actions),
            Some(Outcome::Rejected {
                reason: RejectReason::UnsupportedCapability,
                ..
            })
        ));
    }

    #[test]
    fn an_admitted_responder_accepts_and_states_what_it_already_holds() {
        let mut session = Session::new(Role::Responder, config("me", &[("a", 7)]));
        session.handle(Input::Start);
        session.handle(Input::Received(hello(
            "other",
            VersionRange::CURRENT,
            Capabilities::default(),
        )));
        let actions = session.handle(admit("other", BTreeSet::new()));
        let messages = sent(&actions);
        assert!(matches!(messages[0], Message::Accept { .. }));
        assert!(
            matches!(messages[1], Message::Have { cursors } if *cursors == cursors_of(&[("a", 7)]))
        );
    }

    fn cursors_of(entries: &[(&str, u64)]) -> CursorSet {
        cursors(entries)
    }

    #[test]
    fn the_initiator_refuses_a_responder_that_is_not_who_it_dialed() {
        let mut cfg = config("me", &[]);
        cfg.expected_peer = Some(peer("server"));
        let mut session = Session::new(Role::Initiator, cfg);
        session.handle(Input::Start);
        let actions = session.handle(Input::Received(Message::Accept {
            node: node("impostor"),
            version: ProtocolVersion::CURRENT,
            capabilities: BTreeSet::new(),
            credential: None,
        }));
        assert!(matches!(
            closed(&actions),
            Some(Outcome::Rejected {
                reason: RejectReason::UnexpectedIdentity,
                ..
            })
        ));
    }

    #[test]
    fn a_peer_that_already_has_everything_is_sent_nothing_but_done() {
        let mut session = established(&[("me", 5)], relay());
        let actions = session.handle(have(&[("me", 5)]));
        assert_eq!(sent(&actions), vec![&Message::Done]);
    }

    #[test]
    fn a_resumed_session_asks_only_for_what_the_peer_is_missing() {
        let mut session = established(&[("me", 5)], relay());
        let actions = session.handle(have(&[("me", 3)]));
        assert_eq!(
            actions,
            vec![Action::Load {
                origin: origin("me"),
                after: Sequence::new(3),
                up_to: Sequence::new(5),
                limit: 10
            }]
        );
    }

    #[test]
    fn without_the_relay_capability_only_first_hand_history_is_served() {
        let mut session = established(&[("me", 2), ("third", 9)], BTreeSet::new());
        let actions = session.handle(have(&[]));
        assert!(matches!(&actions[..], [Action::Load { origin: o, .. }] if o.as_str() == "me"));
    }

    #[test]
    fn with_the_relay_capability_third_party_history_is_served_too() {
        let mut session = established(&[("third", 9)], relay());
        let actions = session.handle(have(&[]));
        assert!(matches!(&actions[..], [Action::Load { origin: o, .. }] if o.as_str() == "third"));
    }

    #[test]
    fn a_sent_batch_is_recorded_only_once_the_peer_acknowledges_it() {
        let mut session = established(&[("me", 2)], relay());
        session.handle(have(&[]));
        let actions = session.handle(Input::Loaded {
            origin: origin("me"),
            result: Loaded::Batch(batch("me", 0, 2)),
        });
        assert!(matches!(sent(&actions)[..], [Message::Batch(_)]));
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::RecordAck { .. }))
        );

        let actions = session.handle(Input::Received(Message::Ack {
            origin: origin("me"),
            through: Sequence::new(2),
        }));
        assert!(
            matches!(&actions[0], Action::RecordAck { through, .. } if *through == Sequence::new(2))
        );
        assert_eq!(sent(&actions), vec![&Message::Done]);
    }

    #[test]
    fn a_long_history_is_served_in_several_batches_each_resuming_after_the_last() {
        let mut session = established(&[("me", 4)], relay());
        session.handle(have(&[]));
        session.handle(Input::Loaded {
            origin: origin("me"),
            result: Loaded::Batch(batch("me", 0, 2)),
        });
        let actions = session.handle(Input::Received(Message::Ack {
            origin: origin("me"),
            through: Sequence::new(2),
        }));
        assert!(actions.iter().any(
            |a| matches!(a, Action::Load { after, up_to, .. } if *after == Sequence::new(2) && *up_to == Sequence::new(4))
        ));
    }

    #[test]
    fn an_acknowledgement_that_was_never_awaited_ends_the_session() {
        let mut session = established(&[("me", 2)], relay());
        session.handle(have(&[]));
        let actions = session.handle(Input::Received(Message::Ack {
            origin: origin("me"),
            through: Sequence::new(2),
        }));
        assert!(matches!(closed(&actions), Some(Outcome::Failed { .. })));
    }

    #[test]
    fn history_this_node_cannot_vouch_for_is_skipped_not_sent() {
        let mut session = established(&[("third", 3)], relay());
        session.handle(have(&[]));
        let actions = session.handle(Input::Loaded {
            origin: origin("third"),
            result: Loaded::Unavailable,
        });
        assert_eq!(sent(&actions), vec![&Message::Done]);
    }

    #[test]
    fn a_peer_behind_the_retained_history_is_told_a_snapshot_is_required() {
        let mut session = established(&[("me", 9)], relay());
        session.handle(have(&[]));
        let actions = session.handle(Input::Loaded {
            origin: origin("me"),
            result: Loaded::Compacted {
                oldest: Sequence::new(5),
            },
        });
        assert!(
            matches!(closed(&actions), Some(Outcome::SnapshotRequired { origin: o }) if o.as_str() == "me")
        );
    }

    #[test]
    fn a_received_batch_is_acknowledged_only_after_it_is_applied() {
        let mut session = established(&[], relay());
        let actions = session.handle(Input::Received(Message::Batch(batch("other", 0, 2))));
        assert!(matches!(&actions[..], [Action::Apply(_)]));
        let actions = session.handle(Input::Applied(Ok(Sequence::new(2))));
        assert!(
            matches!(sent(&actions)[..], [Message::Ack { through, .. }] if *through == Sequence::new(2))
        );
    }

    #[test]
    fn a_batch_that_overlaps_what_is_held_is_still_accepted() {
        let mut session = established(&[("other", 3)], relay());
        let actions = session.handle(Input::Received(Message::Batch(batch("other", 1, 5))));
        assert!(matches!(&actions[..], [Action::Apply(_)]));
    }

    #[test]
    fn a_batch_that_skips_ahead_is_refused_with_the_position_actually_held() {
        let mut session = established(&[("other", 3)], relay());
        let actions = session.handle(Input::Received(Message::Batch(batch("other", 5, 7))));
        assert!(matches!(
            sent(&actions)[..],
            [Message::Nack { reason: NackReason::Gap, have: Some(held), .. }] if *held == Sequence::new(3)
        ));
        assert!(closed(&actions).is_some());
    }

    #[test]
    fn a_structurally_invalid_batch_is_refused() {
        let mut session = established(&[], relay());
        let mut bad = batch("other", 0, 2);
        bad.transactions.reverse();
        let actions = session.handle(Input::Received(Message::Batch(bad)));
        assert!(matches!(
            sent(&actions)[..],
            [Message::Nack {
                reason: NackReason::InvalidBatch,
                ..
            }]
        ));
    }

    #[test]
    fn no_peer_can_rewrite_this_nodes_own_history() {
        let mut session = established(&[("me", 2)], relay());
        let actions = session.handle(Input::Received(Message::Batch(batch("me", 0, 5))));
        assert!(matches!(closed(&actions), Some(Outcome::Failed { .. })));
    }

    #[test]
    fn relayed_history_is_refused_when_relaying_was_not_agreed() {
        let mut session = established(&[], BTreeSet::new());
        let actions = session.handle(Input::Received(Message::Batch(batch("third", 0, 1))));
        assert!(matches!(closed(&actions), Some(Outcome::Failed { .. })));
    }

    #[test]
    fn a_second_batch_before_the_first_is_acknowledged_is_refused() {
        let mut session = established(&[], relay());
        session.handle(Input::Received(Message::Batch(batch("other", 0, 1))));
        let actions = session.handle(Input::Received(Message::Batch(batch("other", 1, 2))));
        assert!(matches!(closed(&actions), Some(Outcome::Failed { .. })));
    }

    #[test]
    fn a_failed_apply_is_reported_to_the_peer_and_ends_the_session() {
        let mut session = established(&[], relay());
        session.handle(Input::Received(Message::Batch(batch("other", 0, 1))));
        let actions = session.handle(Input::Applied(Err("disk full".into())));
        assert!(matches!(
            sent(&actions)[..],
            [Message::Nack {
                reason: NackReason::ApplyFailed,
                ..
            }]
        ));
        assert!(closed(&actions).is_some());
    }

    #[test]
    fn the_session_completes_only_when_both_sides_are_done_and_everything_is_acknowledged() {
        let mut session = established(&[], relay());
        // Nothing to send: Done goes out immediately, but the peer is not done yet.
        let actions = session.handle(have(&[]));
        assert_eq!(sent(&actions), vec![&Message::Done]);
        assert!(closed(&actions).is_none());
        // The peer sends a batch, then Done: complete only once the batch is acknowledged.
        session.handle(Input::Received(Message::Batch(batch("other", 0, 1))));
        let actions = session.handle(Input::Received(Message::Done));
        assert!(closed(&actions).is_none());
        let actions = session.handle(Input::Applied(Ok(Sequence::new(1))));
        assert_eq!(closed(&actions), Some(&Outcome::Completed));
    }

    #[test]
    fn a_lost_connection_ends_the_session_and_later_inputs_are_ignored() {
        let mut session = established(&[("me", 2)], relay());
        session.handle(have(&[]));
        let actions = session.handle(Input::Disconnected);
        assert_eq!(closed(&actions), Some(&Outcome::ConnectionLost));
        assert!(session.handle(Input::Received(Message::Done)).is_empty());
        assert!(session.is_closed());
    }

    #[test]
    fn running_out_of_patience_is_reported_as_a_timeout() {
        let mut session = established(&[], relay());
        assert_eq!(
            closed(&session.handle(Input::TimedOut)),
            Some(&Outcome::TimedOut)
        );
    }

    #[test]
    fn a_message_type_from_the_future_is_answered_with_an_explicit_unsupported() {
        let mut session = established(&[], relay());
        let actions = session.handle(Input::UnsupportedReceived {
            kind: "teleport".into(),
        });
        assert!(
            matches!(sent(&actions)[..], [Message::Unsupported { feature }] if feature == "teleport")
        );
        assert!(matches!(
            closed(&actions),
            Some(Outcome::Unsupported { .. })
        ));
    }

    #[test]
    fn a_goodbye_ends_the_session_without_error() {
        let mut session = established(&[], relay());
        let actions = session.handle(Input::Received(Message::Goodbye {
            reason: "bye".into(),
        }));
        assert!(matches!(closed(&actions), Some(Outcome::PeerLeft { .. })));
    }

    #[test]
    fn a_message_out_of_order_ends_the_session() {
        let mut session = Session::new(Role::Responder, config("me", &[]));
        session.handle(Input::Start);
        let actions = session.handle(Input::Received(Message::Done));
        assert!(matches!(closed(&actions), Some(Outcome::Failed { .. })));
    }
}
