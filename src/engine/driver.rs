//! The async half of the engine: executes the session's [`Action`]s.
//!
//! The state machine decides; this module does. Everything here that can fail
//! for a *local* reason (store, database) ends the session with an error;
//! everything that fails for a *remote* reason is an [`Outcome`], because a
//! flaky peer is the normal condition of the mesh, not an exception.

use std::collections::VecDeque;

use super::filter::through_policy;
use super::mesh::DbMesh;
use super::session::{Action, Admission, Input, Loaded, Outcome, Role, Session, SessionConfig};
use crate::adapter::{ApplyOutcome, DatabaseAdapter, Resolutions};
use crate::core::{
    Authentication, Authenticator, Authorization, Authorizer, ChangeBatch, ConflictResolver,
    Direction, Origin, PeerId, PeerStatus, Provenance, Resolution, Sequence, SessionId, SyncPolicy,
    Transaction,
};
use crate::error::{Error, Result};
use crate::protocol::{Decoded, Frame, codec};
use crate::storage::{LogEntry, LogRead, Store};
use crate::transport::Connection;

/// What one session did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionReport {
    /// Local identifier, for correlating logs. The peer never sees it.
    pub id: SessionId,
    /// The remote peer, if the handshake got far enough to know it.
    pub peer: Option<PeerId>,
    /// How the session ended.
    pub outcome: Outcome,
    /// Transactions sent and acknowledged by the peer.
    pub transactions_sent: usize,
    /// Transactions received and applied.
    pub transactions_received: usize,
    /// Received transactions the resolver refused. Their sequence is still consumed.
    pub transactions_rejected: usize,
}

/// What applying one received transaction amounted to.
pub(super) enum Disposition {
    Applied,
    AlreadyApplied,
    Rejected,
}

impl<D, S, P, R, A> DbMesh<D, S, P, R, A>
where
    D: DatabaseAdapter,
    S: Store,
    P: SyncPolicy,
    R: ConflictResolver,
    A: Authenticator + Authorizer,
{
    /// Runs one session to its end.
    pub(super) async fn run<C: Connection>(
        &self,
        role: Role,
        mut connection: C,
        expected_peer: Option<PeerId>,
    ) -> Result<SessionReport> {
        let config = SessionConfig {
            node: self.node.clone(),
            versions: self.versions,
            capabilities: self.capabilities.clone(),
            credential: self.security.credential().await?,
            expected_peer,
            cursors: self.store.cursors().await?,
            batch_limit: self.batch_limit,
        };
        let mut session = Session::new(role, config);
        let mut report = SessionReport {
            id: SessionId::generate(),
            peer: None,
            outcome: Outcome::ConnectionLost,
            transactions_sent: 0,
            transactions_received: 0,
            transactions_rejected: 0,
        };
        // Inputs produced locally are consumed before reading the wire, so the
        // machine always sees the answer to its own question before the peer's next message.
        let mut pending = VecDeque::from([Input::Start]);
        let mut in_flight = 0;
        let mut fault: Option<Error> = None;
        let mut outcome = None;

        while outcome.is_none() {
            let input = match pending.pop_front() {
                Some(input) => input,
                None => receive(&mut connection).await,
            };
            for action in session.handle(input) {
                match action {
                    Action::Send(message) => {
                        let frame = Frame {
                            version: session.frame_version(),
                            message,
                        };
                        if let Message::Batch(batch) = &frame.message {
                            in_flight = batch.transactions.len();
                        }
                        let sent = match codec::encode(&frame) {
                            Ok(bytes) => connection.send(bytes).await,
                            Err(error) => Err(error),
                        };
                        if sent.is_err() {
                            // A send that fails is a connection that broke; the machine decides what that means.
                            pending.push_back(Input::Disconnected);
                        }
                    }
                    Action::Admit {
                        claimed,
                        credential,
                        requested,
                    } => {
                        let info = connection.info().clone();
                        let verdict = match self
                            .security
                            .authenticate(&claimed, credential.as_ref(), &info)
                            .await
                        {
                            Ok(Authentication::Verified(peer)) => {
                                match self.security.authorize(&peer, &requested).await {
                                    Ok(Authorization::Granted(capabilities)) => {
                                        Ok(Ok(Admission { peer, capabilities }))
                                    }
                                    Ok(Authorization::Denied(reason)) => Ok(Err(reason)),
                                    Err(error) => Err(error),
                                }
                            }
                            Ok(Authentication::Rejected(reason)) => Ok(Err(reason)),
                            Err(error) => Err(error),
                        };
                        match verdict {
                            Ok(admission) => pending.push_back(Input::Admitted(admission)),
                            Err(error) => {
                                fault = Some(error);
                                outcome = Some(Outcome::Failed {
                                    detail: "the security hooks failed".to_owned(),
                                });
                            }
                        }
                    }
                    Action::Load {
                        origin,
                        after,
                        up_to,
                        limit,
                    } => {
                        let Some(peer) = session.peer().cloned() else {
                            continue;
                        };
                        match self.load_batch(&peer, &origin, after, up_to, limit).await {
                            Ok(result) => pending.push_back(Input::Loaded { origin, result }),
                            Err(error) => {
                                fault = Some(error);
                                outcome = Some(Outcome::Failed {
                                    detail: "reading the log failed".to_owned(),
                                });
                            }
                        }
                    }
                    Action::Apply(batch) => {
                        let Some(peer) = session.peer().cloned() else {
                            continue;
                        };
                        match self.apply_batch(&peer, batch).await {
                            Ok((through, applied, rejected)) => {
                                report.transactions_received += applied;
                                report.transactions_rejected += rejected;
                                pending.push_back(Input::Applied(Ok(through)));
                            }
                            Err(error) => {
                                // The peer is told, and the local fault is reported to the caller.
                                pending.push_back(Input::Applied(Err(error.to_string())));
                                fault = Some(error);
                            }
                        }
                    }
                    Action::RecordAck { origin, through } => {
                        if let Some(peer) = session.peer() {
                            if let Err(error) = self.store.record_ack(peer, &origin, through).await
                            {
                                fault = Some(error);
                            } else {
                                report.transactions_sent += std::mem::take(&mut in_flight);
                            }
                        }
                    }
                    Action::Close(closed) => outcome = Some(closed),
                }
            }
        }

        // Closing is best effort: the outcome is already decided.
        let _ = connection.close().await;
        report.peer = session.peer().cloned();
        report.outcome = outcome.unwrap_or(Outcome::ConnectionLost);
        if let Some(peer) = &report.peer {
            let status = if report.outcome.is_completed() {
                PeerStatus::Synced
            } else {
                PeerStatus::Failed {
                    reason: format!("{:?}", report.outcome),
                }
            };
            self.store.set_status(peer, status).await?;
        }
        match fault {
            Some(error) => Err(error),
            None => Ok(report),
        }
    }

    /// Reads the next batch for `origin`, with policy applied.
    async fn load_batch(
        &self,
        peer: &PeerId,
        origin: &Origin,
        after: Sequence,
        up_to: Sequence,
        limit: usize,
    ) -> Result<Loaded> {
        let entries = match self.store.read(origin, after, up_to, limit).await? {
            LogRead::Compacted { oldest } => return Ok(Loaded::Compacted { oldest }),
            LogRead::Entries(entries) => entries,
        };
        let mut through = after;
        let mut hops = 0;
        let mut transactions = Vec::new();
        for entry in &entries {
            // Stop at the first entry that cannot be vouched for as whole: sending past
            // it would claim a range this node only partly holds.
            let Some(transaction) = entry.transaction.as_ref().filter(|_| entry.is_relayable())
            else {
                break;
            };
            through = entry.sequence;
            hops = hops.max(entry.provenance.hops);
            // Transactions policy removes still move `through`: the peer learns the range
            // was handled without learning what was in it.
            if let Some(kept) = through_policy(&self.policy, Direction::Outbound, peer, transaction)
            {
                transactions.push(kept);
            }
        }
        Ok(if through == after {
            Loaded::Unavailable
        } else {
            Loaded::Batch(ChangeBatch {
                origin: origin.clone(),
                from: after,
                through,
                hops,
                transactions,
            })
        })
    }

    /// Logs, applies and marks a received batch. Returns how far this node now is,
    /// and how many transactions were applied and rejected.
    async fn apply_batch(
        &self,
        peer: &PeerId,
        batch: ChangeBatch,
    ) -> Result<(Sequence, usize, usize)> {
        let held = self.store.cursors().await?.get(&batch.origin);
        if batch.through <= held {
            // A duplicate delivery: everything in it is already processed.
            return Ok((held, 0, 0));
        }
        let provenance = Provenance {
            sender: Some(peer.clone()),
            hops: batch.hops + 1,
        };
        let mut entries = Vec::new();
        let mut sequence = held.next();
        while sequence <= batch.through {
            let kept = batch
                .transactions
                .iter()
                .find(|transaction| transaction.sequence == sequence)
                // Inbound policy is applied *before* the log write, so what the log holds is exactly
                // what this node is willing to have applied, and recovery needs no policy.
                .and_then(|transaction| {
                    through_policy(&self.policy, Direction::Inbound, peer, transaction)
                });
            entries.push(LogEntry {
                origin: batch.origin.clone(),
                sequence,
                transaction: kept,
                provenance: provenance.clone(),
            });
            sequence = sequence.next();
        }

        // Write-ahead: durable in the log before it touches the database.
        self.store.append_remote(entries.clone()).await?;

        let (mut applied, mut rejected) = (0, 0);
        for entry in entries {
            if let Some(transaction) = &entry.transaction {
                match self.apply_transaction(transaction).await? {
                    Disposition::Applied => applied += 1,
                    Disposition::Rejected => rejected += 1,
                    Disposition::AlreadyApplied => {}
                }
            }
            // Per transaction, not per batch: an interruption resumes where it stopped.
            self.store
                .mark_applied(&entry.origin, entry.sequence)
                .await?;
        }
        Ok((batch.through, applied, rejected))
    }

    /// Offers a transaction to the database, consulting the resolver on conflict.
    pub(super) async fn apply_transaction(&self, transaction: &Transaction) -> Result<Disposition> {
        let outcome = self
            .database
            .apply(transaction, &Resolutions::new())
            .await?;
        let conflicts = match outcome {
            ApplyOutcome::Applied => return Ok(Disposition::Applied),
            ApplyOutcome::AlreadyApplied => return Ok(Disposition::AlreadyApplied),
            ApplyOutcome::Conflicted(conflicts) => conflicts,
        };
        let mut resolutions = Resolutions::new();
        for conflict in &conflicts {
            match self.resolver.resolve(conflict).await {
                // One refusal refuses the whole transaction: applying part of it would break atomicity.
                Resolution::Reject { .. } => return Ok(Disposition::Rejected),
                resolution => {
                    resolutions.insert(conflict.record.clone(), resolution);
                }
            }
        }
        match self.database.apply(transaction, &resolutions).await? {
            ApplyOutcome::Applied => Ok(Disposition::Applied),
            ApplyOutcome::AlreadyApplied => Ok(Disposition::AlreadyApplied),
            ApplyOutcome::Conflicted(again) => Err(Error::ConflictUnresolved {
                record: again
                    .first()
                    .map(|c| c.record.to_string())
                    .unwrap_or_default(),
            }),
        }
    }
}

/// Reads the next input from the wire.
async fn receive<C: Connection>(connection: &mut C) -> Input {
    match connection.recv().await {
        Ok(Some(bytes)) => match codec::decode(&bytes) {
            Ok(Decoded::Frame(frame)) => Input::Received(frame.message),
            Ok(Decoded::Unsupported { kind, .. }) => Input::UnsupportedReceived { kind },
            Err(error) => Input::Malformed {
                detail: error.to_string(),
            },
        },
        // A clean close or a broken connection both mean: no more messages. If the
        // session was not finished, that is a lost connection either way.
        Ok(None) | Err(_) => Input::Disconnected,
    }
}

use crate::protocol::Message;
