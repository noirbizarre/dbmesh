//! The end-to-end path: a change on one node reaches another through the
//! engine and a transport, is acknowledged, checkpointed, and survives
//! reconnects, crashes and restarts.

mod common;

use common::{Network, collection, create, node, node_with, peer_id, sync, sync_over, value};
use dbmesh::adapter::MemoryDatabase;
use dbmesh::core::{
    Conflict, ConflictResolver, Origin, Provenance, Resolution, Sequence, Transaction,
    TransactionId,
};
use dbmesh::protocol::Message;
use dbmesh::storage::{LogEntry, MemoryStore, Store};
use dbmesh::{Error, Outcome};

fn origin(name: &str) -> Origin {
    Origin::new(name).unwrap()
}

#[tokio::test]
async fn a_local_transaction_reaches_the_peer_atomically_and_the_sender_persists_a_checkpoint() {
    let network = Network::default();
    let a = node(&network, "a", &["b"], &["memories", "entities", "mentions"]).await;
    let mut b = node(&network, "b", &["a"], &["memories", "entities", "mentions"]).await;

    // One transaction touching three records, like the BEGIN ... COMMIT in the design.
    a.db.commit(vec![
        create("memories", "foo", 1),
        create("entities", "bar", 2),
        create("mentions", "foo_bar", 3),
    ]);

    let (from_a, from_b) = sync(&network, &a, &mut b).await;
    let (from_a, from_b) = (from_a.unwrap(), from_b.unwrap());

    assert_eq!(from_a.outcome, Outcome::Completed);
    assert_eq!(from_b.outcome, Outcome::Completed);
    assert_eq!(from_a.transactions_sent, 1);
    assert_eq!(from_b.transactions_received, 1);
    assert_eq!(value(&b.db, "memories", "foo"), Some(1));
    assert_eq!(value(&b.db, "entities", "bar"), Some(2));
    assert_eq!(value(&b.db, "mentions", "foo_bar"), Some(3));

    // B's durable cursor, and A's persisted record of B's acknowledgement.
    assert_eq!(
        b.store.cursors().await.unwrap().get(&origin("a")),
        Sequence::new(1)
    );
    let state = a.store.peer_state(&peer_id("b")).await.unwrap().unwrap();
    assert_eq!(state.checkpoint.acked.get(&origin("a")), Sequence::new(1));
}

#[tokio::test]
async fn a_second_session_after_reconnecting_sends_only_what_is_new() {
    let network = Network::default();
    let a = node(&network, "a", &["b"], &["memories"]).await;
    let mut b = node(&network, "b", &["a"], &["memories"]).await;

    a.db.commit(vec![create("memories", "one", 1)]);
    sync(&network, &a, &mut b).await.0.unwrap();
    network.wire.take();

    a.db.commit(vec![create("memories", "two", 2)]);
    a.db.commit(vec![create("memories", "three", 3)]);
    let (from_a, _) = sync(&network, &a, &mut b).await;
    assert_eq!(from_a.unwrap().transactions_sent, 2);

    let batches: Vec<_> = network
        .wire
        .sent_by("a")
        .into_iter()
        .filter_map(|m| match m {
            Message::Batch(batch) => Some(batch),
            _ => None,
        })
        .collect();
    assert_eq!(batches.len(), 1, "both new transactions fit one batch");
    assert_eq!(
        (batches[0].from, batches[0].through),
        (Sequence::new(1), Sequence::new(3))
    );
    assert_eq!(value(&b.db, "memories", "three"), Some(3));
}

#[tokio::test]
async fn a_session_with_nothing_new_transfers_no_changes() {
    let network = Network::default();
    let a = node(&network, "a", &["b"], &["memories"]).await;
    let mut b = node(&network, "b", &["a"], &["memories"]).await;
    a.db.commit(vec![create("memories", "one", 1)]);
    sync(&network, &a, &mut b).await.0.unwrap();
    network.wire.take();

    let (from_a, from_b) = sync(&network, &a, &mut b).await;
    assert_eq!(from_a.unwrap().outcome, Outcome::Completed);
    assert_eq!(from_b.unwrap().outcome, Outcome::Completed);
    let batches = network
        .wire
        .take()
        .into_iter()
        .filter(|(_, frame)| matches!(frame.message, Message::Batch(_)))
        .count();
    assert_eq!(batches, 0);
}

#[tokio::test]
async fn changes_flow_in_both_directions_in_one_session() {
    let network = Network::default();
    let a = node(&network, "a", &["b"], &["memories"]).await;
    let mut b = node(&network, "b", &["a"], &["memories"]).await;
    a.db.commit(vec![create("memories", "from_a", 1)]);
    b.db.commit(vec![create("memories", "from_b", 2)]);

    sync(&network, &a, &mut b).await.0.unwrap();

    assert_eq!(value(&a.db, "memories", "from_b"), Some(2));
    assert_eq!(value(&b.db, "memories", "from_a"), Some(1));
}

#[tokio::test]
async fn a_session_cut_mid_transfer_resumes_from_what_the_peer_durably_holds() {
    let network = Network::default();
    let a = node(&network, "a", &["b"], &["memories"]).await;
    let mut b = node(&network, "b", &["a"], &["memories"]).await;
    // Five transactions at two per batch: three batches.
    for i in 0..5 {
        a.db.commit(vec![create("memories", &format!("k{i}"), i)]);
    }

    // The initiator sends Hello, Have, then the first batch; the fourth frame never leaves.
    let dialer = network.dialer("a");
    dialer.sever_next_after(3);
    let (from_a, from_b) = sync_over(&dialer, &a, &mut b).await;
    assert_eq!(from_a.unwrap().outcome, Outcome::ConnectionLost);
    assert_eq!(from_b.unwrap().outcome, Outcome::ConnectionLost);
    assert_eq!(
        b.store.cursors().await.unwrap().get(&origin("a")),
        Sequence::new(2)
    );

    network.wire.take();
    let (from_a, from_b) = sync(&network, &a, &mut b).await;
    assert_eq!(from_a.unwrap().outcome, Outcome::Completed);
    // Three left, none re-sent: B received exactly what it was missing.
    assert_eq!(from_b.unwrap().transactions_received, 3);
    let first_resent = network.wire.sent_by("a").into_iter().find_map(|m| match m {
        Message::Batch(batch) => Some(batch.from),
        _ => None,
    });
    assert_eq!(first_resent, Some(Sequence::new(2)));
    for i in 0..5 {
        assert_eq!(value(&b.db, "memories", &format!("k{i}")), Some(i));
    }
}

#[tokio::test]
async fn a_crash_between_logging_and_applying_is_recovered_without_duplicates() {
    let network = Network::default();
    let a = node(&network, "a", &["b"], &["memories"]).await;
    a.db.commit(vec![create("memories", "foo", 1)]);

    let db = MemoryDatabase::new();
    let store = MemoryStore::new();
    // The state a crash leaves behind: the transaction is in B's log, but neither
    // applied to its database nor marked applied.
    let transaction = Transaction::new(
        TransactionId::generate(),
        origin("a"),
        Sequence::new(1),
        vec![create("memories", "foo", 1)],
    );
    store
        .append_remote(vec![LogEntry {
            origin: origin("a"),
            sequence: Sequence::new(1),
            transaction: Some(transaction),
            provenance: Provenance::default(),
        }])
        .await
        .unwrap();
    let mut b = node_with(
        &network,
        "b",
        &["a"],
        &["memories"],
        dbmesh::core::RejectOnConflict,
        db,
        store,
    )
    .await;
    assert_eq!(value(&b.db, "memories", "foo"), None);

    let (from_a, from_b) = sync(&network, &a, &mut b).await;

    // Recovered at startup, so B's Have already covered it and nothing was re-sent.
    assert_eq!(from_a.unwrap().transactions_sent, 0);
    assert_eq!(from_b.unwrap().transactions_received, 0);
    assert_eq!(value(&b.db, "memories", "foo"), Some(1));
    assert_eq!(
        b.store.cursors().await.unwrap().get(&origin("a")),
        Sequence::new(1)
    );
}

#[tokio::test]
async fn a_restarted_node_keeps_its_identity_and_resumes_where_it_stopped() {
    let network = Network::default();
    let a = node(&network, "a", &["b"], &["memories"]).await;
    let mut b = node(&network, "b", &["a"], &["memories"]).await;
    a.db.commit(vec![create("memories", "before", 1)]);
    sync(&network, &a, &mut b).await.0.unwrap();

    // "Restart" A: a new engine over the same database and metadata store, no identity given.
    let restarted = dbmesh::DbMesh::builder(a.db.clone(), a.store.clone())
        .policy(dbmesh::core::StaticPolicy::new().exchange(peer_id("b"), [collection("memories")]))
        .security(
            dbmesh::core::StaticTrust::new()
                .trust(peer_id("b"), [dbmesh::core::Capability::relay()]),
        )
        .build()
        .await
        .unwrap();
    assert_eq!(restarted.node().as_str(), "a");

    a.db.commit(vec![create("memories", "after", 2)]);
    network.wire.take();
    let target = peer_id("b");
    let dialer = network.dialer("a");
    let listener = &mut b.listener;
    let (from_a, _) = tokio::join!(restarted.sync_with(&dialer, &target), async {
        let connection = dbmesh::transport::Acceptor::accept(listener).await.unwrap();
        b.mesh.serve(connection).await
    });

    assert_eq!(
        from_a.unwrap().transactions_sent,
        1,
        "only the change made after the restart"
    );
    assert_eq!(value(&b.db, "memories", "after"), Some(2));
}

#[tokio::test]
async fn a_store_that_belongs_to_another_node_is_refused() {
    let store = MemoryStore::new();
    store
        .set_identity(&dbmesh::core::NodeId::new("a").unwrap())
        .await
        .unwrap();
    let result = dbmesh::DbMesh::builder(MemoryDatabase::new(), store)
        .identity(dbmesh::core::NodeId::new("impostor").unwrap())
        .build()
        .await;
    assert!(matches!(result, Err(Error::IdentityMismatch { .. })));
}

#[tokio::test]
async fn an_unreachable_peer_fails_the_connection_but_local_writes_continue_and_sync_later() {
    let network = Network::default();
    let a = node(&network, "a", &["b"], &["memories"]).await;
    let mut b = node(&network, "b", &["a"], &["memories"]).await;
    network.set_offline("b", true);

    a.db.commit(vec![create("memories", "while_offline", 1)]);
    let attempt = a.mesh.sync_with(&network.dialer("a"), &peer_id("b")).await;
    assert!(matches!(attempt, Err(Error::Transport(_))));
    let state = a.store.peer_state(&peer_id("b")).await.unwrap().unwrap();
    assert!(matches!(
        state.status,
        dbmesh::core::PeerStatus::Failed { .. }
    ));
    // The change was still captured: being offline never loses a write.
    assert_eq!(
        a.store.cursors().await.unwrap().get(&origin("a")),
        Sequence::new(1)
    );

    network.set_offline("b", false);
    sync(&network, &a, &mut b).await.0.unwrap();
    assert_eq!(value(&b.db, "memories", "while_offline"), Some(1));
}

#[tokio::test]
async fn a_peer_the_host_does_not_trust_is_refused_and_receives_nothing() {
    let network = Network::default();
    let a = node(&network, "a", &["b"], &["memories"]).await;
    // `c` is not in A's trusted peers, though `c` trusts A.
    let mut c = node(&network, "c", &["a"], &["memories"]).await;
    a.db.commit(vec![create("memories", "secret", 1)]);
    a.mesh
        .register_peer(dbmesh::core::Peer::new(peer_id("c")))
        .await
        .unwrap();

    let (from_a, from_c) = sync(&network, &a, &mut c).await;

    assert!(matches!(from_a.unwrap().outcome, Outcome::Rejected { .. }));
    assert!(matches!(from_c.unwrap().outcome, Outcome::Rejected { .. }));
    assert_eq!(value(&c.db, "memories", "secret"), None);
}

#[tokio::test]
async fn an_incompatible_protocol_version_is_rejected_before_anything_is_transferred() {
    use dbmesh::protocol::{ProtocolVersion, VersionRange};
    let network = Network::default();
    let a = node(&network, "a", &["b"], &["memories"]).await;
    let mut b = node(&network, "b", &["a"], &["memories"]).await;
    // Rebuild A as a node from the future.
    let future = dbmesh::DbMesh::builder(a.db.clone(), MemoryStore::new())
        .identity(dbmesh::core::NodeId::new("a").unwrap())
        .versions(VersionRange {
            min: ProtocolVersion::new(2, 0),
            max: ProtocolVersion::new(2, 0),
        })
        .security(dbmesh::core::StaticTrust::new().trust(peer_id("b"), []))
        .build()
        .await
        .unwrap();
    future
        .register_peer(dbmesh::core::Peer::new(peer_id("b")))
        .await
        .unwrap();
    a.db.commit(vec![create("memories", "x", 1)]);

    let dialer = network.dialer("a");
    let target = peer_id("b");
    let listener = &mut b.listener;
    let (from_a, from_b) = tokio::join!(future.sync_with(&dialer, &target), async {
        let connection = dbmesh::transport::Acceptor::accept(listener).await.unwrap();
        b.mesh.serve(connection).await
    });

    let reason = |outcome: Outcome| {
        matches!(
            outcome,
            Outcome::Rejected {
                reason: dbmesh::protocol::RejectReason::IncompatibleVersion,
                ..
            }
        )
    };
    assert!(reason(from_a.unwrap().outcome));
    assert!(reason(from_b.unwrap().outcome));
    assert_eq!(value(&b.db, "memories", "x"), None);
}

/// Resolves every conflict in favour of the incoming change.
struct RemoteWins;

impl ConflictResolver for RemoteWins {
    async fn resolve(&self, _: &Conflict) -> Resolution {
        Resolution::AcceptRemote
    }
}

#[tokio::test]
async fn a_conflicting_change_is_decided_by_the_hosts_resolver() {
    let network = Network::default();
    let a = node(&network, "a", &["b"], &["memories"]).await;
    let mut b = node_with(
        &network,
        "b",
        &["a"],
        &["memories"],
        RemoteWins,
        MemoryDatabase::new(),
        MemoryStore::new(),
    )
    .await;
    a.db.commit(vec![create("memories", "x", 1)]);
    b.db.commit(vec![create("memories", "x", 2)]);

    sync(&network, &a, &mut b).await.0.unwrap();

    assert_eq!(
        value(&b.db, "memories", "x"),
        Some(1),
        "the host chose the remote change"
    );
}

#[tokio::test]
async fn by_default_a_conflicting_transaction_is_refused_whole_and_not_retried() {
    let network = Network::default();
    let a = node(&network, "a", &["b"], &["memories"]).await;
    let mut b = node(&network, "b", &["a"], &["memories"]).await;
    a.db.commit(vec![create("memories", "x", 1), create("memories", "y", 1)]);
    b.db.commit(vec![create("memories", "x", 2)]);

    let (_, from_b) = sync(&network, &a, &mut b).await;
    let from_b = from_b.unwrap();

    assert_eq!(from_b.transactions_rejected, 1);
    assert_eq!(value(&b.db, "memories", "x"), Some(2), "local state kept");
    assert_eq!(
        value(&b.db, "memories", "y"),
        None,
        "the clean half of the transaction did not leak in"
    );
    // The sequence was consumed: the next session does not offer it again.
    assert_eq!(
        b.store.cursors().await.unwrap().get(&origin("a")),
        Sequence::new(1)
    );
}

#[tokio::test]
async fn a_change_relays_through_a_middle_node_while_its_origin_is_offline() {
    let network = Network::default();
    let a = node(&network, "a", &["b"], &["memories"]).await;
    let mut b = node(&network, "b", &["a", "c"], &["memories"]).await;
    let c = node(&network, "c", &["b"], &["memories"]).await;

    a.db.commit(vec![create("memories", "from_a", 1)]);
    sync(&network, &a, &mut b).await.0.unwrap();
    // A goes away. C only ever talks to B.
    network.set_offline("a", true);
    sync(&network, &c, &mut b).await.0.unwrap();

    assert_eq!(value(&c.db, "memories", "from_a"), Some(1));
    // Origin, sender and distance stay distinct: A made it, B handed it over, and it took two links (A to B, B to C).
    let log = c
        .store
        .read(&origin("a"), Sequence::ZERO, Sequence::new(1), 10)
        .await
        .unwrap();
    let dbmesh::storage::LogRead::Entries(entries) = log else {
        panic!("expected entries")
    };
    assert_eq!(entries[0].origin.as_str(), "a");
    assert_eq!(entries[0].provenance.sender, Some(peer_id("b")));
    assert_eq!(entries[0].provenance.hops, 2);
}

#[tokio::test]
async fn a_node_that_reconnects_catches_up_on_third_party_changes_from_whoever_it_reaches() {
    let network = Network::default();
    let mut a = node(&network, "a", &["b"], &["memories"]).await;
    let b = node(&network, "b", &["a", "c"], &["memories"]).await;
    let mut c = node(&network, "c", &["b"], &["memories"]).await;

    // C writes while A is away; B picks it up; later A reaches B.
    c.db.commit(vec![create("memories", "from_c", 7)]);
    sync(&network, &b, &mut c).await.0.unwrap();
    let (from_a, _) = sync(&network, &b, &mut a).await;

    assert_eq!(from_a.unwrap().outcome, Outcome::Completed);
    assert_eq!(value(&a.db, "memories", "from_c"), Some(7));
}

#[tokio::test]
async fn each_peer_receives_only_the_collections_it_was_granted() {
    let network = Network::default();
    let a = node(&network, "a", &["server"], &["memories", "entities"]).await;
    // The phone is allowed memories only. Re-register A's policy per peer.
    let mesh = dbmesh::DbMesh::builder(a.db.clone(), a.store.clone())
        .policy(
            dbmesh::core::StaticPolicy::new()
                .exchange(
                    peer_id("server"),
                    [collection("memories"), collection("entities")],
                )
                .exchange(peer_id("phone"), [collection("memories")]),
        )
        .security(
            dbmesh::core::StaticTrust::new()
                .trust(peer_id("server"), [dbmesh::core::Capability::relay()])
                .trust(peer_id("phone"), [dbmesh::core::Capability::relay()]),
        )
        .build()
        .await
        .unwrap();
    mesh.register_peer(dbmesh::core::Peer::new(peer_id("phone")))
        .await
        .unwrap();
    let mut phone = node(&network, "phone", &["a"], &["memories", "entities"]).await;
    a.db.commit(vec![create("memories", "m", 1), create("entities", "e", 2)]);

    let dialer = network.dialer("a");
    let target = peer_id("phone");
    let listener = &mut phone.listener;
    let (from_a, _) = tokio::join!(mesh.sync_with(&dialer, &target), async {
        let connection = dbmesh::transport::Acceptor::accept(listener).await.unwrap();
        phone.mesh.serve(connection).await
    });

    assert_eq!(from_a.unwrap().outcome, Outcome::Completed);
    assert_eq!(value(&phone.db, "memories", "m"), Some(1));
    assert_eq!(
        value(&phone.db, "entities", "e"),
        None,
        "outside the phone's grant"
    );
    // Progress still moved past the filtered content.
    assert_eq!(
        phone.store.cursors().await.unwrap().get(&origin("a")),
        Sequence::new(1)
    );
}

#[tokio::test]
async fn a_node_that_only_holds_part_of_a_transaction_does_not_relay_it() {
    let network = Network::default();
    // The phone may only receive memories; the server is trusted with both.
    let a = node(&network, "a", &["phone"], &["memories"]).await;
    let mut phone = node(
        &network,
        "phone",
        &["a", "server"],
        &["memories", "entities"],
    )
    .await;
    let mut server = node(&network, "server", &["phone"], &["memories", "entities"]).await;
    // One transaction with two collections; A only shares `memories` with the phone,
    // so the phone ends up holding a trimmed copy.
    a.db.commit(vec![create("memories", "m", 1), create("entities", "e", 2)]);
    sync(&network, &a, &mut phone).await.0.unwrap();
    assert_eq!(value(&phone.db, "memories", "m"), Some(1));

    let (_, from_server) = sync(&network, &phone, &mut server).await;

    assert_eq!(from_server.unwrap().outcome, Outcome::Completed);
    assert_eq!(
        value(&server.db, "memories", "m"),
        None,
        "a trimmed copy must not pose as the whole transaction"
    );
}
