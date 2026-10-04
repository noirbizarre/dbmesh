//! Duplicate delivery must not corrupt state. Here a scripted peer replays a
//! batch the receiver has already applied, the way a retransmitting transport
//! or a confused peer would.

mod common;

use common::{Network, create, node, pair, value};
use dbmesh::Outcome;
use dbmesh::core::{ChangeBatch, Origin, Sequence, Transaction, TransactionId};
use dbmesh::protocol::{Decoded, Frame, Message, ProtocolVersion, VersionRange, codec};
use dbmesh::transport::Connection;

async fn send(connection: &mut common::ChannelConnection, message: Message) {
    let frame = Frame {
        version: ProtocolVersion::CURRENT,
        message,
    };
    connection
        .send(codec::encode(&frame).unwrap())
        .await
        .unwrap();
}

async fn next(connection: &mut common::ChannelConnection) -> Message {
    let bytes = connection
        .recv()
        .await
        .unwrap()
        .expect("the receiver closed early");
    match codec::decode(&bytes).unwrap() {
        Decoded::Frame(frame) => frame.message,
        Decoded::Unsupported { .. } => panic!("unexpected unsupported frame"),
    }
}

#[tokio::test]
async fn a_batch_delivered_twice_is_applied_once_and_acknowledged_both_times() {
    let network = Network::default();
    let b = node(&network, "b", &["a"], &["memories"]).await;
    let (mut script, server) = pair("a", "b", &network.wire, None);

    let origin = Origin::new("a").unwrap();
    let batch = ChangeBatch {
        origin: origin.clone(),
        from: Sequence::ZERO,
        through: Sequence::new(1),
        hops: 0,
        transactions: vec![Transaction::new(
            TransactionId::generate(),
            origin,
            Sequence::new(1),
            vec![create("memories", "foo", 1)],
        )],
    };

    let scripted = async {
        send(
            &mut script,
            Message::Hello {
                node: dbmesh::core::NodeId::new("a").unwrap(),
                versions: VersionRange::CURRENT,
                capabilities: dbmesh::core::Capabilities::default(),
                credential: None,
            },
        )
        .await;
        assert!(matches!(next(&mut script).await, Message::Accept { .. }));
        assert!(matches!(next(&mut script).await, Message::Have { .. }));
        send(
            &mut script,
            Message::Have {
                cursors: dbmesh::core::CursorSet::new(),
            },
        )
        .await;
        // B has nothing for us, so it says so right away.
        assert!(matches!(next(&mut script).await, Message::Done));

        // Delivered, acknowledged, then delivered again as if the ack had been lost.
        for _ in 0..2 {
            send(&mut script, Message::Batch(batch.clone())).await;
            assert!(
                matches!(next(&mut script).await, Message::Ack { through, .. } if through == Sequence::new(1))
            );
        }
        send(&mut script, Message::Done).await;
    };

    let (report, ()) = tokio::join!(b.mesh.serve(server), scripted);
    let report = report.unwrap();

    assert_eq!(report.outcome, Outcome::Completed);
    assert_eq!(
        report.transactions_received, 1,
        "the replay applied nothing"
    );
    assert_eq!(value(&b.db, "memories", "foo"), Some(1));
    assert_eq!(b.db.len(), 1);
}
