//! The wire format is a public contract: peers running different builds must
//! agree on it. These tests pin it byte-for-byte (as JSON values), so an
//! accidental change shows up as a failing test, not as a mesh that silently
//! stops talking.

use std::collections::BTreeSet;

use dbmesh::core::{
    Capabilities, Capability, ChangeBatch, Collection, CursorSet, Mutation, NodeId, Operation,
    Origin, Payload, RecordId, Sequence, Transaction, TransactionId,
};
use dbmesh::protocol::{
    Decoded, Frame, Message, NackReason, ProtocolVersion, RejectReason, VersionRange, codec,
};
use serde_json::{Value, json};

fn frame(message: Message) -> Frame {
    Frame {
        version: ProtocolVersion::new(1, 0),
        message,
    }
}

/// Asserts the frame encodes to exactly `expected` and decodes back to itself.
fn golden(message: Message, expected: Value) {
    let frame = frame(message);
    let bytes = codec::encode(&frame).unwrap();
    let actual: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        actual,
        json!({ "version": { "major": 1, "minor": 0 }, "message": expected })
    );
    assert_eq!(codec::decode(&bytes).unwrap(), Decoded::Frame(frame));
}

fn origin(name: &str) -> Origin {
    Origin::new(name).unwrap()
}

fn transaction() -> Transaction {
    Transaction::new(
        TransactionId::new("tx-1").unwrap(),
        origin("laptop"),
        Sequence::new(18427),
        vec![Mutation::new(
            RecordId::new(Collection::new("memories").unwrap(), "foo"),
            Operation::Create,
            Some(Payload(json!({ "text": "hello" }))),
        )],
    )
}

#[test]
fn hello_states_identity_versions_and_capabilities() {
    golden(
        Message::Hello {
            node: NodeId::new("laptop").unwrap(),
            versions: VersionRange::CURRENT,
            capabilities: Capabilities::default().support(Capability::relay()),
            credential: None,
        },
        json!({
            "type": "hello",
            "node": "laptop",
            "versions": { "min": { "major": 1, "minor": 0 }, "max": { "major": 1, "minor": 0 } },
            "capabilities": { "required": [], "supported": ["relay"] }
        }),
    );
}

#[test]
fn accept_states_the_settled_terms() {
    golden(
        Message::Accept {
            node: NodeId::new("server").unwrap(),
            version: ProtocolVersion::new(1, 0),
            capabilities: BTreeSet::from([Capability::relay()]),
            credential: None,
        },
        json!({
            "type": "accept",
            "node": "server",
            "version": { "major": 1, "minor": 0 },
            "capabilities": ["relay"]
        }),
    );
}

#[test]
fn reject_carries_a_machine_readable_reason_and_a_human_one() {
    golden(
        Message::Reject {
            reason: RejectReason::IncompatibleVersion,
            detail: "no common version".into(),
        },
        json!({ "type": "reject", "reason": "incompatible_version", "detail": "no common version" }),
    );
}

#[test]
fn have_is_the_durable_cursor_per_origin() {
    // "I have received origin=laptop through sequence=18427."
    let cursors: CursorSet = [(origin("laptop"), Sequence::new(18427))]
        .into_iter()
        .collect();
    golden(
        Message::Have { cursors },
        json!({ "type": "have", "cursors": { "laptop": 18427 } }),
    );
}

#[test]
fn a_batch_names_the_range_it_covers_and_carries_whole_transactions() {
    golden(
        Message::Batch(ChangeBatch {
            origin: origin("laptop"),
            from: Sequence::new(18426),
            through: Sequence::new(18427),
            hops: 0,
            transactions: vec![transaction()],
        }),
        json!({
            "type": "batch",
            "origin": "laptop",
            "from": 18426,
            "through": 18427,
            "hops": 0,
            "transactions": [{
                "id": "tx-1",
                "origin": "laptop",
                "sequence": 18427,
                "mutations": [{
                    "record": { "collection": "memories", "key": "foo" },
                    "operation": "create",
                    "payload": { "text": "hello" }
                }]
            }]
        }),
    );
}

#[test]
fn a_trimmed_transaction_says_so_on_the_wire() {
    let mut partial = transaction();
    partial.partial = true;
    let encoded = serde_json::to_value(&partial).unwrap();
    assert_eq!(encoded["partial"], json!(true));
    assert!(
        serde_json::to_value(transaction())
            .unwrap()
            .get("partial")
            .is_none()
    );
}

#[test]
fn ack_confirms_progress_per_origin() {
    golden(
        Message::Ack {
            origin: origin("laptop"),
            through: Sequence::new(18427),
        },
        json!({ "type": "ack", "origin": "laptop", "through": 18427 }),
    );
}

#[test]
fn nack_reports_where_the_receiver_actually_is() {
    golden(
        Message::Nack {
            reason: NackReason::Gap,
            detail: "missing 3..5".into(),
            have: Some(Sequence::new(2)),
        },
        json!({ "type": "nack", "reason": "gap", "detail": "missing 3..5", "have": 2 }),
    );
}

#[test]
fn done_goodbye_and_unsupported_have_stable_shapes() {
    golden(Message::Done, json!({ "type": "done" }));
    golden(
        Message::Goodbye {
            reason: "shutting down".into(),
        },
        json!({ "type": "goodbye", "reason": "shutting down" }),
    );
    golden(
        Message::Unsupported {
            feature: "teleport".into(),
        },
        json!({ "type": "unsupported", "feature": "teleport" }),
    );
}

#[test]
fn a_refusal_reason_from_a_newer_peer_still_parses() {
    let bytes = br#"{"version":{"major":1,"minor":3},"message":{"type":"reject","reason":"quota_exceeded","detail":"x"}}"#;
    let Decoded::Frame(Frame {
        message: Message::Reject { reason, .. },
        ..
    }) = codec::decode(bytes).unwrap()
    else {
        panic!("expected a reject frame");
    };
    assert_eq!(reason, RejectReason::Unknown);
}

#[test]
fn an_unknown_field_from_a_newer_minor_version_is_ignored() {
    let bytes = br#"{"version":{"major":1,"minor":3},"message":{"type":"ack","origin":"a","through":1,"extra":true}}"#;
    assert!(matches!(codec::decode(bytes).unwrap(), Decoded::Frame(_)));
}

#[test]
fn a_node_name_that_breaks_the_naming_rule_is_malformed_on_the_wire() {
    let bytes = br#"{"version":{"major":1,"minor":0},"message":{"type":"ack","origin":"has space","through":1}}"#;
    assert!(codec::decode(bytes).is_err());
}
