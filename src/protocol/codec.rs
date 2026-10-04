//! Frames to bytes and back.
//!
//! JSON for now: readable on the wire and in tests. Transports only ever see
//! bytes, so swapping the encoding later changes this file and nothing else.

use serde_json::Value;

use super::message::{Frame, Message};
use super::version::ProtocolVersion;
use crate::error::{Error, Result};

/// The outcome of decoding bytes from a peer.
#[derive(Clone, Debug, PartialEq)]
pub enum Decoded {
    /// A frame this build understands.
    Frame(Frame),
    /// A well-formed frame of a type this build does not know.
    ///
    /// Reported instead of failing so the session can answer with an explicit
    /// `Unsupported` rather than dropping the connection mysteriously.
    Unsupported {
        /// The version the frame claims, when readable.
        version: Option<ProtocolVersion>,
        /// The unknown message type.
        kind: String,
    },
}

/// Encodes a frame.
///
/// # Errors
///
/// Returns [`Error::Encode`] if serialization fails.
pub fn encode(frame: &Frame) -> Result<Vec<u8>> {
    serde_json::to_vec(frame).map_err(Error::Encode)
}

/// Decodes bytes received from a peer.
///
/// # Errors
///
/// Returns [`Error::Malformed`] when the bytes are not JSON, or are a frame of
/// a *known* type whose fields are wrong. An unknown type is not an error: it
/// is [`Decoded::Unsupported`].
pub fn decode(bytes: &[u8]) -> Result<Decoded> {
    let value: Value = serde_json::from_slice(bytes).map_err(Error::Malformed)?;
    // Peek at the type first: deserializing straight into `Frame` would turn a
    // message from a newer peer into an opaque error instead of "unsupported".
    let kind = value
        .get("message")
        .and_then(|message| message.get("type"))
        .and_then(Value::as_str);
    if let Some(kind) = kind
        && !Message::KINDS.contains(&kind)
    {
        let version = value
            .get("version")
            .and_then(|version| serde_json::from_value(version.clone()).ok());
        return Ok(Decoded::Unsupported {
            version,
            kind: kind.to_owned(),
        });
    }
    serde_json::from_value(value)
        .map(Decoded::Frame)
        .map_err(Error::Malformed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_frame_survives_a_round_trip() {
        let frame = Frame {
            version: ProtocolVersion::CURRENT,
            message: Message::Done,
        };
        assert_eq!(
            decode(&encode(&frame).unwrap()).unwrap(),
            Decoded::Frame(frame)
        );
    }

    #[test]
    fn a_message_type_from_the_future_is_unsupported_not_malformed() {
        let bytes = br#"{"version":{"major":1,"minor":9},"message":{"type":"teleport","x":1}}"#;
        assert_eq!(
            decode(bytes).unwrap(),
            Decoded::Unsupported {
                version: Some(ProtocolVersion::new(1, 9)),
                kind: "teleport".to_owned()
            }
        );
    }

    #[test]
    fn a_known_type_with_wrong_fields_is_malformed() {
        let bytes = br#"{"version":{"major":1,"minor":0},"message":{"type":"ack"}}"#;
        assert!(matches!(decode(bytes), Err(Error::Malformed(_))));
    }

    #[test]
    fn bytes_that_are_not_json_are_malformed() {
        assert!(matches!(decode(b"\x00\x01"), Err(Error::Malformed(_))));
    }
}
