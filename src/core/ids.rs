//! Identifiers.
//!
//! Every identifier is its own type so a [`PeerId`] can never be passed where
//! an [`Origin`] is expected. They share one validation rule, which is what
//! keeps them safe to use as JSON map keys and in log lines.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// The longest accepted name, in bytes.
const MAX_LEN: usize = 128;

/// Validates the shared name rule.
///
/// Whitespace and control characters are rejected because names travel in
/// JSON keys, logs and (eventually) URLs, where they cause confusion rather
/// than expressiveness.
fn validate(kind: &'static str, value: &str) -> Result<()> {
    let reason = if value.is_empty() {
        Some("it is empty")
    } else if value.len() > MAX_LEN {
        Some("it is longer than 128 bytes")
    } else if value.chars().any(|c| c.is_whitespace() || c.is_control()) {
        Some("it contains whitespace or control characters")
    } else {
        None
    };
    match reason {
        Some(reason) => Err(Error::InvalidName {
            kind,
            value: value.to_owned(),
            reason,
        }),
        None => Ok(()),
    }
}

/// Declares a validated, string-backed newtype.
macro_rules! name_type {
    ($(#[$meta:meta])* $name:ident, $kind:literal) => {
        $(#[$meta])*
        #[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        // Deserialization goes through `TryFrom` so a hostile peer cannot smuggle an
        // invalid name past the validation rule.
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            /// Builds the identifier, validating it.
            ///
            /// # Errors
            ///
            /// Returns [`Error::InvalidName`] when the value is empty, too long or
            /// contains whitespace or control characters.
            pub fn new(value: impl Into<String>) -> Result<Self> {
                let value = value.into();
                validate($kind, &value)?;
                Ok(Self(value))
            }

            /// The identifier as a string slice.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = Error;

            fn try_from(value: String) -> Result<Self> {
                validate($kind, &value)?;
                Ok(Self(value))
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl FromStr for $name {
            type Err = Error;

            fn from_str(value: &str) -> Result<Self> {
                Self::new(value)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

name_type! {
    /// The identity of one DBMesh node: one application instance with one metadata store.
    ///
    /// It is generated once and then persisted; losing it forks the node's
    /// sequence numbers, so it belongs with the store, not in configuration.
    NodeId, "node id"
}

name_type! {
    /// How a node refers to *another* node it synchronizes with.
    ///
    /// Same value space as [`NodeId`], different role: keeping them apart stops
    /// "my identity" and "their identity" being swapped silently.
    PeerId, "peer id"
}

name_type! {
    /// The node that first committed a change.
    ///
    /// An origin never changes as a change is relayed; the node that hands it
    /// over is the *sender*, a different concept.
    Origin, "origin"
}

name_type! {
    /// A group of records of the same shape (a table, in SQL terms).
    ///
    /// The unit of selective synchronization.
    Collection, "collection"
}

name_type! {
    /// The identity of one committed transaction, stable across relays.
    TransactionId, "transaction id"
}

name_type! {
    /// The identity of one synchronization session. Local bookkeeping only: the
    /// remote peer never needs to remember it.
    SessionId, "session id"
}

name_type! {
    /// A feature of the protocol that a peer may or may not support.
    Capability, "capability"
}

name_type! {
    /// The kind of transport an endpoint speaks (`websocket`, `unix`, ...).
    TransportKind, "transport kind"
}

/// Implements infallible conversions between identifiers sharing the name rule.
macro_rules! convert {
    ($from:ident => $to:ident) => {
        impl From<&$from> for $to {
            fn from(value: &$from) -> Self {
                // Same validation rule, so the value is valid by construction.
                Self(value.0.clone())
            }
        }
    };
}

convert!(NodeId => Origin);
convert!(NodeId => PeerId);
convert!(Origin => NodeId);
convert!(Origin => PeerId);
convert!(PeerId => NodeId);
convert!(PeerId => Origin);

/// Implements random generation for identifiers that are minted locally.
macro_rules! generate {
    ($name:ident) => {
        impl $name {
            /// Mints a new random identifier.
            #[must_use]
            pub fn generate() -> Self {
                // A UUID is always a valid name, so this cannot fail.
                Self(uuid::Uuid::new_v4().simple().to_string())
            }
        }
    };
}

generate!(NodeId);
generate!(TransactionId);
generate!(SessionId);

impl Capability {
    /// The sender may relay changes it received from third parties, not only
    /// its own. Without it a pair of nodes exchanges first-hand changes only.
    #[must_use]
    pub fn relay() -> Self {
        Self("relay".to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_name_is_rejected() {
        assert!(matches!(NodeId::new(""), Err(Error::InvalidName { .. })));
    }

    #[test]
    fn a_name_with_whitespace_is_rejected() {
        assert!(Collection::new("my table").is_err());
    }

    #[test]
    fn an_overlong_name_is_rejected() {
        assert!(Origin::new("x".repeat(129)).is_err());
        assert!(Origin::new("x".repeat(128)).is_ok());
    }

    #[test]
    fn deserialization_applies_the_same_validation_as_construction() {
        let parsed: std::result::Result<PeerId, _> = serde_json::from_str("\"bad name\"");
        assert!(parsed.is_err());
    }

    #[test]
    fn a_generated_node_id_is_valid_and_unique() {
        let (a, b) = (NodeId::generate(), NodeId::generate());
        assert_ne!(a, b);
        assert!(NodeId::new(a.as_str()).is_ok());
    }

    #[test]
    fn roles_convert_without_changing_the_underlying_value() {
        let node = NodeId::new("laptop").unwrap();
        assert_eq!(Origin::from(&node).as_str(), "laptop");
        assert_eq!(PeerId::from(&node).as_str(), "laptop");
    }
}
