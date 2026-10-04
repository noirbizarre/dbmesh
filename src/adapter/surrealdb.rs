//! The SurrealDB mapping boundary.
//!
//! This module holds the *pure* part of the SurrealDB integration: how
//! SurrealDB's notions map to DBMesh's, with no SDK and no I/O. It is the
//! seam the live adapter will plug into, and it keeps SurrealDB-specific
//! knowledge out of the core, the protocol and the engine.
//!
//! # How the live adapter is expected to work (not yet implemented)
//!
//! * **Capture** reads `SHOW CHANGES FOR TABLE <t> SINCE <versionstamp>` per
//!   synchronized collection ([`show_changes`]). Mutations that share a
//!   versionstamp were committed in one transaction and become one
//!   [`CapturedTransaction`](super::CapturedTransaction). The versionstamp is the
//!   [`SourcePosition`].
//! * **Apply** replays a transaction inside a single `BEGIN`/`COMMIT` block.
//! * **No echo** requires a way to tell remote-applied writes from local ones
//!   in the changefeed; that mechanism is an open design question.
//! * A changefeed must be enabled per collection ([`enable_changefeed`]), and
//!   its retention bounds how long a peer can stay offline before it needs a
//!   snapshot.

use crate::core::{Collection, RecordId, SourcePosition};
use crate::error::{Error, Result};

/// Opening and closing delimiters SurrealDB uses for identifiers that are not plain words.
const OPEN: char = '⟨';
const CLOSE: char = '⟩';

/// Whether `key` can be written bare in a SurrealDB record id.
fn is_plain(key: &str) -> bool {
    !key.is_empty() && key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Formats a record identity the way SurrealDB writes it: `table:key`.
///
/// Keys that are not plain words are wrapped in `⟨…⟩`.
///
/// # Errors
///
/// Returns [`Error::InvalidName`] if the key contains the closing delimiter,
/// which could not be quoted unambiguously.
pub fn record_id_to_surreal(record: &RecordId) -> Result<String> {
    if record.key.contains(CLOSE) {
        return Err(Error::InvalidName {
            kind: "SurrealDB record key",
            value: record.key.clone(),
            reason: "it contains the closing delimiter `⟩`",
        });
    }
    Ok(if is_plain(&record.key) {
        format!("{}:{}", record.collection, record.key)
    } else {
        format!("{}:{OPEN}{}{CLOSE}", record.collection, record.key)
    })
}

/// Parses `table:key` or `table:⟨key⟩` back into a record identity.
///
/// # Errors
///
/// Returns [`Error::InvalidName`] if there is no table, no key, or the table is not a valid collection.
pub fn record_id_from_surreal(text: &str) -> Result<RecordId> {
    let invalid = |reason| Error::InvalidName {
        kind: "SurrealDB record id",
        value: text.to_owned(),
        reason,
    };
    let (table, key) = text
        .split_once(':')
        .ok_or_else(|| invalid("it has no `:` separator"))?;
    let key = key
        .strip_prefix(OPEN)
        .and_then(|rest| rest.strip_suffix(CLOSE))
        .unwrap_or(key);
    if key.is_empty() {
        return Err(invalid("its key is empty"));
    }
    Ok(RecordId::new(Collection::new(table)?, key))
}

/// A SurrealDB versionstamp, as the [`SourcePosition`] of a captured transaction.
#[must_use]
pub fn position_from_versionstamp(versionstamp: u64) -> SourcePosition {
    SourcePosition(versionstamp)
}

/// Rejects collection names that cannot be used unquoted in a statement.
///
/// The statement helpers interpolate the name, so anything beyond plain words
/// is refused rather than escaped: a table name is configuration, and a
/// surprising one is better surfaced than quoted away.
fn plain_table(collection: &Collection) -> Result<&str> {
    let name = collection.as_str();
    if is_plain(name) {
        Ok(name)
    } else {
        Err(Error::InvalidName {
            kind: "SurrealDB table name",
            value: name.to_owned(),
            reason: "only ASCII letters, digits and `_` are supported in statements",
        })
    }
}

/// The statement that enables change capture on a collection.
///
/// `INCLUDE ORIGINAL` keeps the pre-change state, which conflict detection
/// needs. `IF NOT EXISTS` never overwrites an existing table definition, so
/// a table that is already defined without a changefeed must be altered by
/// its owner: DBMesh does not rewrite the application's schema.
///
/// # Errors
///
/// Returns [`Error::InvalidName`] for a collection that is not a plain identifier.
pub fn enable_changefeed(collection: &Collection, retention: &str) -> Result<String> {
    Ok(format!(
        "DEFINE TABLE IF NOT EXISTS {} CHANGEFEED {retention} INCLUDE ORIGINAL;",
        plain_table(collection)?
    ))
}

/// The statement that reads a collection's changes after a position.
///
/// # Errors
///
/// Returns [`Error::InvalidName`] for a collection that is not a plain identifier.
pub fn show_changes(
    collection: &Collection,
    after: Option<SourcePosition>,
    limit: usize,
) -> Result<String> {
    Ok(format!(
        "SHOW CHANGES FOR TABLE {} SINCE {} LIMIT {limit};",
        plain_table(collection)?,
        after.map_or(0, |position| position.0)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plain_record_id_round_trips_without_quoting() {
        let record = RecordId::new(Collection::new("memory").unwrap(), "foo");
        let text = record_id_to_surreal(&record).unwrap();
        assert_eq!(text, "memory:foo");
        assert_eq!(record_id_from_surreal(&text).unwrap(), record);
    }

    #[test]
    fn a_key_with_punctuation_is_quoted_and_round_trips() {
        let record = RecordId::new(Collection::new("memory").unwrap(), "a b:c");
        let text = record_id_to_surreal(&record).unwrap();
        assert_eq!(text, "memory:⟨a b:c⟩");
        assert_eq!(record_id_from_surreal(&text).unwrap(), record);
    }

    #[test]
    fn a_key_containing_the_closing_delimiter_is_refused() {
        let record = RecordId::new(Collection::new("memory").unwrap(), "evil⟩");
        assert!(record_id_to_surreal(&record).is_err());
    }

    #[test]
    fn an_id_without_a_separator_is_refused() {
        assert!(record_id_from_surreal("memory").is_err());
    }

    #[test]
    fn statements_refuse_table_names_that_would_need_escaping() {
        let odd = Collection::new("my-table").unwrap();
        assert!(enable_changefeed(&odd, "7d").is_err());
        assert!(show_changes(&odd, None, 10).is_err());
    }

    #[test]
    fn show_changes_starts_from_zero_without_a_position() {
        let table = Collection::new("memory").unwrap();
        assert_eq!(
            show_changes(&table, None, 100).unwrap(),
            "SHOW CHANGES FOR TABLE memory SINCE 0 LIMIT 100;"
        );
    }
}
