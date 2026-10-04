# ADR-010: SurrealDB behind the adapter boundary

## Status

Accepted

## Context

SurrealDB is the first backend, but the engine and protocol must not become SurrealDB-shaped, and the SDK is a large
dependency with its own API churn.

## Decision

Backends implement `ChangeSource` (capture) and `ChangeApplier` (apply) over DBMesh's logical `Mutation`s. The
`surrealdb` feature provides only the pure mapping: record identity, versionstamp positions, and changefeed
statements. It carries no SDK dependency yet, and no SurrealDB type appears outside `adapter::surrealdb`, which
`tests/layering.rs` checks. The expected live design is documented in that module.

## Consequences

- The engine is tested against a reference in-memory adapter that obeys the same contract.
- The live adapter has open design questions (echo suppression, mapping changefeed entries to transactions,
  retention versus offline duration) tracked as issues rather than guessed at.
- DBMesh does not replicate storage-engine files or internals: it moves logical changes only.
