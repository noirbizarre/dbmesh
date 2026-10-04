# ADR-004: The log is both the relay source and the write-ahead log

## Status

Accepted

## Context

Multi-hop propagation needs a node to resend history it did not author. Crash recovery needs received work to be
durable before it touches the database. Both need the same thing: a log of transactions per origin.

## Decision

Local and received transactions are appended to the log. A received transaction is logged **before** it is applied
and the processed cursor moves **after**; `Store::unapplied` is what a restart must finish. Applying must be
idempotent (an adapter contract), so replay after a crash is safe. Entries that are empty or partial are kept as
placeholders but never relayed.

## Consequences

- Recovery needs no special format: it is the normal apply path over the unapplied tail.
- The log grows. Retention and snapshots for peers behind it are future work; such peers are reported as
  `SnapshotRequired` rather than served wrongly.
- Adapters carry an obligation (idempotent, atomic apply, no echo) that cannot be checked by the engine, only
  documented and tested against the reference adapter.
