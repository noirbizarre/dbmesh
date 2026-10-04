# Architecture Decisions

Records of the decisions that shape this project, and — more usefully — the
reasons behind them. An ADR is written when a choice is hard to reverse or
likely to be re-proposed.

The point is not the decision; it is the alternatives that were rejected and
why. A record that only states the outcome saves nobody the argument.

A decision is changed by writing a new ADR that supersedes the old one, never
by editing the old one. The history is the value.

## Format

`NNN-kebab-case-title.md`, numbered in the order written, with the sections:

- **Status** — Proposed, Accepted, or Superseded by ADR-NNN
- **Context** — the forces in play, before any decision
- **Decision** — what was decided
- **Consequences** — what this costs, including what it makes harder

## Index

- [ADR-001](001-library-first-no-mandatory-cli.md) — Library first, no mandatory CLI
- [ADR-002](002-single-crate-layers-as-modules.md) — One crate, layers as modules
- [ADR-003](003-sequence-per-transaction-and-origin-cursors.md) — Sequences number transactions per origin
- [ADR-004](004-relay-log-as-write-ahead-log.md) — The log is both the relay source and the write-ahead log
- [ADR-005](005-sans-io-session-state-machine.md) — The session is a sans-IO state machine
- [ADR-006](006-native-async-traits-runtime-agnostic.md) — Native async traits, no runtime in the library
- [ADR-007](007-default-deny-policy-evaluated-both-ways.md) — Default-deny policy, evaluated on send and on receive
- [ADR-008](008-conflict-boundary-adapter-detects-host-resolves.md) — Conflicts: the adapter detects, the host resolves
- [ADR-009](009-transports-carry-opaque-bytes.md) — Transports carry opaque bytes
- [ADR-010](010-surrealdb-behind-the-adapter-boundary.md) — SurrealDB behind the adapter boundary
