# ADR-003: Sequences number transactions per origin

## Status

Accepted

## Context

A node must be able to say what it has, and a peer must be able to send exactly the rest, with no memory of any
previous connection. Numbering individual mutations would let a cursor point inside a transaction, and a single
global counter cannot describe a mesh where history arrives by several paths.

## Decision

Each origin numbers its committed transactions `1, 2, 3, ...` without gaps. A node's progress is a `CursorSet`: the
highest *processed* sequence per origin (applied, or deliberately skipped by policy). `Have` carries it. A batch
covers a half-open range `(from, through]`; transactions policy removes are absent, yet `through` moves past them.

## Consequences

- A cursor can never split a transaction, so retries cannot produce partially duplicated ones.
- Duplicates and multi-path delivery are detected by comparison, with no deduplication table.
- Filtering never stalls progress, at the cost that a cursor means *processed*, not *held*. A node must therefore
  never relay content it holds only in part (see ADR-004).
- Restoring a node from an old backup would reuse sequences; an origin epoch is future work.
