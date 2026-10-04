# ADR-008: Conflicts: the adapter detects, the host resolves

## Status

Accepted

## Context

Whether two writes conflict, and what to do about it, depends on the application's data. A built-in rule
(last-write-wins, a CRDT) would be wrong for some application.

## Decision

The adapter reports `Conflicted` when a remote transaction does not fit local state, without applying any of it. The
host's `ConflictResolver` answers per conflict: accept local, accept remote, merge, or reject. One rejection refuses
the whole transaction, preserving atomicity. The default resolver rejects.

## Consequences

- No semantics are imposed, and nothing is lost by default.
- A rejected or locally-resolved conflict can leave replicas diverged until the application writes again.
- Detection quality is the adapter's. The reference adapter uses a base-revision check; causality tracking is future
  work.
- *Rejected:* CRDTs and global last-write-wins for the bootstrap.
