# ADR-001: Library first, no mandatory CLI

## Status

Accepted

## Context

Synchronization has to keep running inside the application that owns the data. A daemon or CLI that owns the
lifecycle would make synchronization a separate deployment, a separate failure mode and a second thing to trust with
the database.

## Decision

DBMesh is a Rust library embedded by the host. It owns no thread and no socket: the host decides when to capture,
serve and synchronize, and supplies the transport. A CLI may exist later as an optional administration client, but it
never owns the synchronization lifecycle.

## Consequences

- The host must call into DBMesh (`start`, `sync_with`, `serve`); until the supervisor lands, scheduling and retry
  are the host's job.
- Nothing in the design may assume a process whose only purpose is synchronization.
- *Rejected:* a sidecar daemon (extra deployment, extra trust boundary), and a `dbmesh sync` command (models sync as
  a one-shot copy, the opposite of the persistent mesh).
