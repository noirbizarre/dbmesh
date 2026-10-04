# ADR-007: Default-deny policy, evaluated on send and on receive

## Status

Accepted

## Context

Applications own the question of what may be shared and with whom. A mesh that shares everything by default, or
trusts whoever connects, turns a sync library into a data leak.

## Decision

`SyncPolicy` decides per peer, per direction and per mutation, synchronously and purely. The default
(`DenyAll`) allows nothing, and the default security (`DenyUnknown`) trusts nobody. Policy runs before a mutation is
sent and again before a received one is applied. A transaction policy only partly allows is sent or applied trimmed
and marked `partial`.

## Consequences

- A misbehaving peer cannot push data the host did not agree to take.
- A host must opt in explicitly; the first run of a misconfigured node shares nothing, which is the safe failure.
- Policy is synchronous: a rule that needs I/O must be resolved into data beforehand. Field-level redaction fits the
  `Decision` enum later without breaking implementors, which is why it is non-exhaustive.
