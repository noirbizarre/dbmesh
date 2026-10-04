# ADR-009: Transports carry opaque bytes

## Status

Accepted

## Context

HTTP, WebSocket, QUIC, Unix sockets and LAN protocols differ in everything except that they can move a message.
The engine must not change when one is added, and a node may use different ones for different peers.

## Decision

A `Connection` sends and receives whole messages as bytes. The protocol layer owns encoding; the transport layer
imports nothing from it (checked by `tests/layering.rs`). The only requirement is reliable message delivery;
ordering is not assumed, because gaps are detected from cursors anyway. Security properties a transport provides are
reported, not assumed.

## Consequences

- Adding a transport touches no engine code.
- Encoding can change without touching any transport.
- Only an in-process test transport exists in the bootstrap; the first real one is future work.
