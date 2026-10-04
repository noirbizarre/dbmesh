# ADR-005: The session is a sans-IO state machine

## Status

Accepted

## Context

Most protocol bugs live in unusual orderings: a drop between two messages, a duplicate, a late reply. Testing those
through real sockets and timers is slow and flaky.

## Decision

`Session` consumes `Input`s and returns `Action`s. It performs no I/O, reads no clock and spawns nothing. An async
driver executes the actions. A dropped connection is just `Input::Disconnected`; patience is `Input::TimedOut`, fed by
the host. `tests/layering.rs` fails if the state machine starts using `async`, `tokio` or a clock.

## Consequences

- Every failure scenario is a deterministic unit test.
- An extra indirection: reading the log and applying are requested as actions rather than called.
- The machine is stop-and-wait per direction; pipelining would be a protocol change, not a rewrite.
