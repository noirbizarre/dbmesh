# ADR-006: Native async traits, no runtime in the library

## Status

Accepted

## Context

The host application already has an async runtime and an opinion about it. The extension points (`Store`,
`Transport`, `ChangeSource`, `ChangeApplier`, `ConflictResolver`, security hooks) are all I/O-shaped.

## Decision

Traits return `impl Future + Send` (stable since Rust 1.75; the MSRV is 1.88) and are used through generics. The
library depends on no runtime; `tokio` is a dev-dependency used to drive tests.

## Consequences

- Zero boxing, no `async-trait` dependency, no runtime lock-in.
- The traits are not object-safe. A node that wants different transports per peer provides one composite transport
  (an enum over its transports). A dyn-compatible layer can be added later without changing the traits.
- `DbMesh` has several type parameters, hidden by defaults and a builder.
