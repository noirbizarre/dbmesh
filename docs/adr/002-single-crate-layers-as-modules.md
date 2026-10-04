# ADR-002: One crate, layers as modules

## Status

Accepted

## Context

The design has clear conceptual layers (core, protocol, transport, storage, adapter, engine) and a heavy optional
dependency on the horizon (the SurrealDB SDK). Splitting into crates now adds version coordination for a project with
one consumer and no stable API.

## Decision

One crate, `dbmesh`, with one module per layer. Dependencies point inward: every layer may use `core`, only `engine`
uses the others. `tests/layering.rs` enforces this by reading the source, so the boundary does not rely on discipline.

## Consequences

- Cheap to refactor while the API is still moving.
- Promoting a module to a crate stays mechanical because the dependency direction is already enforced.
- Feature flags (`surrealdb`) carry what a separate crate otherwise would, until the SDK dependency arrives.
- *Rejected:* a workspace now. The template supports it, but multiple crates for aesthetics add ceremony without
  protecting anything the layering test does not already protect.
