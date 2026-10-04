# AGENTS.md

Notes for anyone — human or otherwise — changing this repository.

## What this project is

A persistent, decentralized synchronization mesh for application-owned database state.

DBMesh is an **embedded, library-first** engine: applications link it and run it in their own process, so there is no
daemon, no central server and no CLI that owns synchronization. The mesh is *logical* (who shares state), not the set
of open connections: nodes go offline, reach only some peers, and resume from durable per-origin cursors. The
**application owns the data and the decisions**: what is synchronized, with whom, whom to trust, how conflicts are
resolved. DBMesh synchronizes *selected logical changes*, never storage files, and is not a database cluster, a
primary/replica scheme, a CRDT library or a federation layer. SurrealDB is the first backend but stays behind the
adapter boundary. Nothing MemCastle-specific belongs here.

## Non-negotiable invariants

Each of these should be enforced by a hook or a test. An invariant nothing
checks is a comment, and it will be violated.

The full list, with the test behind each, is in [docs/architecture.md](docs/architecture.md#invariants). The ones
to keep in mind while editing:

1. **A cursor never points inside a transaction, and never moves backwards** — `core::sequence` tests and
   `storage::memory` tests.
2. **Received work is logged before it is applied; the cursor moves after** —
   `two_node_sync::a_crash_between_logging_and_applying_is_recovered_without_duplicates`.
3. **Duplicate delivery changes nothing** — `tests/idempotent_replay.rs`.
4. **Nothing is shared or trusted by default; policy runs on send *and* on receive** — `core::policy` tests,
   `two_node_sync::each_peer_receives_only_the_collections_it_was_granted`.
5. **A node never relays content it holds only in part** —
   `two_node_sync::a_node_that_only_holds_part_of_a_transaction_does_not_relay_it`.
6. **Layers depend inward only; SurrealDB appears only in `adapter::surrealdb`; the session state machine does no
   I/O** — `tests/layering.rs`.
7. **The wire format is a public contract** — `tests/protocol_wire.rs`.

Adding a design rule means adding the test that enforces it, and a row in the architecture document. Design
decisions that are hard to reverse get an ADR in `docs/adr/`.

## Layout

```text
src/
├── lib.rs        the library surface and the embedding example
├── error.rs      the crate's error type
├── core/         domain types and host-owned boundaries (policy, conflicts, trust); imports no sibling layer
├── protocol/     versioned messages and the codec
├── transport/    Connection / Transport / Acceptor: opaque bytes only
├── storage/      Store trait and MemoryStore: DBMesh's own durable state
├── adapter/      ChangeSource / ChangeApplier, MemoryDatabase, surrealdb mapping (feature `surrealdb`)
└── engine/       session state machine (pure), async driver, DbMesh
tests/            integration, wire-format and layering tests
docs/             architecture.md and ADRs
```

There is no binary. Anything `pub` in `lib.rs` is public API, so keep the
surface small and deliberate. The layers are modules of one crate on purpose
(ADR-002): add a dependency edge between layers only if `tests/layering.rs`
allows it, and say why in the pull request.

## Style

**Every non-obvious line carries a comment saying why.** Not what — the code
says what. Ideally naming the failure it prevents. A comment that restates the
code is worse than none.

**Errors are typed and actionable.** `thiserror` for the error type, `miette`'s `Diagnostic` so a caller can
render it as it likes. A diagnostic must carry the two things the user does not
already know: what specifically failed, and what to do about it. Diagnostic
codes are `dbmesh::<module>::<kind>`, and a code is a public identifier
users grep for — renaming one is a breaking change.

**Test names are sentences.** `an_unchanged_input_produces_no_output`, not
`test_run_2`. The name should say what would be broken if it failed.

## Commits

Conventional Commits, enforced by commitlint on `commit-msg`. The type becomes a
changelog heading, so choose it as if someone will read it in release notes —
because they will.

## Releases

Driven by gh-ship. Never bump a version or push a tag by hand: `cliff.toml`
derives the version from the commit history, `prepare-release` applies it, and
`.github/ship.yml` is the contract between them. See CONTRIBUTING.md.

## Before you push

```sh
mise run ci
```

Formatting, Clippy, spelling, workflow and Markdown linting, tests and the documentation build. Same as CI.

## This repository is generated from a template

The toolchain, hooks, CI and release workflows come from
[rust.tpl](https://github.com/noirbizarre/rust.tpl) and are updated with
`git tpl update`. Files carrying template-owned content end with a
`# --- project-specific ---` marker: add below it, never above.

Changing template-owned content here fixes it in one repository. Changing it in
the template fixes it in all of them — prefer that.
