# dbmesh

A persistent, decentralized synchronization mesh for application-owned database state.

DBMesh is a library you embed in your application. Each instance is a node of a logical mesh that keeps accepting
writes while offline and catches up from whichever peer it reaches next, resuming from durable cursors. What is
shared, with whom, and how conflicts are resolved stays under your application's control.

## Installation

```bash
cargo add dbmesh
```

## Usage

```rust
let mesh = DbMesh::builder(database, store)
    .policy(policy)      // what may be synchronized, per peer
    .security(trust)     // who is trusted
    .resolver(resolver)  // how conflicts are decided
    .build()
    .await?;

mesh.start().await?;
let report = mesh.sync_with(&transport, &peer).await?;
```

## Where to go next

- The [README](https://github.com/noirbizarre/dbmesh#readme) explains the model: mesh, policy, propagation,
  transactions, checkpoints, conflicts and security.
- [Architecture](architecture.md) lists the layers, the invariants and the failure model.
- [Architecture decisions](adr/README.md) record why each choice was made, and what was rejected.
