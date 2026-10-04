//! A persistent, decentralized synchronization mesh for application-owned database state.
//!
//! DBMesh is a library you embed in your application. Each application
//! instance is a *node*; nodes form a logical mesh that stays the same while
//! the physical connections between them come and go. A node keeps accepting
//! writes while offline, and catches up from whichever peer it reaches next,
//! resuming from durable per-origin cursors.
//!
//! # Layers
//!
//! | Module | Role |
//! |---|---|
//! | [`core`] | domain types and the boundaries the host owns (policy, conflicts, trust) |
//! | [`protocol`] | the versioned node-to-node messages and their encoding |
//! | [`transport`] | how bytes move between nodes; one trait, any mechanism |
//! | [`storage`] | where DBMesh keeps its own state, apart from yours |
//! | [`adapter`] | the bridge to one database (and a SurrealDB mapping boundary) |
//! | [`engine`] | the session state machine, its async driver, and [`DbMesh`] |
//!
//! See `docs/architecture.md` for the invariants these boundaries protect.
//!
//! # Embedding
//!
//! ```
//! use dbmesh::adapter::MemoryDatabase;
//! use dbmesh::core::{Collection, PeerId, StaticPolicy, StaticTrust, Capability};
//! use dbmesh::storage::MemoryStore;
//! use dbmesh::DbMesh;
//!
//! # async fn demo() -> dbmesh::Result<()> {
//! let server = PeerId::new("server")?;
//! let mesh = DbMesh::builder(MemoryDatabase::new(), MemoryStore::new())
//!     // Nothing is synchronized unless the application says so.
//!     .policy(StaticPolicy::new().exchange(server.clone(), [Collection::new("memories")?]))
//!     // Nobody is trusted unless the application says so.
//!     .security(StaticTrust::new().trust(server, [Capability::relay()]))
//!     .build()
//!     .await?;
//!
//! // Finishes interrupted work and records local changes. The host decides when
//! // to open sessions (`sync_with`) and which transport reaches which peer.
//! mesh.start().await?;
//! # Ok(())
//! # }
//! ```

// miette's `Diagnostic` payloads carry source text and spans, which puts most
// error variants past clippy's 128-byte `Result` threshold. The lint is right
// about the cost and wrong about the trade: a large error that says what to do
// beats a small one that does not.
#![allow(clippy::result_large_err)]
#![warn(missing_docs)]

pub mod adapter;
pub mod core;
pub mod engine;
pub mod error;
pub mod protocol;
pub mod storage;
pub mod transport;

pub use engine::{DbMesh, DbMeshBuilder, Outcome, SessionReport, StartReport};
pub use error::{Error, Result};
