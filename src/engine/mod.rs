//! The synchronization engine.
//!
//! * [`session`]: the pure state machine of one session.
//! * `driver`: the async executor of the machine's actions (private).
//! * [`DbMesh`]: the handle an application embeds.

mod driver;
mod filter;
mod mesh;
pub mod session;

pub use driver::SessionReport;
pub use mesh::{DbMesh, DbMeshBuilder, StartReport};
pub use session::Outcome;
