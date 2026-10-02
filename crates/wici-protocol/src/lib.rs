//! Wici protocol: wire types and lifecycles. No I/O.

mod command_state;
mod lifecycle;
pub mod wire;

pub use command_state::CommandState;
pub use lifecycle::{Lifecycle, TransitionError};
pub use wire::{ParseWireError, WireEnum};
