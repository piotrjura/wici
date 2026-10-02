//! Wire types and command lifecycle rules of the Wici protocol.
//!
//! This crate has no storage, network, or runtime dependencies. Other Wici
//! crates use it as the shared definition of protocol semantics.

mod command_state;

pub use command_state::{CommandState, ParseCommandStateError, TransitionError};
