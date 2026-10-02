//! Wici protocol: wire types and lifecycles. No I/O.

mod bytes;
mod command_state;
mod id;
mod lifecycle;
mod pair_state;
pub mod wire;

pub use bytes::{Blob, BytesError, FixedBytes};
pub use command_state::CommandState;
pub use id::{ArtifactId, DeviceId, MessageId, PairId, ParseIdError, StreamId};
pub use lifecycle::{Lifecycle, TransitionError};
pub use pair_state::PairState;
pub use wire::{ParseWireError, WireEnum};
