//! Wici protocol: wire types and lifecycles. No I/O.

mod body;
mod bytes;
mod command_state;
pub mod frame;
mod id;
mod lifecycle;
mod pair_state;
pub mod wire;

pub use body::{ArtifactRef, Body, BodyError, LiveBody, MAX_OPERATION_LEN};
pub use bytes::{Blob, BytesError, FixedBytes};
pub use command_state::CommandState;
pub use frame::{
    ClientFrame, ErrorCode, FrameError, Lane, PROTOCOL_VERSION, PairInfo, Position, ServerFrame,
    Timestamp,
};
pub use id::{ArtifactId, DeviceId, MessageId, PairId, ParseIdError, StreamId};
pub use lifecycle::{Lifecycle, TransitionError};
pub use pair_state::PairState;
pub use wire::{ParseWireError, WireEnum};
