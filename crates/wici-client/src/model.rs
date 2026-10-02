//! Public client data types.

use wici_protocol::{
    Body, CommandState, DeviceId, ErrorCode, Lane, LiveBody, MessageId, PairId, PairState,
    Position, Timestamp, wire_enum,
};

wire_enum! {
    /// This device's side of a pair.
    pub enum Role ("pair role") {
        /// Created the invitation.
        Inviter = "inviter",
        /// Joined with the invitation.
        Invitee = "invitee",
    }
}

wire_enum! {
    /// Pair operation waiting for server confirmation.
    pub enum PendingOp ("pending operation") {
        /// Register the invitation.
        Invite = "invite",
        /// Claim the invitation.
        Claim = "claim",
        /// Approve the claimer.
        Approve = "approve",
        /// End the pair.
        Unpair = "unpair",
    }
}

wire_enum! {
    /// Who sent a command.
    pub enum Direction ("command direction") {
        /// This device sent it.
        Outgoing = "outgoing",
        /// The peer sent it.
        Incoming = "incoming",
    }
}

impl PendingOp {
    /// `true` if a server pair in `state` confirms this operation.
    #[must_use]
    pub const fn confirmed_by(self, state: PairState) -> bool {
        match self {
            Self::Invite => true,
            Self::Claim => matches!(state, PairState::Claimed | PairState::Active),
            Self::Approve => matches!(state, PairState::Active),
            Self::Unpair => matches!(state, PairState::Revoked),
        }
    }
}

/// A pair as this device knows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PairView {
    /// Pair ID.
    pub id: PairId,
    /// This device's side.
    pub role: Role,
    /// Last known state.
    pub state: PairState,
    /// Operation not yet confirmed by the server.
    pub pending: Option<PendingOp>,
    /// The other device, once known.
    pub peer: Option<DeviceId>,
}

/// A received durable message.
#[derive(Debug, Clone, PartialEq)]
pub struct Incoming {
    /// Pair ID.
    pub pair: PairId,
    /// Lane.
    pub lane: Lane,
    /// Position in the lane.
    pub position: Position,
    /// Message ID.
    pub id: MessageId,
    /// Server acceptance time.
    pub accepted_at: Timestamp,
    /// Body.
    pub body: Body,
}

/// Something the app should know about.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// Connected and authenticated.
    Connected,
    /// Connection lost. The client reconnects on its own.
    Disconnected,
    /// A pair changed.
    Pair(PairView),
    /// A device claimed this device's invitation. Call `approve` to accept.
    Claimed {
        /// Pair ID.
        pair: PairId,
        /// Claiming device.
        device: DeviceId,
    },
    /// A durable message arrived. Call `handled` after processing it.
    Message(Incoming),
    /// A live update arrived.
    Live {
        /// Pair ID.
        pair: PairId,
        /// Body.
        body: LiveBody,
    },
    /// The peer's connection changed.
    Presence {
        /// Pair ID.
        pair: PairId,
        /// `true` if connected.
        online: bool,
        /// Last time connected.
        last_seen: Option<Timestamp>,
    },
    /// The server stored a sent message.
    Accepted {
        /// Pair ID.
        pair: PairId,
        /// Message ID.
        id: MessageId,
        /// Position.
        position: Position,
    },
    /// A command this device sent changed state.
    Command {
        /// Pair ID.
        pair: PairId,
        /// Command ID.
        id: MessageId,
        /// New state.
        state: CommandState,
    },
    /// A request failed for good.
    Failed {
        /// Related pair.
        pair: Option<PairId>,
        /// Related message.
        id: Option<MessageId>,
        /// Error code.
        code: ErrorCode,
        /// Reason.
        message: String,
    },
    /// A received message could not be opened. It is kept as evidence.
    Quarantined {
        /// Pair ID.
        pair: PairId,
        /// Lane.
        lane: Lane,
        /// Position.
        position: Position,
    },
}

#[cfg(test)]
mod tests {
    use wici_protocol::WireEnum;

    use super::*;

    #[test]
    fn wire_names_are_stable() {
        let names = |all: &[&str]| all.join(",");
        assert_eq!(
            names(&Role::ALL.iter().map(|r| r.as_str()).collect::<Vec<_>>()),
            "inviter,invitee"
        );
        assert_eq!(
            names(
                &PendingOp::ALL
                    .iter()
                    .map(|r| r.as_str())
                    .collect::<Vec<_>>()
            ),
            "invite,claim,approve,unpair"
        );
        assert_eq!(
            names(
                &Direction::ALL
                    .iter()
                    .map(|r| r.as_str())
                    .collect::<Vec<_>>()
            ),
            "outgoing,incoming"
        );
    }

    #[test]
    fn pending_ops_are_confirmed_by_the_right_states() {
        use PairState::{Active, Claimed, Expired, Invited, Revoked};
        let table = [
            (PendingOp::Invite, [true, true, true, true, true]),
            (PendingOp::Claim, [false, true, true, false, false]),
            (PendingOp::Approve, [false, false, true, false, false]),
            (PendingOp::Unpair, [false, false, false, true, false]),
        ];
        for (op, expected) in table {
            let actual = [Invited, Claimed, Active, Revoked, Expired].map(|s| op.confirmed_by(s));
            assert_eq!(actual, expected, "{op}");
        }
    }
}
