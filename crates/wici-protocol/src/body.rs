//! Plaintext bodies. Devices seal them; the server never sees them.

use std::error::Error;
use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::bytes::FixedBytes;
use crate::command_state::CommandState;
use crate::frame::Timestamp;
use crate::id::{ArtifactId, MessageId, StreamId};

/// Maximum operation name length in bytes.
pub const MAX_OPERATION_LEN: usize = 128;

/// Body of a durable message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Body {
    /// Ask the peer to run an operation. The message ID is the command ID.
    Command {
        /// App-defined operation name.
        operation: String,
        /// App-defined input.
        input: Value,
        /// The peer must not start the command after this time.
        deadline: Timestamp,
        /// Stream for the command's events, if any.
        stream: Option<StreamId>,
    },
    /// Receiver-side state change of a command.
    Status {
        /// Command ID.
        command: MessageId,
        /// New state.
        state: CommandState,
        /// Result or error for terminal states.
        output: Option<Value>,
    },
    /// Ask the peer to cancel a command.
    Cancel {
        /// Command ID.
        command: MessageId,
    },
    /// The agent needs a decision.
    ApprovalRequest {
        /// Command ID.
        command: MessageId,
        /// Approval ID. A decision applies only to this ID.
        approval: MessageId,
        /// App-defined details.
        details: Value,
    },
    /// Decision for an approval request.
    ApprovalDecision {
        /// Command ID.
        command: MessageId,
        /// Approval ID.
        approval: MessageId,
        /// `true` to allow.
        allow: bool,
    },
    /// Durable stream event.
    Event {
        /// Stream ID.
        stream: StreamId,
        /// App-defined data.
        data: Value,
    },
}

/// Describes an uploaded artifact. Put it in a sealed body (for example
/// in `Event::data`) so only the peer learns the key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRef {
    /// Artifact ID.
    pub id: ArtifactId,
    /// Artifact key.
    pub key: FixedBytes<32>,
    /// Plaintext size.
    pub size: u64,
    /// Sealed size stored on the server.
    pub sealed_size: u64,
    /// SHA-256 of the sealed bytes.
    pub hash: FixedBytes<32>,
    /// Media type, for example `image/png`.
    pub media_type: String,
    /// File name, if any.
    pub name: Option<String>,
}

/// Body of a live (non-durable) update.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LiveBody {
    /// Stream ID.
    pub stream: StreamId,
    /// App-defined delta.
    pub data: Value,
}

/// A body breaks a protocol rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyError {
    /// Operation name is empty or longer than [`MAX_OPERATION_LEN`].
    OperationName,
    /// Receivers may not report this state.
    StatusState(CommandState),
}

impl fmt::Display for BodyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OperationName => {
                write!(f, "operation name must be 1 to {MAX_OPERATION_LEN} bytes")
            }
            Self::StatusState(state) => write!(f, "receiver cannot report state `{state}`"),
        }
    }
}

impl Error for BodyError {}

impl Body {
    /// Checks protocol rules that serde cannot express.
    ///
    /// # Errors
    ///
    /// Returns [`BodyError`] for the first broken rule.
    pub fn validate(&self) -> Result<(), BodyError> {
        match self {
            Self::Command { operation, .. } => {
                if operation.is_empty() || operation.len() > MAX_OPERATION_LEN {
                    return Err(BodyError::OperationName);
                }
                Ok(())
            }
            Self::Status { state, .. } => {
                if matches!(
                    state,
                    CommandState::QueuedLocal | CommandState::AcceptedDurable
                ) {
                    return Err(BodyError::StatusState(*state));
                }
                Ok(())
            }
            Self::Cancel { .. }
            | Self::ApprovalRequest { .. }
            | Self::ApprovalDecision { .. }
            | Self::Event { .. } => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::wire::WireEnum;

    fn command(operation: &str) -> Body {
        Body::Command {
            operation: operation.to_owned(),
            input: json!({"text": "hi"}),
            deadline: Timestamp(9),
            stream: None,
        }
    }

    fn status(state: CommandState) -> Body {
        Body::Status {
            command: MessageId::from_bytes([1; 16]),
            state,
            output: None,
        }
    }

    #[test]
    fn command_has_stable_json() {
        let value = serde_json::to_value(command("chat.send")).unwrap();
        assert_eq!(
            value,
            json!({
                "type": "command", "operation": "chat.send",
                "input": {"text": "hi"}, "deadline": 9, "stream": null
            })
        );
        assert_eq!(
            serde_json::from_value::<Body>(value).unwrap(),
            command("chat.send")
        );
    }

    #[test]
    fn operation_name_must_be_bounded() {
        assert_eq!(command("x").validate(), Ok(()));
        assert_eq!(command(&"x".repeat(MAX_OPERATION_LEN)).validate(), Ok(()));
        assert_eq!(command("").validate(), Err(BodyError::OperationName));
        assert_eq!(
            command(&"x".repeat(MAX_OPERATION_LEN + 1)).validate(),
            Err(BodyError::OperationName)
        );
        assert_eq!(
            BodyError::OperationName.to_string(),
            "operation name must be 1 to 128 bytes"
        );
    }

    #[test]
    fn receivers_report_only_their_own_states() {
        for &state in CommandState::ALL {
            let allowed = !matches!(
                state,
                CommandState::QueuedLocal | CommandState::AcceptedDurable
            );
            assert_eq!(status(state).validate().is_ok(), allowed, "{state}");
        }
        assert_eq!(
            BodyError::StatusState(CommandState::QueuedLocal).to_string(),
            "receiver cannot report state `queued_local`"
        );
    }

    #[test]
    fn other_bodies_are_always_valid() {
        let command_id = MessageId::from_bytes([1; 16]);
        let stream = StreamId::from_bytes([2; 16]);
        for body in [
            Body::Cancel {
                command: command_id,
            },
            Body::ApprovalRequest {
                command: command_id,
                approval: command_id,
                details: json!(null),
            },
            Body::ApprovalDecision {
                command: command_id,
                approval: command_id,
                allow: true,
            },
            Body::Event {
                stream,
                data: json!([1]),
            },
        ] {
            assert_eq!(body.validate(), Ok(()));
            let value = serde_json::to_value(&body).unwrap();
            assert_eq!(serde_json::from_value::<Body>(value).unwrap(), body);
        }
        let live = LiveBody {
            stream,
            data: json!("tok"),
        };
        let value = serde_json::to_value(&live).unwrap();
        assert_eq!(serde_json::from_value::<LiveBody>(value).unwrap(), live);
    }
}
