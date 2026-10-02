//! WebSocket frames between devices and the server. JSON text, tagged by `type`.

use std::error::Error;
use std::fmt;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::bytes::{Blob, FixedBytes};
use crate::id::{DeviceId, MessageId, PairId};
use crate::pair_state::PairState;
use crate::wire_enum;

/// Current wire version.
pub const PROTOCOL_VERSION: u16 = 1;

/// Milliseconds since the Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(pub u64);

/// Position of a durable message in one lane of one direction of a pair.
/// Starts at 1. Gaps never occur.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Position(pub u64);

wire_enum! {
    /// Delivery lane. Each lane has its own order, so output never delays control.
    pub enum Lane ("lane") {
        /// Commands, cancels, approvals, results.
        Control = "control",
        /// Stream events, for example agent output.
        Data = "data",
    }
}

wire_enum! {
    /// Server error code.
    pub enum ErrorCode ("error code") {
        /// Frame cannot be decoded or breaks a rule.
        InvalidFrame = "invalid_frame",
        /// Wire version not supported.
        UnsupportedVersion = "unsupported_version",
        /// Authentication missing or failed.
        Unauthenticated = "unauthenticated",
        /// Not allowed for this device or pair state.
        Forbidden = "forbidden",
        /// Unknown pair or message.
        NotFound = "not_found",
        /// Known message ID with a different payload.
        Conflict = "conflict",
        /// Invitation or claim expired.
        Expired = "expired",
        /// A size or count limit was hit.
        LimitExceeded = "limit_exceeded",
        /// Too many requests.
        RateLimited = "rate_limited",
        /// Server fault. Retry with the same ID.
        Internal = "internal",
    }
}

/// Pair as one device sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairInfo {
    /// Pair ID.
    pub id: PairId,
    /// Current state.
    pub state: PairState,
    /// Device that created the invitation.
    pub inviter: DeviceId,
    /// Device that claimed it, once claimed.
    pub invitee: Option<DeviceId>,
    /// Opaque, authenticated invitee keys. Only sent to the inviter.
    pub greeting: Option<Blob>,
    /// Deadline of an unfinished pairing.
    pub expires_at: Option<Timestamp>,
}

/// Frame from a device to the server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientFrame {
    /// First frame. Starts authentication.
    Hello {
        /// Wire version of the device.
        version: u16,
        /// Device identity.
        device: DeviceId,
    },
    /// Ed25519 signature of the server challenge.
    Authenticate {
        /// Signature bytes.
        signature: FixedBytes<64>,
    },
    /// Create an invitation.
    Invite {
        /// New pair ID.
        pair: PairId,
        /// SHA-256 of the claim secret.
        claim_hash: FixedBytes<32>,
    },
    /// Claim an invitation.
    Claim {
        /// Pair ID from the invitation.
        pair: PairId,
        /// Claim secret from the invitation.
        claim_secret: FixedBytes<32>,
        /// Opaque, authenticated keys for the inviter.
        greeting: Blob,
    },
    /// Approve the claimer. Inviter only.
    Approve {
        /// Pair ID.
        pair: PairId,
    },
    /// End the pair. Either device.
    Unpair {
        /// Pair ID.
        pair: PairId,
    },
    /// Durable message to the peer.
    Send {
        /// Pair ID.
        pair: PairId,
        /// Message ID. Retries reuse it.
        id: MessageId,
        /// Delivery lane.
        lane: Lane,
        /// Encrypted body.
        sealed: Blob,
    },
    /// Non-durable update to the peer. Dropped if the peer is offline or slow.
    Live {
        /// Pair ID.
        pair: PairId,
        /// Encrypted body.
        sealed: Blob,
    },
    /// Durable messages up to this position are saved by the device.
    Ack {
        /// Pair ID.
        pair: PairId,
        /// Lane.
        lane: Lane,
        /// Last saved position.
        position: Position,
    },
}

/// Frame from the server to a device.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerFrame {
    /// Sent on connect. The device signs `nonce`.
    Challenge {
        /// Wire version of the server.
        version: u16,
        /// Random challenge.
        nonce: FixedBytes<32>,
    },
    /// Authenticated. Lists the device's pairs.
    Welcome {
        /// Pairs of the device.
        pairs: Vec<PairInfo>,
    },
    /// A pair changed.
    Pair {
        /// New pair view.
        pair: PairInfo,
    },
    /// A `Send` is durably stored.
    Accepted {
        /// Pair ID.
        pair: PairId,
        /// Message ID.
        id: MessageId,
        /// Assigned position.
        position: Position,
    },
    /// A durable message from the peer.
    Deliver {
        /// Pair ID.
        pair: PairId,
        /// Message ID.
        id: MessageId,
        /// Lane.
        lane: Lane,
        /// Position in the lane.
        position: Position,
        /// Server acceptance time.
        accepted_at: Timestamp,
        /// Encrypted body.
        sealed: Blob,
    },
    /// A live update from the peer.
    Live {
        /// Pair ID.
        pair: PairId,
        /// Encrypted body.
        sealed: Blob,
    },
    /// Peer connection changed.
    Presence {
        /// Pair ID.
        pair: PairId,
        /// `true` if the peer is connected.
        online: bool,
        /// Last time the peer was connected.
        last_seen: Option<Timestamp>,
    },
    /// A request failed.
    Error {
        /// Error code.
        code: ErrorCode,
        /// Short reason. Never contains secrets.
        message: String,
        /// Related pair.
        pair: Option<PairId>,
        /// Related message.
        id: Option<MessageId>,
    },
}

/// A frame cannot be decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameError {
    /// Text is longer than the limit.
    TooLarge {
        /// Limit in bytes.
        limit: usize,
    },
    /// Text is not a valid frame.
    Invalid(String),
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLarge { limit } => write!(f, "frame exceeds {limit} bytes"),
            Self::Invalid(reason) => write!(f, "invalid frame: {reason}"),
        }
    }
}

impl Error for FrameError {}

/// Decodes a frame, rejecting text over `limit` bytes before parsing.
///
/// # Errors
///
/// Returns [`FrameError`] for oversized or malformed text.
pub fn decode<T: DeserializeOwned>(text: &str, limit: usize) -> Result<T, FrameError> {
    if text.len() > limit {
        return Err(FrameError::TooLarge { limit });
    }
    serde_json::from_str(text).map_err(|error| FrameError::Invalid(error.to_string()))
}

/// Encodes a frame as JSON text.
///
/// # Errors
///
/// Returns [`FrameError::Invalid`] if serialization fails.
pub fn encode<T: Serialize>(frame: &T) -> Result<String, FrameError> {
    serde_json::to_string(frame).map_err(|error| FrameError::Invalid(error.to_string()))
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use serde_json::json;

    use super::*;
    use crate::wire::testing::check_wire_enum;

    const LIMIT: usize = 1 << 20;

    fn pair() -> PairId {
        PairId::from_bytes([1; 16])
    }

    fn message() -> MessageId {
        MessageId::from_bytes([2; 16])
    }

    fn device() -> DeviceId {
        DeviceId::from_bytes([3; 32])
    }

    fn round_trip<T>(frame: &T, expected: &serde_json::Value)
    where
        T: Serialize + DeserializeOwned + PartialEq + fmt::Debug,
    {
        let text = encode(frame).unwrap();
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(&value, expected);
        assert_eq!(&decode::<T>(&text, LIMIT).unwrap(), frame);
    }

    #[test]
    fn enums_have_stable_names() {
        check_wire_enum::<Lane>();
        check_wire_enum::<ErrorCode>();
    }

    #[test]
    fn client_frames_have_stable_json() {
        let pair_id = pair().to_string();
        round_trip(
            &ClientFrame::Hello {
                version: PROTOCOL_VERSION,
                device: device(),
            },
            &json!({"type": "hello", "version": 1, "device": device().to_string()}),
        );
        round_trip(
            &ClientFrame::Send {
                pair: pair(),
                id: message(),
                lane: Lane::Control,
                sealed: Blob::new(vec![0xff]),
            },
            &json!({
                "type": "send", "pair": pair_id, "id": message().to_string(),
                "lane": "control", "sealed": "_w"
            }),
        );
        round_trip(
            &ClientFrame::Ack {
                pair: pair(),
                lane: Lane::Data,
                position: Position(7),
            },
            &json!({"type": "ack", "pair": pair_id, "lane": "data", "position": 7}),
        );
    }

    #[test]
    fn server_frames_have_stable_json() {
        let info = PairInfo {
            id: pair(),
            state: PairState::Claimed,
            inviter: device(),
            invitee: None,
            greeting: None,
            expires_at: Some(Timestamp(5)),
        };
        round_trip(
            &ServerFrame::Pair { pair: info },
            &json!({"type": "pair", "pair": {
                "id": pair().to_string(), "state": "claimed", "inviter": device().to_string(),
                "invitee": null, "greeting": null, "expires_at": 5
            }}),
        );
        round_trip(
            &ServerFrame::Error {
                code: ErrorCode::Conflict,
                message: "ID reused".to_owned(),
                pair: Some(pair()),
                id: None,
            },
            &json!({
                "type": "error", "code": "conflict", "message": "ID reused",
                "pair": pair().to_string(), "id": null
            }),
        );
    }

    #[test]
    fn decode_rejects_oversized_and_malformed_text() {
        assert_eq!(
            decode::<ClientFrame>("{}", 1),
            Err(FrameError::TooLarge { limit: 1 })
        );
        assert_eq!(
            FrameError::TooLarge { limit: 1 }.to_string(),
            "frame exceeds 1 bytes"
        );
        let error = decode::<ClientFrame>(r#"{"type":"nope"}"#, LIMIT).unwrap_err();
        assert!(error.to_string().starts_with("invalid frame: "), "{error}");
        assert!(decode::<ServerFrame>(r#"{"type":"accepted"}"#, LIMIT).is_err());
    }

    #[test]
    fn unknown_fields_are_ignored_for_forward_compatibility() {
        let text = format!(r#"{{"type":"unpair","pair":"{}","future":true}}"#, pair());
        assert_eq!(
            decode::<ClientFrame>(&text, LIMIT),
            Ok(ClientFrame::Unpair { pair: pair() })
        );
    }

    proptest! {
        #[test]
        fn decode_never_panics(text in ".{0,256}") {
            let _ = decode::<ClientFrame>(&text, LIMIT);
            let _ = decode::<ServerFrame>(&text, LIMIT);
        }

        #[test]
        fn deliver_round_trips(position in any::<u64>(), at in any::<u64>(), bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
            let frame = ServerFrame::Deliver {
                pair: pair(),
                id: message(),
                lane: Lane::Data,
                position: Position(position),
                accepted_at: Timestamp(at),
                sealed: Blob::new(bytes),
            };
            let text = encode(&frame).unwrap();
            prop_assert_eq!(decode::<ServerFrame>(&text, LIMIT).unwrap(), frame);
        }
    }
}
