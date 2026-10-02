//! Typed identifiers.

use std::error::Error;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::bytes::{BytesError, FixedBytes};

/// Text is not a UUID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseIdError {
    label: &'static str,
}

impl fmt::Display for ParseIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid {}", self.label)
    }
}

impl Error for ParseIdError {}

/// Defines a UUID-backed ID type. New IDs are `UUIDv7`, so they sort by time.
macro_rules! uuid_id {
    ($(#[$meta:meta])* $name:ident, $label:literal) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Generates a new time-ordered ID.
            #[must_use]
            pub fn generate() -> Self {
                Self(Uuid::now_v7())
            }

            /// Wraps raw bytes.
            #[must_use]
            pub const fn from_bytes(bytes: [u8; 16]) -> Self {
                Self(Uuid::from_bytes(bytes))
            }

            /// Raw bytes.
            #[must_use]
            pub const fn as_bytes(&self) -> &[u8; 16] {
                self.0.as_bytes()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.hyphenated().fmt(f)
            }
        }

        impl FromStr for $name {
            type Err = ParseIdError;

            fn from_str(text: &str) -> Result<Self, Self::Err> {
                Uuid::try_parse(text)
                    .map(Self)
                    .map_err(|_| ParseIdError { label: $label })
            }
        }
    };
}

uuid_id!(
    /// Pair of devices.
    PairId,
    "pair ID"
);
uuid_id!(
    /// Durable message, chosen by the sender. Retries reuse it.
    MessageId,
    "message ID"
);
uuid_id!(
    /// Ordered stream of events inside a pair.
    StreamId,
    "stream ID"
);
uuid_id!(
    /// Stored artifact.
    ArtifactId,
    "artifact ID"
);

/// Device identity: its Ed25519 public key.
///
/// The protocol does not check that the key is a valid curve point.
/// `wici-crypto` does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DeviceId(FixedBytes<32>);

impl DeviceId {
    /// Wraps public key bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(FixedBytes::new(bytes))
    }

    /// Public key bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        self.0.as_bytes()
    }
}

impl fmt::Display for DeviceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for DeviceId {
    type Err = BytesError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        text.parse().map(Self)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn generated_ids_are_unique_and_time_ordered() {
        let first = MessageId::generate();
        let second = MessageId::generate();
        assert_ne!(first, second);
        assert!(first < second);
    }

    #[test]
    fn ids_use_hyphenated_text() {
        let id = PairId::from_bytes([0xab; 16]);
        let text = "abababab-abab-abab-abab-abababababab";
        assert_eq!(id.to_string(), text);
        assert_eq!(text.parse::<PairId>(), Ok(id));
        assert_eq!(serde_json::to_string(&id).unwrap(), format!("\"{text}\""));
        assert_eq!(id.as_bytes(), &[0xab; 16]);
    }

    #[test]
    fn invalid_id_names_its_type() {
        for (error, label) in [
            ("x".parse::<PairId>().unwrap_err(), "invalid pair ID"),
            ("x".parse::<MessageId>().unwrap_err(), "invalid message ID"),
            ("x".parse::<StreamId>().unwrap_err(), "invalid stream ID"),
            (
                "x".parse::<ArtifactId>().unwrap_err(),
                "invalid artifact ID",
            ),
        ] {
            assert_eq!(error.to_string(), label);
        }
        assert!(serde_json::from_str::<StreamId>("\"nope\"").is_err());
        assert_ne!(StreamId::generate(), StreamId::generate());
        assert_ne!(ArtifactId::generate(), ArtifactId::generate());
    }

    #[test]
    fn device_id_is_a_32_byte_key() {
        let id = DeviceId::from_bytes([7; 32]);
        assert_eq!(id.as_bytes(), &[7; 32]);
        assert_eq!(id.to_string().parse::<DeviceId>(), Ok(id));
        assert_eq!(
            "AAAA".parse::<DeviceId>(),
            Err(BytesError::Length {
                expected: 32,
                actual: 3
            })
        );
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(serde_json::from_str::<DeviceId>(&json).unwrap(), id);
    }

    proptest! {
        #[test]
        fn message_ids_round_trip(bytes in any::<[u8; 16]>()) {
            let id = MessageId::from_bytes(bytes);
            prop_assert_eq!(id.to_string().parse::<MessageId>(), Ok(id));
        }
    }
}
