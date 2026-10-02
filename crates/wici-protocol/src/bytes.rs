//! Byte values encoded as unpadded base64url on the wire.

use std::error::Error;
use std::fmt;
use std::str::FromStr;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Invalid base64url text or wrong length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BytesError {
    /// Not unpadded base64url.
    Encoding,
    /// Decoded length differs from the expected one.
    Length {
        /// Expected byte count.
        expected: usize,
        /// Decoded byte count.
        actual: usize,
    },
}

impl fmt::Display for BytesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Encoding => f.write_str("invalid base64url"),
            Self::Length { expected, actual } => {
                write!(f, "expected {expected} bytes, got {actual}")
            }
        }
    }
}

impl Error for BytesError {}

fn encode(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(bytes)
}

fn decode(text: &str) -> Result<Vec<u8>, BytesError> {
    URL_SAFE_NO_PAD
        .decode(text)
        .map_err(|_| BytesError::Encoding)
}

fn deserialize_text<'de, D: Deserializer<'de>, T: FromStr<Err = BytesError>>(
    deserializer: D,
) -> Result<T, D::Error> {
    String::deserialize(deserializer)?
        .parse()
        .map_err(D::Error::custom)
}

/// Exactly `N` bytes: keys, signatures, hashes.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FixedBytes<const N: usize>([u8; N]);

impl<const N: usize> FixedBytes<N> {
    /// Wraps raw bytes.
    #[must_use]
    pub const fn new(bytes: [u8; N]) -> Self {
        Self(bytes)
    }

    /// Raw bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; N] {
        &self.0
    }
}

impl<const N: usize> fmt::Debug for FixedBytes<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FixedBytes({self})")
    }
}

impl<const N: usize> fmt::Display for FixedBytes<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&encode(&self.0))
    }
}

impl<const N: usize> FromStr for FixedBytes<N> {
    type Err = BytesError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let bytes = decode(text)?;
        let actual = bytes.len();
        <[u8; N]>::try_from(bytes)
            .map(Self)
            .map_err(|_| BytesError::Length {
                expected: N,
                actual,
            })
    }
}

impl<const N: usize> Serialize for FixedBytes<N> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de, const N: usize> Deserialize<'de> for FixedBytes<N> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize_text(deserializer)
    }
}

/// Variable-length bytes, for example ciphertext.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct Blob(Vec<u8>);

impl Blob {
    /// Wraps raw bytes.
    #[must_use]
    pub const fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// Raw bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Byte count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// `true` if empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Unwraps the bytes.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl fmt::Debug for Blob {
    /// Prints only the size. Contents can be secret.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Blob({} bytes)", self.0.len())
    }
}

impl FromStr for Blob {
    type Err = BytesError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        decode(text).map(Self)
    }
}

impl Serialize for Blob {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&encode(&self.0))
    }
}

impl<'de> Deserialize<'de> for Blob {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize_text(deserializer)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn fixed_bytes_use_unpadded_base64url() {
        let value = FixedBytes::new([0xfb, 0xff]);
        assert_eq!(value.to_string(), "-_8");
        assert_eq!(format!("{value:?}"), "FixedBytes(-_8)");
        assert_eq!(serde_json::to_string(&value).unwrap(), "\"-_8\"");
        assert_eq!(value.as_bytes(), &[0xfb, 0xff]);
    }

    #[test]
    fn fixed_bytes_reject_wrong_length_and_encoding() {
        assert_eq!(
            "AAAA".parse::<FixedBytes<2>>(),
            Err(BytesError::Length {
                expected: 2,
                actual: 3
            })
        );
        assert_eq!("-_8=".parse::<FixedBytes<2>>(), Err(BytesError::Encoding));
        assert_eq!("+/8".parse::<FixedBytes<2>>(), Err(BytesError::Encoding));
        assert!(serde_json::from_str::<FixedBytes<2>>("\"AAAA\"").is_err());
        assert_eq!(
            BytesError::Length {
                expected: 2,
                actual: 3
            }
            .to_string(),
            "expected 2 bytes, got 3"
        );
        assert_eq!(BytesError::Encoding.to_string(), "invalid base64url");
    }

    #[test]
    fn blob_hides_contents_in_debug() {
        let blob = Blob::new(b"secret".to_vec());
        assert_eq!(format!("{blob:?}"), "Blob(6 bytes)");
        assert_eq!(blob.len(), 6);
        assert!(!blob.is_empty());
        assert!(Blob::default().is_empty());
        assert_eq!(blob.as_bytes(), b"secret");
        assert_eq!(blob.into_bytes(), b"secret");
        assert_eq!("!".parse::<Blob>(), Err(BytesError::Encoding));
    }

    proptest! {
        #[test]
        fn fixed_bytes_round_trip(bytes in any::<[u8; 32]>()) {
            let value = FixedBytes::new(bytes);
            prop_assert_eq!(value.to_string().parse::<FixedBytes<32>>(), Ok(value));
            let json = serde_json::to_string(&value).unwrap();
            prop_assert_eq!(serde_json::from_str::<FixedBytes<32>>(&json).unwrap(), value);
        }

        #[test]
        fn blob_round_trips(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
            let blob = Blob::new(bytes);
            let json = serde_json::to_string(&blob).unwrap();
            prop_assert_eq!(serde_json::from_str::<Blob>(&json).unwrap(), blob);
        }
    }
}
