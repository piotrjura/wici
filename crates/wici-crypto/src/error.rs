use std::error::Error;
use std::fmt;

/// A cryptographic operation failed. Variants never carry secret data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CryptoError {
    /// Bytes are not a valid public key.
    InvalidKey,
    /// Signature does not verify.
    InvalidSignature,
    /// Key agreement produced a weak (non-contributory) secret.
    WeakKey,
    /// Ciphertext is malformed, tampered, or for another key or context.
    Decrypt,
    /// Plaintext or link cannot be encoded or decoded.
    Encoding,
}

impl fmt::Display for CryptoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidKey => "invalid public key",
            Self::InvalidSignature => "invalid signature",
            Self::WeakKey => "weak key agreement",
            Self::Decrypt => "decryption failed",
            Self::Encoding => "invalid encoding",
        })
    }
}

impl Error for CryptoError {}

#[cfg(test)]
mod tests {
    use super::CryptoError;

    #[test]
    fn messages_are_short_and_distinct() {
        let all = [
            CryptoError::InvalidKey,
            CryptoError::InvalidSignature,
            CryptoError::WeakKey,
            CryptoError::Decrypt,
            CryptoError::Encoding,
        ];
        let texts: std::collections::HashSet<String> =
            all.iter().map(ToString::to_string).collect();
        assert_eq!(texts.len(), all.len());
    }
}
