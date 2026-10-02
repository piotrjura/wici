//! Chunked artifact encryption.
//!
//! Each artifact has its own random key, so nonces can come from the chunk
//! index. The AAD binds the artifact ID, the index, and a final-chunk flag:
//! chunks cannot be reordered, swapped between artifacts, or cut off.
//! Sealing is deterministic, so an interrupted upload can resume by
//! sealing the remaining chunks again.

use std::fmt;

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Nonce};
use rand_core::{OsRng, RngCore};
use wici_protocol::{ArtifactId, FixedBytes};
use zeroize::Zeroizing;

use crate::error::CryptoError;
use crate::kdf::{frame, sha256};

const ARTIFACT_CONTEXT: &[u8] = b"wici/v1/artifact";

/// Plaintext bytes per chunk.
pub const CHUNK_PLAINTEXT: usize = 64 * 1024;

/// Authentication tag bytes added to each chunk.
pub const CHUNK_OVERHEAD: usize = 16;

/// Key of one artifact. Send it to the peer inside a sealed message.
#[derive(Clone)]
pub struct ArtifactKey {
    id: ArtifactId,
    key: Zeroizing<[u8; 32]>,
}

impl fmt::Debug for ArtifactKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ArtifactKey({})", self.id)
    }
}

/// Ciphertext length of an artifact with `plaintext_len` bytes.
#[must_use]
pub const fn sealed_len(plaintext_len: u64) -> u64 {
    let chunks = chunk_count(plaintext_len);
    plaintext_len + chunks * CHUNK_OVERHEAD as u64
}

/// Number of chunks. An empty artifact has one empty chunk.
#[must_use]
pub const fn chunk_count(plaintext_len: u64) -> u64 {
    let size = CHUNK_PLAINTEXT as u64;
    if plaintext_len == 0 {
        1
    } else {
        plaintext_len.div_ceil(size)
    }
}

impl ArtifactKey {
    /// New random key for a new artifact.
    #[must_use]
    pub fn generate() -> Self {
        let mut key = Zeroizing::new([0; 32]);
        OsRng.fill_bytes(key.as_mut());
        Self {
            id: ArtifactId::generate(),
            key,
        }
    }

    /// Restores a key received from the peer.
    #[must_use]
    pub fn from_parts(id: ArtifactId, key: &FixedBytes<32>) -> Self {
        Self {
            id,
            key: Zeroizing::new(*key.as_bytes()),
        }
    }

    /// Artifact ID.
    #[must_use]
    pub const fn id(&self) -> ArtifactId {
        self.id
    }

    /// Key bytes, for the sealed message that announces the artifact.
    #[must_use]
    pub fn key_bytes(&self) -> FixedBytes<32> {
        FixedBytes::new(*self.key)
    }

    fn nonce(index: u64) -> [u8; 12] {
        let mut nonce = [0; 12];
        let (_, counter) = nonce.split_at_mut(4);
        counter.copy_from_slice(&index.to_be_bytes());
        nonce
    }

    fn aad(&self, index: u64, last: bool) -> Vec<u8> {
        frame(&[
            ARTIFACT_CONTEXT,
            self.id.as_bytes(),
            &index.to_be_bytes(),
            &[u8::from(last)],
        ])
    }

    /// Seals chunk `index`. `last` marks the final chunk.
    ///
    /// # Errors
    ///
    /// [`CryptoError::Encoding`] if `plaintext` exceeds [`CHUNK_PLAINTEXT`].
    pub fn seal_chunk(
        &self,
        index: u64,
        last: bool,
        plaintext: &[u8],
    ) -> Result<Vec<u8>, CryptoError> {
        if plaintext.len() > CHUNK_PLAINTEXT {
            return Err(CryptoError::Encoding);
        }
        let payload = Payload {
            msg: plaintext,
            aad: &self.aad(index, last),
        };
        ChaCha20Poly1305::new(self.key.as_ref().into())
            .encrypt(Nonce::from_slice(&Self::nonce(index)), payload)
            .map_err(|_| CryptoError::Encoding)
    }

    /// Opens chunk `index`.
    ///
    /// # Errors
    ///
    /// [`CryptoError::Decrypt`] if the chunk was changed, moved, or its
    /// final flag does not match.
    pub fn open_chunk(
        &self,
        index: u64,
        last: bool,
        sealed: &[u8],
    ) -> Result<Zeroizing<Vec<u8>>, CryptoError> {
        let payload = Payload {
            msg: sealed,
            aad: &self.aad(index, last),
        };
        ChaCha20Poly1305::new(self.key.as_ref().into())
            .decrypt(Nonce::from_slice(&Self::nonce(index)), payload)
            .map(Zeroizing::new)
            .map_err(|_| CryptoError::Decrypt)
    }

    /// Seals a whole artifact held in memory.
    ///
    /// # Errors
    ///
    /// Same as [`ArtifactKey::seal_chunk`].
    pub fn seal_all(&self, plaintext: &[u8]) -> Result<Vec<u8>, CryptoError> {
        let count = chunk_count(plaintext.len() as u64);
        let mut out =
            Vec::with_capacity(usize::try_from(sealed_len(plaintext.len() as u64)).unwrap_or(0));
        let mut chunks = plaintext.chunks(CHUNK_PLAINTEXT);
        for index in 0..count {
            let chunk = chunks.next().unwrap_or_default();
            out.extend(self.seal_chunk(index, index + 1 == count, chunk)?);
        }
        Ok(out)
    }

    /// Opens a whole sealed artifact.
    ///
    /// # Errors
    ///
    /// [`CryptoError::Decrypt`] for any changed, missing, or extra byte.
    pub fn open_all(&self, sealed: &[u8]) -> Result<Zeroizing<Vec<u8>>, CryptoError> {
        let mut out = Zeroizing::new(Vec::with_capacity(sealed.len()));
        let size = CHUNK_PLAINTEXT + CHUNK_OVERHEAD;
        let count = (sealed.len().max(1)).div_ceil(size) as u64;
        let mut chunks = sealed.chunks(size);
        for index in 0..count {
            let chunk = chunks.next().unwrap_or_default();
            out.extend_from_slice(&self.open_chunk(index, index + 1 == count, chunk)?);
        }
        Ok(out)
    }
}

/// SHA-256 of artifact bytes, used by the server to verify an upload.
#[must_use]
pub fn artifact_hash(sealed: &[u8]) -> FixedBytes<32> {
    FixedBytes::new(sha256(sealed))
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn lengths_and_counts() {
        assert_eq!(chunk_count(0), 1);
        assert_eq!(chunk_count(1), 1);
        assert_eq!(chunk_count(CHUNK_PLAINTEXT as u64), 1);
        assert_eq!(chunk_count(CHUNK_PLAINTEXT as u64 + 1), 2);
        assert_eq!(sealed_len(0), 16);
        assert_eq!(
            sealed_len(CHUNK_PLAINTEXT as u64 + 1),
            CHUNK_PLAINTEXT as u64 + 1 + 32
        );
    }

    #[test]
    fn sealing_is_deterministic_per_key() {
        let key = ArtifactKey::generate();
        assert_eq!(
            key.seal_chunk(3, false, b"x").unwrap(),
            key.seal_chunk(3, false, b"x").unwrap()
        );
        assert_ne!(
            key.seal_chunk(3, false, b"x").unwrap(),
            key.seal_chunk(4, false, b"x").unwrap()
        );
        let other = ArtifactKey::generate();
        assert_ne!(
            key.seal_chunk(0, true, b"x").unwrap(),
            other.seal_chunk(0, true, b"x").unwrap()
        );
    }

    #[test]
    fn reorder_truncation_and_swaps_are_detected() {
        let key = ArtifactKey::generate();
        let first = key.seal_chunk(0, false, b"a").unwrap();
        let last = key.seal_chunk(1, true, b"b").unwrap();
        assert_eq!(*key.open_chunk(1, true, &last).unwrap(), b"b");
        assert_eq!(
            key.open_chunk(1, true, &first),
            Err(CryptoError::Decrypt),
            "reordered"
        );
        assert_eq!(
            key.open_chunk(0, true, &first),
            Err(CryptoError::Decrypt),
            "truncated"
        );
        let other = ArtifactKey::from_parts(ArtifactId::generate(), &key.key_bytes());
        assert_eq!(
            other.open_chunk(0, false, &first),
            Err(CryptoError::Decrypt),
            "other artifact"
        );
        let shared = ArtifactKey::from_parts(key.id(), &key.key_bytes());
        assert_eq!(*shared.open_chunk(0, false, &first).unwrap(), b"a");
        assert_eq!(format!("{key:?}"), format!("ArtifactKey({})", key.id()));
    }

    #[test]
    fn oversized_chunk_is_rejected() {
        let key = ArtifactKey::generate();
        let big = vec![0; CHUNK_PLAINTEXT + 1];
        assert_eq!(key.seal_chunk(0, true, &big), Err(CryptoError::Encoding));
    }

    #[test]
    fn whole_artifact_tamper_and_truncation_fail() {
        let key = ArtifactKey::generate();
        let data = vec![7; CHUNK_PLAINTEXT * 2 + 5];
        let sealed = key.seal_all(&data).unwrap();
        assert_eq!(sealed.len() as u64, sealed_len(data.len() as u64));
        let cut = sealed.len() - (5 + CHUNK_OVERHEAD);
        assert_eq!(key.open_all(&sealed[..cut]), Err(CryptoError::Decrypt));
        assert_eq!(
            artifact_hash(&sealed),
            artifact_hash(&key.seal_all(&data).unwrap())
        );
    }

    proptest! {
        // Inputs span several chunks, so a few cases cover the boundaries.
        #![proptest_config(ProptestConfig::with_cases(16))]

        #[test]
        fn round_trip(data in proptest::collection::vec(any::<u8>(), 0..(CHUNK_PLAINTEXT * 2 + 10))) {
            let key = ArtifactKey::generate();
            let sealed = key.seal_all(&data).unwrap();
            prop_assert_eq!(&*key.open_all(&sealed).unwrap(), &data);
        }
    }
}
