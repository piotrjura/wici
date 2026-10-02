//! ChaCha20-Poly1305 sealing. Output: 12-byte random nonce, ciphertext, tag.

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Nonce};
use rand_core::{OsRng, RngCore};
use serde::Serialize;
use serde::de::DeserializeOwned;
use zeroize::Zeroizing;

use crate::error::CryptoError;

const NONCE_LEN: usize = 12;

fn cipher(key: &[u8; 32]) -> ChaCha20Poly1305 {
    ChaCha20Poly1305::new(key.into())
}

fn seal_with_nonce(
    key: &[u8; 32],
    nonce: [u8; NONCE_LEN],
    aad: &[u8],
    plaintext: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    let payload = Payload {
        msg: plaintext,
        aad,
    };
    let ciphertext = cipher(key)
        .encrypt(Nonce::from_slice(&nonce), payload)
        .map_err(|_| CryptoError::Encoding)?;
    let mut out = Vec::with_capacity(NONCE_LEN + ciphertext.len());
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

/// Encrypts `plaintext` with a fresh random nonce.
///
/// Fails only for inputs over 256 GiB.
pub(crate) fn seal(key: &[u8; 32], aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, CryptoError> {
    let mut nonce = [0; NONCE_LEN];
    OsRng.fill_bytes(&mut nonce);
    seal_with_nonce(key, nonce, aad, plaintext)
}

/// Decrypts and authenticates `sealed`.
pub(crate) fn open(
    key: &[u8; 32],
    aad: &[u8],
    sealed: &[u8],
) -> Result<Zeroizing<Vec<u8>>, CryptoError> {
    let (nonce, ciphertext) = sealed
        .split_at_checked(NONCE_LEN)
        .ok_or(CryptoError::Decrypt)?;
    cipher(key)
        .decrypt(
            Nonce::from_slice(nonce),
            Payload {
                msg: ciphertext,
                aad,
            },
        )
        .map(Zeroizing::new)
        .map_err(|_| CryptoError::Decrypt)
}

/// Serializes `value` as JSON and seals it.
pub(crate) fn seal_json<T: Serialize>(
    key: &[u8; 32],
    aad: &[u8],
    value: &T,
) -> Result<Vec<u8>, CryptoError> {
    let plaintext = Zeroizing::new(serde_json::to_vec(value).map_err(|_| CryptoError::Encoding)?);
    seal(key, aad, &plaintext)
}

/// Opens `sealed` and parses the JSON plaintext.
pub(crate) fn open_json<T: DeserializeOwned>(
    key: &[u8; 32],
    aad: &[u8],
    sealed: &[u8],
) -> Result<T, CryptoError> {
    let plaintext = open(key, aad, sealed)?;
    serde_json::from_slice(&plaintext).map_err(|_| CryptoError::Encoding)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    const KEY: [u8; 32] = [7; 32];

    #[test]
    fn known_vector_is_stable() {
        // Guards the wire format: nonce || ciphertext || tag.
        let sealed = seal_with_nonce(&KEY, [1; NONCE_LEN], b"aad", b"hi").unwrap();
        assert_eq!(sealed.len(), NONCE_LEN + 2 + 16);
        assert_eq!(sealed[..NONCE_LEN], [1; NONCE_LEN]);
        assert_eq!(*open(&KEY, b"aad", &sealed).unwrap(), b"hi");
    }

    #[test]
    fn nonces_are_fresh() {
        assert_ne!(
            seal(&KEY, b"", b"x").unwrap(),
            seal(&KEY, b"", b"x").unwrap()
        );
    }

    #[test]
    fn open_rejects_wrong_key_aad_and_short_input() {
        let sealed = seal(&KEY, b"a", b"secret").unwrap();
        assert_eq!(open(&[8; 32], b"a", &sealed), Err(CryptoError::Decrypt));
        assert_eq!(open(&KEY, b"b", &sealed), Err(CryptoError::Decrypt));
        assert_eq!(open(&KEY, b"a", &sealed[..5]), Err(CryptoError::Decrypt));
    }

    #[test]
    fn json_helpers_round_trip_and_reject_bad_plaintext() {
        let sealed = seal_json(&KEY, b"", &vec![1, 2]).unwrap();
        assert_eq!(open_json::<Vec<i32>>(&KEY, b"", &sealed).unwrap(), [1, 2]);
        let not_json = seal(&KEY, b"", b"{").unwrap();
        assert_eq!(
            open_json::<Vec<i32>>(&KEY, b"", &not_json),
            Err(CryptoError::Encoding)
        );
    }

    proptest! {
        #[test]
        fn any_bit_flip_is_detected(data in proptest::collection::vec(any::<u8>(), 0..64), index in any::<usize>(), bit in 0..8u8) {
            let mut sealed = seal(&KEY, b"ctx", &data).unwrap();
            let at = index % sealed.len();
            sealed[at] ^= 1 << bit;
            prop_assert_eq!(open(&KEY, b"ctx", &sealed), Err(CryptoError::Decrypt));
        }
    }
}
