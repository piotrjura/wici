//! Key derivation and unambiguous byte framing.

use hkdf::Hkdf;
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

/// 256-bit secret key that is wiped on drop.
pub(crate) type Key = Zeroizing<[u8; 32]>;

/// Joins parts with a 4-byte big-endian length prefix each, so no two
/// different part lists encode to the same bytes.
pub(crate) fn frame(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(parts.iter().map(|p| p.len() + 4).sum());
    for part in parts {
        // Parts are protocol fields far below 4 GiB.
        let len = u32::try_from(part.len()).unwrap_or(u32::MAX);
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(part);
    }
    out
}

/// HKDF-SHA256 with `info` built by [`frame`].
pub(crate) fn derive(salt: &[u8], ikm: &[u8], info: &[&[u8]]) -> Key {
    let mut key = Zeroizing::new([0; 32]);
    // A 32-byte output is always within HKDF-SHA256 limits.
    let _ = Hkdf::<Sha256>::new(Some(salt), ikm).expand(&frame(info), key.as_mut());
    key
}

/// SHA-256.
pub(crate) fn sha256(data: &[u8]) -> [u8; 32] {
    Sha256::digest(data).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_is_unambiguous() {
        assert_ne!(frame(&[b"ab", b"c"]), frame(&[b"a", b"bc"]));
        assert_eq!(frame(&[b"ab"]), [0, 0, 0, 2, b'a', b'b']);
        assert_eq!(frame(&[]), Vec::<u8>::new());
    }

    #[test]
    fn derive_depends_on_every_input() {
        let base = derive(b"s", b"k", &[b"i"]);
        assert_eq!(*base, *derive(b"s", b"k", &[b"i"]));
        assert_ne!(*base, *derive(b"t", b"k", &[b"i"]));
        assert_ne!(*base, *derive(b"s", b"l", &[b"i"]));
        assert_ne!(*base, *derive(b"s", b"k", &[b"j"]));
    }

    #[test]
    fn sha256_matches_known_vector() {
        let hash = sha256(b"abc");
        assert_eq!(hash[..4], [0xba, 0x78, 0x16, 0xbf]);
    }
}
