//! Pairing invitation, claim proof, and authenticated greeting.

use std::fmt;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rand_core::{OsRng, RngCore};
use serde::{Deserialize, Serialize};
use wici_protocol::{Blob, DeviceId, FixedBytes, PairId, Timestamp};

use crate::aead::{open_json, seal_json};
use crate::device::DeviceKeys;
use crate::error::CryptoError;
use crate::kdf::{Key, derive, frame, sha256};
use crate::pair::PairKeys;

const CLAIM_CONTEXT: &[u8] = b"wici/v1/claim";
const GREETING_CONTEXT: &[u8] = b"wici/v1/greeting";
const LINK_PREFIX: &str = "wici:";

/// Maximum link length accepted by [`Invitation::from_link`].
pub const MAX_LINK_LEN: usize = 2048;

/// Invitation that the inviter shows out of band (QR code, link).
///
/// The token never reaches the server. The server only gets the claim hash.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Invitation {
    /// Pair ID.
    pub pair: PairId,
    /// Inviter identity.
    pub inviter: DeviceId,
    /// Inviter X25519 key.
    pub inviter_agreement: FixedBytes<32>,
    /// Server URL.
    pub server: String,
    /// Claim deadline.
    pub expires_at: Timestamp,
    token: FixedBytes<32>,
}

impl fmt::Debug for Invitation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Invitation")
            .field("pair", &self.pair)
            .field("inviter", &self.inviter)
            .field("server", &self.server)
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}

/// Invitee keys, sealed with a token-derived key.
#[derive(Serialize, Deserialize)]
struct Greeting {
    device: DeviceId,
    agreement: FixedBytes<32>,
}

impl Invitation {
    /// Creates an invitation with a fresh random token.
    #[must_use]
    pub fn create(host: &DeviceKeys, server: String, expires_at: Timestamp) -> Self {
        let mut token = [0; 32];
        OsRng.fill_bytes(&mut token);
        Self {
            pair: PairId::generate(),
            inviter: host.device_id(),
            inviter_agreement: host.agreement_public(),
            server,
            expires_at,
            token: FixedBytes::new(token),
        }
    }

    fn token_key(&self, context: &[u8]) -> Key {
        derive(self.pair.as_bytes(), self.token.as_bytes(), &[context])
    }

    /// Secret that proves possession of the invitation to the server.
    #[must_use]
    pub fn claim_secret(&self) -> FixedBytes<32> {
        FixedBytes::new(*self.token_key(CLAIM_CONTEXT))
    }

    /// Hash of the claim secret. The inviter sends it to the server.
    #[must_use]
    pub fn claim_hash(&self) -> FixedBytes<32> {
        claim_hash(&self.claim_secret())
    }

    /// Invitee's greeting for the inviter, sent through the server.
    ///
    /// # Errors
    ///
    /// [`CryptoError::Encoding`] if sealing fails.
    pub fn greet(&self, guest: &DeviceKeys) -> Result<Blob, CryptoError> {
        let greeting = Greeting {
            device: guest.device_id(),
            agreement: guest.agreement_public(),
        };
        let aad = self.greeting_aad(&greeting.device);
        seal_json(&self.token_key(GREETING_CONTEXT), &aad, &greeting).map(Blob::new)
    }

    /// Inviter side: checks the greeting and derives pair keys.
    ///
    /// # Errors
    ///
    /// [`CryptoError::Decrypt`] if the greeting is forged or not from
    /// `guest`, [`CryptoError::WeakKey`] for a bad agreement key.
    pub fn accept(
        &self,
        host: &DeviceKeys,
        guest: &DeviceId,
        greeting: &Blob,
    ) -> Result<PairKeys, CryptoError> {
        let aad = self.greeting_aad(guest);
        let opened: Greeting =
            open_json(&self.token_key(GREETING_CONTEXT), &aad, greeting.as_bytes())?;
        if opened.device != *guest {
            return Err(CryptoError::Decrypt);
        }
        self.pair_keys(host, guest, &opened.agreement)
    }

    /// Invitee side: derives pair keys.
    ///
    /// # Errors
    ///
    /// [`CryptoError::WeakKey`] for a bad inviter agreement key.
    pub fn join(&self, guest: &DeviceKeys) -> Result<PairKeys, CryptoError> {
        self.pair_keys(guest, &self.inviter, &self.inviter_agreement)
    }

    fn pair_keys(
        &self,
        own: &DeviceKeys,
        peer: &DeviceId,
        peer_agreement: &FixedBytes<32>,
    ) -> Result<PairKeys, CryptoError> {
        let shared = own.agree(peer_agreement)?;
        Ok(PairKeys::derive(
            self.pair,
            own.device_id(),
            *peer,
            self.token.as_bytes(),
            &shared,
        ))
    }

    fn greeting_aad(&self, guest: &DeviceId) -> Vec<u8> {
        frame(&[GREETING_CONTEXT, self.pair.as_bytes(), guest.as_bytes()])
    }

    /// Encodes as `wici:<base64url JSON>` for a QR code or link.
    ///
    /// # Errors
    ///
    /// [`CryptoError::Encoding`] if serialization fails.
    pub fn to_link(&self) -> Result<String, CryptoError> {
        let json = serde_json::to_vec(self).map_err(|_| CryptoError::Encoding)?;
        Ok(format!("{LINK_PREFIX}{}", URL_SAFE_NO_PAD.encode(json)))
    }

    /// Decodes a link from [`Invitation::to_link`].
    ///
    /// # Errors
    ///
    /// [`CryptoError::Encoding`] for oversized or malformed links.
    pub fn from_link(link: &str) -> Result<Self, CryptoError> {
        if link.len() > MAX_LINK_LEN {
            return Err(CryptoError::Encoding);
        }
        let encoded = link
            .strip_prefix(LINK_PREFIX)
            .ok_or(CryptoError::Encoding)?;
        let json = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| CryptoError::Encoding)?;
        serde_json::from_slice(&json).map_err(|_| CryptoError::Encoding)
    }
}

/// SHA-256 of a claim secret. The server compares it with the stored hash.
#[must_use]
pub fn claim_hash(claim_secret: &FixedBytes<32>) -> FixedBytes<32> {
    FixedBytes::new(sha256(claim_secret.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn invitation(host: &DeviceKeys) -> Invitation {
        Invitation::create(host, "wss://relay.example".to_owned(), Timestamp(1_000))
    }

    #[test]
    fn both_sides_derive_matching_pair_keys() {
        let host = DeviceKeys::generate();
        let guest = DeviceKeys::generate();
        let invite = Invitation::from_link(&invitation(&host).to_link().unwrap()).unwrap();
        let greeting = invite.greet(&guest).unwrap();
        let host_keys = invite.accept(&host, &guest.device_id(), &greeting).unwrap();
        let guest_keys = invite.join(&guest).unwrap();
        assert!(host_keys.matches(&guest_keys));
    }

    #[test]
    fn greeting_from_another_device_or_invitation_is_rejected() {
        let host = DeviceKeys::generate();
        let guest = DeviceKeys::generate();
        let invite = invitation(&host);
        let greeting = invite.greet(&guest).unwrap();
        let stranger = DeviceKeys::generate().device_id();
        assert_eq!(
            invite.accept(&host, &stranger, &greeting).unwrap_err(),
            CryptoError::Decrypt
        );
        let other = invitation(&host);
        assert_eq!(
            other
                .accept(&host, &guest.device_id(), &greeting)
                .unwrap_err(),
            CryptoError::Decrypt
        );
    }

    #[test]
    fn greeting_with_mismatched_device_is_rejected() {
        let host = DeviceKeys::generate();
        let invite = invitation(&host);
        let claimed = DeviceKeys::generate().device_id();
        let lie = Greeting {
            device: DeviceKeys::generate().device_id(),
            agreement: host.agreement_public(),
        };
        let sealed = seal_json(
            &invite.token_key(GREETING_CONTEXT),
            &invite.greeting_aad(&claimed),
            &lie,
        )
        .unwrap();
        assert_eq!(
            invite
                .accept(&host, &claimed, &Blob::new(sealed))
                .unwrap_err(),
            CryptoError::Decrypt
        );
    }

    #[test]
    fn claim_hash_matches_secret_and_differs_per_invitation() {
        let host = DeviceKeys::generate();
        let a = invitation(&host);
        let b = invitation(&host);
        assert_eq!(a.claim_hash(), claim_hash(&a.claim_secret()));
        assert_ne!(a.claim_hash(), b.claim_hash());
        assert_ne!(a.claim_secret(), a.claim_hash());
    }

    #[test]
    fn debug_hides_the_token() {
        let invite = invitation(&DeviceKeys::generate());
        let text = format!("{invite:?}");
        assert!(!text.contains(&invite.token.to_string()));
        assert!(text.contains("Invitation"));
    }

    #[test]
    fn bad_links_are_rejected() {
        let link = invitation(&DeviceKeys::generate()).to_link().unwrap();
        assert!(link.len() < MAX_LINK_LEN);
        for bad in ["", "wici:", "http:abc", "wici:!!", "wici:e30"] {
            assert_eq!(
                Invitation::from_link(bad),
                Err(CryptoError::Encoding),
                "{bad}"
            );
        }
        let long = format!("wici:{}", "A".repeat(MAX_LINK_LEN));
        assert_eq!(Invitation::from_link(&long), Err(CryptoError::Encoding));
    }
}
