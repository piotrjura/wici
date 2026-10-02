//! Device identity keys and connection authentication.

use std::fmt;

use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use rand_core::OsRng;
use wici_protocol::{DeviceId, FixedBytes};
use x25519_dalek::{PublicKey, StaticSecret};
use zeroize::Zeroizing;

use crate::error::CryptoError;
use crate::kdf::{Key, frame};

const AUTH_CONTEXT: &[u8] = b"wici/v1/auth";

/// Secret keys of one device: Ed25519 identity and X25519 agreement.
///
/// Generate once and store [`DeviceKeys::to_secret`] in a secure store.
pub struct DeviceKeys {
    signing: SigningKey,
    agreement: StaticSecret,
}

impl fmt::Debug for DeviceKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DeviceKeys({})", self.device_id())
    }
}

impl DeviceKeys {
    /// Generates new keys from the OS random source.
    #[must_use]
    pub fn generate() -> Self {
        Self {
            signing: SigningKey::generate(&mut OsRng),
            agreement: StaticSecret::random_from_rng(OsRng),
        }
    }

    /// Restores keys saved with [`DeviceKeys::to_secret`].
    #[must_use]
    pub fn from_secret(secret: &[u8; 64]) -> Self {
        let (signing, agreement) = secret.split_at(32);
        let mut signing_bytes = Zeroizing::new([0; 32]);
        let mut agreement_bytes = Zeroizing::new([0; 32]);
        signing_bytes.copy_from_slice(signing);
        agreement_bytes.copy_from_slice(agreement);
        Self {
            signing: SigningKey::from_bytes(&signing_bytes),
            agreement: StaticSecret::from(*agreement_bytes),
        }
    }

    /// Secret bytes for storage. Wiped on drop.
    #[must_use]
    pub fn to_secret(&self) -> Zeroizing<[u8; 64]> {
        let mut secret = Zeroizing::new([0; 64]);
        let (signing, agreement) = secret.split_at_mut(32);
        signing.copy_from_slice(self.signing.as_bytes());
        agreement.copy_from_slice(self.agreement.as_bytes());
        secret
    }

    /// Public identity.
    #[must_use]
    pub fn device_id(&self) -> DeviceId {
        DeviceId::from_bytes(self.signing.verifying_key().to_bytes())
    }

    /// Public X25519 key.
    #[must_use]
    pub fn agreement_public(&self) -> FixedBytes<32> {
        FixedBytes::new(PublicKey::from(&self.agreement).to_bytes())
    }

    /// Signs a server challenge.
    #[must_use]
    pub fn sign_challenge(&self, nonce: &FixedBytes<32>) -> FixedBytes<64> {
        let message = frame(&[AUTH_CONTEXT, nonce.as_bytes()]);
        FixedBytes::new(self.signing.sign(&message).to_bytes())
    }

    /// X25519 with a peer key. Rejects low-order peer keys.
    pub(crate) fn agree(&self, peer: &FixedBytes<32>) -> Result<Key, CryptoError> {
        let shared = self
            .agreement
            .diffie_hellman(&PublicKey::from(*peer.as_bytes()));
        if !shared.was_contributory() {
            return Err(CryptoError::WeakKey);
        }
        Ok(Zeroizing::new(shared.to_bytes()))
    }
}

/// Checks a challenge signature from `device`.
///
/// # Errors
///
/// [`CryptoError::InvalidKey`] if `device` is not a valid key,
/// [`CryptoError::InvalidSignature`] if the signature does not verify.
pub fn verify_challenge(
    device: &DeviceId,
    nonce: &FixedBytes<32>,
    signature: &FixedBytes<64>,
) -> Result<(), CryptoError> {
    let key = VerifyingKey::from_bytes(device.as_bytes()).map_err(|_| CryptoError::InvalidKey)?;
    let message = frame(&[AUTH_CONTEXT, nonce.as_bytes()]);
    key.verify_strict(&message, &Signature::from_bytes(signature.as_bytes()))
        .map_err(|_| CryptoError::InvalidSignature)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_verifies_only_for_its_device_and_nonce() {
        let keys = DeviceKeys::generate();
        let other = DeviceKeys::generate();
        let nonce = FixedBytes::new([1; 32]);
        let signature = keys.sign_challenge(&nonce);
        assert_eq!(
            verify_challenge(&keys.device_id(), &nonce, &signature),
            Ok(())
        );
        assert_eq!(
            verify_challenge(&other.device_id(), &nonce, &signature),
            Err(CryptoError::InvalidSignature)
        );
        assert_eq!(
            verify_challenge(&keys.device_id(), &FixedBytes::new([2; 32]), &signature),
            Err(CryptoError::InvalidSignature)
        );
    }

    #[test]
    fn invalid_device_key_is_rejected() {
        // y = 2 is not on the curve.
        let mut bytes = [0; 32];
        bytes[0] = 2;
        let result = verify_challenge(
            &DeviceId::from_bytes(bytes),
            &FixedBytes::new([0; 32]),
            &FixedBytes::new([0; 64]),
        );
        assert_eq!(result, Err(CryptoError::InvalidKey));
    }

    #[test]
    fn secret_round_trips_and_debug_hides_it() {
        let keys = DeviceKeys::generate();
        let restored = DeviceKeys::from_secret(&keys.to_secret());
        assert_eq!(restored.device_id(), keys.device_id());
        assert_eq!(restored.agreement_public(), keys.agreement_public());
        assert_eq!(
            format!("{keys:?}"),
            format!("DeviceKeys({})", keys.device_id())
        );
    }

    #[test]
    fn agreement_is_symmetric_and_rejects_low_order_keys() {
        let a = DeviceKeys::generate();
        let b = DeviceKeys::generate();
        assert_eq!(
            *a.agree(&b.agreement_public()).unwrap(),
            *b.agree(&a.agreement_public()).unwrap()
        );
        assert_eq!(
            a.agree(&FixedBytes::new([0; 32])).unwrap_err(),
            CryptoError::WeakKey
        );
    }
}
