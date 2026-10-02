//! Encryption of local records with a key derived from the device secret.

use std::fmt;

use zeroize::Zeroizing;

use crate::aead::{open, seal};
use crate::device::DeviceKeys;
use crate::error::CryptoError;
use crate::kdf::{Key, derive, frame};

const VAULT_CONTEXT: &[u8] = b"wici/v1/vault";

/// Seals data at rest, for example pair keys in a local database.
///
/// Only the device secret must live in a secure store (Keychain); every
/// other secret can be sealed with this vault.
pub struct LocalVault {
    key: Key,
}

impl fmt::Debug for LocalVault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LocalVault")
    }
}

impl DeviceKeys {
    /// Vault keyed by this device's secret.
    #[must_use]
    pub fn local_vault(&self) -> LocalVault {
        let secret = self.to_secret();
        LocalVault {
            key: derive(VAULT_CONTEXT, secret.as_ref(), &[VAULT_CONTEXT]),
        }
    }
}

impl LocalVault {
    /// Seals `data`. `context` names the record, so a sealed value cannot be
    /// moved to another record.
    ///
    /// # Errors
    ///
    /// [`CryptoError::Encoding`] for inputs over 256 GiB.
    pub fn seal(&self, context: &[u8], data: &[u8]) -> Result<Vec<u8>, CryptoError> {
        seal(&self.key, &frame(&[VAULT_CONTEXT, context]), data)
    }

    /// Opens a value sealed with the same `context`.
    ///
    /// # Errors
    ///
    /// [`CryptoError::Decrypt`] for tampered data, another context, or
    /// another device.
    pub fn open(&self, context: &[u8], sealed: &[u8]) -> Result<Zeroizing<Vec<u8>>, CryptoError> {
        open(&self.key, &frame(&[VAULT_CONTEXT, context]), sealed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_is_bound_to_context_and_device() {
        let keys = DeviceKeys::generate();
        let vault = keys.local_vault();
        let sealed = vault.seal(b"pair/1", b"secret").unwrap();
        assert_eq!(*vault.open(b"pair/1", &sealed).unwrap(), b"secret");
        assert_eq!(vault.open(b"pair/2", &sealed), Err(CryptoError::Decrypt));
        let other = DeviceKeys::generate().local_vault();
        assert_eq!(other.open(b"pair/1", &sealed), Err(CryptoError::Decrypt));
        let restored = DeviceKeys::from_secret(&keys.to_secret()).local_vault();
        assert_eq!(*restored.open(b"pair/1", &sealed).unwrap(), b"secret");
        assert_eq!(format!("{vault:?}"), "LocalVault");
    }
}
