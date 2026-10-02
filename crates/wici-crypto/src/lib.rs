//! Wici cryptography: device keys, pairing, and sealing.
//!
//! Only reviewed primitives: Ed25519, X25519, HKDF-SHA256, ChaCha20-Poly1305.
//!
//! ```
//! use wici_crypto::{DeviceKeys, Invitation};
//! use wici_protocol::{Body, Lane, MessageId, Timestamp};
//!
//! let (alice, bob) = (DeviceKeys::generate(), DeviceKeys::generate());
//! let invite = Invitation::create(&alice, "wss://relay".into(), Timestamp(0));
//! let greeting = invite.greet(&bob)?;
//! let alice_keys = invite.accept(&alice, &bob.device_id(), &greeting)?;
//! let bob_keys = invite.join(&bob)?;
//!
//! let id = MessageId::generate();
//! let body = Body::Cancel { command: id };
//! let sealed = alice_keys.seal_message(id, Lane::Control, &body)?;
//! assert_eq!(bob_keys.open_message(id, Lane::Control, &sealed)?, body);
//! # Ok::<(), wici_crypto::CryptoError>(())
//! ```

mod aead;
mod device;
mod error;
mod invitation;
mod kdf;
mod pair;

pub use device::{DeviceKeys, verify_challenge};
pub use error::CryptoError;
pub use invitation::{Invitation, MAX_LINK_LEN, claim_hash};
pub use pair::PairKeys;
