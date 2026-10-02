//! Per-direction pair keys and message sealing.

use std::fmt;

use serde::Serialize;
use serde::de::DeserializeOwned;
use wici_protocol::{Blob, Body, DeviceId, Lane, LiveBody, MessageId, PairId, WireEnum};
use zeroize::Zeroizing;

use crate::aead::{open_json, seal_json};
use crate::error::CryptoError;
use crate::kdf::{Key, derive, frame};

const PAIR_CONTEXT: &[u8] = b"wici/v1/pair";
const MESSAGE_CONTEXT: &[u8] = b"wici/v1/message";
const LIVE_CONTEXT: &[u8] = b"wici/v1/live";

/// Keys one device uses with one peer. One key per direction.
///
/// Every ciphertext is bound to the pair, the sender, and for durable
/// messages the message ID and lane. Replaying it elsewhere fails to open.
pub struct PairKeys {
    pair: PairId,
    own: DeviceId,
    peer: DeviceId,
    send: Key,
    receive: Key,
}

impl fmt::Debug for PairKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PairKeys")
            .field("pair", &self.pair)
            .field("own", &self.own)
            .field("peer", &self.peer)
            .finish_non_exhaustive()
    }
}

impl PairKeys {
    pub(crate) fn derive(
        pair: PairId,
        own: DeviceId,
        peer: DeviceId,
        token: &[u8],
        shared: &[u8; 32],
    ) -> Self {
        let direction = |from: &DeviceId, to: &DeviceId| {
            derive(
                token,
                shared,
                &[
                    PAIR_CONTEXT,
                    pair.as_bytes(),
                    from.as_bytes(),
                    to.as_bytes(),
                ],
            )
        };
        Self {
            send: direction(&own, &peer),
            receive: direction(&peer, &own),
            pair,
            own,
            peer,
        }
    }

    /// Restores keys saved with [`PairKeys::to_secret`].
    #[must_use]
    pub fn from_secret(pair: PairId, own: DeviceId, peer: DeviceId, secret: &[u8; 64]) -> Self {
        let (send, receive) = secret.split_at(32);
        let mut keys = Self {
            pair,
            own,
            peer,
            send: Zeroizing::new([0; 32]),
            receive: Zeroizing::new([0; 32]),
        };
        keys.send.copy_from_slice(send);
        keys.receive.copy_from_slice(receive);
        keys
    }

    /// Secret bytes for storage. Wiped on drop.
    #[must_use]
    pub fn to_secret(&self) -> Zeroizing<[u8; 64]> {
        let mut secret = Zeroizing::new([0; 64]);
        let (send, receive) = secret.split_at_mut(32);
        send.copy_from_slice(self.send.as_ref());
        receive.copy_from_slice(self.receive.as_ref());
        secret
    }

    /// Pair ID.
    #[must_use]
    pub const fn pair(&self) -> PairId {
        self.pair
    }

    /// Peer identity.
    #[must_use]
    pub const fn peer(&self) -> DeviceId {
        self.peer
    }

    fn message_aad(&self, sender: &DeviceId, id: MessageId, lane: Lane) -> Vec<u8> {
        frame(&[
            MESSAGE_CONTEXT,
            self.pair.as_bytes(),
            sender.as_bytes(),
            id.as_bytes(),
            lane.as_str().as_bytes(),
        ])
    }

    fn live_aad(&self, sender: &DeviceId) -> Vec<u8> {
        frame(&[LIVE_CONTEXT, self.pair.as_bytes(), sender.as_bytes()])
    }

    fn seal<T: Serialize>(&self, aad: &[u8], value: &T) -> Result<Blob, CryptoError> {
        seal_json(&self.send, aad, value).map(Blob::new)
    }

    fn open<T: DeserializeOwned>(&self, aad: &[u8], sealed: &Blob) -> Result<T, CryptoError> {
        open_json(&self.receive, aad, sealed.as_bytes())
    }

    /// Seals a durable message body.
    ///
    /// # Errors
    ///
    /// [`CryptoError::Encoding`] if serialization fails.
    pub fn seal_message(
        &self,
        id: MessageId,
        lane: Lane,
        body: &Body,
    ) -> Result<Blob, CryptoError> {
        self.seal(&self.message_aad(&self.own, id, lane), body)
    }

    /// Opens a durable message body from the peer.
    ///
    /// # Errors
    ///
    /// [`CryptoError::Decrypt`] if it was tampered with or bound to another
    /// pair, sender, ID, or lane. [`CryptoError::Encoding`] for bad JSON.
    pub fn open_message(
        &self,
        id: MessageId,
        lane: Lane,
        sealed: &Blob,
    ) -> Result<Body, CryptoError> {
        self.open(&self.message_aad(&self.peer, id, lane), sealed)
    }

    /// Seals a live update body.
    ///
    /// # Errors
    ///
    /// [`CryptoError::Encoding`] if serialization fails.
    pub fn seal_live(&self, body: &LiveBody) -> Result<Blob, CryptoError> {
        self.seal(&self.live_aad(&self.own), body)
    }

    /// Opens a live update body from the peer.
    ///
    /// # Errors
    ///
    /// Same as [`PairKeys::open_message`].
    pub fn open_live(&self, sealed: &Blob) -> Result<LiveBody, CryptoError> {
        self.open(&self.live_aad(&self.peer), sealed)
    }

    /// `true` if `other` is the peer's view of the same pair.
    #[cfg(test)]
    pub(crate) fn matches(&self, other: &Self) -> bool {
        self.pair == other.pair
            && self.own == other.peer
            && self.peer == other.own
            && *self.send == *other.receive
            && *self.receive == *other.send
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use wici_protocol::StreamId;

    use super::*;
    use crate::device::DeviceKeys;

    fn pair() -> (PairKeys, PairKeys) {
        let a = DeviceKeys::generate();
        let b = DeviceKeys::generate();
        let pair = PairId::generate();
        let shared = a.agree(&b.agreement_public()).unwrap();
        (
            PairKeys::derive(pair, a.device_id(), b.device_id(), b"token", &shared),
            PairKeys::derive(pair, b.device_id(), a.device_id(), b"token", &shared),
        )
    }

    fn event() -> Body {
        Body::Event {
            stream: StreamId::from_bytes([1; 16]),
            data: json!("out"),
        }
    }

    #[test]
    fn peer_opens_what_the_other_seals() {
        let (a, b) = pair();
        assert!(a.matches(&b));
        let id = MessageId::generate();
        let sealed = a.seal_message(id, Lane::Data, &event()).unwrap();
        assert_eq!(b.open_message(id, Lane::Data, &sealed).unwrap(), event());
        let live = LiveBody {
            stream: StreamId::from_bytes([2; 16]),
            data: json!("tok"),
        };
        assert_eq!(b.open_live(&a.seal_live(&live).unwrap()).unwrap(), live);
    }

    #[test]
    fn ciphertext_is_bound_to_id_lane_direction_and_kind() {
        let (a, b) = pair();
        let id = MessageId::generate();
        let sealed = a.seal_message(id, Lane::Data, &event()).unwrap();
        let fail = Err(CryptoError::Decrypt);
        assert_eq!(
            b.open_message(MessageId::generate(), Lane::Data, &sealed),
            fail
        );
        assert_eq!(b.open_message(id, Lane::Control, &sealed), fail);
        assert_eq!(
            a.open_message(id, Lane::Data, &sealed),
            fail,
            "reflected to sender"
        );
        assert_eq!(b.open_live(&sealed).map(|_| ()), Err(CryptoError::Decrypt));
    }

    #[test]
    fn other_pair_cannot_open() {
        let (a, _) = pair();
        let (_, stranger) = pair();
        let id = MessageId::generate();
        let sealed = a.seal_message(id, Lane::Data, &event()).unwrap();
        assert_eq!(
            stranger.open_message(id, Lane::Data, &sealed),
            Err(CryptoError::Decrypt)
        );
    }

    #[test]
    fn secret_round_trips_and_debug_hides_keys() {
        let (a, b) = pair();
        let restored = PairKeys::from_secret(a.pair(), a.own, a.peer(), &a.to_secret());
        assert!(restored.matches(&b));
        let text = format!("{a:?}");
        assert!(
            text.starts_with("PairKeys") && text.ends_with(".. }"),
            "{text}"
        );
    }
}
