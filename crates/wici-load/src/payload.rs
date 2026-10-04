//! Load messages. The server never reads them, so they are not encrypted.

use wici_protocol::Blob;

/// Header bytes: kind, send time, origin time.
const HEADER: usize = 17;

/// Message kinds in the Sfora pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    /// Mac to phone: something changed.
    Notice,
    /// Phone to Mac: send a snapshot.
    Request,
    /// Mac to phone: the snapshot.
    Snapshot,
}

impl Kind {
    const fn tag(self) -> u8 {
        match self {
            Self::Notice => 0,
            Self::Request => 1,
            Self::Snapshot => 2,
        }
    }

    const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(Self::Notice),
            1 => Some(Self::Request),
            2 => Some(Self::Snapshot),
            _ => None,
        }
    }
}

/// A load message. Times are microseconds on the run clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Payload {
    pub(crate) kind: Kind,
    /// When this message was sent.
    pub(crate) sent: u64,
    /// When the notice that started this exchange was sent.
    pub(crate) origin: u64,
}

impl Payload {
    /// Encodes to `size` bytes, or the header size if `size` is smaller.
    pub(crate) fn encode(self, size: usize) -> Blob {
        let mut bytes = Vec::with_capacity(size.max(HEADER));
        bytes.push(self.kind.tag());
        bytes.extend_from_slice(&self.sent.to_le_bytes());
        bytes.extend_from_slice(&self.origin.to_le_bytes());
        bytes.resize(size.max(HEADER), 0);
        Blob::new(bytes)
    }

    /// Decodes a message. `None` if it is not a load message.
    pub(crate) fn decode(bytes: &[u8]) -> Option<Self> {
        let (&tag, rest) = bytes.split_first()?;
        let (sent, rest) = rest.split_first_chunk::<8>()?;
        let (origin, _) = rest.split_first_chunk::<8>()?;
        Some(Self {
            kind: Kind::from_tag(tag)?,
            sent: u64::from_le_bytes(*sent),
            origin: u64::from_le_bytes(*origin),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_pads() {
        for kind in [Kind::Notice, Kind::Request, Kind::Snapshot] {
            let payload = Payload {
                kind,
                sent: 7,
                origin: 3,
            };
            let blob = payload.encode(100);
            assert_eq!(blob.len(), 100);
            assert_eq!(Payload::decode(blob.as_bytes()), Some(payload));
        }
    }

    #[test]
    fn small_size_keeps_the_header() {
        let payload = Payload {
            kind: Kind::Notice,
            sent: 1,
            origin: 1,
        };
        assert_eq!(payload.encode(0).len(), HEADER);
    }

    #[test]
    fn rejects_short_and_unknown_messages() {
        assert_eq!(Payload::decode(&[]), None);
        assert_eq!(Payload::decode(&[0; HEADER - 1]), None);
        assert_eq!(Payload::decode(&[9; HEADER]), None);
    }
}
