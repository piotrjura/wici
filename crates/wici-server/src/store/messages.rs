//! Durable message rules: send, fetch, ack.

use sha2::{Digest, Sha256};
use wici_protocol::{
    Blob, DeviceId, Lane, MessageId, PairId, PairState, Position, Timestamp, WireEnum,
};

use super::adapter::{Adapter, LockMode};
use super::pairs::lock;
use super::sql::messages::{
    MessageRow, allocate, cursors, drop_acked, fetch, find_sent, insert_message, raise_ack,
};
use super::sql::{position_from, position_value};
use super::{Adapters, Store, StoreError, StoreLimits, StoreResult, dispatch};

/// One recipient lane of a pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LaneKey {
    /// Pair ID.
    pub pair: PairId,
    /// Receiving device.
    pub recipient: DeviceId,
    /// Lane.
    pub lane: Lane,
}

/// A durable message to store.
#[derive(Debug, Clone, Copy)]
pub struct NewMessage<'a> {
    /// Pair ID.
    pub pair: PairId,
    /// Message ID.
    pub id: MessageId,
    /// Lane.
    pub lane: Lane,
    /// Encrypted body.
    pub sealed: &'a Blob,
}

/// Result of a durable send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Accepted {
    /// Assigned position.
    pub position: Position,
    /// Device that receives the message.
    pub recipient: DeviceId,
    /// `true` if this ID was already stored with the same content.
    pub duplicate: bool,
}

/// Durable message waiting for its recipient.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    /// Pair ID.
    pub pair: PairId,
    /// Lane.
    pub lane: Lane,
    /// Position.
    pub position: Position,
    /// Message ID.
    pub id: MessageId,
    /// Acceptance time.
    pub accepted_at: Timestamp,
    /// Encrypted body.
    pub sealed: Blob,
}

/// Delivery progress of one lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LaneCursor {
    /// Pair ID.
    pub pair: PairId,
    /// Lane.
    pub lane: Lane,
    /// Last position the recipient saved.
    pub acked: Position,
    /// Last accepted position. Equals `acked` when nothing waits.
    pub last: Position,
}

/// Binds a message ID to its lane and ciphertext.
fn payload_hash(lane: Lane, sealed: &Blob) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(lane.as_str().as_bytes());
    hasher.update([0]);
    hasher.update(sealed.as_bytes());
    hasher.finalize().to_vec()
}

impl Store {
    /// Stores a durable message. Retrying with the same ID and content
    /// returns the original position.
    ///
    /// # Errors
    ///
    /// [`StoreError::Forbidden`] if the pair is not active or the sender is
    /// not a member. [`StoreError::Conflict`] for a reused ID with other
    /// content or lane. [`StoreError::LimitExceeded`] if the lane is full.
    pub async fn send(&self, sender: &DeviceId, message: &NewMessage<'_>) -> StoreResult<Accepted> {
        dispatch!(self, |a| send(a, self.limits, sender, message).await)
    }

    /// Acknowledged and last positions of every active lane the device
    /// receives on.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`] on failure.
    pub async fn cursors(&self, recipient: &DeviceId) -> StoreResult<Vec<LaneCursor>> {
        dispatch!(self, |a| {
            cursors(&mut *a.reader().acquire().await?, recipient).await
        })
    }

    /// Up to `limit` unacknowledged messages after `after`, in position order.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`] on failure.
    pub async fn fetch(
        &self,
        key: &LaneKey,
        after: Position,
        limit: i64,
    ) -> StoreResult<Vec<Delivery>> {
        let after = position_value(after)?;
        dispatch!(self, |a| {
            fetch(&mut *a.reader().acquire().await?, key, after, limit).await
        })
    }

    /// Records that the recipient saved everything up to `up_to` and drops
    /// those payloads. Lower or repeated acks are no-ops.
    ///
    /// # Errors
    ///
    /// [`StoreError::Forbidden`] if `up_to` was never assigned or the device
    /// does not receive on this lane.
    pub async fn ack(&self, key: &LaneKey, up_to: Position) -> StoreResult<()> {
        let up_to = position_value(up_to)?;
        dispatch!(self, |a| ack(a, key, up_to).await)
    }
}

async fn send<A: Adapter>(
    adapter: &A,
    limits: StoreLimits,
    sender: &DeviceId,
    message: &NewMessage<'_>,
) -> StoreResult<Accepted> {
    let hash = payload_hash(message.lane, message.sealed);
    let mut tx = adapter.writer().begin().await?;
    // Shared lock: sends run in parallel but never after a committed revoke.
    let pair = lock(&mut *tx, message.pair, LockMode::Share).await?.record;
    let (PairState::Active, Some(recipient)) = (pair.state, pair.peer_of(sender)) else {
        return Err(StoreError::Forbidden);
    };
    if let Some((stored_hash, position)) = find_sent(&mut *tx, message, sender).await? {
        return if stored_hash == hash {
            Ok(Accepted {
                position: position_from(position)?,
                recipient,
                duplicate: true,
            })
        } else {
            Err(StoreError::Conflict)
        };
    }
    let key = LaneKey {
        pair: message.pair,
        recipient,
        lane: message.lane,
    };
    // A rollback returns the position, so positions never skip.
    let slot = allocate(&mut *tx, &key).await?;
    if slot.assigned - slot.acked > limits.max_pending_per_lane {
        return Err(StoreError::LimitExceeded);
    }
    let row = MessageRow {
        key: &key,
        position: slot.assigned,
        id: message.id,
        sender,
        payload_hash: &hash,
        sealed: message.sealed,
    };
    insert_message(&mut *tx, row).await?;
    tx.commit().await?;
    Ok(Accepted {
        position: position_from(slot.assigned)?,
        recipient,
        duplicate: false,
    })
}

async fn ack<A: Adapter>(adapter: &A, key: &LaneKey, up_to: i64) -> StoreResult<()> {
    let mut tx = adapter.writer().begin().await?;
    if raise_ack(&mut *tx, key, up_to).await? == 0 {
        return Err(StoreError::Forbidden);
    }
    drop_acked(&mut *tx, key, up_to).await?;
    tx.commit().await?;
    Ok(())
}
