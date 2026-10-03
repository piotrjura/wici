//! Durable messages: send, fetch, ack.

use sha2::{Digest, Sha256};
use sqlx::{PgConnection, Row};
use wici_protocol::{
    Blob, DeviceId, Lane, MessageId, PairId, PairState, Position, Timestamp, WireEnum,
};

use super::pairs::{LockMode, lock};
use super::{
    Store, StoreError, StoreResult, lane_from, message_id_from, millis_of, pair_id_from, pg_uuid,
    position_from, position_value, timestamp,
};

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
        let hash = payload_hash(message.lane, message.sealed);
        let mut tx = self.pool.begin().await?;
        // Shared lock: sends run in parallel but never after a committed revoke.
        let pair = lock(&mut tx, message.pair, LockMode::Share).await?.record;
        let (PairState::Active, Some(recipient)) = (pair.state, pair.peer_of(sender)) else {
            return Err(StoreError::Forbidden);
        };
        if let Some((stored_hash, position)) = find_sent(&mut tx, sender, message).await? {
            return if stored_hash == hash {
                Ok(Accepted {
                    position,
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
        let position = self.allocate(&mut tx, &key).await?;
        sqlx::query(
            "INSERT INTO messages (pair_id, recipient, lane, position, id, sender, payload_hash, sealed) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(pg_uuid(key.pair.as_bytes()))
        .bind(recipient.as_bytes().as_slice())
        .bind(key.lane.as_str())
        .bind(position_value(position)?)
        .bind(pg_uuid(message.id.as_bytes()))
        .bind(sender.as_bytes().as_slice())
        .bind(hash)
        .bind(message.sealed.as_bytes())
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Accepted {
            position,
            recipient,
            duplicate: false,
        })
    }

    /// Takes the next position of a lane. The row lock orders concurrent
    /// senders, and a rollback returns the position, so positions never skip.
    async fn allocate(&self, conn: &mut PgConnection, key: &LaneKey) -> StoreResult<Position> {
        let row = sqlx::query(
            "UPDATE lanes SET next_position = next_position + 1 \
             WHERE pair_id = $1 AND recipient = $2 AND lane = $3 \
             RETURNING next_position - 1 AS position, acked_position",
        )
        .bind(pg_uuid(key.pair.as_bytes()))
        .bind(key.recipient.as_bytes().as_slice())
        .bind(key.lane.as_str())
        .fetch_one(conn)
        .await?;
        let assigned: i64 = row.try_get("position")?;
        let acked: i64 = row.try_get("acked_position")?;
        if assigned - acked > self.limits.max_pending_per_lane {
            return Err(StoreError::LimitExceeded);
        }
        position_from(assigned)
    }

    /// Acknowledged and last positions of every active lane the device
    /// receives on.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`] on failure.
    pub async fn cursors(&self, recipient: &DeviceId) -> StoreResult<Vec<LaneCursor>> {
        let rows = sqlx::query(
            "SELECT l.pair_id, l.lane, l.acked_position, l.next_position - 1 AS last_position \
             FROM lanes l \
             JOIN pairs p ON p.id = l.pair_id \
             WHERE l.recipient = $1 AND p.state = 'active' ORDER BY l.pair_id, l.lane",
        )
        .bind(recipient.as_bytes().as_slice())
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                Ok(LaneCursor {
                    pair: pair_id_from(row, "pair_id")?,
                    lane: lane_from(row, "lane")?,
                    acked: position_from(row.try_get("acked_position")?)?,
                    last: position_from(row.try_get("last_position")?)?,
                })
            })
            .collect()
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
        let rows = sqlx::query(&format!(
            "SELECT position, id, sealed, {} AS accepted_ms FROM messages \
             WHERE pair_id = $1 AND recipient = $2 AND lane = $3 \
             AND position > $4 AND sealed IS NOT NULL ORDER BY position LIMIT $5",
            millis_of("accepted_at")
        ))
        .bind(pg_uuid(key.pair.as_bytes()))
        .bind(key.recipient.as_bytes().as_slice())
        .bind(key.lane.as_str())
        .bind(position_value(after)?)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                Ok(Delivery {
                    pair: key.pair,
                    lane: key.lane,
                    position: position_from(row.try_get("position")?)?,
                    id: message_id_from(row, "id")?,
                    accepted_at: timestamp(row.try_get("accepted_ms")?).unwrap_or(Timestamp(0)),
                    sealed: Blob::new(row.try_get("sealed")?),
                })
            })
            .collect()
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
        let mut tx = self.pool.begin().await?;
        let updated = exec_on_lane(
            &mut tx,
            "UPDATE lanes SET acked_position = greatest(acked_position, $4) \
             WHERE pair_id = $1 AND recipient = $2 AND lane = $3 AND $4 < next_position",
            key,
            up_to,
        )
        .await?;
        if updated == 0 {
            return Err(StoreError::Forbidden);
        }
        exec_on_lane(
            &mut tx,
            "UPDATE messages SET sealed = NULL WHERE pair_id = $1 AND recipient = $2 \
             AND lane = $3 AND position <= $4 AND sealed IS NOT NULL",
            key,
            up_to,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
}

/// Runs `sql` with `$1..$3` bound to the lane key and `$4` to `position`.
/// Returns the affected row count.
async fn exec_on_lane(
    conn: &mut PgConnection,
    sql: &str,
    key: &LaneKey,
    position: i64,
) -> StoreResult<u64> {
    let result = sqlx::query(sql)
        .bind(pg_uuid(key.pair.as_bytes()))
        .bind(key.recipient.as_bytes().as_slice())
        .bind(key.lane.as_str())
        .bind(position)
        .execute(conn)
        .await?;
    Ok(result.rows_affected())
}

/// Hash and position of a message the sender already stored with this ID.
async fn find_sent(
    conn: &mut PgConnection,
    sender: &DeviceId,
    message: &NewMessage<'_>,
) -> StoreResult<Option<(Vec<u8>, Position)>> {
    let row = sqlx::query(
        "SELECT payload_hash, position FROM messages \
         WHERE pair_id = $1 AND sender = $2 AND id = $3",
    )
    .bind(pg_uuid(message.pair.as_bytes()))
    .bind(sender.as_bytes().as_slice())
    .bind(pg_uuid(message.id.as_bytes()))
    .fetch_optional(conn)
    .await?;
    row.map(|row| {
        Ok((
            row.try_get("payload_hash")?,
            position_from(row.try_get("position")?)?,
        ))
    })
    .transpose()
}
