//! Durable message queries.

use wici_protocol::{Blob, DeviceId, MessageId, Timestamp, WireEnum};

use super::rows::{Fields, get, lane_from, pair_id_from, position_from, timestamp, uuid_bytes};
use super::{Exec, Param, id};
use crate::store::StoreResult;
use crate::store::adapter::Driver;
use crate::store::messages::{Delivery, LaneCursor, LaneKey};

/// Lane counters after taking a position.
#[derive(Debug, Clone, Copy)]
pub(in crate::store) struct Slot {
    pub(in crate::store) assigned: i64,
    pub(in crate::store) acked: i64,
}

/// A message row to insert.
#[derive(Debug, Clone, Copy)]
pub(in crate::store) struct MessageRow<'a> {
    pub(in crate::store) key: &'a LaneKey,
    pub(in crate::store) position: i64,
    pub(in crate::store) id: MessageId,
    pub(in crate::store) sender: &'a DeviceId,
    pub(in crate::store) payload_hash: &'a [u8],
    pub(in crate::store) sealed: &'a Blob,
}

/// Selects the lane of a key. Binds `$1..$3`.
const LANE: &str = "pair_id = $1 AND recipient = $2 AND lane = $3";

fn lane_params(key: &LaneKey) -> [Param<'_>; 3] {
    [
        id(key.pair.as_bytes()),
        Param::Bytes(key.recipient.as_bytes()),
        Param::Text(key.lane.as_str()),
    ]
}

/// Lane parameters, then `$4` set to `position`.
fn lane_and(key: &LaneKey, position: i64) -> [Param<'_>; 4] {
    let [pair, recipient, lane] = lane_params(key);
    [pair, recipient, lane, Param::Int(position)]
}

fn cursor_from(row: &impl Fields) -> StoreResult<LaneCursor> {
    Ok(LaneCursor {
        pair: pair_id_from(row, "pair_id")?,
        lane: lane_from(row, "lane")?,
        acked: position_from(get(row, "acked_position")?)?,
        last: position_from(get(row, "last_position")?)?,
    })
}

fn delivery_from(key: &LaneKey, row: &impl Fields) -> StoreResult<Delivery> {
    Ok(Delivery {
        pair: key.pair,
        lane: key.lane,
        position: position_from(get(row, "position")?)?,
        id: MessageId::from_bytes(uuid_bytes(row, "id")?),
        accepted_at: timestamp(get(row, "accepted_ms")?).unwrap_or(Timestamp(0)),
        sealed: Blob::new(get(row, "sealed")?),
    })
}

/// Payload hash and position of a message the sender stored with this ID.
pub(in crate::store) async fn find_sent<C: Exec>(
    conn: &mut C,
    message: &crate::store::NewMessage<'_>,
    sender: &DeviceId,
) -> StoreResult<Option<(Vec<u8>, i64)>> {
    let sql = "SELECT payload_hash, position FROM messages \
               WHERE pair_id = $1 AND sender = $2 AND id = $3";
    let params = [
        id(message.pair.as_bytes()),
        Param::Bytes(sender.as_bytes()),
        id(message.id.as_bytes()),
    ];
    let row = conn.optional(sql, &params).await?;
    row.map(|row| Ok((get(&row, "payload_hash")?, get(&row, "position")?)))
        .transpose()
}

/// Takes the next position of a lane. A rollback returns it.
pub(in crate::store) async fn allocate<C: Exec>(conn: &mut C, key: &LaneKey) -> StoreResult<Slot> {
    // The row lock orders concurrent senders.
    let sql = format!(
        "UPDATE lanes SET next_position = next_position + 1 WHERE {LANE} \
         RETURNING next_position - 1 AS position, acked_position"
    );
    let row = conn.one(&sql, &lane_params(key)).await?;
    Ok(Slot {
        assigned: get(&row, "position")?,
        acked: get(&row, "acked_position")?,
    })
}

/// Inserts a message.
pub(in crate::store) async fn insert_message<C: Exec>(
    conn: &mut C,
    row: MessageRow<'_>,
) -> StoreResult<()> {
    let sql = "INSERT INTO messages \
               (pair_id, recipient, lane, position, id, sender, payload_hash, sealed) \
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)";
    let [pair, recipient, lane, position] = lane_and(row.key, row.position);
    let params = [
        pair,
        recipient,
        lane,
        position,
        id(row.id.as_bytes()),
        Param::Bytes(row.sender.as_bytes()),
        Param::Bytes(row.payload_hash),
        Param::Bytes(row.sealed.as_bytes()),
    ];
    conn.run(sql, &params).await?;
    Ok(())
}

/// Cursors of every lane the device receives on.
pub(in crate::store) async fn cursors<C: Exec>(
    conn: &mut C,
    recipient: &DeviceId,
) -> StoreResult<Vec<LaneCursor>> {
    // Lanes exist only while their pair is active, so no join with pairs.
    let sql = "SELECT pair_id, lane, acked_position, next_position - 1 AS last_position \
               FROM lanes WHERE recipient = $1 ORDER BY pair_id, lane";
    let rows = conn.all(sql, &[Param::Bytes(recipient.as_bytes())]).await?;
    rows.iter().map(cursor_from).collect()
}

/// Up to `limit` unacknowledged messages after `after`, in order.
pub(in crate::store) async fn fetch<C: Exec>(
    conn: &mut C,
    key: &LaneKey,
    after: i64,
    limit: i64,
) -> StoreResult<Vec<Delivery>> {
    let sql = format!(
        "SELECT position, id, sealed, {} AS accepted_ms FROM messages \
         WHERE {LANE} AND position > $4 AND sealed IS NOT NULL ORDER BY position LIMIT $5",
        C::Db::millis("accepted_at")
    );
    let [pair, recipient, lane, after] = lane_and(key, after);
    let params = [pair, recipient, lane, after, Param::Int(limit)];
    let rows = conn.all(&sql, &params).await?;
    rows.iter().map(|row| delivery_from(key, row)).collect()
}

/// Raises the acked position to `up_to` if it was assigned. Returns the
/// number of matching lanes.
pub(in crate::store) async fn raise_ack<C: Exec>(
    conn: &mut C,
    key: &LaneKey,
    up_to: i64,
) -> StoreResult<u64> {
    let sql = format!(
        "UPDATE lanes SET acked_position = \
         CASE WHEN $4 > acked_position THEN $4 ELSE acked_position END \
         WHERE {LANE} AND $4 < next_position"
    );
    conn.run(&sql, &lane_and(key, up_to)).await
}

/// Drops payloads up to `up_to`. Rows stay for deduplication.
pub(in crate::store) async fn drop_acked<C: Exec>(
    conn: &mut C,
    key: &LaneKey,
    up_to: i64,
) -> StoreResult<()> {
    let sql = format!(
        "UPDATE messages SET sealed = NULL WHERE {LANE} \
         AND position <= $4 AND sealed IS NOT NULL"
    );
    conn.run(&sql, &lane_and(key, up_to)).await?;
    Ok(())
}
