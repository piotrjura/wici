//! Device and pair queries.

use std::time::Duration;

use wici_protocol::{Blob, DeviceId, FixedBytes, Lane, PairId, PairState, WireEnum};

use super::rows::{Fields, device_from, get, pair_id_from, timestamp, wire_from};
use super::{Exec, Param, id, millis};
use crate::store::adapter::{Driver, LockMode};
use crate::store::{PairRecord, StoreResult};

/// Pair row plus server-only fields.
#[derive(Debug)]
pub(in crate::store) struct Locked {
    pub(in crate::store) record: PairRecord,
    pub(in crate::store) claim_hash: Vec<u8>,
    /// The deadline has passed.
    pub(in crate::store) due: bool,
}

/// New pair state. `ttl` sets a new deadline, `None` clears it. `claim`
/// sets the invitee and greeting.
#[derive(Debug, Clone, Copy)]
pub(in crate::store) struct PairChange<'a> {
    pub(in crate::store) state: PairState,
    pub(in crate::store) ttl: Option<Duration>,
    pub(in crate::store) claim: Option<(&'a DeviceId, &'a Blob)>,
}

fn columns<D: Driver>() -> String {
    format!(
        "id, state, inviter, invitee, greeting, claim_hash, \
         coalesce(expires_at <= {}, FALSE) AS due, {} AS expires_ms",
        D::NOW,
        D::millis("expires_at")
    )
}

fn locked_from(row: &impl Fields) -> StoreResult<Locked> {
    let state: String = get(row, "state")?;
    let first: Vec<u8> = get(row, "inviter")?;
    let second: Option<Vec<u8>> = get(row, "invitee")?;
    let greeting: Option<Vec<u8>> = get(row, "greeting")?;
    Ok(Locked {
        record: PairRecord {
            id: pair_id_from(row, "id")?,
            state: wire_from(&state)?,
            inviter: device_from(&first)?,
            invitee: second.as_deref().map(device_from).transpose()?,
            greeting: greeting.map(Blob::new),
            expires_at: timestamp(get(row, "expires_ms")?),
        },
        claim_hash: get(row, "claim_hash")?,
        due: get(row, "due")?,
    })
}

fn records_from(rows: &[impl Fields]) -> StoreResult<Vec<PairRecord>> {
    rows.iter()
        .map(|row| Ok(locked_from(row)?.record))
        .collect()
}

const fn device(id: &DeviceId) -> Param<'_> {
    Param::Bytes(id.as_bytes())
}

/// Inserts the device or updates its last-seen time.
pub(in crate::store) async fn touch_device<C: Exec>(
    conn: &mut C,
    id: &DeviceId,
) -> StoreResult<()> {
    let sql = format!(
        "INSERT INTO devices (id) VALUES ($1) \
         ON CONFLICT (id) DO UPDATE SET last_seen_at = {}",
        C::Db::NOW
    );
    conn.run(&sql, &[device(id)]).await?;
    Ok(())
}

/// Last time the device connected, in milliseconds.
pub(in crate::store) async fn last_seen<C: Exec>(
    conn: &mut C,
    id: &DeviceId,
) -> StoreResult<Option<i64>> {
    let sql = format!(
        "SELECT {} AS seen_ms FROM devices WHERE id = $1",
        C::Db::millis("last_seen_at")
    );
    let row = conn.optional(&sql, &[device(id)]).await?;
    row.map(|row| get(&row, "seen_ms")).transpose()
}

/// Serializes per-device limit checks until the transaction ends.
pub(in crate::store) async fn lock_device<C: Exec>(conn: &mut C, id: &DeviceId) -> StoreResult<()> {
    let sql = format!(
        "SELECT 1 FROM devices WHERE id = $1{}",
        C::Db::lock(LockMode::Update)
    );
    conn.run(&sql, &[device(id)]).await?;
    Ok(())
}

/// Pairs of a device that are open or changed in the last day, newest first.
pub(in crate::store) async fn pairs_of<C: Exec>(
    conn: &mut C,
    id: &DeviceId,
) -> StoreResult<Vec<PairRecord>> {
    let sql = format!(
        "SELECT {} FROM pairs WHERE (inviter = $1 OR invitee = $1) \
         AND (state IN ('invited', 'claimed', 'active') OR updated_at > {}) \
         ORDER BY created_at DESC",
        columns::<C::Db>(),
        C::Db::now_plus("-86400000")
    );
    records_from(&conn.all(&sql, &[device(id)]).await?)
}

/// Reads and locks a pair row until the transaction ends.
pub(in crate::store) async fn lock_pair<C: Exec>(
    conn: &mut C,
    pair: PairId,
    mode: LockMode,
) -> StoreResult<Option<Locked>> {
    let sql = format!(
        "SELECT {} FROM pairs WHERE id = $1{}",
        columns::<C::Db>(),
        C::Db::lock(mode)
    );
    let row = conn.optional(&sql, &[id(pair.as_bytes())]).await?;
    row.as_ref().map(locked_from).transpose()
}

/// Invited, claimed, or active pairs of a device.
pub(in crate::store) async fn count_open_pairs<C: Exec>(
    conn: &mut C,
    device_id: &DeviceId,
) -> StoreResult<i64> {
    let sql = "SELECT count(*) AS n FROM pairs WHERE (inviter = $1 OR invitee = $1) \
               AND state IN ('invited', 'claimed', 'active')";
    get(&conn.one(sql, &[device(device_id)]).await?, "n")
}

/// Inserts an invited pair that expires after `ttl`.
pub(in crate::store) async fn insert_pair<C: Exec>(
    conn: &mut C,
    pair: PairId,
    inviter: &DeviceId,
    claim_hash: &FixedBytes<32>,
    ttl: Duration,
) -> StoreResult<PairRecord> {
    let sql = format!(
        "INSERT INTO pairs (id, state, inviter, claim_hash, expires_at) \
         VALUES ($1, 'invited', $2, $3, {}) RETURNING {}",
        C::Db::now_plus("$4"),
        columns::<C::Db>()
    );
    let params = [
        id(pair.as_bytes()),
        device(inviter),
        Param::Bytes(claim_hash.as_bytes()),
        Param::Int(millis(ttl)?),
    ];
    Ok(locked_from(&conn.one(&sql, &params).await?)?.record)
}

/// Writes a state change. The caller checks the lifecycle.
pub(in crate::store) async fn update_pair<C: Exec>(
    conn: &mut C,
    pair: PairId,
    change: PairChange<'_>,
) -> StoreResult<PairRecord> {
    let sql = format!(
        "UPDATE pairs SET state = $2, updated_at = {}, expires_at = {}, \
         invitee = coalesce($4, invitee), \
         greeting = CASE WHEN $2 = 'claimed' THEN $5 ELSE NULL END \
         WHERE id = $1 RETURNING {}",
        C::Db::NOW,
        C::Db::now_plus("$3"),
        columns::<C::Db>()
    );
    let params = [
        id(pair.as_bytes()),
        Param::Text(change.state.as_str()),
        Param::MaybeInt(change.ttl.map(millis).transpose()?),
        Param::MaybeBytes(
            change
                .claim
                .map(|(invitee, _)| invitee.as_bytes().as_slice()),
        ),
        Param::MaybeBytes(change.claim.map(|(_, greeting)| greeting.as_bytes())),
    ];
    Ok(locked_from(&conn.one(&sql, &params).await?)?.record)
}

/// Creates both lanes for each member.
pub(in crate::store) async fn create_lanes<C: Exec>(
    conn: &mut C,
    pair: PairId,
    members: &[DeviceId],
) -> StoreResult<()> {
    let sql = "INSERT INTO lanes (pair_id, recipient, lane) VALUES ($1, $2, $3)";
    for member in members {
        for lane in Lane::ALL {
            let params = [
                id(pair.as_bytes()),
                device(member),
                Param::Text(lane.as_str()),
            ];
            conn.run(sql, &params).await?;
        }
    }
    Ok(())
}

/// Deletes lanes, queued messages, and artifacts of a pair.
pub(in crate::store) async fn delete_pair_data<C: Exec>(
    conn: &mut C,
    pair: PairId,
) -> StoreResult<()> {
    for sql in [
        "DELETE FROM lanes WHERE pair_id = $1",
        "DELETE FROM artifacts WHERE pair_id = $1",
    ] {
        conn.run(sql, &[id(pair.as_bytes())]).await?;
    }
    Ok(())
}

/// Marks up to `limit` overdue invitations and claims as expired.
pub(in crate::store) async fn expire_due<C: Exec>(
    conn: &mut C,
    limit: i64,
) -> StoreResult<Vec<PairRecord>> {
    let sql = format!(
        "UPDATE pairs SET state = 'expired', expires_at = NULL, greeting = NULL, updated_at = {now} \
         WHERE id IN (SELECT id FROM pairs WHERE state IN ('invited', 'claimed') \
         AND expires_at <= {now} ORDER BY expires_at LIMIT $1{skip}) \
         RETURNING {columns}",
        now = C::Db::NOW,
        skip = C::Db::SKIP_LOCKED,
        columns = columns::<C::Db>()
    );
    records_from(&conn.all(&sql, &[Param::Int(limit)]).await?)
}
