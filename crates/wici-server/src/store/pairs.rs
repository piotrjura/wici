//! Pairing: invite, claim, approve, unpair, expire.

use std::time::Duration;

use sqlx::postgres::PgRow;
use sqlx::{PgConnection, Postgres, Row, Transaction};
use subtle::ConstantTimeEq;
use wici_protocol::{Blob, DeviceId, FixedBytes, Lifecycle, PairId, PairState, WireEnum};

use super::{
    PairRecord, Store, StoreError, StoreResult, device_from, lock_device, millis_of, pair_id_from,
    pg_uuid, timestamp, wire_from,
};

fn pair_columns() -> String {
    format!(
        "id, state, inviter, invitee, greeting, claim_hash, \
         coalesce(expires_at <= now(), false) AS due, {} AS expires_ms",
        millis_of("expires_at")
    )
}

/// Pair row plus server-only fields.
pub(super) struct Locked {
    pub(super) record: PairRecord,
    claim_hash: Vec<u8>,
    due: bool,
}

/// Row lock strength.
#[derive(Clone, Copy)]
pub(super) enum LockMode {
    /// Blocks other writers and sharers.
    Update,
    /// Blocks writers only.
    Share,
}

fn locked_from(row: &PgRow) -> StoreResult<Locked> {
    let state: String = row.try_get("state")?;
    let first: Vec<u8> = row.try_get("inviter")?;
    let second: Option<Vec<u8>> = row.try_get("invitee")?;
    let greeting: Option<Vec<u8>> = row.try_get("greeting")?;
    Ok(Locked {
        record: PairRecord {
            id: pair_id_from(row, "id")?,
            state: wire_from(&state)?,
            inviter: device_from(&first)?,
            invitee: second.as_deref().map(device_from).transpose()?,
            greeting: greeting.map(Blob::new),
            expires_at: timestamp(row.try_get("expires_ms")?),
        },
        claim_hash: row.try_get("claim_hash")?,
        due: row.try_get("due")?,
    })
}

/// Reads and locks a pair row until the transaction ends.
pub(super) async fn lock(
    conn: &mut PgConnection,
    pair: PairId,
    mode: LockMode,
) -> StoreResult<Locked> {
    let mode = match mode {
        LockMode::Update => "UPDATE",
        LockMode::Share => "SHARE",
    };
    let row = sqlx::query(&format!(
        "SELECT {} FROM pairs WHERE id = $1 FOR {mode}",
        pair_columns()
    ))
    .bind(pg_uuid(pair.as_bytes()))
    .fetch_optional(conn)
    .await?
    .ok_or(StoreError::NotFound)?;
    locked_from(&row)
}

/// Moves a pair to `next` if the lifecycle allows it. `ttl` sets a new
/// deadline. `claim` sets the invitee and greeting. Other states drop the
/// greeting.
async fn transition(
    conn: &mut PgConnection,
    current: &PairRecord,
    next: PairState,
    ttl: Option<Duration>,
    claim: Option<(&DeviceId, &Blob)>,
) -> StoreResult<PairRecord> {
    current
        .state
        .transition_to(next)
        .map_err(|_| StoreError::Forbidden)?;
    let row = sqlx::query(&format!(
        "UPDATE pairs SET state = $2, updated_at = now(), \
         expires_at = CASE WHEN $3::FLOAT8 IS NULL THEN NULL ELSE now() + make_interval(secs => $3) END, \
         invitee = coalesce($4, invitee), \
         greeting = CASE WHEN $2 = 'claimed' THEN $5 ELSE NULL END \
         WHERE id = $1 RETURNING {}",
        pair_columns()
    ))
    .bind(pg_uuid(current.id.as_bytes()))
    .bind(next.as_str())
    .bind(ttl.map(|t| t.as_secs_f64()))
    .bind(claim.map(|(device, _)| device.as_bytes().to_vec()))
    .bind(claim.map(|(_, greeting)| greeting.as_bytes().to_vec()))
    .fetch_one(conn)
    .await?;
    Ok(locked_from(&row)?.record)
}

/// A claim of an invitation.
#[derive(Debug, Clone, Copy)]
pub struct Claim<'a> {
    /// Pair ID.
    pub pair: PairId,
    /// SHA-256 of the claim secret.
    pub claim_hash: &'a FixedBytes<32>,
    /// Sealed invitee keys for the inviter.
    pub greeting: &'a Blob,
}

impl Store {
    /// Pairs of a device that are open or changed in the last day.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`] on failure.
    pub async fn pairs_of(&self, id: &DeviceId) -> StoreResult<Vec<PairRecord>> {
        let rows = sqlx::query(&format!(
            "SELECT {} FROM pairs WHERE (inviter = $1 OR invitee = $1) \
             AND (state IN ('invited', 'claimed', 'active') OR updated_at > now() - interval '1 day') \
             ORDER BY created_at DESC",
            pair_columns()
        ))
        .bind(id.as_bytes().as_slice())
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| Ok(locked_from(row)?.record))
            .collect()
    }

    /// Creates an invitation. Repeating it with the same hash is a no-op.
    ///
    /// # Errors
    ///
    /// [`StoreError::Conflict`] if the ID exists with other data,
    /// [`StoreError::LimitExceeded`] if the device has too many open pairs.
    pub async fn invite(
        &self,
        inviter: &DeviceId,
        pair: PairId,
        claim_hash: &FixedBytes<32>,
        ttl: Duration,
    ) -> StoreResult<PairRecord> {
        let mut tx = self.pool.begin().await?;
        lock_device(&mut tx, inviter).await?;
        if let Some(existing) = lock(&mut tx, pair, LockMode::Update)
            .await
            .map(Some)
            .or_else(not_found_as_none)?
        {
            let same =
                existing.record.inviter == *inviter && existing.claim_hash == claim_hash.as_bytes();
            return if same {
                Ok(existing.record)
            } else {
                Err(StoreError::Conflict)
            };
        }
        let open: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pairs WHERE (inviter = $1 OR invitee = $1) \
             AND state IN ('invited', 'claimed', 'active')",
        )
        .bind(inviter.as_bytes().as_slice())
        .fetch_one(&mut *tx)
        .await?;
        if open >= self.limits.max_pairs_per_device {
            return Err(StoreError::LimitExceeded);
        }
        let row = sqlx::query(&format!(
            "INSERT INTO pairs (id, state, inviter, claim_hash, expires_at) \
             VALUES ($1, 'invited', $2, $3, now() + make_interval(secs => $4)) RETURNING {}",
            pair_columns()
        ))
        .bind(pg_uuid(pair.as_bytes()))
        .bind(inviter.as_bytes().as_slice())
        .bind(claim_hash.as_bytes().as_slice())
        .bind(ttl.as_secs_f64())
        .fetch_one(&mut *tx)
        .await?;
        let record = locked_from(&row)?.record;
        tx.commit().await?;
        Ok(record)
    }

    /// Claims an invitation. A repeated claim by the same device is a no-op.
    ///
    /// # Errors
    ///
    /// [`StoreError::Forbidden`] for a wrong secret, own invitation, or wrong
    /// state. [`StoreError::Expired`] after the deadline. The expiry is saved.
    pub async fn claim(
        &self,
        invitee: &DeviceId,
        claim: &Claim<'_>,
        ttl: Duration,
    ) -> StoreResult<PairRecord> {
        let Claim {
            pair,
            claim_hash,
            greeting,
        } = *claim;
        let mut tx = self.pool.begin().await?;
        let locked = lock(&mut tx, pair, LockMode::Update).await?;
        let record = &locked.record;
        let secret_ok = bool::from(locked.claim_hash.as_slice().ct_eq(claim_hash.as_bytes()));
        if !secret_ok || record.inviter == *invitee {
            return Err(StoreError::Forbidden);
        }
        // Same device, right secret: a retry. Keep the first greeting.
        let repeat = record.state == PairState::Claimed && record.invitee == Some(*invitee);
        if repeat {
            return Ok(locked.record);
        }
        if record.state != PairState::Invited {
            return Err(StoreError::Forbidden);
        }
        if locked.due {
            return Err(save_expiry(tx, record).await);
        }
        let record = transition(
            &mut tx,
            record,
            PairState::Claimed,
            Some(ttl),
            Some((invitee, greeting)),
        )
        .await?;
        tx.commit().await?;
        Ok(record)
    }

    /// Approves a claimed pair. Inviter only. Repeating it is a no-op.
    ///
    /// # Errors
    ///
    /// [`StoreError::Forbidden`] for non-inviters or wrong state,
    /// [`StoreError::Expired`] after the deadline. The expiry is saved.
    pub async fn approve(&self, inviter: &DeviceId, pair: PairId) -> StoreResult<PairRecord> {
        let mut tx = self.pool.begin().await?;
        let locked = lock(&mut tx, pair, LockMode::Update).await?;
        let record = &locked.record;
        if record.inviter != *inviter {
            return Err(StoreError::Forbidden);
        }
        if record.state == PairState::Active {
            return Ok(locked.record);
        }
        if record.state == PairState::Claimed && locked.due {
            return Err(save_expiry(tx, record).await);
        }
        let record = transition(&mut tx, record, PairState::Active, None, None).await?;
        sqlx::query(
            "INSERT INTO lanes (pair_id, recipient, lane) \
             SELECT $1, member, lane FROM unnest($2::BYTEA[]) AS member \
             CROSS JOIN unnest(ARRAY['control', 'data']) AS lane",
        )
        .bind(pg_uuid(pair.as_bytes()))
        .bind(
            record
                .members()
                .iter()
                .map(|d| d.as_bytes().to_vec())
                .collect::<Vec<_>>(),
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(record)
    }

    /// Revokes a pair and deletes its queued messages and artifacts. Either member.
    /// Repeating it is a no-op.
    ///
    /// # Errors
    ///
    /// [`StoreError::Forbidden`] for non-members or expired pairs.
    pub async fn unpair(&self, member: &DeviceId, pair: PairId) -> StoreResult<PairRecord> {
        let mut tx = self.pool.begin().await?;
        let locked = lock(&mut tx, pair, LockMode::Update).await?;
        if !locked.record.members().contains(member) {
            return Err(StoreError::Forbidden);
        }
        if locked.record.state == PairState::Revoked {
            return Ok(locked.record);
        }
        let record = transition(&mut tx, &locked.record, PairState::Revoked, None, None).await?;
        for table in ["lanes", "artifacts"] {
            sqlx::query(&format!("DELETE FROM {table} WHERE pair_id = $1"))
                .bind(pg_uuid(pair.as_bytes()))
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(record)
    }

    /// Marks up to `limit` overdue invitations and claims as expired.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`] on failure.
    pub async fn expire_due(&self, limit: i64) -> StoreResult<Vec<PairRecord>> {
        let rows = sqlx::query(&format!(
            "UPDATE pairs SET state = 'expired', expires_at = NULL, greeting = NULL, updated_at = now() \
             WHERE id IN (SELECT id FROM pairs WHERE state IN ('invited', 'claimed') \
             AND expires_at <= now() ORDER BY expires_at LIMIT $1 FOR UPDATE SKIP LOCKED) \
             RETURNING {}",
            pair_columns()
        ))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| Ok(locked_from(row)?.record))
            .collect()
    }
}

/// Commits the pair as expired and returns [`StoreError::Expired`], or the
/// error that stopped the commit.
async fn save_expiry(mut tx: Transaction<'_, Postgres>, record: &PairRecord) -> StoreError {
    if let Err(error) = transition(&mut tx, record, PairState::Expired, None, None).await {
        return error;
    }
    match tx.commit().await {
        Ok(()) => StoreError::Expired,
        Err(error) => error.into(),
    }
}

fn not_found_as_none(error: StoreError) -> StoreResult<Option<Locked>> {
    if matches!(error, StoreError::NotFound) {
        Ok(None)
    } else {
        Err(error)
    }
}
