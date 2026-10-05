//! Pairing rules: invite, claim, approve, unpair, expire.

use std::time::Duration;

use sqlx::{Database, Transaction};
use subtle::ConstantTimeEq;
use wici_protocol::{Blob, DeviceId, FixedBytes, Lifecycle, PairId, PairState};

use super::adapter::{Adapter, LockMode};
use super::sql::Exec;
use super::sql::pairs::{
    Locked, PairChange, count_open_pairs, create_lanes, delete_pair_data, expire_due, insert_pair,
    lock_device, lock_pair, pairs_of, update_pair,
};
use super::{Adapters, PairRecord, Store, StoreError, StoreLimits, StoreResult, dispatch};

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
        dispatch!(self, |a| pairs_of(&mut *a.reader().acquire().await?, id)
            .await)
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
        let request = Invite {
            inviter,
            pair,
            claim_hash,
            ttl,
        };
        dispatch!(self, |a| invite(a, self.limits, &request).await)
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
        dispatch!(self, |a| self::claim(a, invitee, claim, ttl).await)
    }

    /// Approves a claimed pair. Inviter only. Repeating it is a no-op.
    ///
    /// # Errors
    ///
    /// [`StoreError::Forbidden`] for non-inviters or wrong state,
    /// [`StoreError::Expired`] after the deadline. The expiry is saved.
    pub async fn approve(&self, inviter: &DeviceId, pair: PairId) -> StoreResult<PairRecord> {
        dispatch!(self, |a| approve(a, inviter, pair).await)
    }

    /// Revokes a pair and deletes its queued messages and artifacts. Either member.
    /// Repeating it is a no-op.
    ///
    /// # Errors
    ///
    /// [`StoreError::Forbidden`] for non-members or expired pairs.
    pub async fn unpair(&self, member: &DeviceId, pair: PairId) -> StoreResult<PairRecord> {
        dispatch!(self, |a| unpair(a, member, pair).await)
    }

    /// Marks up to `limit` overdue invitations and claims as expired.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`] on failure.
    pub async fn expire_due(&self, limit: i64) -> StoreResult<Vec<PairRecord>> {
        dispatch!(self, |a| {
            expire_due(&mut *a.writer().acquire().await?, limit).await
        })
    }
}

/// Reads and locks a pair row until the transaction ends.
pub(super) async fn lock(
    conn: &mut impl Exec,
    pair: PairId,
    mode: LockMode,
) -> StoreResult<Locked> {
    lock_pair(conn, pair, mode)
        .await?
        .ok_or(StoreError::NotFound)
}

/// Starts a write transaction and locks the pair for update.
async fn begin_locked<A: Adapter>(
    adapter: &A,
    pair: PairId,
) -> StoreResult<(Transaction<'static, A::Db>, Locked)> {
    let mut tx = adapter.writer().begin().await?;
    let locked = lock(&mut *tx, pair, LockMode::Update).await?;
    Ok((tx, locked))
}

/// Moves a pair to `change.state` if the lifecycle allows it.
async fn transition(
    conn: &mut impl Exec,
    current: &PairRecord,
    change: PairChange<'_>,
) -> StoreResult<PairRecord> {
    current
        .state
        .transition_to(change.state)
        .map_err(|_| StoreError::Forbidden)?;
    update_pair(conn, current.id, change).await
}

/// A state change without deadline or claim.
const fn plain(state: PairState) -> PairChange<'static> {
    PairChange {
        state,
        ttl: None,
        claim: None,
    }
}

/// An invitation to store.
struct Invite<'a> {
    inviter: &'a DeviceId,
    pair: PairId,
    claim_hash: &'a FixedBytes<32>,
    ttl: Duration,
}

async fn invite<A: Adapter>(
    adapter: &A,
    limits: StoreLimits,
    request: &Invite<'_>,
) -> StoreResult<PairRecord> {
    let Invite {
        inviter,
        pair,
        claim_hash,
        ttl,
    } = *request;
    let mut tx = adapter.writer().begin().await?;
    lock_device(&mut *tx, inviter).await?;
    if let Some(existing) = lock_pair(&mut *tx, pair, LockMode::Update).await? {
        let same =
            existing.record.inviter == *inviter && existing.claim_hash == claim_hash.as_bytes();
        return if same {
            Ok(existing.record)
        } else {
            Err(StoreError::Conflict)
        };
    }
    if count_open_pairs(&mut *tx, inviter).await? >= limits.max_pairs_per_device {
        return Err(StoreError::LimitExceeded);
    }
    let record = insert_pair(&mut *tx, pair, inviter, claim_hash, ttl).await?;
    tx.commit().await?;
    Ok(record)
}

async fn claim<A: Adapter>(
    adapter: &A,
    invitee: &DeviceId,
    claim: &Claim<'_>,
    ttl: Duration,
) -> StoreResult<PairRecord> {
    let (mut tx, locked) = begin_locked(adapter, claim.pair).await?;
    let record = &locked.record;
    let secret_ok = bool::from(
        locked
            .claim_hash
            .as_slice()
            .ct_eq(claim.claim_hash.as_bytes()),
    );
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
    let change = PairChange {
        state: PairState::Claimed,
        ttl: Some(ttl),
        claim: Some((invitee, claim.greeting)),
    };
    let record = transition(&mut *tx, record, change).await?;
    tx.commit().await?;
    Ok(record)
}

async fn approve<A: Adapter>(
    adapter: &A,
    inviter: &DeviceId,
    pair: PairId,
) -> StoreResult<PairRecord> {
    let (mut tx, locked) = begin_locked(adapter, pair).await?;
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
    let record = transition(&mut *tx, record, plain(PairState::Active)).await?;
    create_lanes(&mut *tx, pair, &record.members()).await?;
    tx.commit().await?;
    Ok(record)
}

async fn unpair<A: Adapter>(
    adapter: &A,
    member: &DeviceId,
    pair: PairId,
) -> StoreResult<PairRecord> {
    let (mut tx, locked) = begin_locked(adapter, pair).await?;
    if !locked.record.members().contains(member) {
        return Err(StoreError::Forbidden);
    }
    if locked.record.state == PairState::Revoked {
        return Ok(locked.record);
    }
    let record = transition(&mut *tx, &locked.record, plain(PairState::Revoked)).await?;
    delete_pair_data(&mut *tx, pair).await?;
    tx.commit().await?;
    Ok(record)
}

/// Commits the pair as expired and returns [`StoreError::Expired`], or the
/// error that stopped the commit.
async fn save_expiry<Db: Database<Connection: Exec>>(
    mut tx: Transaction<'static, Db>,
    record: &PairRecord,
) -> StoreError {
    if let Err(error) = transition(&mut *tx, record, plain(PairState::Expired)).await {
        return error;
    }
    match tx.commit().await {
        Ok(()) => StoreError::Expired,
        Err(error) => error.into(),
    }
}
