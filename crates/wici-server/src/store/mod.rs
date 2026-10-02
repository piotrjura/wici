//! PostgreSQL storage. Every state change commits before the server replies.

mod artifacts;
mod messages;
mod pairs;

use std::error::Error;
use std::fmt;
use std::time::Duration;

use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::{PgConnection, Row};
use uuid::Uuid;
use wici_protocol::{
    Blob, DeviceId, ErrorCode, Lane, MessageId, PairId, PairInfo, PairState, Position, Timestamp,
    WireEnum,
};

pub use artifacts::{ArtifactLimits, ChunkUpload, Progress, StoredChunk};
pub use messages::{Accepted, Delivery, LaneCursor, LaneKey, NewMessage};
pub use pairs::Claim;

/// A storage request failed.
#[derive(Debug)]
pub enum StoreError {
    /// Unknown pair or message.
    NotFound,
    /// Device or pair state does not allow the request.
    Forbidden,
    /// Known ID with different content.
    Conflict,
    /// Invitation or claim expired.
    Expired,
    /// A count limit was hit.
    LimitExceeded,
    /// Database failure.
    Database(sqlx::Error),
}

impl StoreError {
    /// Wire error code.
    #[must_use]
    pub const fn code(&self) -> ErrorCode {
        match self {
            Self::NotFound => ErrorCode::NotFound,
            Self::Forbidden => ErrorCode::Forbidden,
            Self::Conflict => ErrorCode::Conflict,
            Self::Expired => ErrorCode::Expired,
            Self::LimitExceeded => ErrorCode::LimitExceeded,
            Self::Database(_) => ErrorCode::Internal,
        }
    }

    fn corrupt(what: &'static str) -> Self {
        Self::Database(sqlx::Error::Decode(what.into()))
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NotFound => "not found",
            Self::Forbidden => "not allowed",
            Self::Conflict => "ID reused with different content",
            Self::Expired => "expired",
            Self::LimitExceeded => "limit exceeded",
            Self::Database(_) => "database error",
        })
    }
}

impl Error for StoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            Self::NotFound
            | Self::Forbidden
            | Self::Conflict
            | Self::Expired
            | Self::LimitExceeded => None,
        }
    }
}

impl From<sqlx::Error> for StoreError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

/// Store result.
pub type StoreResult<T> = Result<T, StoreError>;

/// Stored pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PairRecord {
    /// Pair ID.
    pub id: PairId,
    /// State.
    pub state: PairState,
    /// Inviter.
    pub inviter: DeviceId,
    /// Invitee, once claimed.
    pub invitee: Option<DeviceId>,
    /// Sealed invitee keys for the inviter.
    pub greeting: Option<Blob>,
    /// Deadline while invited or claimed.
    pub expires_at: Option<Timestamp>,
}

impl PairRecord {
    /// The other member, if any.
    #[must_use]
    pub fn peer_of(&self, device: &DeviceId) -> Option<DeviceId> {
        if *device == self.inviter {
            self.invitee
        } else if Some(*device) == self.invitee {
            Some(self.inviter)
        } else {
            None
        }
    }

    /// Members of the pair.
    #[must_use]
    pub fn members(&self) -> Vec<DeviceId> {
        std::iter::once(self.inviter).chain(self.invitee).collect()
    }

    /// The pair as `viewer` sees it. Only the inviter gets the greeting.
    #[must_use]
    pub fn view_for(&self, viewer: &DeviceId) -> PairInfo {
        PairInfo {
            id: self.id,
            state: self.state,
            inviter: self.inviter,
            invitee: self.invitee,
            greeting: (*viewer == self.inviter)
                .then(|| self.greeting.clone())
                .flatten(),
            expires_at: self.expires_at,
        }
    }
}

/// Limits enforced by the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreLimits {
    /// Open (invited, claimed, or active) pairs per device.
    pub max_pairs_per_device: i64,
    /// Unacknowledged messages per recipient lane.
    pub max_pending_per_lane: i64,
}

/// PostgreSQL store.
#[derive(Debug, Clone)]
pub struct Store {
    pool: PgPool,
    limits: StoreLimits,
}

impl Store {
    /// Connects and runs migrations.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`] if the database is unreachable or a
    /// migration fails.
    pub async fn connect(
        url: &str,
        max_connections: u32,
        limits: StoreLimits,
    ) -> StoreResult<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(5))
            .connect(url)
            .await?;
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .map_err(|e| StoreError::Database(e.into()))?;
        Ok(Self { pool, limits })
    }

    /// Records that a device connected.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`] on failure.
    pub async fn touch_device(&self, id: &DeviceId) -> StoreResult<()> {
        sqlx::query(
            "INSERT INTO devices (id) VALUES ($1) \
             ON CONFLICT (id) DO UPDATE SET last_seen_at = now()",
        )
        .bind(id.as_bytes().as_slice())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Last time a device was connected.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`] on failure.
    pub async fn last_seen(&self, id: &DeviceId) -> StoreResult<Option<Timestamp>> {
        let millis: Option<i64> = sqlx::query_scalar(&format!(
            "SELECT {} FROM devices WHERE id = $1",
            millis_of("last_seen_at")
        ))
        .bind(id.as_bytes().as_slice())
        .fetch_optional(&self.pool)
        .await?;
        Ok(timestamp(millis))
    }
}

// Row and type conversion shared by the submodules.

fn millis_of(column: &str) -> String {
    format!("(extract(epoch FROM {column}) * 1000)::BIGINT")
}

const fn pg_uuid(bytes: &[u8; 16]) -> Uuid {
    Uuid::from_bytes(*bytes)
}

fn device_from(bytes: &[u8]) -> StoreResult<DeviceId> {
    <[u8; 32]>::try_from(bytes)
        .map(DeviceId::from_bytes)
        .map_err(|_| StoreError::corrupt("device ID length"))
}

fn timestamp(millis: Option<i64>) -> Option<Timestamp> {
    millis.and_then(|ms| u64::try_from(ms).ok()).map(Timestamp)
}

fn position_from(value: i64) -> StoreResult<Position> {
    u64::try_from(value)
        .map(Position)
        .map_err(|_| StoreError::corrupt("negative position"))
}

fn position_value(position: Position) -> StoreResult<i64> {
    i64::try_from(position.0).map_err(|_| StoreError::Forbidden)
}

fn wire_from<T: WireEnum>(text: &str) -> StoreResult<T> {
    wici_protocol::wire::parse(text).map_err(|_| StoreError::corrupt("unknown wire name"))
}

fn pair_id_from(row: &PgRow, column: &str) -> StoreResult<PairId> {
    let id: Uuid = row.try_get(column)?;
    Ok(PairId::from_bytes(*id.as_bytes()))
}

fn message_id_from(row: &PgRow, column: &str) -> StoreResult<MessageId> {
    let id: Uuid = row.try_get(column)?;
    Ok(MessageId::from_bytes(*id.as_bytes()))
}

fn lane_from(row: &PgRow, column: &str) -> StoreResult<Lane> {
    wire_from(&row.try_get::<String, _>(column)?)
}

/// Locks the device row. Serializes per-device limit checks.
async fn lock_device(conn: &mut PgConnection, id: &DeviceId) -> StoreResult<()> {
    sqlx::query("SELECT 1 FROM devices WHERE id = $1 FOR UPDATE")
        .bind(id.as_bytes().as_slice())
        .execute(conn)
        .await?;
    Ok(())
}
