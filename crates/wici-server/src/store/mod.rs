//! Durable storage. Every state change commits before the server replies.
//!
//! Layers, top down: shared rules (pairs, messages, artifacts), shared SQL,
//! and one adapter per database with its pools and SQL dialect:
//! [`Backend::Postgres`] for shared servers, [`Backend::Sqlite`] for one file
//! on one machine.

mod adapter;
mod artifacts;
mod messages;
mod pairs;
mod postgres;
mod sql;
mod sqlite;

use std::error::Error;
use std::fmt;

use wici_protocol::{Blob, DeviceId, ErrorCode, PairId, PairInfo, PairState, Timestamp};

use self::adapter::Adapter;

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

/// Database behind a store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Backend {
    /// PostgreSQL. URL `postgres://...` or `postgresql://...`.
    Postgres,
    /// SQLite file. URL `sqlite:path` or `sqlite://path`. One server
    /// process per file.
    Sqlite,
}

impl Backend {
    /// The backend a database URL names, or `None` for other schemes.
    #[must_use]
    pub fn of_url(url: &str) -> Option<Self> {
        let scheme = url.split_once(':')?.0;
        match scheme {
            "postgres" | "postgresql" => Some(Self::Postgres),
            "sqlite" => Some(Self::Sqlite),
            _ => None,
        }
    }
}

/// One connected adapter.
#[derive(Debug, Clone)]
enum Adapters {
    Postgres(postgres::Postgres),
    Sqlite(sqlite::Sqlite),
}

/// Runs `$body` with `$adapter` bound to the concrete adapter.
macro_rules! dispatch {
    ($store:expr, |$adapter:ident| $body:expr) => {
        match &$store.adapter {
            Adapters::Postgres($adapter) => $body,
            Adapters::Sqlite($adapter) => $body,
        }
    };
}
use dispatch;

/// Durable store on one database.
#[derive(Debug, Clone)]
pub struct Store {
    adapter: Adapters,
    limits: StoreLimits,
}

impl Store {
    /// Connects to the database [`Backend::of_url`] picks and runs its
    /// migrations. A SQLite file is created if missing.
    ///
    /// `max_connections` caps PostgreSQL connections. SQLite uses one writer
    /// and up to `max_connections` readers.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`] for an unsupported URL, an unreachable
    /// database, or a failed migration. The error never contains the URL.
    pub async fn connect(
        url: &str,
        max_connections: u32,
        limits: StoreLimits,
    ) -> StoreResult<Self> {
        let adapter = match Backend::of_url(url) {
            Some(Backend::Postgres) => {
                Adapters::Postgres(postgres::Postgres::connect(url, max_connections).await?)
            }
            Some(Backend::Sqlite) => {
                Adapters::Sqlite(sqlite::Sqlite::connect(url, max_connections).await?)
            }
            None => {
                let error = "database URL must start with postgres:, postgresql:, or sqlite:";
                return Err(StoreError::Database(sqlx::Error::Configuration(
                    error.into(),
                )));
            }
        };
        Ok(Self { adapter, limits })
    }

    /// The database this store uses.
    #[must_use]
    pub const fn backend(&self) -> Backend {
        match self.adapter {
            Adapters::Postgres(_) => Backend::Postgres,
            Adapters::Sqlite(_) => Backend::Sqlite,
        }
    }

    /// Records that a device connected.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`] on failure.
    pub async fn touch_device(&self, id: &DeviceId) -> StoreResult<()> {
        dispatch!(self, |a| {
            sql::pairs::touch_device(&mut *a.writer().acquire().await?, id).await
        })
    }

    /// Last time a device was connected.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`] on failure.
    pub async fn last_seen(&self, id: &DeviceId) -> StoreResult<Option<Timestamp>> {
        let millis = dispatch!(self, |a| {
            sql::pairs::last_seen(&mut *a.reader().acquire().await?, id).await
        })?;
        Ok(sql::timestamp(millis))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_follows_the_url_scheme() {
        let cases = [
            ("postgres://u@h/db", Some(Backend::Postgres)),
            ("postgresql://u@h/db", Some(Backend::Postgres)),
            ("sqlite:wici.db", Some(Backend::Sqlite)),
            ("sqlite:///var/lib/wici.db", Some(Backend::Sqlite)),
            ("mysql://u@h/db", None),
            ("wici.db", None),
            ("", None),
        ];
        for (url, expected) in cases {
            assert_eq!(Backend::of_url(url), expected, "{url}");
        }
    }

    #[tokio::test]
    async fn unsupported_url_fails_without_echoing_it() {
        let limits = StoreLimits {
            max_pairs_per_device: 1,
            max_pending_per_lane: 1,
        };
        let error = Store::connect("mysql://user:secret@host/db", 1, limits)
            .await
            .unwrap_err();
        assert!(matches!(error, StoreError::Database(_)));
        let detail = error.source().map(ToString::to_string).unwrap_or_default();
        assert!(detail.contains("must start with"), "{detail}");
        assert!(!detail.contains("secret"), "{detail}");
    }
}
