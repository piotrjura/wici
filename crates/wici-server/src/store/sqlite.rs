//! SQLite adapter: one database file, one server process.
//!
//! One writer connection runs every write and transaction, so writes never
//! race and rows need no locks. Readers use separate read-only connections.
//! WAL with full sync: a commit survives power loss.

use std::str::FromStr;
use std::time::Duration;

use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteQueryResult,
    SqliteSynchronous,
};
use sqlx::{Pool, Sqlite as Lite};

use super::StoreResult;
use super::adapter::{Adapter, Driver, LockMode};

/// Wait for a lock that another process holds before failing.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// SQLite writer and reader pools.
#[derive(Debug, Clone)]
pub(super) struct Sqlite {
    writer: SqlitePool,
    reader: SqlitePool,
}

impl Sqlite {
    /// Opens or creates the file, runs migrations, and opens up to
    /// `max_readers` read-only connections.
    pub(super) async fn connect(url: &str, max_readers: u32) -> StoreResult<Self> {
        let options = SqliteConnectOptions::from_str(url)?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .foreign_keys(true)
            .busy_timeout(BUSY_TIMEOUT);
        let writer = pool(1).connect_with(options.clone()).await?;
        sqlx::migrate!("./migrations/sqlite")
            .run(&writer)
            .await
            .map_err(sqlx::Error::from)?;
        let reader = pool(max_readers.max(1))
            .connect_with(options.read_only(true))
            .await?;
        Ok(Self { writer, reader })
    }
}

fn pool(max_connections: u32) -> SqlitePoolOptions {
    SqlitePoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_secs(5))
}

impl Adapter for Sqlite {
    type Db = Lite;

    fn writer(&self) -> &Pool<Lite> {
        &self.writer
    }

    fn reader(&self) -> &Pool<Lite> {
        &self.reader
    }
}

/// Times are milliseconds since the Unix epoch, stored as integers.
impl Driver for Lite {
    const NOW: &'static str = "CAST(unixepoch('subsec') * 1000 AS INTEGER)";
    const SKIP_LOCKED: &'static str = "";

    fn millis(column: &str) -> String {
        column.to_owned()
    }

    fn now_plus(millis: &str) -> String {
        format!("{} + ({millis})", Self::NOW)
    }

    /// The single writer connection already orders transactions.
    fn lock(_mode: LockMode) -> &'static str {
        ""
    }

    fn rows_affected(result: &SqliteQueryResult) -> u64 {
        result.rows_affected()
    }
}
