//! Adapter contract. A database adapter provides connection pools
//! ([`Adapter`]) and its SQL dialect ([`Driver`]). Shared queries in
//! [`super::sql`] and shared rules use only these traits.

use sqlx::{Database, Pool};

use super::sql::Exec;

/// Connection pools of one database.
pub(super) trait Adapter {
    /// Database driver. Its connections run the shared queries.
    type Db: Driver<Connection: Exec>;

    /// Pool for writes and for transactions that write.
    fn writer(&self) -> &Pool<Self::Db>;

    /// Pool for reads.
    fn reader(&self) -> &Pool<Self::Db>;
}

/// Row lock strength.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LockMode {
    /// Blocks other writers and sharers.
    Update,
    /// Blocks writers only.
    Share,
}

/// SQL that differs between databases.
pub(super) trait Driver: Database {
    /// The current time, in the type of time columns.
    const NOW: &'static str;

    /// Clause that locks selected rows and skips rows another transaction
    /// holds.
    const SKIP_LOCKED: &'static str;

    /// A time column as milliseconds since the Unix epoch.
    fn millis(column: &str) -> String;

    /// The current time plus `millis` milliseconds. `millis` is SQL.
    fn now_plus(millis: &str) -> String;

    /// Clause that locks selected rows until the transaction ends.
    fn lock(mode: LockMode) -> &'static str;

    /// Rows a statement changed.
    fn rows_affected(result: &Self::QueryResult) -> u64;
}
