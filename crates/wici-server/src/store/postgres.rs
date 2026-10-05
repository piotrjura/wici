//! PostgreSQL adapter. Row locks order concurrent writers, so many server
//! processes can share one database.

use std::time::Duration;

use sqlx::postgres::{PgPool, PgPoolOptions, PgQueryResult};
use sqlx::{Pool, Postgres as Pg};

use super::StoreResult;
use super::adapter::{Adapter, Driver, LockMode};

/// PostgreSQL connection pool.
#[derive(Debug, Clone)]
pub(super) struct Postgres {
    pool: PgPool,
}

impl Postgres {
    /// Connects and runs migrations.
    pub(super) async fn connect(url: &str, max_connections: u32) -> StoreResult<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(5))
            .connect(url)
            .await?;
        sqlx::migrate!("./migrations/postgres")
            .run(&pool)
            .await
            .map_err(sqlx::Error::from)?;
        Ok(Self { pool })
    }
}

impl Adapter for Postgres {
    type Db = Pg;

    fn writer(&self) -> &Pool<Pg> {
        &self.pool
    }

    fn reader(&self) -> &Pool<Pg> {
        &self.pool
    }
}

impl Driver for Pg {
    const NOW: &'static str = "now()";
    const SKIP_LOCKED: &'static str = " FOR UPDATE SKIP LOCKED";

    fn millis(column: &str) -> String {
        format!("(extract(epoch FROM {column}) * 1000)::BIGINT")
    }

    fn now_plus(millis: &str) -> String {
        format!("now() + ({millis}) * interval '1 millisecond'")
    }

    fn lock(mode: LockMode) -> &'static str {
        match mode {
            LockMode::Update => " FOR UPDATE",
            LockMode::Share => " FOR SHARE",
        }
    }

    fn rows_affected(result: &PgQueryResult) -> u64 {
        result.rows_affected()
    }
}
