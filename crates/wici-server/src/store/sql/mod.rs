//! SQL shared by every adapter. Queries use `$n` parameters and portable
//! SQL; the [`Driver`] fills in what differs.

pub(super) mod artifacts;
pub(super) mod messages;
pub(super) mod pairs;
mod rows;

use std::time::Duration;

use futures_util::TryStreamExt;
use sha2::{Digest, Sha256};
use sqlx::query::Query;
use sqlx::{Database, Encode, Executor, IntoArguments, Type};
use uuid::Uuid;

use super::adapter::Driver;
use super::{StoreError, StoreResult};

pub(super) use rows::{position_from, position_value, timestamp};

use rows::{Field, Fields};

/// A query parameter.
#[derive(Debug, Clone, Copy)]
pub(super) enum Param<'a> {
    Id(Uuid),
    Bytes(&'a [u8]),
    MaybeBytes(Option<&'a [u8]>),
    Text(&'a str),
    Int(i64),
    MaybeInt(Option<i64>),
    Flag(bool),
}

/// A pair, message, or artifact ID parameter.
pub(super) const fn id(bytes: &[u8; 16]) -> Param<'static> {
    Param::Id(Uuid::from_bytes(*bytes))
}

/// Milliseconds of `duration`.
fn millis(duration: Duration) -> StoreResult<i64> {
    i64::try_from(duration.as_millis()).map_err(|_| StoreError::LimitExceeded)
}

/// A database that encodes every [`Param`].
pub(super) trait Binds: Database {
    fn bind<'q>(
        query: Query<'q, Self, Self::Arguments<'q>>,
        param: Param<'q>,
    ) -> Query<'q, Self, Self::Arguments<'q>>;
}

impl<Db> Binds for Db
where
    Db: Database,
    for<'q> Uuid: Encode<'q, Db> + Type<Db>,
    for<'q> &'q [u8]: Encode<'q, Db> + Type<Db>,
    for<'q> Option<&'q [u8]>: Encode<'q, Db> + Type<Db>,
    for<'q> &'q str: Encode<'q, Db> + Type<Db>,
    for<'q> i64: Encode<'q, Db> + Type<Db>,
    for<'q> Option<i64>: Encode<'q, Db> + Type<Db>,
    for<'q> bool: Encode<'q, Db> + Type<Db>,
{
    fn bind<'q>(
        query: Query<'q, Self, Self::Arguments<'q>>,
        param: Param<'q>,
    ) -> Query<'q, Self, Self::Arguments<'q>> {
        match param {
            Param::Id(value) => query.bind(value),
            Param::Bytes(value) => query.bind(value),
            Param::MaybeBytes(value) => query.bind(value),
            Param::Text(value) => query.bind(value),
            Param::Int(value) => query.bind(value),
            Param::MaybeInt(value) => query.bind(value),
            Param::Flag(value) => query.bind(value),
        }
    }
}

fn query<'q, Db: Binds>(sql: &'q str, params: &[Param<'q>]) -> Query<'q, Db, Db::Arguments<'q>> {
    params
        .iter()
        .fold(sqlx::query(sql), |query, param| Db::bind(query, *param))
}

/// A connection or transaction that runs shared queries.
pub(super) trait Exec: Send {
    /// Database driver.
    type Db: Driver;
    /// Result row.
    type Row: Fields;

    /// Runs a statement. Returns the changed row count.
    async fn run<'q>(&mut self, sql: &'q str, params: &[Param<'q>]) -> StoreResult<u64>;

    /// Exactly one row.
    async fn one<'q>(&mut self, sql: &'q str, params: &[Param<'q>]) -> StoreResult<Self::Row>;

    /// Zero or one row.
    async fn optional<'q>(
        &mut self,
        sql: &'q str,
        params: &[Param<'q>],
    ) -> StoreResult<Option<Self::Row>>;

    /// Every row.
    async fn all<'q>(&mut self, sql: &'q str, params: &[Param<'q>]) -> StoreResult<Vec<Self::Row>>;

    /// SHA-256 of the `data` column of every row, streamed in order.
    async fn hash_data<'q>(&mut self, sql: &'q str, params: &[Param<'q>]) -> StoreResult<Vec<u8>>;
}

impl<C> Exec for C
where
    C: sqlx::Connection,
    C::Database: Driver + Binds,
    for<'c> &'c mut C: Executor<'c, Database = C::Database>,
    for<'q> <C::Database as Database>::Arguments<'q>: IntoArguments<'q, C::Database>,
    <C::Database as Database>::Row: Fields,
{
    type Db = C::Database;
    type Row = <C::Database as Database>::Row;

    async fn run<'q>(&mut self, sql: &'q str, params: &[Param<'q>]) -> StoreResult<u64> {
        let result = query::<C::Database>(sql, params).execute(self).await?;
        Ok(C::Database::rows_affected(&result))
    }

    async fn one<'q>(&mut self, sql: &'q str, params: &[Param<'q>]) -> StoreResult<Self::Row> {
        Ok(query(sql, params).fetch_one(self).await?)
    }

    async fn optional<'q>(
        &mut self,
        sql: &'q str,
        params: &[Param<'q>],
    ) -> StoreResult<Option<Self::Row>> {
        Ok(query(sql, params).fetch_optional(self).await?)
    }

    async fn all<'q>(&mut self, sql: &'q str, params: &[Param<'q>]) -> StoreResult<Vec<Self::Row>> {
        Ok(query(sql, params).fetch_all(self).await?)
    }

    async fn hash_data<'q>(&mut self, sql: &'q str, params: &[Param<'q>]) -> StoreResult<Vec<u8>> {
        let mut rows = query::<C::Database>(sql, params).fetch(self);
        let mut hasher = Sha256::new();
        while let Some(row) = rows.try_next().await? {
            hasher.update(Field::<Vec<u8>>::field(&row, "data")?);
        }
        Ok(hasher.finalize().to_vec())
    }
}
