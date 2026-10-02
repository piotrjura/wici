//! Local SQLite storage. WAL with full sync: a commit survives power loss.

use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteRow,
    SqliteSynchronous,
};
use sqlx::{Row, SqliteConnection};
use wici_protocol::{
    CommandState, DeviceId, Lane, MessageId, PairId, PairState, Position, Timestamp, WireEnum,
};

use crate::error::{ClientError, ClientResult};
use crate::model::{Direction, PendingOp, Role};

/// Milliseconds since the Unix epoch.
pub(crate) fn now_ms() -> i64 {
    let since = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
}

fn wire<T: WireEnum>(text: &str) -> ClientResult<T> {
    wici_protocol::wire::parse(text).map_err(|_| ClientError::Corrupt("wire name"))
}

fn bytes<const N: usize>(value: &[u8]) -> ClientResult<[u8; N]> {
    <[u8; N]>::try_from(value).map_err(|_| ClientError::Corrupt("byte length"))
}

fn pair_id(row: &SqliteRow, column: &str) -> ClientResult<PairId> {
    Ok(PairId::from_bytes(bytes(
        &row.try_get::<Vec<u8>, _>(column)?,
    )?))
}

fn message_id(row: &SqliteRow, column: &str) -> ClientResult<MessageId> {
    Ok(MessageId::from_bytes(bytes(
        &row.try_get::<Vec<u8>, _>(column)?,
    )?))
}

fn position(value: i64) -> ClientResult<Position> {
    u64::try_from(value)
        .map(Position)
        .map_err(|_| ClientError::Corrupt("position"))
}

fn position_value(value: Position) -> ClientResult<i64> {
    i64::try_from(value.0).map_err(|_| ClientError::Corrupt("position"))
}

/// Stored pair.
#[derive(Debug, Clone)]
pub(crate) struct PairRow {
    pub(crate) id: PairId,
    pub(crate) role: Role,
    pub(crate) state: PairState,
    pub(crate) pending: Option<PendingOp>,
    pub(crate) peer: Option<DeviceId>,
    pub(crate) invitation: Vec<u8>,
    pub(crate) keys: Option<Vec<u8>>,
}

fn pair_row(row: &SqliteRow) -> ClientResult<PairRow> {
    let pending: Option<String> = row.try_get("pending")?;
    let peer: Option<Vec<u8>> = row.try_get("peer")?;
    Ok(PairRow {
        id: pair_id(row, "id")?,
        role: wire(&row.try_get::<String, _>("role")?)?,
        state: wire(&row.try_get::<String, _>("state")?)?,
        pending: pending.as_deref().map(wire).transpose()?,
        peer: peer
            .as_deref()
            .map(bytes)
            .transpose()?
            .map(DeviceId::from_bytes),
        invitation: row.try_get("invitation")?,
        keys: row.try_get("keys")?,
    })
}

/// Durable message waiting for server acceptance.
#[derive(Debug, Clone)]
pub(crate) struct OutboxRow {
    pub(crate) pair: PairId,
    pub(crate) id: MessageId,
    pub(crate) lane: Lane,
    pub(crate) sealed: Vec<u8>,
}

/// Stored received message.
#[derive(Debug, Clone)]
pub(crate) struct InboxRow {
    pub(crate) pair: PairId,
    pub(crate) lane: Lane,
    pub(crate) position: Position,
    pub(crate) id: MessageId,
    pub(crate) accepted_at: Timestamp,
    pub(crate) body: Vec<u8>,
}

/// Local database.
#[derive(Debug, Clone)]
pub(crate) struct Db {
    pool: SqlitePool,
}

impl Db {
    pub(crate) async fn open(path: &Path) -> ClientResult<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(Duration::from_secs(5));
        // One connection: SQLite writes are serial anyway, and callers never
        // hold a transaction while waiting on another.
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        sqlx::migrate!("./migrations").run(&pool).await?;
        Ok(Self { pool })
    }

    pub(crate) async fn begin(&self) -> ClientResult<sqlx::Transaction<'static, sqlx::Sqlite>> {
        Ok(self.pool.begin().await?)
    }

    pub(crate) async fn conn(&self) -> ClientResult<sqlx::pool::PoolConnection<sqlx::Sqlite>> {
        Ok(self.pool.acquire().await?)
    }
}

// Pairs.

pub(crate) async fn insert_pair(conn: &mut SqliteConnection, row: &PairRow) -> ClientResult<()> {
    sqlx::query(
        "INSERT INTO pairs (id, role, state, pending, peer, invitation, keys, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(row.id.as_bytes().as_slice())
    .bind(row.role.as_str())
    .bind(row.state.as_str())
    .bind(row.pending.map(PendingOp::as_str))
    .bind(row.peer.map(|p| p.as_bytes().to_vec()))
    .bind(&row.invitation)
    .bind(&row.keys)
    .bind(now_ms())
    .execute(conn)
    .await?;
    Ok(())
}

pub(crate) async fn find_pair(
    conn: &mut SqliteConnection,
    pair: PairId,
) -> ClientResult<Option<PairRow>> {
    let row = sqlx::query("SELECT * FROM pairs WHERE id = $1")
        .bind(pair.as_bytes().as_slice())
        .fetch_optional(conn)
        .await?;
    row.as_ref().map(pair_row).transpose()
}

pub(crate) async fn get_pair(conn: &mut SqliteConnection, pair: PairId) -> ClientResult<PairRow> {
    find_pair(conn, pair).await?.ok_or(ClientError::UnknownPair)
}

pub(crate) async fn all_pairs(conn: &mut SqliteConnection) -> ClientResult<Vec<PairRow>> {
    let rows = sqlx::query("SELECT * FROM pairs ORDER BY updated_at DESC")
        .fetch_all(conn)
        .await?;
    rows.iter().map(pair_row).collect()
}

/// Saves the mutable pair fields from `row`.
pub(crate) async fn update_pair(conn: &mut SqliteConnection, row: &PairRow) -> ClientResult<()> {
    sqlx::query(
        "UPDATE pairs SET state = $2, pending = $3, peer = $4, keys = $5, updated_at = $6 WHERE id = $1",
    )
    .bind(row.id.as_bytes().as_slice())
    .bind(row.state.as_str())
    .bind(row.pending.map(PendingOp::as_str))
    .bind(row.peer.map(|p| p.as_bytes().to_vec()))
    .bind(&row.keys)
    .bind(now_ms())
    .execute(conn)
    .await?;
    Ok(())
}

/// Drops queued traffic of a finished pair. Received messages stay.
pub(crate) async fn drop_pair_traffic(
    conn: &mut SqliteConnection,
    pair: PairId,
) -> ClientResult<()> {
    for table in ["outbox", "cursors"] {
        sqlx::query(&format!("DELETE FROM {table} WHERE pair = $1"))
            .bind(pair.as_bytes().as_slice())
            .execute(&mut *conn)
            .await?;
    }
    Ok(())
}

// Outbox.

pub(crate) async fn enqueue(conn: &mut SqliteConnection, row: &OutboxRow) -> ClientResult<()> {
    sqlx::query("INSERT INTO outbox (pair, id, lane, sealed) VALUES ($1, $2, $3, $4)")
        .bind(row.pair.as_bytes().as_slice())
        .bind(row.id.as_bytes().as_slice())
        .bind(row.lane.as_str())
        .bind(&row.sealed)
        .execute(conn)
        .await?;
    Ok(())
}

/// Messages due for (re)sending, oldest first.
pub(crate) async fn due_outbox(
    conn: &mut SqliteConnection,
    limit: i64,
) -> ClientResult<Vec<OutboxRow>> {
    let rows = sqlx::query(
        "SELECT pair, id, lane, sealed FROM outbox WHERE retry_at <= $1 ORDER BY seq LIMIT $2",
    )
    .bind(now_ms())
    .bind(limit)
    .fetch_all(conn)
    .await?;
    rows.iter()
        .map(|row| {
            Ok(OutboxRow {
                pair: pair_id(row, "pair")?,
                id: message_id(row, "id")?,
                lane: wire(&row.try_get::<String, _>("lane")?)?,
                sealed: row.try_get("sealed")?,
            })
        })
        .collect()
}

pub(crate) async fn remove_outbox(
    conn: &mut SqliteConnection,
    pair: PairId,
    id: MessageId,
) -> ClientResult<bool> {
    let result = sqlx::query("DELETE FROM outbox WHERE pair = $1 AND id = $2")
        .bind(pair.as_bytes().as_slice())
        .bind(id.as_bytes().as_slice())
        .execute(conn)
        .await?;
    Ok(result.rows_affected() > 0)
}

pub(crate) async fn delay_outbox(
    conn: &mut SqliteConnection,
    pair: PairId,
    id: MessageId,
    delay: Duration,
) -> ClientResult<()> {
    let delay = i64::try_from(delay.as_millis()).unwrap_or(i64::MAX);
    sqlx::query("UPDATE outbox SET retry_at = $3 WHERE pair = $1 AND id = $2")
        .bind(pair.as_bytes().as_slice())
        .bind(id.as_bytes().as_slice())
        .bind(now_ms().saturating_add(delay))
        .execute(conn)
        .await?;
    Ok(())
}

// Inbox.

pub(crate) async fn cursor(
    conn: &mut SqliteConnection,
    pair: PairId,
    lane: Lane,
) -> ClientResult<Position> {
    let value: Option<i64> =
        sqlx::query_scalar("SELECT position FROM cursors WHERE pair = $1 AND lane = $2")
            .bind(pair.as_bytes().as_slice())
            .bind(lane.as_str())
            .fetch_optional(conn)
            .await?;
    position(value.unwrap_or(0))
}

async fn advance_cursor(
    conn: &mut SqliteConnection,
    pair: PairId,
    lane: Lane,
    to: Position,
) -> ClientResult<()> {
    sqlx::query(
        "INSERT INTO cursors (pair, lane, position) VALUES ($1, $2, $3) \
         ON CONFLICT (pair, lane) DO UPDATE SET position = excluded.position",
    )
    .bind(pair.as_bytes().as_slice())
    .bind(lane.as_str())
    .bind(position_value(to)?)
    .execute(conn)
    .await?;
    Ok(())
}

type Query<'q> = sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>;

/// Binds `$1..$5` to the message's pair, lane, position, ID, and bytes.
fn bind_received<'q>(query: Query<'q>, row: &'q InboxRow) -> ClientResult<Query<'q>> {
    Ok(query
        .bind(row.pair.as_bytes().as_slice())
        .bind(row.lane.as_str())
        .bind(position_value(row.position)?)
        .bind(row.id.as_bytes().as_slice())
        .bind(&row.body))
}

/// Saves a received message and advances the lane cursor.
pub(crate) async fn save_inbox(
    conn: &mut SqliteConnection,
    row: &InboxRow,
    handled: bool,
) -> ClientResult<()> {
    let sql = "INSERT INTO inbox (pair, lane, position, id, body, accepted_at, handled) \
               VALUES ($1, $2, $3, $4, $5, $6, $7)";
    bind_received(sqlx::query(sql), row)?
        .bind(i64::try_from(row.accepted_at.0).unwrap_or(i64::MAX))
        .bind(handled)
        .execute(&mut *conn)
        .await?;
    advance_cursor(conn, row.pair, row.lane, row.position).await
}

/// Keeps an unreadable message as evidence and advances the lane cursor.
pub(crate) async fn quarantine(
    conn: &mut SqliteConnection,
    row: &InboxRow,
    reason: &str,
) -> ClientResult<()> {
    let sql = "INSERT INTO quarantine (pair, lane, position, id, sealed, reason) \
               VALUES ($1, $2, $3, $4, $5, $6)";
    bind_received(sqlx::query(sql), row)?
        .bind(reason)
        .execute(&mut *conn)
        .await?;
    advance_cursor(conn, row.pair, row.lane, row.position).await
}

pub(crate) async fn unhandled(conn: &mut SqliteConnection) -> ClientResult<Vec<InboxRow>> {
    let rows = sqlx::query(
        "SELECT pair, lane, position, id, body, accepted_at FROM inbox WHERE handled = 0 \
         ORDER BY accepted_at, position",
    )
    .fetch_all(conn)
    .await?;
    rows.iter()
        .map(|row| {
            Ok(InboxRow {
                pair: pair_id(row, "pair")?,
                lane: wire(&row.try_get::<String, _>("lane")?)?,
                position: position(row.try_get("position")?)?,
                id: message_id(row, "id")?,
                accepted_at: Timestamp(
                    u64::try_from(row.try_get::<i64, _>("accepted_at")?).unwrap_or(0),
                ),
                body: row.try_get("body")?,
            })
        })
        .collect()
}

pub(crate) async fn mark_handled(
    conn: &mut SqliteConnection,
    pair: PairId,
    lane: Lane,
    at: Position,
) -> ClientResult<bool> {
    let result =
        sqlx::query("UPDATE inbox SET handled = 1 WHERE pair = $1 AND lane = $2 AND position = $3")
            .bind(pair.as_bytes().as_slice())
            .bind(lane.as_str())
            .bind(position_value(at)?)
            .execute(conn)
            .await?;
    Ok(result.rows_affected() > 0)
}

// Commands.

pub(crate) async fn command_state(
    conn: &mut SqliteConnection,
    pair: PairId,
    id: MessageId,
    direction: Direction,
) -> ClientResult<Option<CommandState>> {
    let state: Option<String> = sqlx::query_scalar(
        "SELECT state FROM commands WHERE pair = $1 AND id = $2 AND direction = $3",
    )
    .bind(pair.as_bytes().as_slice())
    .bind(id.as_bytes().as_slice())
    .bind(direction.as_str())
    .fetch_optional(conn)
    .await?;
    state.as_deref().map(wire).transpose()
}

pub(crate) async fn set_command(
    conn: &mut SqliteConnection,
    pair: PairId,
    id: MessageId,
    direction: Direction,
    state: CommandState,
) -> ClientResult<()> {
    sqlx::query(
        "INSERT INTO commands (pair, id, direction, state, updated_at) VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (pair, id, direction) DO UPDATE SET state = excluded.state, updated_at = excluded.updated_at",
    )
    .bind(pair.as_bytes().as_slice())
    .bind(id.as_bytes().as_slice())
    .bind(direction.as_str())
    .bind(state.as_str())
    .bind(now_ms())
    .execute(conn)
    .await?;
    Ok(())
}

/// Commands in one of `states`.
pub(crate) async fn commands_in(
    conn: &mut SqliteConnection,
    direction: Direction,
    states: &[CommandState],
) -> ClientResult<Vec<(PairId, MessageId)>> {
    let names: Vec<&str> = states.iter().map(|s| s.as_str()).collect();
    let rows = sqlx::query(
        "SELECT pair, id FROM commands WHERE direction = $1 AND state IN (SELECT value FROM json_each($2))",
    )
    .bind(direction.as_str())
    .bind(serde_json::to_string(&names).map_err(|_| ClientError::Corrupt("state list"))?)
    .fetch_all(conn)
    .await?;
    rows.iter()
        .map(|row| Ok((pair_id(row, "pair")?, message_id(row, "id")?)))
        .collect()
}
