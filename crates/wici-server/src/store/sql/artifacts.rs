//! Artifact queries.

use std::time::Duration;

use wici_protocol::{ArtifactId, Blob, PairId};

use super::rows::get;
use super::{Exec, Param, id, millis};
use crate::store::StoreResult;
use crate::store::adapter::{Driver, LockMode};

/// Upload row.
#[derive(Debug)]
pub(in crate::store) struct Upload {
    pub(in crate::store) uploader: Vec<u8>,
    pub(in crate::store) total: i64,
    pub(in crate::store) hash: Vec<u8>,
    pub(in crate::store) received: i64,
    pub(in crate::store) complete: bool,
}

/// Selects the artifact. Binds `$1` and `$2`.
const ARTIFACT: &str = "pair_id = $1 AND id = $2";

const fn key(pair: PairId, artifact: ArtifactId) -> [Param<'static>; 2] {
    [id(pair.as_bytes()), id(artifact.as_bytes())]
}

/// Reads and locks an upload row.
pub(in crate::store) async fn lock_upload<C: Exec>(
    conn: &mut C,
    pair: PairId,
    artifact: ArtifactId,
) -> StoreResult<Option<Upload>> {
    let sql = format!(
        "SELECT uploader, total, hash, received, complete FROM artifacts WHERE {ARTIFACT}{}",
        C::Db::lock(LockMode::Update)
    );
    let row = conn.optional(&sql, &key(pair, artifact)).await?;
    row.map(|row| {
        Ok(Upload {
            uploader: get(&row, "uploader")?,
            total: get(&row, "total")?,
            hash: get(&row, "hash")?,
            received: get(&row, "received")?,
            complete: get(&row, "complete")?,
        })
    })
    .transpose()
}

/// Unfinished uploads of a pair.
pub(in crate::store) async fn count_incomplete<C: Exec>(
    conn: &mut C,
    pair: PairId,
) -> StoreResult<i64> {
    let sql = "SELECT count(*) AS n FROM artifacts WHERE pair_id = $1 AND NOT complete";
    get(&conn.one(sql, &[id(pair.as_bytes())]).await?, "n")
}

/// Inserts an empty upload.
pub(in crate::store) async fn insert_upload<C: Exec>(
    conn: &mut C,
    pair: PairId,
    artifact: ArtifactId,
    upload: &Upload,
) -> StoreResult<()> {
    let sql = "INSERT INTO artifacts (pair_id, id, uploader, total, hash) \
               VALUES ($1, $2, $3, $4, $5)";
    let [pair, artifact] = key(pair, artifact);
    let params = [
        pair,
        artifact,
        Param::Bytes(&upload.uploader),
        Param::Int(upload.total),
        Param::Bytes(&upload.hash),
    ];
    conn.run(sql, &params).await?;
    Ok(())
}

/// Inserts a chunk.
pub(in crate::store) async fn insert_chunk<C: Exec>(
    conn: &mut C,
    pair: PairId,
    artifact: ArtifactId,
    start: i64,
    data: &Blob,
) -> StoreResult<()> {
    let sql = "INSERT INTO artifact_chunks (pair_id, artifact_id, start, data) \
               VALUES ($1, $2, $3, $4)";
    let [pair, artifact] = key(pair, artifact);
    let params = [
        pair,
        artifact,
        Param::Int(start),
        Param::Bytes(data.as_bytes()),
    ];
    conn.run(sql, &params).await?;
    Ok(())
}

/// SHA-256 of all chunks in order.
pub(in crate::store) async fn artifact_hash<C: Exec>(
    conn: &mut C,
    pair: PairId,
    artifact: ArtifactId,
) -> StoreResult<Vec<u8>> {
    let sql = "SELECT data FROM artifact_chunks \
               WHERE pair_id = $1 AND artifact_id = $2 ORDER BY start";
    conn.hash_data(sql, &key(pair, artifact)).await
}

/// Saves upload progress.
pub(in crate::store) async fn save_progress<C: Exec>(
    conn: &mut C,
    pair: PairId,
    artifact: ArtifactId,
    upload: &Upload,
) -> StoreResult<()> {
    let sql = format!("UPDATE artifacts SET received = $3, complete = $4 WHERE {ARTIFACT}");
    let [pair, artifact] = key(pair, artifact);
    let params = [
        pair,
        artifact,
        Param::Int(upload.received),
        Param::Flag(upload.complete),
    ];
    conn.run(&sql, &params).await?;
    Ok(())
}

/// Total size and bytes of the chunk at `start` of a complete artifact.
pub(in crate::store) async fn complete_chunk<C: Exec>(
    conn: &mut C,
    pair: PairId,
    artifact: ArtifactId,
    start: i64,
) -> StoreResult<Option<(i64, Vec<u8>)>> {
    let sql = "SELECT a.total, c.data FROM artifacts a JOIN artifact_chunks c \
               ON c.pair_id = a.pair_id AND c.artifact_id = a.id \
               WHERE a.pair_id = $1 AND a.id = $2 AND a.complete AND c.start = $3";
    let [pair, artifact] = key(pair, artifact);
    let row = conn
        .optional(sql, &[pair, artifact, Param::Int(start)])
        .await?;
    row.map(|row| Ok((get(&row, "total")?, get(&row, "data")?)))
        .transpose()
}

/// Deletes an artifact and its chunks.
pub(in crate::store) async fn delete_artifact<C: Exec>(
    conn: &mut C,
    pair: PairId,
    artifact: ArtifactId,
) -> StoreResult<()> {
    let sql = format!("DELETE FROM artifacts WHERE {ARTIFACT}");
    conn.run(&sql, &key(pair, artifact)).await?;
    Ok(())
}

/// Deletes up to `limit` artifacts older than their retention.
pub(in crate::store) async fn expire_artifacts<C: Exec>(
    conn: &mut C,
    incomplete: Duration,
    complete: Duration,
    limit: i64,
) -> StoreResult<u64> {
    let sql = format!(
        "DELETE FROM artifacts WHERE (pair_id, id) IN (SELECT pair_id, id FROM artifacts \
         WHERE created_at < {} LIMIT $3)",
        C::Db::now_plus("-(CASE WHEN complete THEN $2 ELSE $1 END)")
    );
    let params = [
        Param::Int(millis(incomplete)?),
        Param::Int(millis(complete)?),
        Param::Int(limit),
    ];
    conn.run(&sql, &params).await
}
