//! Sealed artifacts: ordered chunk upload, hash check, download, cleanup.

use std::time::Duration;

use futures_util::TryStreamExt;
use sha2::{Digest, Sha256};
use sqlx::{PgConnection, Row};
use wici_protocol::{ArtifactId, Blob, DeviceId, FixedBytes, PairId, PairState};

use super::pairs::{LockMode, lock};
use super::{Store, StoreError, StoreResult, pg_uuid};

/// Artifact limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArtifactLimits {
    /// Largest sealed artifact.
    pub max_bytes: u64,
    /// Unfinished uploads per pair.
    pub max_incomplete: i64,
}

/// One uploaded chunk.
#[derive(Debug, Clone, Copy)]
pub struct ChunkUpload<'a> {
    /// Pair ID.
    pub pair: PairId,
    /// Artifact ID.
    pub artifact: ArtifactId,
    /// Total sealed size.
    pub total: u64,
    /// SHA-256 of all sealed bytes.
    pub hash: &'a FixedBytes<32>,
    /// Chunk start.
    pub offset: u64,
    /// Chunk bytes.
    pub data: &'a Blob,
}

/// Upload progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Progress {
    /// Contiguous bytes stored.
    pub received: u64,
    /// All bytes stored and the hash matched.
    pub complete: bool,
}

/// A stored chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredChunk {
    /// Chunk start.
    pub offset: u64,
    /// Total sealed size.
    pub total: u64,
    /// Chunk bytes.
    pub data: Blob,
}

fn int(value: u64) -> StoreResult<i64> {
    i64::try_from(value).map_err(|_| StoreError::LimitExceeded)
}

fn uint(value: i64) -> StoreResult<u64> {
    u64::try_from(value).map_err(|_| StoreError::corrupt("negative size"))
}

/// Fails unless `device` belongs to the active pair. Holds a shared lock,
/// so the pair cannot be revoked during the request.
async fn check_member(conn: &mut PgConnection, device: &DeviceId, pair: PairId) -> StoreResult<()> {
    let record = lock(conn, pair, LockMode::Share).await?.record;
    if record.state == PairState::Active && record.peer_of(device).is_some() {
        Ok(())
    } else {
        Err(StoreError::Forbidden)
    }
}

/// Upload row, locked.
struct Upload {
    uploader: Vec<u8>,
    total: i64,
    hash: Vec<u8>,
    received: i64,
    complete: bool,
}

async fn lock_upload(
    conn: &mut PgConnection,
    pair: PairId,
    artifact: ArtifactId,
) -> StoreResult<Option<Upload>> {
    let row = sqlx::query(
        "SELECT uploader, total, hash, received, complete FROM artifacts \
         WHERE pair_id = $1 AND id = $2 FOR UPDATE",
    )
    .bind(pg_uuid(pair.as_bytes()))
    .bind(pg_uuid(artifact.as_bytes()))
    .fetch_optional(conn)
    .await?;
    row.map(|row| {
        Ok(Upload {
            uploader: row.try_get("uploader")?,
            total: row.try_get("total")?,
            hash: row.try_get("hash")?,
            received: row.try_get("received")?,
            complete: row.try_get("complete")?,
        })
    })
    .transpose()
}

impl Store {
    /// Starts an upload or checks that a repeat matches the first request.
    async fn open_upload(
        &self,
        conn: &mut PgConnection,
        uploader: &DeviceId,
        chunk: &ChunkUpload<'_>,
        limits: ArtifactLimits,
    ) -> StoreResult<Upload> {
        if let Some(upload) = lock_upload(conn, chunk.pair, chunk.artifact).await? {
            let same = upload.uploader == uploader.as_bytes()
                && upload.total == int(chunk.total)?
                && upload.hash == chunk.hash.as_bytes();
            return if same {
                Ok(upload)
            } else {
                Err(StoreError::Conflict)
            };
        }
        if chunk.total == 0 || chunk.total > limits.max_bytes {
            return Err(StoreError::LimitExceeded);
        }
        let open: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifacts WHERE pair_id = $1 AND NOT complete",
        )
        .bind(pg_uuid(chunk.pair.as_bytes()))
        .fetch_one(&mut *conn)
        .await?;
        if open >= limits.max_incomplete {
            return Err(StoreError::LimitExceeded);
        }
        sqlx::query("INSERT INTO artifacts (pair_id, id, uploader, total, hash) VALUES ($1, $2, $3, $4, $5)")
            .bind(pg_uuid(chunk.pair.as_bytes()))
            .bind(pg_uuid(chunk.artifact.as_bytes()))
            .bind(uploader.as_bytes().as_slice())
            .bind(int(chunk.total)?)
            .bind(chunk.hash.as_bytes().as_slice())
            .execute(&mut *conn)
            .await?;
        Ok(Upload {
            uploader: uploader.as_bytes().to_vec(),
            total: int(chunk.total)?,
            hash: chunk.hash.as_bytes().to_vec(),
            received: 0,
            complete: false,
        })
    }

    /// Stores the next chunk. A chunk before the stored end is a no-op; a
    /// chunk after it is rejected. The last chunk triggers the hash check,
    /// and a mismatch deletes the upload.
    ///
    /// # Errors
    ///
    /// [`StoreError::Forbidden`] for non-members, inactive pairs, or gaps;
    /// [`StoreError::Conflict`] for changed metadata or a hash mismatch;
    /// [`StoreError::LimitExceeded`] for size or count limits.
    pub async fn put_chunk(
        &self,
        uploader: &DeviceId,
        chunk: &ChunkUpload<'_>,
        limits: ArtifactLimits,
    ) -> StoreResult<Progress> {
        let mut tx = self.pool.begin().await?;
        check_member(&mut tx, uploader, chunk.pair).await?;
        let mut upload = self.open_upload(&mut tx, uploader, chunk, limits).await?;
        let offset = int(chunk.offset)?;
        if upload.complete || offset < upload.received {
            tx.commit().await?;
            return Ok(Progress {
                received: uint(upload.received)?,
                complete: upload.complete,
            });
        }
        let end = offset
            .checked_add(int(chunk.data.len() as u64)?)
            .ok_or(StoreError::LimitExceeded)?;
        if offset > upload.received || chunk.data.is_empty() || end > upload.total {
            return Err(StoreError::Forbidden);
        }
        sqlx::query("INSERT INTO artifact_chunks (pair_id, artifact_id, start, data) VALUES ($1, $2, $3, $4)")
            .bind(pg_uuid(chunk.pair.as_bytes()))
            .bind(pg_uuid(chunk.artifact.as_bytes()))
            .bind(offset)
            .bind(chunk.data.as_bytes())
            .execute(&mut *tx)
            .await?;
        upload.received = end;
        upload.complete = end == upload.total;
        if upload.complete && hash_of(&mut tx, chunk.pair, chunk.artifact).await? != upload.hash {
            drop(tx);
            self.delete_artifact(uploader, chunk.pair, chunk.artifact)
                .await?;
            return Err(StoreError::Conflict);
        }
        sqlx::query(
            "UPDATE artifacts SET received = $3, complete = $4 WHERE pair_id = $1 AND id = $2",
        )
        .bind(pg_uuid(chunk.pair.as_bytes()))
        .bind(pg_uuid(chunk.artifact.as_bytes()))
        .bind(upload.received)
        .bind(upload.complete)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Progress {
            received: uint(upload.received)?,
            complete: upload.complete,
        })
    }

    /// The chunk of a complete artifact that starts at `offset`.
    ///
    /// # Errors
    ///
    /// [`StoreError::Forbidden`] for non-members; [`StoreError::NotFound`]
    /// for unknown or incomplete artifacts or a non-chunk offset.
    pub async fn get_chunk(
        &self,
        reader: &DeviceId,
        pair: PairId,
        artifact: ArtifactId,
        offset: u64,
    ) -> StoreResult<StoredChunk> {
        let mut tx = self.pool.begin().await?;
        check_member(&mut tx, reader, pair).await?;
        let row = sqlx::query(
            "SELECT a.total, c.data FROM artifacts a JOIN artifact_chunks c \
             ON c.pair_id = a.pair_id AND c.artifact_id = a.id \
             WHERE a.pair_id = $1 AND a.id = $2 AND a.complete AND c.start = $3",
        )
        .bind(pg_uuid(pair.as_bytes()))
        .bind(pg_uuid(artifact.as_bytes()))
        .bind(int(offset)?)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(StoreError::NotFound)?;
        tx.commit().await?;
        Ok(StoredChunk {
            offset,
            total: uint(row.try_get("total")?)?,
            data: Blob::new(row.try_get("data")?),
        })
    }

    /// Deletes an artifact. Either pair member. Unknown artifacts are a no-op.
    ///
    /// # Errors
    ///
    /// [`StoreError::Forbidden`] for non-members.
    pub async fn delete_artifact(
        &self,
        member: &DeviceId,
        pair: PairId,
        artifact: ArtifactId,
    ) -> StoreResult<()> {
        let mut tx = self.pool.begin().await?;
        let record = lock(&mut tx, pair, LockMode::Share).await?.record;
        if !record.members().contains(member) {
            return Err(StoreError::Forbidden);
        }
        sqlx::query("DELETE FROM artifacts WHERE pair_id = $1 AND id = $2")
            .bind(pg_uuid(pair.as_bytes()))
            .bind(pg_uuid(artifact.as_bytes()))
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Deletes up to `limit` artifacts: unfinished ones older than
    /// `incomplete`, finished ones older than `complete`. Returns the count.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`] on failure.
    pub async fn expire_artifacts(
        &self,
        incomplete: Duration,
        complete: Duration,
        limit: i64,
    ) -> StoreResult<u64> {
        let deleted = sqlx::query(
            "DELETE FROM artifacts WHERE (pair_id, id) IN (SELECT pair_id, id FROM artifacts \
             WHERE created_at < now() - make_interval(secs => CASE WHEN complete THEN $2 ELSE $1 END) \
             LIMIT $3)",
        )
        .bind(incomplete.as_secs_f64())
        .bind(complete.as_secs_f64())
        .bind(limit)
        .execute(&self.pool)
        .await?;
        Ok(deleted.rows_affected())
    }
}

/// SHA-256 of an artifact's chunks in order, streamed.
async fn hash_of(
    conn: &mut PgConnection,
    pair: PairId,
    artifact: ArtifactId,
) -> StoreResult<Vec<u8>> {
    let mut rows = sqlx::query(
        "SELECT data FROM artifact_chunks WHERE pair_id = $1 AND artifact_id = $2 ORDER BY start",
    )
    .bind(pg_uuid(pair.as_bytes()))
    .bind(pg_uuid(artifact.as_bytes()))
    .fetch(conn);
    let mut hasher = Sha256::new();
    while let Some(row) = rows.try_next().await? {
        hasher.update(row.try_get::<Vec<u8>, _>("data")?);
    }
    Ok(hasher.finalize().to_vec())
}
