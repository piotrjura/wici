//! Sealed artifact rules: ordered chunk upload, hash check, download, cleanup.

use std::time::Duration;

use wici_protocol::{ArtifactId, Blob, DeviceId, FixedBytes, PairId, PairState};

use super::adapter::{Adapter, LockMode};
use super::pairs::lock;
use super::sql::Exec;
use super::sql::artifacts::{
    Upload, artifact_hash, complete_chunk, count_incomplete, delete_artifact as delete_row,
    expire_artifacts, insert_chunk, insert_upload, lock_upload, save_progress,
};
use super::{Adapters, Store, StoreError, StoreResult, dispatch};

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

impl Upload {
    fn progress(&self) -> StoreResult<Progress> {
        Ok(Progress {
            received: uint(self.received)?,
            complete: self.complete,
        })
    }
}

impl Store {
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
        dispatch!(self, |a| put_chunk(a, uploader, chunk, limits).await)
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
        let start = int(offset)?;
        let (total, data) = dispatch!(self, |a| {
            get_chunk(a, reader, pair, artifact, start).await
        })?;
        Ok(StoredChunk {
            offset,
            total: uint(total)?,
            data: Blob::new(data),
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
        dispatch!(self, |a| delete_artifact(a, member, pair, artifact).await)
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
        dispatch!(self, |a| {
            let mut conn = a.writer().acquire().await?;
            expire_artifacts(&mut *conn, incomplete, complete, limit).await
        })
    }
}

/// Fails unless `device` belongs to the active pair. Holds a shared lock,
/// so the pair cannot be revoked during the request.
async fn check_member(conn: &mut impl Exec, device: &DeviceId, pair: PairId) -> StoreResult<()> {
    let record = lock(conn, pair, LockMode::Share).await?.record;
    if record.state == PairState::Active && record.peer_of(device).is_some() {
        Ok(())
    } else {
        Err(StoreError::Forbidden)
    }
}

/// Starts an upload or checks that a repeat matches the first request.
async fn open_upload(
    conn: &mut impl Exec,
    uploader: &DeviceId,
    chunk: &ChunkUpload<'_>,
    limits: ArtifactLimits,
) -> StoreResult<Upload> {
    let total = int(chunk.total)?;
    if let Some(upload) = lock_upload(conn, chunk.pair, chunk.artifact).await? {
        let same = upload.uploader == uploader.as_bytes()
            && upload.total == total
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
    if count_incomplete(conn, chunk.pair).await? >= limits.max_incomplete {
        return Err(StoreError::LimitExceeded);
    }
    let upload = Upload {
        uploader: uploader.as_bytes().to_vec(),
        total,
        hash: chunk.hash.as_bytes().to_vec(),
        received: 0,
        complete: false,
    };
    insert_upload(conn, chunk.pair, chunk.artifact, &upload).await?;
    Ok(upload)
}

async fn put_chunk<A: Adapter>(
    adapter: &A,
    uploader: &DeviceId,
    chunk: &ChunkUpload<'_>,
    limits: ArtifactLimits,
) -> StoreResult<Progress> {
    let (pair, artifact) = (chunk.pair, chunk.artifact);
    let mut tx = adapter.writer().begin().await?;
    check_member(&mut *tx, uploader, pair).await?;
    let mut upload = open_upload(&mut *tx, uploader, chunk, limits).await?;
    let offset = int(chunk.offset)?;
    if upload.complete || offset < upload.received {
        tx.commit().await?;
        return upload.progress();
    }
    let end = offset
        .checked_add(int(chunk.data.len() as u64)?)
        .ok_or(StoreError::LimitExceeded)?;
    if offset > upload.received || chunk.data.is_empty() || end > upload.total {
        return Err(StoreError::Forbidden);
    }
    insert_chunk(&mut *tx, pair, artifact, offset, chunk.data).await?;
    upload.received = end;
    upload.complete = end == upload.total;
    if upload.complete && artifact_hash(&mut *tx, pair, artifact).await? != upload.hash {
        delete_row(&mut *tx, pair, artifact).await?;
        tx.commit().await?;
        return Err(StoreError::Conflict);
    }
    save_progress(&mut *tx, pair, artifact, &upload).await?;
    tx.commit().await?;
    upload.progress()
}

async fn get_chunk<A: Adapter>(
    adapter: &A,
    reader: &DeviceId,
    pair: PairId,
    artifact: ArtifactId,
    start: i64,
) -> StoreResult<(i64, Vec<u8>)> {
    let mut tx = adapter.reader().begin().await?;
    check_member(&mut *tx, reader, pair).await?;
    let chunk = complete_chunk(&mut *tx, pair, artifact, start)
        .await?
        .ok_or(StoreError::NotFound)?;
    tx.commit().await?;
    Ok(chunk)
}

async fn delete_artifact<A: Adapter>(
    adapter: &A,
    member: &DeviceId,
    pair: PairId,
    artifact: ArtifactId,
) -> StoreResult<()> {
    let mut tx = adapter.writer().begin().await?;
    let record = lock(&mut *tx, pair, LockMode::Share).await?.record;
    if !record.members().contains(member) {
        return Err(StoreError::Forbidden);
    }
    delete_row(&mut *tx, pair, artifact).await?;
    tx.commit().await?;
    Ok(())
}
