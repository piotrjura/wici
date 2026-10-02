//! Artifact upload and download over the client connection.

use std::collections::HashMap;
use std::sync::atomic::Ordering;

use tokio::sync::oneshot;
use wici_crypto::{ArtifactKey, artifact_hash};
use wici_protocol::{ArtifactId, ArtifactRef, Blob, ClientFrame, PairId, ServerFrame};

use crate::db;
use crate::error::{ClientError, ClientResult};
use crate::shared::Shared;

/// Bytes sent per upload frame. Below the server's default chunk limit.
const UPLOAD_PIECE: usize = 128 * 1024;

/// A frame for the connection task and where to send the answer.
#[derive(Debug)]
pub(crate) struct Request {
    pub(crate) artifact: ArtifactId,
    pub(crate) frame: ClientFrame,
    pub(crate) reply: oneshot::Sender<ServerFrame>,
}

/// Answers awaited on the current connection, by artifact.
#[derive(Debug, Default)]
pub(crate) struct Waiters(HashMap<ArtifactId, oneshot::Sender<ServerFrame>>);

impl Waiters {
    pub(crate) fn insert(&mut self, artifact: ArtifactId, reply: oneshot::Sender<ServerFrame>) {
        self.0.insert(artifact, reply);
    }

    /// Hands an artifact answer to its waiter. Returns other frames.
    pub(crate) fn route(&mut self, frame: ServerFrame) -> Option<ServerFrame> {
        let artifact = match &frame {
            ServerFrame::ArtifactStored { artifact, .. }
            | ServerFrame::ArtifactChunk { artifact, .. }
            | ServerFrame::Error {
                artifact: Some(artifact),
                ..
            } => *artifact,
            ServerFrame::Challenge { .. }
            | ServerFrame::Welcome { .. }
            | ServerFrame::Pair { .. }
            | ServerFrame::Accepted { .. }
            | ServerFrame::Deliver { .. }
            | ServerFrame::Live { .. }
            | ServerFrame::Presence { .. }
            | ServerFrame::Error { .. } => return Some(frame),
        };
        if let Some(waiter) = self.0.remove(&artifact) {
            let _ = waiter.send(frame);
        }
        None
    }
}

/// Sends `frame` and waits for the answer about `artifact`.
async fn request(
    shared: &Shared,
    artifact: ArtifactId,
    frame: ClientFrame,
) -> ClientResult<ServerFrame> {
    if !shared.connected.load(Ordering::Acquire) {
        return Err(ClientError::Offline);
    }
    let (reply, answer) = oneshot::channel();
    shared
        .requests
        .send(Request {
            artifact,
            frame,
            reply,
        })
        .await
        .map_err(|_| ClientError::Offline)?;
    let answer = tokio::time::timeout(shared.config.request_timeout, answer)
        .await
        .map_err(|_| ClientError::Offline)?;
    let frame = answer.map_err(|_| ClientError::Offline)?;
    if let ServerFrame::Error { code, message, .. } = frame {
        return Err(ClientError::Server { code, message });
    }
    Ok(frame)
}

async fn active_pair(shared: &Shared, pair: PairId) -> ClientResult<()> {
    let mut conn = shared.db.conn().await?;
    let row = db::get_pair(&mut conn, pair).await?;
    shared.pair_keys(&row).map(|_| ())
}

/// Seals `data` off the async runtime.
async fn seal(key: ArtifactKey, data: Vec<u8>) -> ClientResult<(ArtifactKey, Vec<u8>)> {
    tokio::task::spawn_blocking(move || key.seal_all(&data).map(|sealed| (key, sealed)))
        .await
        .map_err(|_| ClientError::Corrupt("sealing task"))?
        .map_err(ClientError::from)
}

/// Encrypts and uploads `data`. Resumes after the last stored byte.
pub(crate) async fn upload(
    shared: &Shared,
    pair: PairId,
    data: Vec<u8>,
    media_type: &str,
    name: Option<String>,
) -> ClientResult<ArtifactRef> {
    if data.len() as u64 > shared.config.max_artifact_bytes {
        return Err(ClientError::TooLarge);
    }
    active_pair(shared, pair).await?;
    let size = data.len() as u64;
    let (key, sealed) = seal(ArtifactKey::generate(), data).await?;
    let hash = artifact_hash(&sealed);
    let total = sealed.len() as u64;
    let mut offset = 0;
    while offset < sealed.len() {
        let end = (offset + UPLOAD_PIECE).min(sealed.len());
        let chunk = Blob::new(sealed.get(offset..end).unwrap_or_default().to_vec());
        let frame = ClientFrame::ArtifactPut {
            pair,
            artifact: key.id(),
            total,
            hash,
            offset: offset as u64,
            chunk,
        };
        let ServerFrame::ArtifactStored { received, .. } = request(shared, key.id(), frame).await?
        else {
            return Err(ClientError::Corrupt("upload answer"));
        };
        offset = usize::try_from(received).map_err(|_| ClientError::Corrupt("upload offset"))?;
    }
    Ok(ArtifactRef {
        id: key.id(),
        key: key.key_bytes(),
        size,
        sealed_size: total,
        hash,
        media_type: media_type.to_owned(),
        name,
    })
}

/// Downloads, verifies, and decrypts an artifact.
pub(crate) async fn download(
    shared: &Shared,
    pair: PairId,
    artifact: &ArtifactRef,
) -> ClientResult<Vec<u8>> {
    if artifact.sealed_size > shared.config.max_artifact_bytes.saturating_mul(2) {
        return Err(ClientError::TooLarge);
    }
    active_pair(shared, pair).await?;
    let mut sealed = Vec::new();
    while (sealed.len() as u64) < artifact.sealed_size {
        let frame = ClientFrame::ArtifactGet {
            pair,
            artifact: artifact.id,
            offset: sealed.len() as u64,
        };
        let ServerFrame::ArtifactChunk { chunk, total, .. } =
            request(shared, artifact.id, frame).await?
        else {
            return Err(ClientError::Corrupt("download answer"));
        };
        if total != artifact.sealed_size || chunk.is_empty() {
            return Err(ClientError::Corrupt("artifact size"));
        }
        sealed.extend(chunk.into_bytes());
    }
    if artifact_hash(&sealed) != artifact.hash {
        return Err(ClientError::Corrupt("artifact hash"));
    }
    let key = ArtifactKey::from_parts(artifact.id, &artifact.key);
    tokio::task::spawn_blocking(move || key.open_all(&sealed).map(|plain| plain.to_vec()))
        .await
        .map_err(|_| ClientError::Corrupt("opening task"))?
        .map_err(ClientError::from)
}

/// Deletes an artifact on the server.
pub(crate) async fn delete(
    shared: &Shared,
    pair: PairId,
    artifact: ArtifactId,
) -> ClientResult<()> {
    let (reply, _answer) = oneshot::channel();
    let frame = ClientFrame::ArtifactDelete { pair, artifact };
    shared
        .requests
        .send(Request {
            artifact,
            frame,
            reply,
        })
        .await
        .map_err(|_| ClientError::Offline)
}

#[cfg(test)]
mod tests {
    use wici_protocol::{ErrorCode, Position};

    use super::*;

    #[tokio::test]
    async fn waiters_get_artifact_answers_and_other_frames_pass() {
        let mut waiters = Waiters::default();
        let artifact = ArtifactId::generate();
        let pair = PairId::generate();
        let (reply, answer) = oneshot::channel();
        waiters.insert(artifact, reply);
        let stored = ServerFrame::ArtifactStored {
            pair,
            artifact,
            received: 1,
            complete: true,
        };
        assert_eq!(waiters.route(stored.clone()), None);
        assert_eq!(answer.await.unwrap(), stored);

        let other = ServerFrame::Accepted {
            pair,
            id: wici_protocol::MessageId::generate(),
            position: Position(1),
        };
        assert_eq!(waiters.route(other.clone()), Some(other));
        let plain_error = ServerFrame::Error {
            code: ErrorCode::Internal,
            message: String::new(),
            pair: None,
            id: None,
            artifact: None,
        };
        assert_eq!(waiters.route(plain_error.clone()), Some(plain_error));
        let orphan = ServerFrame::Error {
            code: ErrorCode::NotFound,
            message: String::new(),
            pair: None,
            id: None,
            artifact: Some(artifact),
        };
        assert_eq!(waiters.route(orphan), None, "no waiter: dropped");
    }
}
