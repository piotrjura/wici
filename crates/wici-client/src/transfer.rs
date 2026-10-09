//! Artifact upload and download over the client connection.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures_util::FutureExt;
use tokio::sync::oneshot;
use wici_crypto::{ArtifactKey, artifact_hash};
use wici_protocol::{ArtifactId, ArtifactRef, Blob, ClientFrame, PairId, ServerFrame};

use crate::db;
use crate::error::{ClientError, ClientResult};
use crate::shared::Shared;

/// Bytes sent per upload frame. Below the server's default chunk limit.
const UPLOAD_PIECE: usize = 128 * 1024;
const MAX_REQUESTS: usize = 16;

/// A frame for the connection task and where to send the answer.
#[derive(Debug)]
pub(crate) struct Request {
    pub(crate) artifact: ArtifactId,
    pub(crate) frame: ClientFrame,
    /// Deletions have no wire acknowledgement and need no waiter.
    pub(crate) reply: Option<oneshot::Sender<ServerFrame>>,
}

#[derive(Debug)]
struct Waiting {
    reply: oneshot::Sender<ServerFrame>,
    deadline: tokio::time::Instant,
}

/// A cancelled queued request must not leave a reusable reply slot.
struct RequestGuard<'a> {
    reset: &'a tokio::sync::Notify,
    armed: bool,
}

impl Drop for RequestGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.reset.notify_one();
        }
    }
}

/// Answers awaited on the current connection, by artifact.
#[derive(Debug, Default)]
pub(crate) struct Waiters(HashMap<ArtifactId, Waiting>);

impl Waiters {
    /// Registers before sending. Rejects overlap and excess work without
    /// replacing an accepted waiter or sending the rejected frame.
    pub(crate) fn insert(
        &mut self,
        artifact: ArtifactId,
        reply: oneshot::Sender<ServerFrame>,
        timeout: Duration,
    ) -> bool {
        if reply.is_closed() {
            return false;
        }
        if self.0.contains_key(&artifact) || self.0.len() >= MAX_REQUESTS {
            let _ = reply.send(ServerFrame::Error {
                code: wici_protocol::ErrorCode::LimitExceeded,
                message: "artifact request already pending or limit reached".to_owned(),
                pair: None,
                id: None,
                artifact: Some(artifact),
            });
            return false;
        }
        self.0.insert(
            artifact,
            Waiting {
                reply,
                deadline: tokio::time::Instant::now() + timeout,
            },
        );
        true
    }

    /// A sent request expired or its caller left. End the connection before
    /// reusing its artifact ID: wire replies carry no request ID.
    pub(crate) async fn ended(&mut self) {
        let futures: Vec<_> = self
            .0
            .values_mut()
            .map(|waiting| {
                async move {
                    tokio::select! {
                        () = waiting.reply.closed() => {}
                        () = tokio::time::sleep_until(waiting.deadline) => {}
                    }
                }
                .boxed()
            })
            .collect();
        if futures.is_empty() {
            std::future::pending::<()>().await;
        }
        let _ = futures_util::future::select_all(futures).await;
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
            let _ = waiter.reply.send(frame);
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
    let waiting = async {
        shared
            .requests
            .send(Request {
                artifact,
                frame,
                reply: Some(reply),
            })
            .await
            .map_err(|_| ClientError::Offline)?;
        let mut guard = RequestGuard {
            reset: &shared.request_reset,
            armed: true,
        };
        let received = answer.await;
        guard.armed = false;
        received.map_err(|_| ClientError::Offline)
    };
    let frame = tokio::select! {
        () = shared.stop.cancelled() => return Err(ClientError::Offline),
        result = tokio::time::timeout(shared.config.request_timeout, waiting) => result.map_err(|_| ClientError::Offline)??,
    };
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
    let frame = ClientFrame::ArtifactDelete { pair, artifact };
    shared
        .requests
        .send(Request {
            artifact,
            frame,
            reply: None,
        })
        .await
        .map_err(|_| ClientError::Offline)
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use wici_protocol::{ErrorCode, Position};

    use super::*;

    #[tokio::test]
    async fn duplicate_artifact_request_keeps_the_first_waiter() {
        let mut waiters = Waiters::default();
        let artifact = ArtifactId::generate();
        let (first, mut answer) = oneshot::channel();
        let (second, other) = oneshot::channel();
        waiters.insert(artifact, first, Duration::from_secs(30));
        waiters.insert(artifact, second, Duration::from_secs(30));
        assert!(matches!(
            other.await.unwrap(),
            ServerFrame::Error {
                code: ErrorCode::LimitExceeded,
                ..
            }
        ));
        assert!(
            matches!(answer.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "second request replaced first"
        );
        let frame = ServerFrame::ArtifactStored {
            pair: PairId::generate(),
            artifact,
            received: 1,
            complete: true,
        };
        waiters.route(frame.clone());
        assert_eq!(answer.await.unwrap(), frame);
    }

    #[test]
    fn unanswered_artifact_requests_are_bounded() {
        let mut waiters = Waiters::default();
        let mut receivers = Vec::new();
        for _ in 0..1000 {
            let (reply, answer) = oneshot::channel();
            receivers.push(answer);
            waiters.insert(ArtifactId::generate(), reply, Duration::from_secs(30));
        }
        assert!(waiters.0.len() <= 16, "unbounded outstanding requests");
        let accepted = receivers
            .iter_mut()
            .filter_map(|answer| {
                matches!(answer.try_recv(), Err(oneshot::error::TryRecvError::Empty)).then_some(())
            })
            .count();
        assert_eq!(accepted, 16);
    }

    #[test]
    fn cancelled_unsent_request_does_not_take_a_waiter_slot() {
        let mut waiters = Waiters::default();
        let (reply, answer) = oneshot::channel();
        drop(answer);
        waiters.insert(ArtifactId::generate(), reply, Duration::from_secs(30));
        assert!(waiters.0.is_empty());
    }

    #[tokio::test]
    async fn sent_request_cancellation_ends_the_connection() {
        let mut waiters = Waiters::default();
        let (reply, answer) = oneshot::channel();
        waiters.insert(ArtifactId::generate(), reply, Duration::from_secs(30));
        drop(answer);
        tokio::time::timeout(Duration::from_millis(100), waiters.ended())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn unanswered_request_deadline_ends_the_connection() {
        let mut waiters = Waiters::default();
        let (reply, answer) = oneshot::channel();
        waiters.insert(ArtifactId::generate(), reply, Duration::from_millis(20));
        tokio::time::timeout(Duration::from_millis(200), waiters.ended())
            .await
            .unwrap();
        drop(waiters);
        assert!(answer.await.is_err());
    }

    async fn connected_fixture(timeout: Duration) -> crate::testing::Fixture {
        let mut f = crate::testing::fixture(wici_protocol::PairState::Active).await;
        std::sync::Arc::get_mut(&mut f.shared)
            .unwrap()
            .config
            .request_timeout = timeout;
        f.shared.connected.store(true, Ordering::Release);
        f
    }

    fn get_frame(pair: PairId, artifact: ArtifactId) -> ClientFrame {
        ClientFrame::ArtifactGet {
            pair,
            artifact,
            offset: 0,
        }
    }

    #[tokio::test]
    async fn queued_request_deadline_includes_waiting_for_space() {
        let f = connected_fixture(Duration::from_millis(20)).await;
        let pair = f.invitation.pair;
        let artifact = ArtifactId::generate();
        delete(&f.shared, pair, artifact).await.unwrap();
        let frame = get_frame(pair, artifact);
        let result = tokio::time::timeout(
            Duration::from_millis(200),
            request(&f.shared, artifact, frame),
        )
        .await
        .unwrap();
        assert!(matches!(result, Err(ClientError::Offline)));
        assert_eq!(f.requests.len(), 1);
    }

    #[tokio::test]
    async fn queued_request_timeout_notifies_connection_reset() {
        let mut f = connected_fixture(Duration::from_millis(20)).await;
        let artifact = ArtifactId::generate();
        let frame = get_frame(f.invitation.pair, artifact);
        let caller = request(&f.shared, artifact, frame);
        let connection = async {
            let queued = f.requests.recv().await.unwrap();
            tokio::time::timeout(
                Duration::from_millis(200),
                f.shared.request_reset.notified(),
            )
            .await
            .unwrap();
            assert!(queued.reply.unwrap().is_closed());
        };
        let (result, ()) = tokio::join!(caller, connection);
        assert!(matches!(result, Err(ClientError::Offline)));
    }

    #[tokio::test]
    async fn dropping_queued_request_notifies_connection_reset() {
        let mut f = connected_fixture(Duration::from_secs(30)).await;
        let artifact = ArtifactId::generate();
        let frame = get_frame(f.invitation.pair, artifact);
        let mut caller = Box::pin(request(&f.shared, artifact, frame));
        std::future::poll_fn(|cx| {
            assert!(caller.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        let queued = f.requests.recv().await.unwrap();
        drop(caller);
        tokio::time::timeout(
            Duration::from_millis(100),
            f.shared.request_reset.notified(),
        )
        .await
        .unwrap();
        assert!(queued.reply.unwrap().is_closed());
    }

    #[tokio::test]
    async fn answered_request_does_not_reset_connection() {
        let mut f = connected_fixture(Duration::from_secs(30)).await;
        let artifact = ArtifactId::generate();
        let pair = f.invitation.pair;
        let frame = get_frame(pair, artifact);
        let answer = ServerFrame::ArtifactChunk {
            pair,
            artifact,
            offset: 0,
            total: 1,
            chunk: Blob::new(vec![1]),
        };
        let caller = request(&f.shared, artifact, frame);
        let connection = async {
            let queued = f.requests.recv().await.unwrap();
            queued.reply.unwrap().send(answer.clone()).unwrap();
        };
        let (result, ()) = tokio::join!(caller, connection);
        assert_eq!(result.unwrap(), answer);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), f.shared.request_reset.notified())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn deletion_has_no_reply_waiter() {
        let mut f = crate::testing::fixture(wici_protocol::PairState::Active).await;
        let artifact = ArtifactId::generate();
        delete(&f.shared, f.invitation.pair, artifact)
            .await
            .unwrap();
        let queued = f.requests.recv().await.unwrap();
        assert!(queued.reply.is_none());
        assert!(
            matches!(queued.frame, ClientFrame::ArtifactDelete { artifact: id, .. } if id == artifact)
        );
    }

    #[tokio::test]
    async fn waiters_get_artifact_answers_and_other_frames_pass() {
        let mut waiters = Waiters::default();
        let artifact = ArtifactId::generate();
        let pair = PairId::generate();
        let (reply, answer) = oneshot::channel();
        waiters.insert(artifact, reply, Duration::from_secs(30));
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
