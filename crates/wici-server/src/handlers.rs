//! Handling of authenticated client frames.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use wici_protocol::{
    Blob, ClientFrame, DeviceId, ErrorCode, FixedBytes, Lane, MessageId, PairId, PairState,
    Position, ServerFrame,
};

use crate::App;
use crate::hub::Peer;
use crate::rate::RateLimit;
use crate::store::{Claim, LaneKey, NewMessage, PairRecord, StoreError};

/// A request failed. Sent to the device as an `error` frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Failure {
    code: ErrorCode,
    message: String,
    pair: Option<PairId>,
    id: Option<MessageId>,
}

impl Failure {
    pub(crate) fn new(code: ErrorCode, message: &str) -> Self {
        Self {
            code,
            message: message.to_owned(),
            pair: None,
            id: None,
        }
    }

    /// Appends detail to the message.
    pub(crate) fn detail(mut self, detail: &str) -> Self {
        self.message = format!("{}: {detail}", self.message);
        self
    }

    const fn about(mut self, pair: PairId, id: Option<MessageId>) -> Self {
        self.pair = Some(pair);
        self.id = id;
        self
    }

    pub(crate) fn into_frame(self) -> ServerFrame {
        ServerFrame::Error {
            code: self.code,
            message: self.message,
            pair: self.pair,
            id: self.id,
        }
    }
}

impl From<StoreError> for Failure {
    fn from(error: StoreError) -> Self {
        if let StoreError::Database(cause) = &error {
            tracing::error!(error = %cause, "database error");
        }
        Self::new(error.code(), &error.to_string())
    }
}

type Handled = Result<(), Failure>;

/// State of one authenticated connection.
pub(crate) struct Connection {
    app: Arc<App>,
    device: DeviceId,
    peer: Peer,
    /// Active pairs and their peers. Reloaded when `pairs_changed` is set.
    active: HashMap<PairId, DeviceId>,
    rate: RateLimit,
}

impl Connection {
    pub(crate) fn new(app: Arc<App>, device: DeviceId, peer: Peer) -> Self {
        let limits = app.config.limits;
        peer.pairs_changed.store(true, Ordering::Release);
        Self {
            app,
            device,
            peer,
            active: HashMap::new(),
            rate: RateLimit::new(limits.frames_per_second, limits.frame_burst, Instant::now()),
        }
    }

    /// Queues an error frame.
    pub(crate) fn fail(&self, failure: Failure) {
        self.peer.send_control(failure.into_frame());
    }

    /// Sends `welcome` with all pairs, then presence of active peers.
    pub(crate) async fn welcome(&mut self) {
        let pairs = match self.app.store.pairs_of(&self.device).await {
            Ok(pairs) => pairs,
            Err(error) => {
                self.fail(error.into());
                self.peer.stop.cancel();
                return;
            }
        };
        self.peer.pairs_changed.store(false, Ordering::Release);
        self.active = active_peers(&self.device, &pairs);
        let views = pairs.iter().map(|p| p.view_for(&self.device)).collect();
        self.peer
            .send_control(ServerFrame::Welcome { pairs: views });
        for (pair, other) in self.active.clone() {
            self.app.exchange_presence(pair, &self.device, &other).await;
        }
    }

    /// Tells online peers that this device went offline.
    pub(crate) async fn announce_offline(&mut self) {
        let _ = self.refresh_active().await;
        if let Err(error) = self.app.store.touch_device(&self.device).await {
            tracing::warn!(%error, "cannot record last seen");
        }
        let last_seen = self.app.store.last_seen(&self.device).await.ok().flatten();
        for (pair, other) in &self.active {
            if let Some(peer) = self.app.hub.get(other) {
                peer.send_control(ServerFrame::Presence {
                    pair: *pair,
                    online: false,
                    last_seen,
                });
            }
        }
    }

    async fn refresh_active(&mut self) -> Handled {
        if self.peer.pairs_changed.swap(false, Ordering::AcqRel) {
            let pairs = self.app.store.pairs_of(&self.device).await?;
            self.active = active_peers(&self.device, &pairs);
        }
        Ok(())
    }

    /// Handles one frame.
    pub(crate) async fn handle(&mut self, frame: ClientFrame) -> Handled {
        if !self.rate.allow(Instant::now()) {
            return Err(Failure::new(ErrorCode::RateLimited, "too many frames"));
        }
        match frame {
            ClientFrame::Hello { .. } | ClientFrame::Authenticate { .. } => Err(Failure::new(
                ErrorCode::InvalidFrame,
                "already authenticated",
            )),
            ClientFrame::Invite { pair, claim_hash } => self.invite(pair, &claim_hash).await,
            ClientFrame::Claim {
                pair,
                claim_secret,
                greeting,
            } => self.claim(pair, &claim_secret, &greeting).await,
            ClientFrame::Approve { pair } => self.approve(pair).await,
            ClientFrame::Unpair { pair } => {
                let record = self.app.store.unpair(&self.device, pair).await;
                self.app
                    .notify_pair(&record.map_err(|e| Failure::from(e).about(pair, None))?);
                Ok(())
            }
            ClientFrame::Send {
                pair,
                id,
                lane,
                sealed,
            } => self.send(pair, id, lane, &sealed).await,
            ClientFrame::Live { pair, sealed } => self.live(pair, sealed).await,
            ClientFrame::Ack {
                pair,
                lane,
                position,
            } => self.ack(pair, lane, position).await,
        }
    }

    async fn invite(&self, pair: PairId, claim_hash: &FixedBytes<32>) -> Handled {
        let ttl = self.app.config.timeouts.invite;
        let record = self
            .app
            .store
            .invite(&self.device, pair, claim_hash, ttl)
            .await;
        self.app
            .notify_pair(&record.map_err(|e| Failure::from(e).about(pair, None))?);
        Ok(())
    }

    async fn claim(&self, pair: PairId, secret: &FixedBytes<32>, greeting: &Blob) -> Handled {
        if greeting.len() > self.app.config.limits.max_greeting_bytes {
            return Err(
                Failure::new(ErrorCode::LimitExceeded, "greeting too large").about(pair, None)
            );
        }
        let claim_hash = wici_crypto::claim_hash(secret);
        let claim = Claim {
            pair,
            claim_hash: &claim_hash,
            greeting,
        };
        let ttl = self.app.config.timeouts.claim;
        let record = self.app.store.claim(&self.device, &claim, ttl).await;
        self.app
            .notify_pair(&record.map_err(|e| Failure::from(e).about(pair, None))?);
        Ok(())
    }

    async fn approve(&self, pair: PairId) -> Handled {
        let record = self.app.store.approve(&self.device, pair).await;
        let record = record.map_err(|e| Failure::from(e).about(pair, None))?;
        self.app.notify_pair(&record);
        if let Some(other) = record.peer_of(&self.device) {
            self.app.exchange_presence(pair, &self.device, &other).await;
        }
        Ok(())
    }

    async fn send(&self, pair: PairId, id: MessageId, lane: Lane, sealed: &Blob) -> Handled {
        let about = |failure: Failure| failure.about(pair, Some(id));
        if sealed.len() > self.app.config.limits.max_sealed_bytes {
            return Err(about(Failure::new(
                ErrorCode::LimitExceeded,
                "message too large",
            )));
        }
        let message = NewMessage {
            pair,
            id,
            lane,
            sealed,
        };
        let accepted = self
            .app
            .store
            .send(&self.device, &message)
            .await
            .map_err(|e| about(e.into()))?;
        self.peer.send_control(ServerFrame::Accepted {
            pair,
            id,
            position: accepted.position,
        });
        self.app.hub.wake(&accepted.recipient);
        Ok(())
    }

    async fn live(&mut self, pair: PairId, sealed: Blob) -> Handled {
        if sealed.len() > self.app.config.limits.max_live_bytes {
            return Err(
                Failure::new(ErrorCode::LimitExceeded, "live update too large").about(pair, None),
            );
        }
        self.refresh_active().await?;
        let Some(other) = self.active.get(&pair) else {
            return Err(Failure::new(ErrorCode::Forbidden, "pair not active").about(pair, None));
        };
        if let Some(peer) = self.app.hub.get(other) {
            peer.send_live(ServerFrame::Live { pair, sealed });
        }
        Ok(())
    }

    async fn ack(&self, pair: PairId, lane: Lane, position: Position) -> Handled {
        let key = LaneKey {
            pair,
            recipient: self.device,
            lane,
        };
        self.app
            .store
            .ack(&key, position)
            .await
            .map_err(|e| Failure::from(e).about(pair, None))?;
        self.peer.wake.notify_one();
        Ok(())
    }
}

fn active_peers(device: &DeviceId, pairs: &[PairRecord]) -> HashMap<PairId, DeviceId> {
    pairs
        .iter()
        .filter(|p| p.state == PairState::Active)
        .filter_map(|p| Some((p.id, p.peer_of(device)?)))
        .collect()
}

impl App {
    /// Pushes a pair change to its online members.
    pub(crate) fn notify_pair(&self, record: &PairRecord) {
        for member in record.members() {
            if let Some(peer) = self.hub.get(&member) {
                peer.pairs_changed.store(true, Ordering::Release);
                peer.send_control(ServerFrame::Pair {
                    pair: record.view_for(&member),
                });
                peer.wake.notify_one();
            }
        }
    }

    /// Tells `device` whether `other` is online and, if it is, tells `other`
    /// that `device` is online.
    pub(crate) async fn exchange_presence(
        &self,
        pair: PairId,
        device: &DeviceId,
        other: &DeviceId,
    ) {
        let Some(own) = self.hub.get(device) else {
            return;
        };
        if let Some(peer) = self.hub.get(other) {
            peer.send_control(ServerFrame::Presence {
                pair,
                online: true,
                last_seen: None,
            });
            own.send_control(ServerFrame::Presence {
                pair,
                online: true,
                last_seen: None,
            });
        } else {
            let last_seen = self.store.last_seen(other).await.ok().flatten();
            own.send_control(ServerFrame::Presence {
                pair,
                online: false,
                last_seen,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_frames_carry_context() {
        let pair = PairId::from_bytes([1; 16]);
        let id = MessageId::from_bytes([2; 16]);
        let frame = Failure::new(ErrorCode::Conflict, "reused")
            .detail("x")
            .about(pair, Some(id))
            .into_frame();
        assert_eq!(
            frame,
            ServerFrame::Error {
                code: ErrorCode::Conflict,
                message: "reused: x".to_owned(),
                pair: Some(pair),
                id: Some(id),
            }
        );
    }

    #[test]
    fn store_errors_map_to_codes() {
        let failure = Failure::from(StoreError::Expired);
        assert_eq!(failure.code, ErrorCode::Expired);
        assert_eq!(failure.message, "expired");
        let failure = Failure::from(StoreError::Database(sqlx::Error::PoolTimedOut));
        assert_eq!(
            (failure.code, failure.message.as_str()),
            (ErrorCode::Internal, "database error")
        );
    }
}
