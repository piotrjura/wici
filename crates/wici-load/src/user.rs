//! One simulated user: a paired Mac and phone running the Sfora pattern.

use std::collections::HashMap;
use std::time::Duration;

use tokio::time::Instant;
use wici_crypto::Invitation;
use wici_protocol::{
    Blob, ClientFrame, Lane, MessageId, PairId, PairState, ServerFrame, Timestamp,
};

use crate::LoadError;
use crate::device::Device;
use crate::payload::{Kind, Payload};
use crate::report::Tally;

/// Size of notices and requests.
const SMALL: usize = 128;
/// Sends waiting for `accepted`. A device sends nothing more past this.
const MAX_PENDING: usize = 1024;

/// A paired Mac and phone.
#[derive(Debug)]
pub(crate) struct User {
    pub(crate) mac: Device,
    pub(crate) phone: Device,
    pub(crate) pair: PairId,
}

/// Connects a Mac and a phone and pairs them over the protocol.
pub(crate) async fn pair(url: &str) -> Result<User, LoadError> {
    let mut mac = Device::connect(url).await?;
    let mut phone = Device::connect(url).await?;
    let invitation = Invitation::create(&mac.keys, url.to_owned(), Timestamp(0));
    let pair = invitation.pair;
    let claim_hash = invitation.claim_hash();
    mac.send(&ClientFrame::Invite { pair, claim_hash }).await?;
    mac.wait_pair(pair, PairState::Invited).await?;
    let claim = ClientFrame::Claim {
        pair,
        claim_secret: invitation.claim_secret(),
        greeting: invitation.greet(&phone.keys)?,
    };
    phone.send(&claim).await?;
    mac.wait_pair(pair, PairState::Claimed).await?;
    mac.send(&ClientFrame::Approve { pair }).await?;
    mac.wait_pair(pair, PairState::Active).await?;
    phone.wait_pair(pair, PairState::Active).await?;
    Ok(User { mac, phone, pair })
}

/// Which side of the pair a device plays.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Role {
    /// Sends notices and snapshots.
    Mac,
    /// Requests snapshots.
    Phone,
}

/// Times shared by every device in a run.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Schedule {
    /// Time zero for payload timestamps.
    pub(crate) clock: Instant,
    /// No new notices after this.
    pub(crate) stop_sending: Instant,
    /// Devices disconnect at this time.
    pub(crate) stop: Instant,
    /// Snapshot size.
    pub(crate) snapshot_bytes: usize,
}

/// What one device does in the traffic phase.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Script {
    pub(crate) role: Role,
    pub(crate) pair: PairId,
    /// First notice and the interval. `None` for devices that only answer.
    pub(crate) notices: Option<(Instant, Duration)>,
    pub(crate) schedule: Schedule,
}

/// Runs `script` on `device` until the schedule stops, then disconnects.
pub(crate) async fn exchange(device: Device, script: Script) -> Tally {
    let mut session = Session {
        device,
        script,
        pending: HashMap::new(),
        tally: Tally::default(),
    };
    session.run().await;
    // Sends never accepted are failures too.
    session.tally.errors += u64::try_from(session.pending.len()).unwrap_or(u64::MAX);
    session.device.close().await;
    session.tally
}

struct Session {
    device: Device,
    script: Script,
    /// Send time of each message waiting for `accepted`.
    pending: HashMap<MessageId, u64>,
    tally: Tally,
}

impl Session {
    async fn run(&mut self) {
        let schedule = self.script.schedule;
        let (first, every) = self
            .script
            .notices
            .unwrap_or((schedule.stop, Duration::from_secs(3600)));
        let mut ticker = tokio::time::interval_at(first, every);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let sending = self.script.notices.is_some() && Instant::now() < schedule.stop_sending;
            let step = tokio::select! {
                () = tokio::time::sleep_until(schedule.stop) => return,
                _ = ticker.tick(), if sending => self.send(Kind::Notice, None, SMALL).await,
                frame = self.device.recv() => match frame {
                    Ok(frame) => self.on_frame(frame).await,
                    Err(error) => Err(error),
                },
            };
            match step {
                Ok(()) => {}
                Err(LoadError::Closed) => {
                    self.tally.disconnects += 1;
                    return;
                }
                Err(_) => self.tally.errors += 1,
            }
        }
    }

    fn now(&self) -> u64 {
        let elapsed = Instant::now().saturating_duration_since(self.script.schedule.clock);
        u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX)
    }

    /// Sends a message of `kind`. `origin` is the notice time; `None` starts
    /// a new exchange.
    async fn send(
        &mut self,
        kind: Kind,
        origin: Option<u64>,
        size: usize,
    ) -> Result<(), LoadError> {
        if self.pending.len() >= MAX_PENDING {
            self.tally.errors += 1;
            return Ok(());
        }
        let sent = self.now();
        let id = MessageId::generate();
        let payload = Payload {
            kind,
            sent,
            origin: origin.unwrap_or(sent),
        };
        self.pending.insert(id, sent);
        let frame = ClientFrame::Send {
            pair: self.script.pair,
            id,
            lane: Lane::Data,
            sealed: payload.encode(size),
        };
        self.device.send(&frame).await
    }

    async fn on_frame(&mut self, frame: ServerFrame) -> Result<(), LoadError> {
        match frame {
            ServerFrame::Accepted { id, .. } => {
                self.accepted(id);
                Ok(())
            }
            ServerFrame::Deliver {
                pair,
                lane,
                position,
                sealed,
                ..
            } => {
                let ack = ClientFrame::Ack {
                    pair,
                    lane,
                    position,
                };
                self.device.send(&ack).await?;
                self.delivered(&sealed).await
            }
            ServerFrame::Error { .. } => {
                self.tally.errors += 1;
                Ok(())
            }
            ServerFrame::Challenge { .. }
            | ServerFrame::Welcome { .. }
            | ServerFrame::Pair { .. }
            | ServerFrame::Live { .. }
            | ServerFrame::Presence { .. }
            | ServerFrame::ArtifactStored { .. }
            | ServerFrame::ArtifactChunk { .. } => Ok(()),
        }
    }

    fn accepted(&mut self, id: MessageId) {
        if let Some(sent) = self.pending.remove(&id) {
            self.tally.accepted += 1;
            self.tally.accept.record(self.now().saturating_sub(sent));
        } else {
            self.tally.errors += 1;
        }
    }

    async fn delivered(&mut self, sealed: &Blob) -> Result<(), LoadError> {
        let Some(payload) = Payload::decode(sealed.as_bytes()) else {
            self.tally.errors += 1;
            return Ok(());
        };
        let now = self.now();
        self.tally.delivered += 1;
        self.tally.deliver.record(now.saturating_sub(payload.sent));
        let origin = Some(payload.origin);
        match (self.script.role, payload.kind) {
            (Role::Phone, Kind::Notice) => self.send(Kind::Request, origin, SMALL).await,
            (Role::Mac, Kind::Request) => {
                let size = self.script.schedule.snapshot_bytes;
                self.send(Kind::Snapshot, origin, size).await
            }
            (Role::Phone, Kind::Snapshot) => {
                self.tally
                    .round_trip
                    .record(now.saturating_sub(payload.origin));
                Ok(())
            }
            (Role::Mac, Kind::Notice | Kind::Snapshot) | (Role::Phone, Kind::Request) => {
                self.tally.errors += 1;
                Ok(())
            }
        }
    }
}
