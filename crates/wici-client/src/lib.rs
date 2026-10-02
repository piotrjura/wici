//! Wici device client.
//!
//! Pairs this device with others and exchanges commands, events, and live
//! updates through a Wici server. Everything durable is saved in a local
//! SQLite database before it is sent or acknowledged, so restarts and
//! network loss never drop accepted work.
//!
//! Keep the device secret ([`wici_crypto::DeviceKeys::to_secret`]) in a
//! secure store such as the Keychain. All other secrets are sealed with it.

mod backoff;
mod db;
mod error;
mod inbound;
mod model;
mod outbound;
mod runtime;
mod shared;
#[cfg(test)]
mod testing;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use wici_crypto::{DeviceKeys, Invitation};
use wici_protocol::{
    Body, ClientFrame, CommandState, DeviceId, Lane, Lifecycle, LiveBody, MessageId, PairId,
    PairState, Position, StreamId, Timestamp,
};

pub use error::{ClientError, ClientResult};
pub use model::{Direction, Event, Incoming, PairView, PendingOp, Role};

use crate::db::{Db, PairRow, now_ms};
use crate::shared::{Shared, view};

/// Client settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfig {
    /// Server WebSocket URL, for example `wss://relay.example/v1/ws`.
    pub server_url: String,
    /// Local database file.
    pub database: PathBuf,
    /// Lifetime of new invitations.
    pub invite_ttl: Duration,
    /// Largest sealed message this client sends.
    pub max_sealed_bytes: usize,
    /// First reconnect delay.
    pub reconnect_min: Duration,
    /// Longest reconnect delay.
    pub reconnect_max: Duration,
    /// Interval for retrying transient failures.
    pub retry_interval: Duration,
    /// Reconnect after this long without any server frame.
    pub idle: Duration,
    /// Time to connect and authenticate.
    pub connect_timeout: Duration,
    /// Events buffered for the app.
    pub event_buffer: usize,
}

impl ClientConfig {
    /// Defaults for `server_url` and `database`.
    #[must_use]
    pub fn new(server_url: impl Into<String>, database: impl Into<PathBuf>) -> Self {
        Self {
            server_url: server_url.into(),
            database: database.into(),
            invite_ttl: Duration::from_secs(120),
            max_sealed_bytes: 1024 * 1024,
            reconnect_min: Duration::from_millis(200),
            reconnect_max: Duration::from_secs(30),
            retry_interval: Duration::from_secs(1),
            idle: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(10),
            event_buffer: 256,
        }
    }
}

/// A command to send with [`Client::command`].
#[derive(Debug, Clone, PartialEq)]
pub struct NewCommand {
    /// App-defined operation name.
    pub operation: String,
    /// App-defined input.
    pub input: Value,
    /// The peer must not start it after this time.
    pub deadline: Timestamp,
    /// Stream for its events.
    pub stream: Option<StreamId>,
}

/// A running client. Dropping it stops the connection task.
#[derive(Debug)]
pub struct Client {
    shared: Arc<Shared>,
    task: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for Shared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Shared({})", self.keys.device_id())
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.shared.stop.cancel();
    }
}

impl Client {
    /// Opens the local database, recovers interrupted commands, and starts
    /// connecting. Returns the client and its event stream.
    ///
    /// # Errors
    ///
    /// [`ClientError::Storage`] if the database cannot be opened or migrated.
    pub async fn open(
        config: ClientConfig,
        keys: DeviceKeys,
    ) -> ClientResult<(Self, mpsc::Receiver<Event>)> {
        let db = Db::open(&config.database).await?;
        let (events, receiver) = mpsc::channel(config.event_buffer.max(1));
        let (live, live_rx) = mpsc::channel(config.event_buffer.max(1));
        let shared = Arc::new(Shared::new(db, keys, config, events, live));
        recover(&shared).await?;
        let task = tokio::spawn(runtime::run(Arc::clone(&shared), live_rx));
        Ok((
            Self {
                shared,
                task: Some(task),
            },
            receiver,
        ))
    }

    /// Stops the connection task and waits for it.
    pub async fn close(mut self) {
        self.shared.stop.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }

    /// This device's identity.
    #[must_use]
    pub fn device_id(&self) -> DeviceId {
        self.shared.keys.device_id()
    }

    /// Skips the reconnect delay, for example after a network change.
    pub fn reconnect_now(&self) {
        self.shared.reconnect.notify_one();
    }

    /// Creates an invitation. Show [`Invitation::to_link`] to the other device.
    ///
    /// # Errors
    ///
    /// [`ClientError::Storage`] if it cannot be saved.
    pub async fn invite(&self) -> ClientResult<Invitation> {
        let ttl = i64::try_from(self.shared.config.invite_ttl.as_millis()).unwrap_or(i64::MAX);
        let expires = u64::try_from(now_ms().saturating_add(ttl)).unwrap_or(0);
        let server = self.shared.config.server_url.clone();
        let invitation = Invitation::create(&self.shared.keys, server, Timestamp(expires));
        let row = PairRow {
            id: invitation.pair,
            role: Role::Inviter,
            state: PairState::Invited,
            pending: Some(PendingOp::Invite),
            peer: None,
            invitation: self.shared.seal_invitation(&invitation)?,
            keys: None,
        };
        self.insert_pair(&row).await?;
        Ok(invitation)
    }

    /// Joins with an invitation link from another device.
    ///
    /// # Errors
    ///
    /// [`ClientError::Crypto`] for a bad link, [`ClientError::OwnInvitation`]
    /// for this device's own invitation.
    pub async fn join(&self, link: &str) -> ClientResult<PairId> {
        let invitation = Invitation::from_link(link)?;
        if invitation.inviter == self.device_id() {
            return Err(ClientError::OwnInvitation);
        }
        let keys = invitation.join(&self.shared.keys)?;
        let row = PairRow {
            id: invitation.pair,
            role: Role::Invitee,
            state: PairState::Invited,
            pending: Some(PendingOp::Claim),
            peer: Some(invitation.inviter),
            invitation: self.shared.seal_invitation(&invitation)?,
            keys: Some(self.shared.seal_keys(&keys)?),
        };
        self.insert_pair(&row).await?;
        Ok(invitation.pair)
    }

    async fn insert_pair(&self, row: &PairRow) -> ClientResult<()> {
        let mut conn = self.shared.db.conn().await?;
        db::insert_pair(&mut conn, row).await?;
        drop(conn);
        self.shared.wake.notify_one();
        Ok(())
    }

    /// Approves the device that claimed this device's invitation.
    ///
    /// # Errors
    ///
    /// [`ClientError::PairState`] unless this device invited and the pair
    /// is claimed.
    pub async fn approve(&self, pair: PairId) -> ClientResult<()> {
        self.set_pending(pair, PendingOp::Approve, |row| {
            row.role == Role::Inviter && row.state == PairState::Claimed && row.keys.is_some()
        })
        .await
    }

    /// Ends a pair. Works offline; the server is told on reconnect.
    ///
    /// # Errors
    ///
    /// [`ClientError::PairState`] if the pair already ended.
    pub async fn unpair(&self, pair: PairId) -> ClientResult<()> {
        self.set_pending(pair, PendingOp::Unpair, |row| !row.state.is_terminal())
            .await
    }

    async fn set_pending(
        &self,
        pair: PairId,
        op: PendingOp,
        allowed: impl Fn(&PairRow) -> bool,
    ) -> ClientResult<()> {
        let mut conn = self.shared.db.conn().await?;
        let mut row = db::get_pair(&mut conn, pair).await?;
        if !allowed(&row) {
            return Err(ClientError::PairState);
        }
        row.pending = Some(op);
        db::update_pair(&mut conn, &row).await?;
        drop(conn);
        self.shared.wake.notify_one();
        Ok(())
    }

    /// All pairs of this device, newest first.
    ///
    /// # Errors
    ///
    /// [`ClientError::Storage`] on database failure.
    pub async fn pairs(&self) -> ClientResult<Vec<PairView>> {
        let mut conn = self.shared.db.conn().await?;
        Ok(db::all_pairs(&mut conn).await?.iter().map(view).collect())
    }

    /// Queues a durable message. It is saved before this returns and sent
    /// until the server accepts it.
    ///
    /// # Errors
    ///
    /// [`ClientError::PairState`] unless the pair is active,
    /// [`ClientError::Body`] for invalid bodies, [`ClientError::TooLarge`].
    pub async fn send(&self, pair: PairId, lane: Lane, body: &Body) -> ClientResult<MessageId> {
        self.enqueue(pair, lane, body, None).await
    }

    /// Asks the peer to run a command. Tracks the command's lifecycle.
    ///
    /// # Errors
    ///
    /// Same as [`Client::send`].
    pub async fn command(&self, pair: PairId, command: NewCommand) -> ClientResult<MessageId> {
        let body = Body::Command {
            operation: command.operation,
            input: command.input,
            deadline: command.deadline,
            stream: command.stream,
        };
        let track = Some((None, CommandState::QueuedLocal));
        self.enqueue(pair, Lane::Control, &body, track).await
    }

    /// Reports progress of a command received from the peer.
    ///
    /// # Errors
    ///
    /// [`ClientError::Transition`] if the lifecycle does not allow `state`,
    /// plus the errors of [`Client::send`].
    pub async fn report(
        &self,
        pair: PairId,
        command: MessageId,
        state: CommandState,
        output: Option<Value>,
    ) -> ClientResult<MessageId> {
        let body = Body::Status {
            command,
            state,
            output,
        };
        self.enqueue(pair, Lane::Control, &body, Some((Some(command), state)))
            .await
    }

    /// Seals and saves a message. `track` updates a command: `None` as ID
    /// means the new message is an outgoing command; `Some(id)` reports on an
    /// incoming one.
    async fn enqueue(
        &self,
        pair: PairId,
        lane: Lane,
        body: &Body,
        track: Option<(Option<MessageId>, CommandState)>,
    ) -> ClientResult<MessageId> {
        let mut tx = self.shared.db.begin().await?;
        let row = db::get_pair(&mut tx, pair).await?;
        let message = self.shared.outgoing(&row, lane, body)?;
        match track {
            Some((None, state)) => {
                db::set_command(&mut tx, pair, message.id, Direction::Outgoing, state).await?;
            }
            Some((Some(command), state)) => {
                let current =
                    db::command_state(&mut tx, pair, command, Direction::Incoming).await?;
                if !current.is_some_and(|from| from.can_transition_to(state)) {
                    return Err(ClientError::Transition {
                        from: current,
                        to: state,
                    });
                }
                db::set_command(&mut tx, pair, command, Direction::Incoming, state).await?;
            }
            None => {}
        }
        db::enqueue(&mut tx, &message).await?;
        tx.commit().await?;
        self.shared.wake.notify_one();
        Ok(message.id)
    }

    /// Sends a live update. Not saved: it is dropped while offline.
    ///
    /// # Errors
    ///
    /// [`ClientError::PairState`] unless the pair is active.
    pub async fn live(&self, pair: PairId, body: &LiveBody) -> ClientResult<()> {
        let mut conn = self.shared.db.conn().await?;
        let row = db::get_pair(&mut conn, pair).await?;
        drop(conn);
        let sealed = self.shared.pair_keys(&row)?.seal_live(body)?;
        let _ = self
            .shared
            .live
            .try_send(ClientFrame::Live { pair, sealed });
        Ok(())
    }

    /// Received messages not yet marked handled, oldest first. Call after a
    /// restart to process what arrived before it.
    ///
    /// # Errors
    ///
    /// [`ClientError::Storage`] or [`ClientError::Crypto`] for unreadable rows.
    pub async fn pending(&self) -> ClientResult<Vec<Incoming>> {
        let mut conn = self.shared.db.conn().await?;
        let rows = db::unhandled(&mut conn).await?;
        drop(conn);
        rows.iter()
            .map(|row| {
                Ok(Incoming {
                    pair: row.pair,
                    lane: row.lane,
                    position: row.position,
                    id: row.id,
                    accepted_at: row.accepted_at,
                    body: self.shared.open_body(row)?,
                })
            })
            .collect()
    }

    /// Marks a received message handled. Returns `false` if unknown.
    ///
    /// # Errors
    ///
    /// [`ClientError::Storage`] on database failure.
    pub async fn handled(
        &self,
        pair: PairId,
        lane: Lane,
        position: Position,
    ) -> ClientResult<bool> {
        let mut conn = self.shared.db.conn().await?;
        db::mark_handled(&mut conn, pair, lane, position).await
    }

    /// State of a command.
    ///
    /// # Errors
    ///
    /// [`ClientError::Storage`] on database failure.
    pub async fn command_state(
        &self,
        pair: PairId,
        id: MessageId,
        direction: Direction,
    ) -> ClientResult<Option<CommandState>> {
        let mut conn = self.shared.db.conn().await?;
        db::command_state(&mut conn, pair, id, direction).await
    }
}

/// Incoming commands that were running when the app stopped may have had
/// effects. They become `outcome_unknown` and the peer is told. They are
/// never restarted automatically.
async fn recover(shared: &Shared) -> ClientResult<()> {
    let interrupted = [CommandState::Running, CommandState::AwaitingApproval];
    let mut tx = shared.db.begin().await?;
    for (pair, command) in db::commands_in(&mut tx, Direction::Incoming, &interrupted).await? {
        report_unknown(shared, &mut tx, pair, command).await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn report_unknown(
    shared: &Shared,
    conn: &mut sqlx::SqliteConnection,
    pair: PairId,
    command: MessageId,
) -> ClientResult<()> {
    let state = CommandState::OutcomeUnknown;
    db::set_command(conn, pair, command, Direction::Incoming, state).await?;
    let row = db::get_pair(conn, pair).await?;
    let body = Body::Status {
        command,
        state,
        output: None,
    };
    // An inactive pair cannot be told; the local record still changes.
    if let Ok(message) = shared.outgoing(&row, Lane::Control, &body) {
        db::enqueue(conn, &message).await?;
    }
    Ok(())
}
