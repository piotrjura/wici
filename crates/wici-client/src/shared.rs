//! State shared by the client handle and its connection task.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use wici_crypto::{DeviceKeys, Invitation, LocalVault, PairKeys};
use wici_protocol::{Body, ClientFrame, Lane, MessageId, PairId, PairState};
use zeroize::Zeroizing;

use crate::ClientConfig;
use crate::db::{Db, InboxRow, OutboxRow, PairRow};
use crate::error::{ClientError, ClientResult};
use crate::model::{Event, PairView};

pub(crate) struct Shared {
    pub(crate) db: Db,
    pub(crate) keys: DeviceKeys,
    vault: LocalVault,
    pub(crate) config: ClientConfig,
    /// Wakes the connection task to send queued work.
    pub(crate) wake: Notify,
    /// Skips the reconnect delay.
    pub(crate) reconnect: Notify,
    pub(crate) stop: CancellationToken,
    events: mpsc::Sender<Event>,
    pub(crate) live: mpsc::Sender<ClientFrame>,
    cache: Mutex<HashMap<PairId, Arc<PairKeys>>>,
}

fn context(pair: PairId, what: &str) -> Vec<u8> {
    format!("{pair}/{what}").into_bytes()
}

impl Shared {
    pub(crate) fn new(
        db: Db,
        keys: DeviceKeys,
        config: ClientConfig,
        events: mpsc::Sender<Event>,
        live: mpsc::Sender<ClientFrame>,
    ) -> Self {
        Self {
            vault: keys.local_vault(),
            db,
            keys,
            config,
            wake: Notify::new(),
            reconnect: Notify::new(),
            stop: CancellationToken::new(),
            events,
            live,
            cache: Mutex::new(HashMap::new()),
        }
    }

    fn cache(&self) -> MutexGuard<'_, HashMap<PairId, Arc<PairKeys>>> {
        // No code panics while holding the lock.
        self.cache.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Sends an event. Waits if the app is slow; drops it if the app is gone.
    pub(crate) async fn emit(&self, event: Event) {
        let _ = self.events.send(event).await;
    }

    pub(crate) fn seal_invitation(&self, invitation: &Invitation) -> ClientResult<Vec<u8>> {
        let json =
            serde_json::to_vec(invitation).map_err(|_| ClientError::Corrupt("invitation"))?;
        Ok(self
            .vault
            .seal(&context(invitation.pair, "invitation"), &json)?)
    }

    pub(crate) fn open_invitation(&self, row: &PairRow) -> ClientResult<Invitation> {
        let json = self
            .vault
            .open(&context(row.id, "invitation"), &row.invitation)?;
        serde_json::from_slice(&json).map_err(|_| ClientError::Corrupt("invitation"))
    }

    pub(crate) fn seal_keys(&self, keys: &PairKeys) -> ClientResult<Vec<u8>> {
        Ok(self
            .vault
            .seal(&context(keys.pair(), "keys"), keys.to_secret().as_ref())?)
    }

    /// Keys of an active pair.
    pub(crate) fn pair_keys(&self, row: &PairRow) -> ClientResult<Arc<PairKeys>> {
        if row.state != PairState::Active {
            return Err(ClientError::PairState);
        }
        if let Some(keys) = self.cache().get(&row.id) {
            return Ok(Arc::clone(keys));
        }
        let (Some(sealed), Some(peer)) = (&row.keys, row.peer) else {
            return Err(ClientError::PairState);
        };
        let secret = secret_64(&self.vault.open(&context(row.id, "keys"), sealed)?)?;
        let keys = Arc::new(PairKeys::from_secret(
            row.id,
            self.keys.device_id(),
            peer,
            &secret,
        ));
        self.cache().insert(row.id, Arc::clone(&keys));
        Ok(keys)
    }

    pub(crate) fn forget_keys(&self, pair: PairId) {
        self.cache().remove(&pair);
    }

    /// Seals `body` for the peer under a new message ID.
    pub(crate) fn outgoing(
        &self,
        row: &PairRow,
        lane: Lane,
        body: &Body,
    ) -> ClientResult<OutboxRow> {
        body.validate()?;
        let keys = self.pair_keys(row)?;
        let id = MessageId::generate();
        let sealed = keys.seal_message(id, lane, body)?.into_bytes();
        if sealed.len() > self.config.max_sealed_bytes {
            return Err(ClientError::TooLarge);
        }
        Ok(OutboxRow {
            pair: row.id,
            id,
            lane,
            sealed,
        })
    }

    fn inbox_context(row: &InboxRow) -> Vec<u8> {
        context(row.pair, &format!("inbox/{}/{}", row.lane, row.position.0))
    }

    /// Seals a received body for storage.
    pub(crate) fn seal_body(&self, row: &InboxRow, body: &Body) -> ClientResult<Vec<u8>> {
        let json = serde_json::to_vec(body).map_err(|_| ClientError::Corrupt("body"))?;
        Ok(self.vault.seal(&Self::inbox_context(row), &json)?)
    }

    pub(crate) fn open_body(&self, row: &InboxRow) -> ClientResult<Body> {
        let json = self.vault.open(&Self::inbox_context(row), &row.body)?;
        serde_json::from_slice(&json).map_err(|_| ClientError::Corrupt("body"))
    }
}

/// Public view of a stored pair.
pub(crate) const fn view(row: &PairRow) -> PairView {
    PairView {
        id: row.id,
        role: row.role,
        state: row.state,
        pending: row.pending,
        peer: row.peer,
    }
}

/// Copies a 64-byte secret into a buffer that is wiped on drop.
fn secret_64(bytes: &[u8]) -> ClientResult<Zeroizing<[u8; 64]>> {
    if bytes.len() != 64 {
        return Err(ClientError::Corrupt("key length"));
    }
    let mut secret = Zeroizing::new([0; 64]);
    secret.copy_from_slice(bytes);
    Ok(secret)
}
