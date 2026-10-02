//! Unit test setup: a client state with an active pair, no network.

use std::sync::Arc;

use tokio::sync::mpsc;
use wici_crypto::{DeviceKeys, Invitation, PairKeys};
use wici_protocol::{PairState, Timestamp};

use crate::ClientConfig;
use crate::db::{self, Db, PairRow};
use crate::model::{Event, PendingOp, Role};
use crate::shared::Shared;

pub(crate) struct Fixture {
    pub(crate) shared: Arc<Shared>,
    pub(crate) events: mpsc::Receiver<Event>,
    /// The peer's view of the pair, to seal messages to this device.
    pub(crate) peer: PairKeys,
    pub(crate) peer_device: DeviceKeys,
    pub(crate) invitation: Invitation,
    _dir: tempfile::TempDir,
}

/// This device invited; the peer joined. `state` is the stored pair state.
pub(crate) async fn fixture(state: PairState) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(&dir.path().join("client.db")).await.unwrap();
    let keys = DeviceKeys::generate();
    let peer_device = DeviceKeys::generate();
    let invitation = Invitation::create(&keys, "ws://unused".to_owned(), Timestamp(0));
    let greeting = invitation.greet(&peer_device).unwrap();
    let own = invitation
        .accept(&keys, &peer_device.device_id(), &greeting)
        .unwrap();
    let peer = invitation.join(&peer_device).unwrap();
    let (events, receiver) = mpsc::channel(64);
    let (live, _) = mpsc::channel(1);
    let (requests, _) = mpsc::channel(1);
    let config = ClientConfig::new("ws://unused", dir.path().join("client.db"));
    let channels = crate::shared::Channels {
        events,
        live,
        requests,
    };
    let shared = Arc::new(Shared::new(db, keys, config, channels));
    let row = PairRow {
        id: invitation.pair,
        role: Role::Inviter,
        state,
        pending: None::<PendingOp>,
        peer: Some(peer_device.device_id()),
        invitation: shared.seal_invitation(&invitation).unwrap(),
        keys: Some(shared.seal_keys(&own).unwrap()),
    };
    db::insert_pair(&mut shared.db.conn().await.unwrap(), &row)
        .await
        .unwrap();
    Fixture {
        shared,
        events: receiver,
        peer,
        peer_device,
        invitation,
        _dir: dir,
    }
}

impl Fixture {
    /// Next queued event, if any.
    pub(crate) fn event(&mut self) -> Option<Event> {
        self.events.try_recv().ok()
    }

    pub(crate) async fn pair_row(&self) -> PairRow {
        db::get_pair(
            &mut self.shared.db.conn().await.unwrap(),
            self.invitation.pair,
        )
        .await
        .unwrap()
    }
}
