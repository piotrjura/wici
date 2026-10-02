//! Devices backed by temporary databases.

use std::time::Duration;

use tokio::sync::mpsc;
use wici_client::{Client, ClientConfig, Event};
use wici_crypto::DeviceKeys;
use wici_protocol::{PairId, PairState};
use wici_testkit::{TestServer, WAIT};

/// A client with its event stream and enough state to reopen it.
pub(crate) struct Device {
    pub(crate) client: Client,
    pub(crate) events: mpsc::Receiver<Event>,
    secret: [u8; 64],
    url: String,
    dir: tempfile::TempDir,
}

fn config(url: &str, dir: &tempfile::TempDir) -> ClientConfig {
    let mut config = ClientConfig::new(url, dir.path().join("client.db"));
    config.reconnect_min = Duration::from_millis(20);
    config.reconnect_max = Duration::from_millis(200);
    config.retry_interval = Duration::from_millis(100);
    config
}

impl Device {
    pub(crate) async fn open(url: &str) -> Self {
        let keys = DeviceKeys::generate();
        let secret = *keys.to_secret();
        let dir = tempfile::tempdir().unwrap();
        let (client, events) = Client::open(config(url, &dir), keys).await.unwrap();
        Self {
            client,
            events,
            secret,
            url: url.to_owned(),
            dir,
        }
    }

    /// Stops the client, as if the app quit.
    pub(crate) async fn close(self) -> Closed {
        self.client.close().await;
        Closed {
            secret: self.secret,
            url: self.url,
            dir: self.dir,
        }
    }

    /// Next event matching `wanted`, skipping others.
    pub(crate) async fn expect(&mut self, wanted: impl Fn(&Event) -> bool) -> Event {
        loop {
            let event = tokio::time::timeout(WAIT, self.events.recv())
                .await
                .expect("timed out waiting for event")
                .expect("event stream ended");
            if wanted(&event) {
                return event;
            }
        }
    }

    pub(crate) async fn expect_pair(&mut self, pair: PairId, state: PairState) {
        self.expect(|e| matches!(e, Event::Pair(view) if view.id == pair && view.state == state))
            .await;
    }
}

/// A closed device that can be reopened with the same keys and database.
pub(crate) struct Closed {
    secret: [u8; 64],
    url: String,
    dir: tempfile::TempDir,
}

impl Closed {
    pub(crate) async fn reopen(self) -> Device {
        let keys = DeviceKeys::from_secret(&self.secret);
        let (client, events) = Client::open(config(&self.url, &self.dir), keys)
            .await
            .unwrap();
        Device {
            client,
            events,
            secret: self.secret,
            url: self.url,
            dir: self.dir,
        }
    }
}

/// Pairs `a` (inviter) and `b` (invitee) and waits until both are active.
pub(crate) async fn pair(a: &mut Device, b: &mut Device) -> PairId {
    let invitation = a.client.invite().await.unwrap();
    let pair = b.client.join(&invitation.to_link().unwrap()).await.unwrap();
    assert_eq!(pair, invitation.pair);
    let claimed = a.expect(|e| matches!(e, Event::Claimed { .. })).await;
    assert_eq!(
        claimed,
        Event::Claimed {
            pair,
            device: b.client.device_id()
        }
    );
    a.client.approve(pair).await.unwrap();
    a.expect_pair(pair, PairState::Active).await;
    b.expect_pair(pair, PairState::Active).await;
    pair
}

/// A server and two paired devices.
pub(crate) async fn paired_devices() -> (TestServer, Device, Device, PairId) {
    let server = TestServer::start().await;
    let mut a = Device::open(&server.url).await;
    let mut b = Device::open(&server.url).await;
    let pair = pair(&mut a, &mut b).await;
    (server, a, b, pair)
}
