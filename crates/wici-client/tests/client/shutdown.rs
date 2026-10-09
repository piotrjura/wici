//! Closing must not depend on the app draining its event queue.

use std::time::Duration;

use wici_client::{Client, ClientConfig};
use wici_crypto::DeviceKeys;
use wici_testkit::{Backend, TestServer, WAIT};

async fn closes_with_full_events(backend: Backend) {
    let mut server = TestServer::on(backend, |_| {}).await;
    let dir = tempfile::tempdir().unwrap();
    let mut config = ClientConfig::new(&server.url, dir.path().join("client.db"));
    config.event_buffer = 1;
    let keys = DeviceKeys::generate();
    let secret = keys.to_secret();
    let (client, events) = Client::open(config.clone(), keys).await.unwrap();
    // Connected fills the queue. Applying the invitation commits before
    // its Pair event waits for space, so persisted state is our barrier.
    let invitation = client.invite().await.unwrap();
    tokio::time::timeout(WAIT, async {
        loop {
            if client
                .pairs()
                .await
                .unwrap()
                .iter()
                .any(|p| p.id == invitation.pair && p.pending.is_none())
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(events.len(), 1);
    let mut close = tokio::spawn(client.close());
    let result = tokio::time::timeout(Duration::from_millis(500), &mut close).await;
    // Clean up even when the regression fails.
    drop(events);
    if result.is_err() {
        tokio::time::timeout(WAIT, close).await.unwrap().unwrap();
    }
    assert!(result.is_ok(), "close waited for the app to drain events");
    result.unwrap().unwrap();
    let (client, events) = Client::open(config, DeviceKeys::from_secret(&secret))
        .await
        .unwrap();
    assert!(
        client
            .pairs()
            .await
            .unwrap()
            .iter()
            .any(|p| p.id == invitation.pair)
    );
    client.close().await;
    drop(events);
    server.stop().await;
}

#[tokio::test]
async fn full_event_queue_does_not_block_close_sqlite() {
    closes_with_full_events(Backend::Sqlite).await;
}

#[tokio::test]
async fn full_event_queue_does_not_block_close_postgres() {
    closes_with_full_events(Backend::Postgres).await;
}
