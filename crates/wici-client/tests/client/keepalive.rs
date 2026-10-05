//! Dead-connection detection against a scripted relay.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use wici_client::{Client, ClientConfig, Event};
use wici_crypto::DeviceKeys;
use wici_protocol::frame::{decode, encode};
use wici_protocol::{ClientFrame, FixedBytes, PROTOCOL_VERSION, ServerFrame};
use wici_testkit::WAIT;

/// What the relay does after `welcome`.
#[derive(Clone, Copy)]
enum After {
    /// Reads, so it answers pings, but never sends.
    Answer,
    /// Neither reads nor sends, like a socket lost on a network change.
    Vanish,
}

/// Accepts connections, authenticates without checks, then acts as `after`.
async fn relay(after: After) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        loop {
            let (tcp, _) = listener.accept().await.unwrap();
            tokio::spawn(serve(tcp, after));
        }
    });
    url
}

async fn serve(tcp: tokio::net::TcpStream, after: After) {
    let mut socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
    let send = |frame: &ServerFrame| Message::Text(encode(frame).unwrap().into());
    let challenge = ServerFrame::Challenge {
        version: PROTOCOL_VERSION,
        nonce: FixedBytes::new([0; 32]),
    };
    socket.send(send(&challenge)).await.unwrap();
    for _ in 0..2 {
        let Some(Ok(Message::Text(text))) = socket.next().await else {
            return;
        };
        let _: ClientFrame = decode(text.as_str(), usize::MAX).unwrap();
    }
    let welcome = ServerFrame::Welcome { pairs: Vec::new() };
    socket.send(send(&welcome)).await.unwrap();
    match after {
        After::Answer => while socket.next().await.is_some() {},
        After::Vanish => std::future::pending::<()>().await,
    }
}

async fn open(url: &str, retry: Duration) -> (Client, mpsc::Receiver<Event>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = ClientConfig::new(url, dir.path().join("client.db"));
    config.idle = Duration::from_millis(300);
    config.ping = Duration::from_millis(100);
    config.retry_interval = retry;
    let (client, events) = Client::open(config, DeviceKeys::generate()).await.unwrap();
    (client, events, dir)
}

/// Next event that is not a pair or presence update, if any within `wait`.
async fn next_link_event(events: &mut mpsc::Receiver<Event>, wait: Duration) -> Option<Event> {
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        let event = tokio::time::timeout_at(deadline, events.recv())
            .await
            .ok()??;
        if matches!(event, Event::Connected | Event::Disconnected) {
            return Some(event);
        }
    }
}

#[tokio::test]
async fn silent_connection_is_dropped_despite_local_activity() {
    let url = relay(After::Vanish).await;
    // Retries tick faster than `idle`. They must not keep the socket alive.
    let (client, mut events, _dir) = open(&url, Duration::from_millis(50)).await;
    assert_eq!(
        next_link_event(&mut events, WAIT).await,
        Some(Event::Connected)
    );
    assert_eq!(
        next_link_event(&mut events, WAIT).await,
        Some(Event::Disconnected)
    );
    client.close().await;
}

#[tokio::test]
async fn answered_pings_keep_a_quiet_connection_open() {
    let url = relay(After::Answer).await;
    // Retries tick slower than `idle`, so only pings keep the socket alive.
    let (client, mut events, _dir) = open(&url, Duration::from_secs(10)).await;
    assert_eq!(
        next_link_event(&mut events, WAIT).await,
        Some(Event::Connected)
    );
    assert_eq!(
        next_link_event(&mut events, Duration::from_secs(1)).await,
        None
    );
    client.close().await;
}
