//! Test server and WebSocket client.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use wici_crypto::{DeviceKeys, Invitation, PairKeys};
use wici_protocol::frame::{decode, encode};
use wici_protocol::{
    ClientFrame, PROTOCOL_VERSION, PairId, PairInfo, PairState, ServerFrame, Timestamp,
};
use wici_server::{Config, Limits, Timeouts};

use crate::support::store_with;

pub(crate) const WAIT: Duration = Duration::from_secs(5);

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// A running server on a fresh database.
pub(crate) struct Server {
    pub(crate) url: String,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<std::io::Result<()>>>,
}

impl Server {
    pub(crate) async fn start() -> Self {
        Self::with(|_| {}).await
    }

    pub(crate) async fn with(change: impl FnOnce(&mut Config)) -> Self {
        let mut config = Config {
            listen: "127.0.0.1:0".parse().unwrap(),
            database_url: String::new(),
            database_connections: 8,
            limits: Limits::default(),
            timeouts: Timeouts::default(),
        };
        change(&mut config);
        let store = store_with(config.limits.store).await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/v1/ws", listener.local_addr().unwrap());
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(wici_server::serve(listener, store, config, async {
            let _ = stopped.await;
        }));
        Self {
            url,
            stop: Some(stop),
            task: Some(task),
        }
    }

    pub(crate) fn http_address(&self) -> String {
        self.url
            .trim_start_matches("ws://")
            .trim_end_matches("/v1/ws")
            .to_owned()
    }

    pub(crate) async fn stop(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            tokio::time::timeout(WAIT, task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
    }
}

/// An authenticated device connection.
pub(crate) struct Client {
    socket: Socket,
    pub(crate) keys: DeviceKeys,
}

/// Opens a socket and returns it with the server challenge nonce.
pub(crate) async fn open(url: &str) -> (Socket, wici_protocol::FixedBytes<32>) {
    let (mut socket, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let ServerFrame::Challenge { version, nonce } = next(&mut socket).await.unwrap() else {
        panic!("expected challenge");
    };
    assert_eq!(version, PROTOCOL_VERSION);
    (socket, nonce)
}

pub(crate) async fn put(socket: &mut Socket, frame: &ClientFrame) {
    socket
        .send(Message::Text(encode(frame).unwrap().into()))
        .await
        .unwrap();
}

/// Next server frame, skipping pings. `None` when the socket closes.
pub(crate) async fn next(socket: &mut Socket) -> Option<ServerFrame> {
    loop {
        let message = tokio::time::timeout(WAIT, socket.next())
            .await
            .expect("timed out")?;
        match message.ok()? {
            Message::Text(text) => return Some(decode(text.as_str(), usize::MAX).unwrap()),
            Message::Close(_) => return None,
            Message::Binary(_) | Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
        }
    }
}

impl Client {
    /// Connects as a new device.
    pub(crate) async fn new(url: &str) -> (Self, Vec<PairInfo>) {
        Self::connect(url, DeviceKeys::generate()).await
    }

    /// Connects with existing keys and returns the welcome pairs.
    pub(crate) async fn connect(url: &str, keys: DeviceKeys) -> (Self, Vec<PairInfo>) {
        let (mut socket, nonce) = open(url).await;
        put(
            &mut socket,
            &ClientFrame::Hello {
                version: PROTOCOL_VERSION,
                device: keys.device_id(),
            },
        )
        .await;
        put(
            &mut socket,
            &ClientFrame::Authenticate {
                signature: keys.sign_challenge(&nonce),
            },
        )
        .await;
        let Some(ServerFrame::Welcome { pairs }) = next(&mut socket).await else {
            panic!("expected welcome");
        };
        (Self { socket, keys }, pairs)
    }

    pub(crate) async fn send(&mut self, frame: &ClientFrame) {
        put(&mut self.socket, frame).await;
    }

    pub(crate) async fn recv(&mut self) -> Option<ServerFrame> {
        next(&mut self.socket).await
    }

    /// Next frame matching `wanted`, skipping others.
    pub(crate) async fn recv_where(
        &mut self,
        wanted: impl Fn(&ServerFrame) -> bool,
    ) -> ServerFrame {
        loop {
            let frame = self.recv().await.expect("closed");
            if wanted(&frame) {
                return frame;
            }
        }
    }

    /// Next frame that is not presence.
    pub(crate) async fn recv_no_presence(&mut self) -> ServerFrame {
        self.recv_where(|f| !matches!(f, ServerFrame::Presence { .. }))
            .await
    }

    /// Asserts that no frame other than presence arrives within `wait`.
    pub(crate) async fn expect_quiet(&mut self, wait: Duration) {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout(left, self.socket.next()).await {
                Err(_) => return,
                Ok(Some(Ok(Message::Text(text)))) => {
                    let frame: ServerFrame = decode(text.as_str(), usize::MAX).unwrap();
                    assert!(
                        matches!(frame, ServerFrame::Presence { .. }),
                        "unexpected {frame:?}"
                    );
                }
                Ok(Some(Ok(_))) => {}
                Ok(other) => panic!("socket ended: {other:?}"),
            }
        }
    }

    pub(crate) async fn close(self) {
        let _ = self.disconnect().await;
    }

    /// Closes the socket and returns the device keys for a later reconnect.
    pub(crate) async fn disconnect(mut self) -> DeviceKeys {
        let _ = self.socket.close(None).await;
        self.keys
    }
}

fn pair_state(frame: &ServerFrame, pair: PairId, state: PairState) -> bool {
    matches!(frame, ServerFrame::Pair { pair: info } if info.id == pair && info.state == state)
}

/// Waits for a pair update with `state`.
pub(crate) async fn expect_pair(client: &mut Client, pair: PairId, state: PairState) -> PairInfo {
    let ServerFrame::Pair { pair: info } = client.recv_where(|f| pair_state(f, pair, state)).await
    else {
        panic!("unexpected frame")
    };
    info
}

/// Two connected, paired devices and their keys.
pub(crate) struct Paired {
    pub(crate) a: Client,
    pub(crate) b: Client,
    pub(crate) a_keys: PairKeys,
    pub(crate) b_keys: PairKeys,
    pub(crate) pair: PairId,
}

/// Pairs two new devices over the WebSocket protocol.
pub(crate) async fn paired(server: &Server) -> Paired {
    let (mut a, _) = Client::new(&server.url).await;
    let (mut b, _) = Client::new(&server.url).await;
    let invitation = Invitation::create(&a.keys, server.url.clone(), Timestamp(0));
    let pair = invitation.pair;
    a.send(&ClientFrame::Invite {
        pair,
        claim_hash: invitation.claim_hash(),
    })
    .await;
    expect_pair(&mut a, pair, PairState::Invited).await;

    let link = invitation.to_link().unwrap();
    let joined = Invitation::from_link(&link).unwrap();
    let greeting = joined.greet(&b.keys).unwrap();
    b.send(&ClientFrame::Claim {
        pair,
        claim_secret: joined.claim_secret(),
        greeting,
    })
    .await;
    let seen_by_b = expect_pair(&mut b, pair, PairState::Claimed).await;
    assert_eq!(seen_by_b.greeting, None);
    let seen_by_a = expect_pair(&mut a, pair, PairState::Claimed).await;
    let invitee = seen_by_a.invitee.unwrap();
    assert_eq!(invitee, b.keys.device_id());
    let a_keys = invitation
        .accept(&a.keys, &invitee, &seen_by_a.greeting.unwrap())
        .unwrap();

    a.send(&ClientFrame::Approve { pair }).await;
    expect_pair(&mut a, pair, PairState::Active).await;
    expect_pair(&mut b, pair, PairState::Active).await;
    let b_keys = joined.join(&b.keys).unwrap();
    Paired {
        a,
        b,
        a_keys,
        b_keys,
        pair,
    }
}
