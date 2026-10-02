//! One WebSocket connection: authentication, reading, writing.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use rand_core::{OsRng, RngCore};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use wici_protocol::frame::{decode, encode};
use wici_protocol::{ClientFrame, DeviceId, ErrorCode, FixedBytes, PROTOCOL_VERSION, ServerFrame};

use crate::App;
use crate::delivery;
use crate::handlers::{Connection, Failure};
use crate::hub::{Outbox, Peer};

/// Serves one connection until it closes, idles out, or is replaced.
pub(crate) async fn run(socket: WebSocket, app: Arc<App>) {
    let (mut sink, mut stream) = socket.split();
    let Some(device) = admit(&app, &mut sink, &mut stream).await else {
        let _ = sink.close().await;
        return;
    };
    let (peer, writer) = attach(&app, device, sink);
    let mut connection = Connection::new(Arc::clone(&app), device, peer.clone());
    connection.welcome().await;
    let deliveries = tokio::spawn(delivery::run(
        Arc::clone(&app),
        device,
        Arc::clone(&peer.wake),
        peer.outbox.clone(),
        peer.stop.clone(),
    ));
    read(&mut connection, &mut stream, &app, &peer.stop).await;

    peer.stop.cancel();
    app.hub.unregister(&device, &peer);
    connection.announce_offline().await;
    let _ = writer.await;
    let _ = deliveries.await;
}

/// Authenticates and records the device. On failure, tells the device why.
async fn admit<Si, St>(app: &App, sink: &mut Si, stream: &mut St) -> Option<DeviceId>
where
    Si: Sink<Message> + Unpin,
    St: Stream<Item = Result<Message, axum::Error>> + Unpin,
{
    let admitted = match handshake(app, sink, stream).await {
        Ok(device) => app
            .store
            .touch_device(&device)
            .await
            .map(|()| device)
            .map_err(Failure::from),
        Err(failure) => Err(failure),
    };
    match admitted {
        Ok(device) => Some(device),
        Err(failure) => {
            let _ = send(sink, &failure.into_frame()).await;
            None
        }
    }
}

/// Registers the connection and starts its writer.
fn attach<Si>(app: &App, device: DeviceId, sink: Si) -> (Peer, tokio::task::JoinHandle<()>)
where
    Si: Sink<Message> + Unpin + Send + 'static,
{
    let limits = app.config.limits;
    let stop = app.shutdown.child_token();
    let (control, control_rx) = mpsc::channel(limits.control_queue);
    let (data, data_rx) = mpsc::channel(limits.data_queue);
    let peer = Peer::new(Outbox { control, data }, stop.clone());
    app.hub.register(device, peer.clone());
    let ping = app.config.timeouts.ping;
    let writer = tokio::spawn(write(sink, control_rx, data_rx, ping, stop));
    (peer, writer)
}

/// Sends a challenge and checks `hello` and `authenticate`.
async fn handshake<Si, St>(app: &App, sink: &mut Si, stream: &mut St) -> Result<DeviceId, Failure>
where
    Si: Sink<Message> + Unpin,
    St: Stream<Item = Result<Message, axum::Error>> + Unpin,
{
    let mut nonce = [0; 32];
    OsRng.fill_bytes(&mut nonce);
    let nonce = FixedBytes::new(nonce);
    let challenge = ServerFrame::Challenge {
        version: PROTOCOL_VERSION,
        nonce,
    };
    send(sink, &challenge)
        .await
        .map_err(|()| Failure::new(ErrorCode::Internal, "send failed"))?;
    let limit = app.config.limits.max_frame_bytes;
    let steps = async {
        let Some(Ok(ClientFrame::Hello { version, device })) = next_frame(stream, limit).await
        else {
            return Err(Failure::new(ErrorCode::InvalidFrame, "expected hello"));
        };
        if version != PROTOCOL_VERSION {
            return Err(Failure::new(
                ErrorCode::UnsupportedVersion,
                "unsupported version",
            ));
        }
        let Some(Ok(ClientFrame::Authenticate { signature })) = next_frame(stream, limit).await
        else {
            return Err(Failure::new(
                ErrorCode::InvalidFrame,
                "expected authenticate",
            ));
        };
        wici_crypto::verify_challenge(&device, &nonce, &signature)
            .map_err(|_| Failure::new(ErrorCode::Unauthenticated, "bad signature"))?;
        Ok(device)
    };
    tokio::time::timeout(app.config.timeouts.auth, steps)
        .await
        .map_err(|_| Failure::new(ErrorCode::Unauthenticated, "authentication timed out"))?
}

/// Reads frames until close, idle timeout, or `stop`. Any message, including
/// a ping or pong, restarts the idle timer.
async fn read<St>(connection: &mut Connection, stream: &mut St, app: &App, stop: &CancellationToken)
where
    St: Stream<Item = Result<Message, axum::Error>> + Unpin,
{
    let limit = app.config.limits.max_frame_bytes;
    let idle = app.config.timeouts.idle;
    loop {
        let next = tokio::select! {
            () = stop.cancelled() => return,
            next = tokio::time::timeout(idle, stream.next()) => next,
        };
        let Ok(message) = next else { return };
        let result = match classify(message, limit) {
            Incoming::Frame(Ok(frame)) => connection.handle(frame).await,
            Incoming::Frame(Err(failure)) => Err(failure),
            Incoming::Keepalive => Ok(()),
            Incoming::Closed => return,
        };
        if let Err(failure) = result {
            connection.fail(failure);
        }
    }
}

/// What one WebSocket message means to the reader.
enum Incoming {
    /// A client frame, or why it is invalid.
    Frame(Result<ClientFrame, Failure>),
    /// A ping or pong.
    Keepalive,
    /// The connection ended.
    Closed,
}

/// Decodes one message from the stream. `None` means the stream ended.
fn classify(message: Option<Result<Message, axum::Error>>, limit: usize) -> Incoming {
    match message {
        Some(Ok(Message::Text(text))) => {
            Incoming::Frame(decode(text.as_str(), limit).map_err(|error| {
                Failure::new(ErrorCode::InvalidFrame, "invalid frame").detail(&error.to_string())
            }))
        }
        Some(Ok(Message::Binary(_))) => Incoming::Frame(Err(Failure::new(
            ErrorCode::InvalidFrame,
            "binary frames unsupported",
        ))),
        Some(Ok(Message::Ping(_) | Message::Pong(_))) => Incoming::Keepalive,
        Some(Ok(Message::Close(_)) | Err(_)) | None => Incoming::Closed,
    }
}

/// Next client frame. `None` when the connection ends. Skips pings and pongs.
async fn next_frame<St>(stream: &mut St, limit: usize) -> Option<Result<ClientFrame, Failure>>
where
    St: Stream<Item = Result<Message, axum::Error>> + Unpin,
{
    loop {
        match classify(stream.next().await, limit) {
            Incoming::Frame(frame) => return Some(frame),
            Incoming::Keepalive => {}
            Incoming::Closed => return None,
        }
    }
}

async fn send<Si: Sink<Message> + Unpin>(sink: &mut Si, frame: &ServerFrame) -> Result<(), ()> {
    let text = encode(frame).map_err(|_| ())?;
    sink.send(Message::Text(text.into())).await.map_err(|_| ())
}

/// Writes queued frames, control first, and pings on an interval.
pub(crate) async fn write<Si: Sink<Message> + Unpin>(
    mut sink: Si,
    mut control: mpsc::Receiver<ServerFrame>,
    mut data: mpsc::Receiver<ServerFrame>,
    ping: Duration,
    stop: CancellationToken,
) {
    let mut ticker = tokio::time::interval(ping);
    ticker.reset();
    loop {
        let sent = tokio::select! {
            biased;
            () = stop.cancelled() => break,
            Some(frame) = control.recv() => send(&mut sink, &frame).await,
            Some(frame) = data.recv() => send(&mut sink, &frame).await,
            _ = ticker.tick() => sink.send(Message::Ping(Vec::new().into())).await.map_err(|_| ()),
        };
        if sent.is_err() {
            break;
        }
    }
    stop.cancel();
    let _ = sink.close().await;
}

#[cfg(test)]
mod tests {
    use tokio_util::sync::PollSender;
    use wici_protocol::{Blob, PairId};

    use super::*;

    fn live(byte: u8) -> ServerFrame {
        ServerFrame::Live {
            pair: PairId::from_bytes([0; 16]),
            sealed: Blob::new(vec![byte]),
        }
    }

    fn text_of(message: Message) -> ServerFrame {
        let Message::Text(text) = message else {
            panic!("not text")
        };
        decode(text.as_str(), usize::MAX).unwrap()
    }

    #[tokio::test]
    async fn writer_sends_control_before_queued_data() {
        let (control_tx, control_rx) = mpsc::channel(4);
        let (data_tx, data_rx) = mpsc::channel(4);
        data_tx.send(live(1)).await.unwrap();
        data_tx.send(live(2)).await.unwrap();
        control_tx.send(live(9)).await.unwrap();
        let (out_tx, mut out_rx) = mpsc::channel(8);
        let stop = CancellationToken::new();
        let task = tokio::spawn(write(
            PollSender::new(out_tx),
            control_rx,
            data_rx,
            Duration::from_secs(60),
            stop.clone(),
        ));
        let order: Vec<ServerFrame> = vec![
            text_of(out_rx.recv().await.unwrap()),
            text_of(out_rx.recv().await.unwrap()),
            text_of(out_rx.recv().await.unwrap()),
        ];
        assert_eq!(order, [live(9), live(1), live(2)]);
        stop.cancel();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn writer_pings_and_stops_when_the_sink_fails() {
        let (_control_tx, control_rx) = mpsc::channel(1);
        let (_data_tx, data_rx) = mpsc::channel(1);
        let (out_tx, mut out_rx) = mpsc::channel(1);
        let stop = CancellationToken::new();
        let task = tokio::spawn(write(
            PollSender::new(out_tx),
            control_rx,
            data_rx,
            Duration::from_millis(10),
            stop.clone(),
        ));
        assert!(matches!(out_rx.recv().await, Some(Message::Ping(_))));
        drop(out_rx);
        task.await.unwrap();
        assert!(stop.is_cancelled());
    }
}
