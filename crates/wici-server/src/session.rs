//! One WebSocket connection: authentication, reading, writing.

use std::sync::Arc;

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
    let writer = tokio::spawn(write(sink, control_rx, data_rx, app.config.timeouts, stop));
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
    timeouts: crate::Timeouts,
    stop: CancellationToken,
) {
    let mut ticker = tokio::time::interval(timeouts.ping);
    ticker.reset();
    loop {
        let frame = tokio::select! {
            biased;
            () = stop.cancelled() => break,
            Some(frame) = control.recv() => Some(frame),
            Some(frame) = data.recv() => Some(frame),
            _ = ticker.tick() => None,
        };
        let sending = async {
            if let Some(frame) = frame {
                send(&mut sink, &frame).await
            } else {
                sink.send(Message::Ping(Vec::new().into()))
                    .await
                    .map_err(|_| ())
            }
        };
        let sent = tokio::select! {
            biased;
            () = stop.cancelled() => break,
            result = tokio::time::timeout(timeouts.idle, sending) => result,
        };
        if !matches!(sent, Ok(Ok(()))) {
            break;
        }
    }
    stop.cancel();
    // Drop the socket. A close handshake can block on the same stalled peer.
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tokio::sync::Notify;
    use tokio_util::sync::PollSender;
    use wici_protocol::{Blob, PairId};

    use super::*;

    #[derive(Clone, Copy)]
    enum Stall {
        Ready,
        Flush,
        Close,
    }

    struct StalledSink {
        stall: Stall,
        polled: Arc<Notify>,
    }

    impl Sink<Message> for StalledSink {
        type Error = ();
        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
            if matches!(self.stall, Stall::Ready) {
                self.polled.notify_one();
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        }
        fn start_send(self: Pin<&mut Self>, _: Message) -> Result<(), ()> {
            Ok(())
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
            if matches!(self.stall, Stall::Flush) {
                self.polled.notify_one();
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        }
        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
            if matches!(self.stall, Stall::Close) {
                self.polled.notify_one();
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        }
    }

    fn writer_timeouts(ping: Duration) -> crate::Timeouts {
        crate::Timeouts {
            ping,
            idle: Duration::from_secs(1),
            ..crate::Timeouts::default()
        }
    }

    async fn cancelled_writer_finishes(stall: Stall) {
        let (control_tx, control_rx) = mpsc::channel(1);
        let (_data_tx, data_rx) = mpsc::channel(1);
        control_tx.send(live(1)).await.unwrap();
        let polled = Arc::new(Notify::new());
        let stop = CancellationToken::new();
        if matches!(stall, Stall::Close) {
            stop.cancel();
        }
        let mut task = tokio::spawn(write(
            StalledSink {
                stall,
                polled: Arc::clone(&polled),
            },
            control_rx,
            data_rx,
            writer_timeouts(Duration::from_secs(60)),
            stop.clone(),
        ));
        if !matches!(stall, Stall::Close) {
            tokio::time::timeout(Duration::from_secs(1), polled.notified())
                .await
                .unwrap();
        }
        stop.cancel();
        let result = tokio::time::timeout(Duration::from_millis(250), &mut task).await;
        if result.is_err() {
            task.abort();
            let _ = task.await;
        }
        assert!(result.is_ok(), "writer ignored cancellation");
        result.unwrap().unwrap();
    }

    #[tokio::test]
    async fn writer_cancels_a_blocked_send() {
        cancelled_writer_finishes(Stall::Ready).await;
    }

    #[tokio::test]
    async fn writer_cancels_a_blocked_flush() {
        cancelled_writer_finishes(Stall::Flush).await;
    }

    #[tokio::test]
    async fn writer_cancels_a_blocked_close() {
        cancelled_writer_finishes(Stall::Close).await;
    }

    #[tokio::test]
    async fn writer_deadline_stops_a_stalled_ping() {
        let (_control, control_rx) = mpsc::channel(1);
        let (_data, data_rx) = mpsc::channel(1);
        let stop = CancellationToken::new();
        let polled = Arc::new(Notify::new());
        let task = tokio::spawn(write(
            StalledSink {
                stall: Stall::Flush,
                polled: Arc::clone(&polled),
            },
            control_rx,
            data_rx,
            crate::Timeouts {
                ping: Duration::from_millis(5),
                idle: Duration::from_millis(20),
                ..crate::Timeouts::default()
            },
            stop.clone(),
        ));
        tokio::time::timeout(Duration::from_secs(1), polled.notified())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        assert!(stop.is_cancelled());
    }

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
            writer_timeouts(Duration::from_secs(60)),
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
            writer_timeouts(Duration::from_millis(10)),
            stop.clone(),
        ));
        assert!(matches!(out_rx.recv().await, Some(Message::Ping(_))));
        drop(out_rx);
        task.await.unwrap();
        assert!(stop.is_cancelled());
    }
}
