//! Connection task: connect, authenticate, exchange frames, detect dead
//! connections with pings, reconnect.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::Sleep;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use wici_protocol::frame::{decode, encode};
use wici_protocol::{ClientFrame, PROTOCOL_VERSION, PairInfo, ServerFrame};

use crate::ClientConfig;
use crate::backoff::Backoff;
use crate::inbound;
use crate::model::Event;
use crate::outbound::{self, InFlight};
use crate::shared::Shared;
use crate::transfer::{Request, Waiters};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type Sink = SplitSink<Socket, Message>;
type Stream = SplitStream<Socket>;

/// Largest server frame the client accepts.
const MAX_FRAME: usize = 4 * 1024 * 1024;

/// The connection ended or could not be made. Details are logged.
#[derive(Debug)]
struct Closed;

/// Runs until the client stops.
pub(crate) async fn run(
    shared: Arc<Shared>,
    live: mpsc::Receiver<ClientFrame>,
    requests: mpsc::Receiver<Request>,
) {
    let mut inputs = Inputs { live, requests };
    let config = &shared.config;
    let mut backoff = Backoff::new(config.reconnect_min, config.reconnect_max);
    while !shared.stop.is_cancelled() {
        if let Ok((socket, pairs)) = connect(&shared).await {
            backoff.reset();
            shared.connected.store(true, Ordering::Release);
            shared.emit(Event::Connected).await;
            session(&shared, socket, &pairs, &mut inputs).await;
            shared.connected.store(false, Ordering::Release);
            // Requests that missed the connection fail now, not on timeout.
            while inputs.requests.try_recv().is_ok() {}
            shared.emit(Event::Disconnected).await;
        }
        tokio::select! {
            () = shared.stop.cancelled() => return,
            () = tokio::time::sleep(backoff.next()) => {}
            () = shared.reconnect.notified() => {}
        }
    }
}

async fn connect(shared: &Shared) -> Result<(Socket, Vec<PairInfo>), Closed> {
    let steps = async {
        let (mut socket, _) = tokio_tungstenite::connect_async(shared.config.server_url.as_str())
            .await
            .map_err(|error| tracing::debug!(%error, "connect failed"))
            .map_err(|()| Closed)?;
        let Some(ServerFrame::Challenge { nonce, .. }) = read(&mut socket).await else {
            return Err(Closed);
        };
        let device = shared.keys.device_id();
        let signature = shared.keys.sign_challenge(&nonce);
        write(
            &mut socket,
            &ClientFrame::Hello {
                version: PROTOCOL_VERSION,
                device,
            },
        )
        .await?;
        write(&mut socket, &ClientFrame::Authenticate { signature }).await?;
        match read(&mut socket).await {
            Some(ServerFrame::Welcome { pairs }) => Ok((socket, pairs)),
            other => {
                tracing::warn!(?other, "authentication failed");
                Err(Closed)
            }
        }
    };
    tokio::time::timeout(shared.config.connect_timeout, steps)
        .await
        .map_err(|_| Closed)?
}

/// Next server frame. `None` when the socket closes or sends garbage.
async fn read<S>(stream: &mut S) -> Option<ServerFrame>
where
    S: futures_util::Stream<Item = WsResult> + Unpin,
{
    loop {
        if let Some(frame) = classify(stream.next().await).ok()? {
            return Some(frame);
        }
    }
}

type WsResult = Result<Message, tokio_tungstenite::tungstenite::Error>;

/// What one WebSocket message means: a frame, `None` for a ping or pong, or
/// `Closed` when the socket ends or sends garbage.
fn classify(message: Option<WsResult>) -> Result<Option<ServerFrame>, Closed> {
    match message.ok_or(Closed)?.map_err(|_| Closed)? {
        Message::Text(text) => decode(text.as_str(), MAX_FRAME)
            .map(Some)
            .map_err(|_| Closed),
        Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => Ok(None),
        Message::Binary(_) | Message::Close(_) => Err(Closed),
    }
}

async fn write<S>(sink: &mut S, frame: &ClientFrame) -> Result<(), Closed>
where
    S: futures_util::Sink<Message> + Unpin,
{
    let text = encode(frame).map_err(|_| Closed)?;
    sink.send(Message::Text(text.into()))
        .await
        .map_err(|_| Closed)
}

/// Frames the app asks the connection task to send.
pub(crate) struct Inputs {
    live: mpsc::Receiver<ClientFrame>,
    requests: mpsc::Receiver<Request>,
}

/// Per-connection state. Dropped on disconnect, which fails open requests.
#[derive(Default)]
struct Connection {
    flight: InFlight,
    waiters: Waiters,
}

/// Exchanges frames until the connection ends.
async fn session(shared: &Shared, socket: Socket, pairs: &[PairInfo], inputs: &mut Inputs) {
    let (mut sink, mut stream) = socket.split();
    let mut connection = Connection::default();
    for info in pairs {
        if let Err(error) = inbound::on_pair(shared, info, &mut connection.flight).await {
            tracing::warn!(%error, "cannot apply pair");
        }
    }
    if flush(shared, &mut sink, &mut connection.flight)
        .await
        .is_ok()
    {
        let _ = exchange(shared, &mut sink, &mut stream, &mut connection, inputs).await;
    }
    let _ = sink.close().await;
}

async fn exchange(
    shared: &Shared,
    sink: &mut Sink,
    stream: &mut Stream,
    connection: &mut Connection,
    inputs: &mut Inputs,
) -> Result<(), Closed> {
    let mut timers = Timers::new(&shared.config);
    loop {
        match next_step(shared, stream, inputs, &mut timers).await {
            Step::Stop => return Ok(()),
            Step::Idle => {
                tracing::debug!("connection idle");
                return Err(Closed);
            }
            Step::Message(message) => {
                timers.alive(&shared.config);
                if let Some(frame) = classify(message)? {
                    receive(shared, sink, frame, connection).await?;
                }
            }
            Step::Local(local) => send_local(shared, sink, local, connection).await?,
        }
    }
}

/// Timers of one connection.
struct Timers {
    retry: tokio::time::Interval,
    ping: tokio::time::Interval,
    /// Only server messages restart it. Local work must not hide a dead socket.
    idle: Pin<Box<Sleep>>,
}

impl Timers {
    fn new(config: &ClientConfig) -> Self {
        let mut ping = tokio::time::interval(config.ping);
        ping.reset();
        Self {
            retry: tokio::time::interval(config.retry_interval),
            ping,
            idle: Box::pin(tokio::time::sleep(config.idle)),
        }
    }

    /// The server sent something: restart the idle timer.
    fn alive(&mut self, config: &ClientConfig) {
        self.idle
            .as_mut()
            .reset(tokio::time::Instant::now() + config.idle);
    }
}

/// What the connection does next.
enum Step {
    Stop,
    Idle,
    Message(Option<WsResult>),
    Local(Local),
}

/// Something to send, asked for by a timer or the app.
enum Local {
    Ping,
    Wake,
    Live(ClientFrame),
    Request(Request),
    Retry,
}

async fn next_step(
    shared: &Shared,
    stream: &mut Stream,
    inputs: &mut Inputs,
    timers: &mut Timers,
) -> Step {
    tokio::select! {
        () = shared.stop.cancelled() => Step::Stop,
        () = &mut timers.idle => Step::Idle,
        message = stream.next() => Step::Message(message),
        _ = timers.ping.tick() => Step::Local(Local::Ping),
        () = shared.wake.notified() => Step::Local(Local::Wake),
        Some(frame) = inputs.live.recv() => Step::Local(Local::Live(frame)),
        Some(request) = inputs.requests.recv() => Step::Local(Local::Request(request)),
        _ = timers.retry.tick() => Step::Local(Local::Retry),
    }
}

/// Sends what a local step asks for.
async fn send_local(
    shared: &Shared,
    sink: &mut Sink,
    local: Local,
    connection: &mut Connection,
) -> Result<(), Closed> {
    match local {
        Local::Ping => sink
            .send(Message::Ping(Vec::new().into()))
            .await
            .map_err(|_| Closed),
        Local::Wake => flush(shared, sink, &mut connection.flight).await,
        Local::Live(frame) => write(sink, &frame).await,
        Local::Request(request) => {
            write(sink, &request.frame).await?;
            connection.waiters.insert(request.artifact, request.reply);
            Ok(())
        }
        Local::Retry => {
            connection.flight.retry();
            flush(shared, sink, &mut connection.flight).await
        }
    }
}

async fn receive(
    shared: &Shared,
    sink: &mut Sink,
    frame: ServerFrame,
    connection: &mut Connection,
) -> Result<(), Closed> {
    let Some(frame) = connection.waiters.route(frame) else {
        return Ok(());
    };
    match inbound::handle(shared, frame, &mut connection.flight).await {
        Ok(Some(reply)) => write(sink, &reply).await,
        Ok(None) => Ok(()),
        Err(error) => {
            // Local storage failed: reconnect so the server resends.
            tracing::warn!(%error, "cannot handle frame");
            Err(Closed)
        }
    }
}

async fn flush(shared: &Shared, sink: &mut Sink, flight: &mut InFlight) -> Result<(), Closed> {
    let frames = outbound::due_frames(shared, flight)
        .await
        .map_err(|error| {
            tracing::warn!(%error, "cannot read outbox");
            Closed
        })?;
    for frame in &frames {
        write(sink, frame).await?;
    }
    Ok(())
}
