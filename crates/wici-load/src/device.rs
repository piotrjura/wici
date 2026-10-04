//! One simulated device on the raw WebSocket protocol.

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use wici_crypto::DeviceKeys;
use wici_protocol::frame::{decode, encode};
use wici_protocol::{ClientFrame, PROTOCOL_VERSION, PairId, PairInfo, PairState, ServerFrame};

use crate::LoadError;

/// Largest server frame a device accepts.
const MAX_FRAME: usize = 2 * 1024 * 1024;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// An authenticated device connection.
#[derive(Debug)]
pub(crate) struct Device {
    socket: Socket,
    pub(crate) keys: DeviceKeys,
}

impl Device {
    /// Connects as a new device and authenticates.
    pub(crate) async fn connect(url: &str) -> Result<Self, LoadError> {
        let (socket, _) = tokio_tungstenite::connect_async(url)
            .await
            .map_err(|error| LoadError::Connect(error.to_string()))?;
        let mut device = Self {
            socket,
            keys: DeviceKeys::generate(),
        };
        let ServerFrame::Challenge { nonce, .. } = device.recv().await? else {
            return Err(LoadError::Protocol("expected challenge"));
        };
        let hello = ClientFrame::Hello {
            version: PROTOCOL_VERSION,
            device: device.keys.device_id(),
        };
        device.send(&hello).await?;
        let signature = device.keys.sign_challenge(&nonce);
        device
            .send(&ClientFrame::Authenticate { signature })
            .await?;
        let frame = device.recv().await?;
        if matches!(frame, ServerFrame::Welcome { .. }) {
            Ok(device)
        } else {
            Err(refusal(frame, "expected welcome"))
        }
    }

    pub(crate) async fn send(&mut self, frame: &ClientFrame) -> Result<(), LoadError> {
        let text = encode(frame).map_err(|_| LoadError::Protocol("cannot encode"))?;
        self.socket
            .send(Message::Text(text.into()))
            .await
            .map_err(|_| LoadError::Closed)
    }

    /// Next server frame. Pings are answered while reading. Cancel safe.
    pub(crate) async fn recv(&mut self) -> Result<ServerFrame, LoadError> {
        loop {
            let message = self.socket.next().await.ok_or(LoadError::Closed)?;
            match message.map_err(|_| LoadError::Closed)? {
                Message::Text(text) => {
                    return decode(text.as_str(), MAX_FRAME)
                        .map_err(|_| LoadError::Protocol("cannot decode"));
                }
                Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
                Message::Binary(_) | Message::Close(_) => return Err(LoadError::Closed),
            }
        }
    }

    /// Waits for `pair` to reach `state`, skipping other frames.
    pub(crate) async fn wait_pair(
        &mut self,
        pair: PairId,
        state: PairState,
    ) -> Result<PairInfo, LoadError> {
        loop {
            let frame = self.recv().await?;
            if matches!(frame, ServerFrame::Error { .. }) {
                return Err(refusal(frame, "error"));
            }
            if let Some(info) = reached(frame, pair, state) {
                return Ok(info);
            }
        }
    }

    pub(crate) async fn close(mut self) {
        let _ = self.socket.close(None).await;
    }
}

/// The pair update in `frame` if it puts `pair` in `state`.
fn reached(frame: ServerFrame, pair: PairId, state: PairState) -> Option<PairInfo> {
    let ServerFrame::Pair { pair: info } = frame else {
        return None;
    };
    (info.id == pair && info.state == state).then_some(info)
}

/// Turns an error frame into [`LoadError::Rejected`], or anything else into
/// [`LoadError::Protocol`] with `what`.
fn refusal(frame: ServerFrame, what: &'static str) -> LoadError {
    if let ServerFrame::Error { code, message, .. } = frame {
        LoadError::Rejected { code, message }
    } else {
        LoadError::Protocol(what)
    }
}
