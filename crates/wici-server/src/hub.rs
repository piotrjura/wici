//! Registry of connected devices and their outgoing queues.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use wici_protocol::{DeviceId, ServerFrame};

/// Outgoing queues of one connection. Control frames go first.
#[derive(Debug, Clone)]
pub(crate) struct Outbox {
    pub(crate) control: mpsc::Sender<ServerFrame>,
    pub(crate) data: mpsc::Sender<ServerFrame>,
}

/// One live connection.
#[derive(Debug, Clone)]
pub(crate) struct Peer {
    id: u64,
    pub(crate) outbox: Outbox,
    /// Wakes the delivery loop.
    pub(crate) wake: Arc<Notify>,
    /// Set when the device's pairs changed.
    pub(crate) pairs_changed: Arc<AtomicBool>,
    /// Ends the connection.
    pub(crate) stop: CancellationToken,
}

impl Peer {
    pub(crate) fn new(outbox: Outbox, stop: CancellationToken) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self {
            id: NEXT.fetch_add(1, Ordering::Relaxed),
            outbox,
            wake: Arc::new(Notify::new()),
            pairs_changed: Arc::new(AtomicBool::new(false)),
            stop,
        }
    }

    /// Queues a control frame. A full queue means the device is too slow, so
    /// the connection ends; the device resyncs from storage on reconnect.
    pub(crate) fn send_control(&self, frame: ServerFrame) {
        if self.outbox.control.try_send(frame).is_err() {
            self.stop.cancel();
        }
    }

    /// Queues a live update. Dropped if the queue is full.
    pub(crate) fn send_live(&self, frame: ServerFrame) {
        let _ = self.outbox.data.try_send(frame);
    }
}

/// Connected devices. One connection per device.
#[derive(Debug, Default)]
pub(crate) struct Hub {
    peers: Mutex<HashMap<DeviceId, Peer>>,
}

impl Hub {
    fn peers(&self) -> MutexGuard<'_, HashMap<DeviceId, Peer>> {
        // No code panics while holding the lock, so the map stays consistent.
        self.peers.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Registers a connection and ends the device's previous one.
    pub(crate) fn register(&self, device: DeviceId, peer: Peer) {
        if let Some(old) = self.peers().insert(device, peer) {
            old.stop.cancel();
        }
    }

    /// Removes a connection unless a newer one replaced it.
    pub(crate) fn unregister(&self, device: &DeviceId, peer: &Peer) {
        let mut peers = self.peers();
        if peers
            .get(device)
            .is_some_and(|current| current.id == peer.id)
        {
            peers.remove(device);
        }
    }

    /// The device's connection, if online.
    pub(crate) fn get(&self, device: &DeviceId) -> Option<Peer> {
        self.peers().get(device).cloned()
    }

    /// Wakes the device's delivery loop, if online.
    pub(crate) fn wake(&self, device: &DeviceId) {
        if let Some(peer) = self.get(device) {
            peer.wake.notify_one();
        }
    }

    /// Ends every connection.
    pub(crate) fn stop_all(&self) {
        for peer in self.peers().values() {
            peer.stop.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_util::FutureExt;
    use wici_protocol::{ErrorCode, PairId};

    use super::*;

    fn peer(
        capacity: usize,
    ) -> (
        Peer,
        mpsc::Receiver<ServerFrame>,
        mpsc::Receiver<ServerFrame>,
    ) {
        let (control, control_rx) = mpsc::channel(capacity);
        let (data, data_rx) = mpsc::channel(capacity);
        (
            Peer::new(Outbox { control, data }, CancellationToken::new()),
            control_rx,
            data_rx,
        )
    }

    fn frame() -> ServerFrame {
        ServerFrame::Error {
            code: ErrorCode::Internal,
            message: String::new(),
            pair: Some(PairId::generate()),
            id: None,
            artifact: None,
        }
    }

    #[test]
    fn new_connection_replaces_and_stops_the_old_one() {
        let hub = Hub::default();
        let device = DeviceId::from_bytes([1; 32]);
        let (old, _, _) = peer(1);
        let (new, _, _) = peer(1);
        hub.register(device, old.clone());
        hub.register(device, new.clone());
        assert!(old.stop.is_cancelled());
        hub.unregister(&device, &old);
        assert!(
            hub.get(&device).is_some(),
            "stale unregister keeps the new one"
        );
        hub.unregister(&device, &new);
        assert!(hub.get(&device).is_none());
    }

    #[test]
    fn full_control_queue_stops_the_connection() {
        let (peer, _control, _data) = peer(1);
        peer.send_control(frame());
        assert!(!peer.stop.is_cancelled());
        peer.send_control(frame());
        assert!(peer.stop.is_cancelled());
    }

    #[test]
    fn full_data_queue_drops_live_updates() {
        let (peer, _control, mut data) = peer(1);
        peer.send_live(frame());
        peer.send_live(frame());
        assert!(!peer.stop.is_cancelled());
        assert!(data.try_recv().is_ok());
        assert!(data.try_recv().is_err());
    }

    #[test]
    fn wake_and_stop_all_reach_registered_peers() {
        let hub = Hub::default();
        let device = DeviceId::from_bytes([2; 32]);
        let (peer, _, _) = peer(1);
        hub.register(device, peer.clone());
        hub.wake(&device);
        hub.wake(&DeviceId::from_bytes([3; 32]));
        assert!(peer.wake.notified().now_or_never().is_some());
        hub.stop_all();
        assert!(peer.stop.is_cancelled());
    }
}
