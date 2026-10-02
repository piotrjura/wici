//! Pushes stored messages to a connected recipient, in order, with a
//! bounded number in flight per lane.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use wici_protocol::{DeviceId, Lane, PairId, Position, ServerFrame};

use crate::App;
use crate::hub::Outbox;
use crate::store::{Delivery, LaneCursor, LaneKey, StoreResult};

/// Pause after a storage error before the next round.
const RETRY_DELAY: Duration = Duration::from_secs(1);

/// Runs until `stop`. Each wake starts a round. Sent positions reset on
/// reconnect, so unacknowledged messages are sent again.
pub(crate) async fn run(
    app: Arc<App>,
    device: DeviceId,
    wake: Arc<Notify>,
    outbox: Outbox,
    stop: CancellationToken,
) {
    let mut sent = HashMap::new();
    loop {
        let more = match round(&app, &device, &outbox, &mut sent).await {
            Ok(more) => more,
            Err(error) => {
                tracing::warn!(%error, "delivery round failed");
                if !pause(&stop, RETRY_DELAY).await {
                    return;
                }
                true
            }
        };
        if !more && !wait(&stop, &wake).await {
            return;
        }
    }
}

/// Sleeps for `delay`. `false` if stopped first.
async fn pause(stop: &CancellationToken, delay: Duration) -> bool {
    tokio::select! {
        () = stop.cancelled() => false,
        () = tokio::time::sleep(delay) => true,
    }
}

/// Waits for a wake. `false` if stopped first.
async fn wait(stop: &CancellationToken, wake: &Notify) -> bool {
    tokio::select! {
        () = stop.cancelled() => false,
        () = wake.notified() => true,
    }
}

/// Sends what fits in each lane's window. `true` if a lane may have more.
async fn round(
    app: &App,
    device: &DeviceId,
    outbox: &Outbox,
    sent: &mut HashMap<(PairId, Lane), Position>,
) -> StoreResult<bool> {
    let cursors = app.store.cursors(device).await?;
    sent.retain(|key, _| cursors.iter().any(|c| (c.pair, c.lane) == *key));
    let mut more = false;
    for cursor in cursors {
        let from = sent
            .get(&(cursor.pair, cursor.lane))
            .copied()
            .unwrap_or(cursor.acked)
            .max(cursor.acked);
        let limit = room(&cursor, from, app.config.limits.delivery_window)
            .min(app.config.limits.fetch_batch);
        if limit == 0 {
            continue;
        }
        let key = LaneKey {
            pair: cursor.pair,
            recipient: *device,
            lane: cursor.lane,
        };
        let batch = app
            .store
            .fetch(&key, from, i64::try_from(limit).unwrap_or(i64::MAX))
            .await?;
        more |= batch.len() as u64 == limit;
        for delivery in batch {
            let position = delivery.position;
            if !push(outbox, delivery).await {
                return Ok(false);
            }
            sent.insert((cursor.pair, cursor.lane), position);
        }
    }
    Ok(more)
}

/// Free slots in a lane's window after `from`.
const fn room(cursor: &LaneCursor, from: Position, window: u64) -> u64 {
    let in_flight = from.0.saturating_sub(cursor.acked.0);
    window.saturating_sub(in_flight)
}

/// Queues a delivery on its lane's queue. `false` if the connection closed.
async fn push(outbox: &Outbox, delivery: Delivery) -> bool {
    let queue = match delivery.lane {
        Lane::Control => &outbox.control,
        Lane::Data => &outbox.data,
    };
    let frame = ServerFrame::Deliver {
        pair: delivery.pair,
        id: delivery.id,
        lane: delivery.lane,
        position: delivery.position,
        accepted_at: delivery.accepted_at,
        sealed: delivery.sealed,
    };
    queue.send(frame).await.is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn room_shrinks_with_unacked_messages() {
        let cursor = LaneCursor {
            pair: PairId::from_bytes([0; 16]),
            lane: Lane::Data,
            acked: Position(10),
        };
        assert_eq!(room(&cursor, Position(10), 4), 4);
        assert_eq!(room(&cursor, Position(12), 4), 2);
        assert_eq!(room(&cursor, Position(14), 4), 0);
        assert_eq!(room(&cursor, Position(99), 4), 0);
    }
}
