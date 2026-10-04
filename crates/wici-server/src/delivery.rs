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
        let limits = app.config.limits;
        let (limit, beyond) = take(&cursor, from, limits.delivery_window, limits.fetch_batch);
        more |= beyond;
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

/// Messages to fetch from a lane after `from`, and whether more wait beyond
/// them. Skips lanes with nothing new. A full window waits for an ack, which
/// wakes the next round.
fn take(cursor: &LaneCursor, from: Position, window: u64, batch: u64) -> (u64, bool) {
    let room = room(cursor, from, window);
    if room == 0 {
        return (0, false);
    }
    let waiting = cursor.last.0.saturating_sub(from.0);
    let limit = room.min(batch).min(waiting);
    (limit, waiting > limit)
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
    use proptest::prelude::*;

    use super::*;

    fn cursor(acked: u64, last: u64) -> LaneCursor {
        LaneCursor {
            pair: PairId::from_bytes([0; 16]),
            lane: Lane::Data,
            acked: Position(acked),
            last: Position(last),
        }
    }

    #[test]
    fn room_shrinks_with_unacked_messages() {
        let cursor = cursor(10, 99);
        assert_eq!(room(&cursor, Position(10), 4), 4);
        assert_eq!(room(&cursor, Position(12), 4), 2);
        assert_eq!(room(&cursor, Position(14), 4), 0);
        assert_eq!(room(&cursor, Position(99), 4), 0);
    }

    #[test]
    fn take_skips_empty_lanes_and_reports_what_is_left() {
        // Nothing new after `from`: no fetch.
        assert_eq!(take(&cursor(10, 10), Position(10), 8, 4), (0, false));
        assert_eq!(take(&cursor(10, 12), Position(12), 8, 4), (0, false));
        // Everything fits.
        assert_eq!(take(&cursor(10, 13), Position(10), 8, 4), (3, false));
        // Batch limit: fetch again at once.
        assert_eq!(take(&cursor(10, 30), Position(10), 8, 4), (4, true));
        // Window limit: one more round, which then finds the window full.
        assert_eq!(take(&cursor(10, 30), Position(16), 8, 4), (2, true));
        // Full window: wait for an ack.
        assert_eq!(take(&cursor(10, 30), Position(18), 8, 4), (0, false));
    }

    proptest! {
        /// Rounds stay within limits and stop when nothing can be sent, so
        /// delivery never spins.
        #[test]
        fn take_stays_in_bounds(
            acked in 0..1000u64,
            sent in 0..100u64,
            new in 0..100u64,
            window in 1..64u64,
            batch in 1..64u64,
        ) {
            let lane = cursor(acked, acked + sent + new);
            let from = Position(acked + sent);
            let (limit, more) = take(&lane, from, window, batch);
            prop_assert!(limit <= new && limit <= batch);
            prop_assert!(sent + limit <= window || limit == 0);
            prop_assert_eq!(limit == 0 && more, false);
            prop_assert_eq!(more, sent < window && limit < new);
        }
    }
}
