use std::collections::{HashMap, HashSet};

use sqlx::Connection;

use wici_protocol::{Blob, DeviceId, Lane, MessageId, PairId, Position};
use wici_server::store::{Accepted, LaneKey, NewMessage, Store, StoreError, StoreLimits};

use crate::support::{active_pair, device, store, store_with};

fn blob(byte: u8) -> Blob {
    Blob::new(vec![byte; 4])
}

/// A message with a new ID.
fn message(pair: PairId, lane: Lane, sealed: &Blob) -> NewMessage<'_> {
    NewMessage {
        pair,
        id: MessageId::generate(),
        lane,
        sealed,
    }
}

async fn send(
    store: &Store,
    from: &DeviceId,
    pair: PairId,
    lane: Lane,
) -> Result<Accepted, StoreError> {
    store.send(from, &message(pair, lane, &blob(1))).await
}

fn positions(deliveries: &[wici_server::store::Delivery]) -> Vec<u64> {
    deliveries.iter().map(|d| d.position.0).collect()
}

#[tokio::test]
async fn messages_get_ordered_positions_per_lane() {
    let store = store().await;
    let (pair, a, b) = active_pair(&store).await;
    for (lane, expected) in [(Lane::Control, 1), (Lane::Control, 2), (Lane::Data, 1)] {
        let accepted = send(&store, &a, pair, lane).await.unwrap();
        assert_eq!(accepted.position, Position(expected));
        assert_eq!(accepted.recipient, b);
        assert!(!accepted.duplicate);
    }
    let key = LaneKey {
        pair,
        recipient: b,
        lane: Lane::Control,
    };
    let delivered = store.fetch(&key, Position(0), 10).await.unwrap();
    assert_eq!(positions(&delivered), [1, 2]);
    assert!(delivered.iter().all(|d| d.accepted_at.0 > 0));
    assert_eq!(
        positions(&store.fetch(&key, Position(1), 10).await.unwrap()),
        [2]
    );
}

#[tokio::test]
async fn retry_with_same_content_returns_the_original_position() {
    let store = store().await;
    let (pair, a, _) = active_pair(&store).await;
    let sealed = blob(1);
    let message = message(pair, Lane::Control, &sealed);
    let first = store.send(&a, &message).await.unwrap();
    let again = store.send(&a, &message).await.unwrap();
    assert_eq!(again.position, first.position);
    assert!(again.duplicate);
}

#[tokio::test]
async fn reused_id_with_other_content_or_lane_conflicts() {
    let store = store().await;
    let (pair, a, _) = active_pair(&store).await;
    let (one, two) = (blob(1), blob(2));
    let original = message(pair, Lane::Control, &one);
    store.send(&a, &original).await.unwrap();
    let other_content = NewMessage {
        sealed: &two,
        ..original
    };
    let other_lane = NewMessage {
        lane: Lane::Data,
        ..original
    };
    for retry in [other_content, other_lane] {
        assert!(matches!(
            store.send(&a, &retry).await,
            Err(StoreError::Conflict)
        ));
    }
}

#[tokio::test]
async fn retry_after_ack_is_still_deduplicated() {
    let store = store().await;
    let (pair, a, b) = active_pair(&store).await;
    let sealed = blob(1);
    let message = message(pair, Lane::Data, &sealed);
    store.send(&a, &message).await.unwrap();
    let key = LaneKey {
        pair,
        recipient: b,
        lane: Lane::Data,
    };
    store.ack(&key, Position(1)).await.unwrap();
    assert!(store.send(&a, &message).await.unwrap().duplicate);
    assert_eq!(
        store.fetch(&key, Position(0), 10).await.unwrap().len(),
        0,
        "no redelivery"
    );
}

#[tokio::test]
async fn concurrent_senders_never_skip_or_reuse_positions() {
    let store = store().await;
    let (pair, a, b) = active_pair(&store).await;
    let tasks: Vec<_> = (0..40)
        .map(|i| {
            let store = store.clone();
            let from = if i % 2 == 0 { a } else { b };
            tokio::spawn(async move { (from, send(&store, &from, pair, Lane::Data).await) })
        })
        .collect();
    let mut seen = HashMap::<DeviceId, HashSet<u64>>::new();
    let mut full = 0;
    for task in tasks {
        let (from, result) = task.await.unwrap();
        match result {
            Ok(accepted) => assert!(seen.entry(from).or_default().insert(accepted.position.0)),
            Err(StoreError::LimitExceeded) => full += 1,
            Err(other) => panic!("{other}"),
        }
    }
    for positions in seen.values() {
        let expected: HashSet<u64> = (1..=positions.len() as u64).collect();
        assert_eq!(*positions, expected, "contiguous from 1");
    }
    assert_eq!(full, 40 - seen.values().map(HashSet::len).sum::<usize>());
}

#[tokio::test]
async fn full_lane_rejects_until_acked() {
    let limits = StoreLimits {
        max_pairs_per_device: 4,
        max_pending_per_lane: 2,
    };
    let store = store_with(limits).await;
    let (pair, a, b) = active_pair(&store).await;
    send(&store, &a, pair, Lane::Data).await.unwrap();
    send(&store, &a, pair, Lane::Data).await.unwrap();
    let full = send(&store, &a, pair, Lane::Data).await;
    assert!(matches!(full, Err(StoreError::LimitExceeded)));
    assert!(
        send(&store, &a, pair, Lane::Control).await.is_ok(),
        "lanes are independent"
    );
    store
        .ack(
            &LaneKey {
                pair,
                recipient: b,
                lane: Lane::Data,
            },
            Position(1),
        )
        .await
        .unwrap();
    let next = send(&store, &a, pair, Lane::Data).await.unwrap();
    assert_eq!(next.position, Position(3), "no gap after rejection");
}

#[tokio::test]
async fn ack_drops_payloads_and_validates_position() {
    let store = store().await;
    let (pair, a, b) = active_pair(&store).await;
    for _ in 0..3 {
        send(&store, &a, pair, Lane::Data).await.unwrap();
    }
    let key = LaneKey {
        pair,
        recipient: b,
        lane: Lane::Data,
    };
    store.ack(&key, Position(2)).await.unwrap();
    store.ack(&key, Position(1)).await.unwrap();
    assert_eq!(
        positions(&store.fetch(&key, Position(0), 10).await.unwrap()),
        [3]
    );
    let cursors = store.cursors(&b).await.unwrap();
    assert!(
        cursors
            .iter()
            .any(|c| c.lane == Lane::Data && c.acked == Position(2))
    );
    assert!(matches!(
        store.ack(&key, Position(4)).await,
        Err(StoreError::Forbidden)
    ));
    let wrong = LaneKey {
        recipient: a,
        ..key
    };
    assert!(matches!(
        store.ack(&wrong, Position(1)).await,
        Err(StoreError::Forbidden)
    ));
    assert!(matches!(
        store.ack(&key, Position(u64::MAX)).await,
        Err(StoreError::Forbidden)
    ));
}

#[tokio::test]
async fn non_members_and_inactive_pairs_cannot_send() {
    let store = store().await;
    let (pair, a, _) = active_pair(&store).await;
    let stranger = device(&store).await.device_id();
    let forbidden = send(&store, &stranger, pair, Lane::Data).await;
    assert!(matches!(forbidden, Err(StoreError::Forbidden)));
    store.unpair(&a, pair).await.unwrap();
    assert!(matches!(
        send(&store, &a, pair, Lane::Data).await,
        Err(StoreError::Forbidden)
    ));
    let unknown = send(&store, &a, PairId::generate(), Lane::Data).await;
    assert!(matches!(unknown, Err(StoreError::NotFound)));
}

#[tokio::test]
async fn revoke_deletes_queued_messages() {
    let store = store().await;
    let (pair, a, b) = active_pair(&store).await;
    send(&store, &a, pair, Lane::Data).await.unwrap();
    store.unpair(&b, pair).await.unwrap();
    let key = LaneKey {
        pair,
        recipient: b,
        lane: Lane::Data,
    };
    assert_eq!(store.fetch(&key, Position(0), 10).await.unwrap().len(), 0);
}

#[tokio::test]
async fn recipient_lanes_are_found_through_an_index() {
    let url = wici_testkit::database_url().await;
    let store = Store::connect(&url, 1, crate::support::LIMITS)
        .await
        .unwrap();
    let (_, _, b) = active_pair(&store).await;
    let mut conn = sqlx::PgConnection::connect(&url).await.unwrap();
    sqlx::query("SET enable_seqscan = off")
        .execute(&mut conn)
        .await
        .unwrap();
    let plan: Vec<String> =
        sqlx::query_scalar("EXPLAIN SELECT acked_position FROM lanes WHERE recipient = $1")
            .bind(b.as_bytes().as_slice())
            .fetch_all(&mut conn)
            .await
            .unwrap();
    let plan = plan.join("\n");
    assert!(plan.contains("lanes_recipient"), "{plan}");
}
