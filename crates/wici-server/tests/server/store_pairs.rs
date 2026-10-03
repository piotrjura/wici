use std::time::Duration;

use wici_protocol::{FixedBytes, PairId, PairState};
use wici_server::store::{StoreError, StoreLimits};

use crate::support::{
    TTL, active_pair, claim, device, invite, invite_with_ttl, store, store_with, try_claim,
};

#[tokio::test]
async fn full_pairing_flow() {
    let store = store().await;
    let a = device(&store).await;
    let b = device(&store).await;
    let invitation = invite(&store, &a).await;

    claim(&store, &invitation, &b).await;
    let pairs = store.pairs_of(&a.device_id()).await.unwrap();
    assert_eq!(pairs.len(), 1);
    assert_eq!(pairs[0].state, PairState::Claimed);
    assert_eq!(pairs[0].invitee, Some(b.device_id()));
    let greeting = pairs[0].greeting.clone().unwrap();
    assert_eq!(pairs[0].view_for(&b.device_id()).greeting, None);
    assert_eq!(
        pairs[0].view_for(&a.device_id()).greeting,
        Some(greeting.clone())
    );

    // The inviter derives keys from the stored greeting.
    let keys = invitation.accept(&a, &b.device_id(), &greeting).unwrap();
    assert_eq!(keys.peer(), b.device_id());

    // Lanes exist only for active pairs.
    assert_eq!(store.cursors(&a.device_id()).await.unwrap().len(), 0);
    assert_eq!(store.cursors(&b.device_id()).await.unwrap().len(), 0);
    let active = store
        .approve(&a.device_id(), invitation.pair)
        .await
        .unwrap();
    assert_eq!(active.state, PairState::Active);
    assert_eq!(active.expires_at, None);
    assert_eq!(active.greeting, None);
    assert_eq!(store.cursors(&a.device_id()).await.unwrap().len(), 2);
    assert_eq!(store.cursors(&b.device_id()).await.unwrap().len(), 2);
}

#[tokio::test]
async fn repeated_requests_are_idempotent() {
    let store = store().await;
    let a = device(&store).await;
    let b = device(&store).await;
    let invitation = invite(&store, &a).await;
    let again = store
        .invite(
            &a.device_id(),
            invitation.pair,
            &invitation.claim_hash(),
            TTL,
        )
        .await
        .unwrap();
    assert_eq!(again.state, PairState::Invited);
    claim(&store, &invitation, &b).await;
    claim(&store, &invitation, &b).await;
    store
        .approve(&a.device_id(), invitation.pair)
        .await
        .unwrap();
    let again = store
        .approve(&a.device_id(), invitation.pair)
        .await
        .unwrap();
    assert_eq!(again.state, PairState::Active);
    store.unpair(&b.device_id(), invitation.pair).await.unwrap();
    let again = store.unpair(&a.device_id(), invitation.pair).await.unwrap();
    assert_eq!(again.state, PairState::Revoked);
}

#[tokio::test]
async fn invitation_id_reuse_with_other_data_conflicts() {
    let store = store().await;
    let a = device(&store).await;
    let b = device(&store).await;
    let invitation = invite(&store, &a).await;
    let other_hash = FixedBytes::new([9; 32]);
    let result = store
        .invite(&a.device_id(), invitation.pair, &other_hash, TTL)
        .await;
    assert!(matches!(result, Err(StoreError::Conflict)));
    let hash = invitation.claim_hash();
    let result = store
        .invite(&b.device_id(), invitation.pair, &hash, TTL)
        .await;
    assert!(matches!(result, Err(StoreError::Conflict)));
}

#[tokio::test]
async fn claim_needs_the_secret_and_another_device() {
    let store = store().await;
    let a = device(&store).await;
    let b = device(&store).await;
    let invitation = invite(&store, &a).await;
    let wrong = Some(FixedBytes::new([1; 32]));
    let result = try_claim(&store, &invitation, &b, wrong, TTL).await;
    assert!(matches!(result, Err(StoreError::Forbidden)));
    let result = try_claim(&store, &invitation, &a, None, TTL).await;
    assert!(
        matches!(result, Err(StoreError::Forbidden)),
        "own invitation"
    );
}

#[tokio::test]
async fn claimed_invitation_cannot_be_stolen() {
    let store = store().await;
    let a = device(&store).await;
    let b = device(&store).await;
    let thief = device(&store).await;
    let invitation = invite(&store, &a).await;
    claim(&store, &invitation, &b).await;
    let result = try_claim(&store, &invitation, &thief, None, TTL).await;
    assert!(matches!(result, Err(StoreError::Forbidden)));
}

#[tokio::test]
async fn only_the_inviter_approves_a_claimed_pair() {
    let store = store().await;
    let a = device(&store).await;
    let b = device(&store).await;
    let invitation = invite(&store, &a).await;
    let early = store.approve(&a.device_id(), invitation.pair).await;
    assert!(
        matches!(early, Err(StoreError::Forbidden)),
        "not claimed yet"
    );
    claim(&store, &invitation, &b).await;
    let result = store.approve(&b.device_id(), invitation.pair).await;
    assert!(matches!(result, Err(StoreError::Forbidden)));
}

#[tokio::test]
async fn expired_invitation_cannot_be_claimed() {
    let store = store().await;
    let a = device(&store).await;
    let b = device(&store).await;
    let invitation = invite_with_ttl(&store, &a, Duration::ZERO).await;
    let result = try_claim(&store, &invitation, &b, None, TTL).await;
    assert!(matches!(result, Err(StoreError::Expired)));
    let pairs = store.pairs_of(&a.device_id()).await.unwrap();
    assert_eq!(pairs[0].state, PairState::Expired, "expiry is saved");
}

#[tokio::test]
async fn expired_claim_cannot_be_approved() {
    let store = store().await;
    let a = device(&store).await;
    let b = device(&store).await;
    let invitation = invite(&store, &a).await;
    try_claim(&store, &invitation, &b, None, Duration::ZERO)
        .await
        .unwrap();
    let result = store.approve(&a.device_id(), invitation.pair).await;
    assert!(matches!(result, Err(StoreError::Expired)));
}

#[tokio::test]
async fn sweeper_expires_due_pairs_only() {
    let store = store().await;
    let a = device(&store).await;
    let due = invite_with_ttl(&store, &a, Duration::ZERO).await;
    let fresh = invite(&store, &a).await;
    let expired = store.expire_due(10).await.unwrap();
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].id, due.pair);
    assert_eq!(expired[0].state, PairState::Expired);
    assert_eq!(store.expire_due(10).await.unwrap().len(), 0);
    let pairs = store.pairs_of(&a.device_id()).await.unwrap();
    assert!(
        pairs
            .iter()
            .any(|p| p.id == fresh.pair && p.state == PairState::Invited)
    );
}

#[tokio::test]
async fn open_pairs_per_device_are_limited() {
    let limits = StoreLimits {
        max_pairs_per_device: 2,
        max_pending_per_lane: 8,
    };
    let store = store_with(limits).await;
    let a = device(&store).await;
    invite(&store, &a).await;
    invite(&store, &a).await;
    let third = wici_crypto::Invitation::create(&a, String::new(), wici_protocol::Timestamp(0));
    let result = store
        .invite(&a.device_id(), third.pair, &third.claim_hash(), TTL)
        .await;
    assert!(matches!(result, Err(StoreError::LimitExceeded)));
}

#[tokio::test]
async fn strangers_cannot_unpair_and_unknown_pairs_are_not_found() {
    let store = store().await;
    let (pair, _, _) = active_pair(&store).await;
    let stranger = device(&store).await.device_id();
    assert!(matches!(
        store.unpair(&stranger, pair).await,
        Err(StoreError::Forbidden)
    ));
    let unknown = store.approve(&stranger, PairId::generate()).await;
    assert!(matches!(unknown, Err(StoreError::NotFound)));
}

#[tokio::test]
async fn revoked_pair_has_no_lanes_and_never_reactivates() {
    let store = store().await;
    let (pair, a, b) = active_pair(&store).await;
    store.unpair(&a, pair).await.unwrap();
    assert_eq!(store.cursors(&b).await.unwrap().len(), 0);
    assert!(matches!(
        store.approve(&a, pair).await,
        Err(StoreError::Forbidden)
    ));
}

#[tokio::test]
async fn last_seen_is_recorded() {
    let store = store().await;
    let a = device(&store).await;
    assert!(store.last_seen(&a.device_id()).await.unwrap().is_some());
    let unknown = wici_crypto::DeviceKeys::generate().device_id();
    assert_eq!(store.last_seen(&unknown).await.unwrap(), None);
}
