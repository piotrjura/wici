//! Shared test setup.

use std::time::Duration;

use sqlx::Connection;
use wici_crypto::{DeviceKeys, Invitation};
use wici_protocol::{DeviceId, FixedBytes, PairId, Timestamp};
use wici_server::store::{Claim, PairRecord, Store, StoreLimits, StoreResult};

pub(crate) const LIMITS: StoreLimits = StoreLimits {
    max_pairs_per_device: 4,
    max_pending_per_lane: 8,
};

pub(crate) const TTL: Duration = Duration::from_secs(60);

/// Creates a fresh database and a store connected to it.
pub(crate) async fn store() -> Store {
    store_with(LIMITS).await
}

/// [`store`] with custom limits.
pub(crate) async fn store_with(limits: StoreLimits) -> Store {
    let admin_url = std::env::var("WICI_TEST_DATABASE_URL")
        .expect("WICI_TEST_DATABASE_URL is not set; run tests through scripts/with-postgres.sh");
    let name = format!("wici_{}", uuid::Uuid::now_v7().simple());
    let mut admin = sqlx::PgConnection::connect(&admin_url).await.unwrap();
    sqlx::query(&format!("CREATE DATABASE {name}"))
        .execute(&mut admin)
        .await
        .unwrap();
    let base = admin_url.rsplit_once('/').unwrap().0;
    Store::connect(&format!("{base}/{name}"), 16, limits)
        .await
        .unwrap()
}

/// A registered device with real keys.
pub(crate) async fn device(store: &Store) -> DeviceKeys {
    let keys = DeviceKeys::generate();
    store.touch_device(&keys.device_id()).await.unwrap();
    keys
}

/// An invitation from `inviter`, stored on the server.
pub(crate) async fn invite(store: &Store, inviter: &DeviceKeys) -> Invitation {
    invite_with_ttl(store, inviter, TTL).await
}

/// [`invite`] with a custom time to live.
pub(crate) async fn invite_with_ttl(
    store: &Store,
    inviter: &DeviceKeys,
    ttl: Duration,
) -> Invitation {
    let invitation = Invitation::create(inviter, "wss://test".to_owned(), Timestamp(0));
    store
        .invite(
            &inviter.device_id(),
            invitation.pair,
            &invitation.claim_hash(),
            ttl,
        )
        .await
        .unwrap();
    invitation
}

/// Claims `invitation` as `invitee`, with `hash` replacing the real claim
/// hash if given.
pub(crate) async fn try_claim(
    store: &Store,
    invitation: &Invitation,
    invitee: &DeviceKeys,
    hash: Option<FixedBytes<32>>,
    ttl: Duration,
) -> StoreResult<PairRecord> {
    let hash = hash.unwrap_or_else(|| wici_crypto::claim_hash(&invitation.claim_secret()));
    let greeting = invitation.greet(invitee).unwrap();
    let claim = Claim {
        pair: invitation.pair,
        claim_hash: &hash,
        greeting: &greeting,
    };
    store.claim(&invitee.device_id(), &claim, ttl).await
}

/// Claims `invitation` as `invitee`.
pub(crate) async fn claim(store: &Store, invitation: &Invitation, invitee: &DeviceKeys) {
    try_claim(store, invitation, invitee, None, TTL)
        .await
        .unwrap();
}

/// An active pair of two new devices.
pub(crate) async fn active_pair(store: &Store) -> (PairId, DeviceId, DeviceId) {
    let a = device(store).await;
    let b = device(store).await;
    let invitation = invite(store, &a).await;
    claim(store, &invitation, &b).await;
    store
        .approve(&a.device_id(), invitation.pair)
        .await
        .unwrap();
    (invitation.pair, a.device_id(), b.device_id())
}
