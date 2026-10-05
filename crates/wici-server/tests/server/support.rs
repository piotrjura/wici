//! Shared test setup.

use std::time::Duration;

use wici_crypto::{DeviceKeys, Invitation};
use wici_protocol::{DeviceId, FixedBytes, PairId, Timestamp};
use wici_server::store::{Claim, PairRecord, Store, StoreLimits, StoreResult};

pub(crate) const LIMITS: StoreLimits = StoreLimits {
    max_pairs_per_device: 4,
    max_pending_per_lane: 8,
};

pub(crate) const TTL: Duration = Duration::from_secs(60);

pub(crate) use wici_testkit::Backend;

/// Runs each listed `async fn name(backend: Backend)` once per backend, as
/// the tests `name::postgres` and `name::sqlite`.
macro_rules! on_every_backend {
    ($($test:ident),+ $(,)?) => {
        $(mod $test {
            #[tokio::test]
            async fn postgres() {
                super::$test(crate::support::Backend::Postgres).await;
            }

            #[tokio::test]
            async fn sqlite() {
                super::$test(crate::support::Backend::Sqlite).await;
            }
        })+
    };
}
pub(crate) use on_every_backend;

/// Creates a fresh database and a store connected to it.
pub(crate) async fn store(backend: Backend) -> Store {
    store_with(backend, LIMITS).await
}

/// [`store`] with custom limits.
pub(crate) async fn store_with(backend: Backend, limits: StoreLimits) -> Store {
    let store = wici_testkit::store(backend, limits).await;
    assert_eq!(store.backend(), backend);
    store
}

/// A registered device with real keys.
pub(crate) async fn device(store: &Store) -> DeviceKeys {
    let keys = DeviceKeys::generate();
    store.touch_device(&keys.device_id()).await.unwrap();
    keys
}

/// A fresh store, two devices, and an invitation from the first.
pub(crate) async fn invited(backend: Backend) -> (Store, DeviceKeys, DeviceKeys, Invitation) {
    let store = store(backend).await;
    let a = device(&store).await;
    let b = device(&store).await;
    let invitation = invite(&store, &a).await;
    (store, a, b, invitation)
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
