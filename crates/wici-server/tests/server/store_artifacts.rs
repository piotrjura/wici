use std::time::Duration;

use wici_crypto::{ArtifactKey, artifact_hash};
use wici_protocol::{ArtifactId, Blob, DeviceId, FixedBytes, PairId};
use wici_server::store::{ArtifactLimits, ChunkUpload, Progress, Store, StoreError};

use crate::support::{Backend, active_pair, device, on_every_backend, store};

const LIMITS: ArtifactLimits = ArtifactLimits {
    max_bytes: 1 << 20,
    max_incomplete: 2,
};

/// A sealed artifact split into chunks of `size` bytes.
struct Sealed {
    id: ArtifactId,
    bytes: Vec<u8>,
    hash: FixedBytes<32>,
}

fn sealed(plaintext_len: usize) -> Sealed {
    let key = ArtifactKey::generate();
    let bytes = key.seal_all(&vec![9; plaintext_len]).unwrap();
    Sealed {
        id: key.id(),
        hash: artifact_hash(&bytes),
        bytes,
    }
}

/// Uploads `artifact.bytes[range]`, clamped to the artifact size.
async fn put(
    store: &Store,
    from: &DeviceId,
    pair: PairId,
    artifact: &Sealed,
    range: std::ops::Range<usize>,
) -> Result<Progress, StoreError> {
    let end = range.end.min(artifact.bytes.len());
    let data = Blob::new(artifact.bytes[range.start..end].to_vec());
    let chunk = ChunkUpload {
        pair,
        artifact: artifact.id,
        total: artifact.bytes.len() as u64,
        hash: &artifact.hash,
        offset: range.start as u64,
        data: &data,
    };
    store.put_chunk(from, &chunk, LIMITS).await
}

async fn upload_in_order_then_download(backend: Backend) {
    let store = store(backend).await;
    let (pair, a, b) = active_pair(&store).await;
    let artifact = sealed(100);
    let total = artifact.bytes.len();
    let first = put(&store, &a, pair, &artifact, 0..50).await.unwrap();
    assert_eq!(
        first,
        Progress {
            received: 50,
            complete: false
        }
    );
    let missing = store.get_chunk(&b, pair, artifact.id, 0).await;
    assert!(
        matches!(missing, Err(StoreError::NotFound)),
        "incomplete is hidden"
    );
    let done = put(&store, &a, pair, &artifact, 50..total).await.unwrap();
    assert_eq!(
        done,
        Progress {
            received: total as u64,
            complete: true
        }
    );

    let chunk = store.get_chunk(&b, pair, artifact.id, 0).await.unwrap();
    assert_eq!((chunk.offset, chunk.total), (0, total as u64));
    let rest = store.get_chunk(&b, pair, artifact.id, 50).await.unwrap();
    let mut joined = chunk.data.into_bytes();
    joined.extend(rest.data.into_bytes());
    assert_eq!(joined, artifact.bytes);
    assert!(matches!(
        store.get_chunk(&b, pair, artifact.id, 7).await,
        Err(StoreError::NotFound)
    ));
}

async fn repeated_chunks_resume_and_gaps_are_rejected(backend: Backend) {
    let store = store(backend).await;
    let (pair, a, _) = active_pair(&store).await;
    let artifact = sealed(100);
    put(&store, &a, pair, &artifact, 0..40).await.unwrap();
    let again = put(&store, &a, pair, &artifact, 0..40).await.unwrap();
    assert_eq!(again.received, 40, "repeat reports progress");
    let gap = put(&store, &a, pair, &artifact, 60..70).await;
    assert!(matches!(gap, Err(StoreError::Forbidden)));
    let empty = put(&store, &a, pair, &artifact, 40..40).await;
    assert!(matches!(empty, Err(StoreError::Forbidden)));
}

async fn hash_mismatch_deletes_the_upload(backend: Backend) {
    let store = store(backend).await;
    let (pair, a, b) = active_pair(&store).await;
    let mut artifact = sealed(10);
    artifact.hash = FixedBytes::new([0; 32]);
    let result = put(&store, &a, pair, &artifact, 0..100).await;
    assert!(matches!(result, Err(StoreError::Conflict)));
    assert!(matches!(
        store.get_chunk(&b, pair, artifact.id, 0).await,
        Err(StoreError::NotFound)
    ));
    let fixed = Sealed {
        hash: artifact_hash(&artifact.bytes),
        ..artifact
    };
    assert!(
        put(&store, &a, pair, &fixed, 0..100)
            .await
            .unwrap()
            .complete,
        "restart works"
    );
}

async fn changed_metadata_conflicts(backend: Backend) {
    let store = store(backend).await;
    let (pair, a, b) = active_pair(&store).await;
    let artifact = sealed(10);
    put(&store, &a, pair, &artifact, 0..5).await.unwrap();
    let other_hash = Sealed {
        hash: FixedBytes::new([1; 32]),
        id: artifact.id,
        bytes: artifact.bytes.clone(),
    };
    assert!(matches!(
        put(&store, &a, pair, &other_hash, 5..10).await,
        Err(StoreError::Conflict)
    ));
    assert!(
        matches!(
            put(&store, &b, pair, &artifact, 5..10).await,
            Err(StoreError::Conflict)
        ),
        "other uploader"
    );
}

async fn size_and_count_limits_apply(backend: Backend) {
    let store = store(backend).await;
    let (pair, a, _) = active_pair(&store).await;
    let huge = sealed(2 << 20);
    assert!(matches!(
        put(&store, &a, pair, &huge, 0..10).await,
        Err(StoreError::LimitExceeded)
    ));
    put(&store, &a, pair, &sealed(10), 0..1).await.unwrap();
    put(&store, &a, pair, &sealed(10), 0..1).await.unwrap();
    let third = put(&store, &a, pair, &sealed(10), 0..1).await;
    assert!(
        matches!(third, Err(StoreError::LimitExceeded)),
        "two unfinished uploads at most"
    );
}

async fn only_members_of_active_pairs_use_artifacts(backend: Backend) {
    let store = store(backend).await;
    let (pair, a, b) = active_pair(&store).await;
    let stranger = device(&store).await.device_id();
    let artifact = sealed(10);
    put(&store, &a, pair, &artifact, 0..100).await.unwrap();
    assert!(matches!(
        put(&store, &stranger, pair, &sealed(10), 0..1).await,
        Err(StoreError::Forbidden)
    ));
    assert!(matches!(
        store.get_chunk(&stranger, pair, artifact.id, 0).await,
        Err(StoreError::Forbidden)
    ));
    let delete = store.delete_artifact(&stranger, pair, artifact.id).await;
    assert!(matches!(delete, Err(StoreError::Forbidden)));
    store.delete_artifact(&b, pair, artifact.id).await.unwrap();
    store.delete_artifact(&b, pair, artifact.id).await.unwrap();
    assert!(matches!(
        store.get_chunk(&a, pair, artifact.id, 0).await,
        Err(StoreError::NotFound)
    ));
}

async fn unpair_and_retention_delete_artifacts(backend: Backend) {
    let store = store(backend).await;
    let (pair, a, _) = active_pair(&store).await;
    put(&store, &a, pair, &sealed(10), 0..100).await.unwrap();
    put(&store, &a, pair, &sealed(10), 0..1).await.unwrap();
    let kept = store
        .expire_artifacts(Duration::from_secs(3600), Duration::from_secs(3600), 10)
        .await
        .unwrap();
    assert_eq!(kept, 0);
    let removed = store
        .expire_artifacts(Duration::ZERO, Duration::ZERO, 10)
        .await
        .unwrap();
    assert_eq!(removed, 2);

    let (pair, a, b) = active_pair(&store).await;
    let artifact = sealed(10);
    put(&store, &a, pair, &artifact, 0..100).await.unwrap();
    store.unpair(&b, pair).await.unwrap();
    assert_eq!(
        store
            .expire_artifacts(Duration::ZERO, Duration::ZERO, 10)
            .await
            .unwrap(),
        0,
        "already gone"
    );
}

on_every_backend!(
    upload_in_order_then_download,
    repeated_chunks_resume_and_gaps_are_rejected,
    hash_mismatch_deletes_the_upload,
    changed_metadata_conflicts,
    size_and_count_limits_apply,
    only_members_of_active_pairs_use_artifacts,
    unpair_and_retention_delete_artifacts,
);
