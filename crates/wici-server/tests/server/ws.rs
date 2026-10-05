use std::time::Duration;

use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use wici_crypto::{DeviceKeys, Invitation};
use wici_protocol::{
    Blob, Body, ClientFrame, ErrorCode, Lane, LiveBody, MessageId, PROTOCOL_VERSION, PairState,
    Position, ServerFrame, StreamId, Timestamp,
};

use wici_testkit::{Backend, TestServer as Server};

use crate::support::on_every_backend;

use crate::client::{Client, Paired, expect_pair, next, open, paired, paired_on, put};

fn event(text: &str) -> Body {
    Body::Event {
        stream: StreamId::from_bytes([1; 16]),
        data: json!(text),
    }
}

/// Seals `body` from `from_keys` and sends it as `id` on `lane`.
async fn send_body(
    client: &mut Client,
    keys: &wici_crypto::PairKeys,
    id: MessageId,
    lane: Lane,
    body: &Body,
) {
    let sealed = keys.seal_message(id, lane, body).unwrap();
    client
        .send(&ClientFrame::Send {
            pair: keys.pair(),
            id,
            lane,
            sealed,
        })
        .await;
}

fn error_code(frame: &ServerFrame) -> Option<ErrorCode> {
    if let ServerFrame::Error { code, .. } = frame {
        Some(*code)
    } else {
        None
    }
}

async fn accepted(client: &mut Client) -> Position {
    let ServerFrame::Accepted { position, .. } = client
        .recv_where(|f| matches!(f, ServerFrame::Accepted { .. }))
        .await
    else {
        panic!("unexpected frame")
    };
    position
}

async fn delivered(client: &mut Client) -> (MessageId, Lane, Position, Blob) {
    let ServerFrame::Deliver {
        id,
        lane,
        position,
        sealed,
        ..
    } = client
        .recv_where(|f| matches!(f, ServerFrame::Deliver { .. }))
        .await
    else {
        panic!("unexpected frame")
    };
    (id, lane, position, sealed)
}

async fn handshake_rejects_bad_signature_version_and_order(backend: Backend) {
    let server = Server::on(backend, |_| {}).await;
    let keys = DeviceKeys::generate();
    let other = DeviceKeys::generate();

    let (mut socket, nonce) = open(&server.url).await;
    put(
        &mut socket,
        &ClientFrame::Hello {
            version: PROTOCOL_VERSION,
            device: keys.device_id(),
        },
    )
    .await;
    put(
        &mut socket,
        &ClientFrame::Authenticate {
            signature: other.sign_challenge(&nonce),
        },
    )
    .await;
    assert_eq!(
        next(&mut socket).await.as_ref().and_then(error_code),
        Some(ErrorCode::Unauthenticated)
    );
    assert_eq!(next(&mut socket).await, None, "closed");

    let (mut socket, _) = open(&server.url).await;
    put(
        &mut socket,
        &ClientFrame::Hello {
            version: 99,
            device: keys.device_id(),
        },
    )
    .await;
    let code = next(&mut socket).await.as_ref().and_then(error_code);
    assert_eq!(code, Some(ErrorCode::UnsupportedVersion));

    let (mut socket, _) = open(&server.url).await;
    put(
        &mut socket,
        &ClientFrame::Unpair {
            pair: wici_protocol::PairId::generate(),
        },
    )
    .await;
    assert_eq!(
        next(&mut socket).await.as_ref().and_then(error_code),
        Some(ErrorCode::InvalidFrame)
    );
}

async fn handshake_times_out(backend: Backend) {
    let server = Server::on(backend, |c| c.timeouts.auth = Duration::from_millis(100)).await;
    let (mut socket, _) = open(&server.url).await;
    assert_eq!(
        next(&mut socket).await.as_ref().and_then(error_code),
        Some(ErrorCode::Unauthenticated)
    );
}

async fn answered_pings_keep_an_idle_connection_open(backend: Backend) {
    let server = Server::on(backend, |c| {
        c.timeouts.idle = Duration::from_millis(300);
        c.timeouts.ping = Duration::from_millis(50);
    })
    .await;
    let (mut client, _) = Client::new(&server.url).await;
    // Reading answers each ping with a pong.
    client.expect_quiet(Duration::from_millis(1000)).await;
}

async fn silent_connection_idles_out(backend: Backend) {
    let server = Server::on(backend, |c| {
        c.timeouts.idle = Duration::from_millis(100);
        c.timeouts.ping = Duration::from_millis(50);
    })
    .await;
    let (mut client, _) = Client::new(&server.url).await;
    // Not reading means no pongs.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(client.recv().await, None);
}

async fn sealed_message_round_trip_with_ack(backend: Backend) {
    let (
        server,
        Paired {
            mut a,
            mut b,
            a_keys,
            b_keys,
            pair,
        },
    ) = paired_on(backend).await;
    let id = MessageId::generate();
    send_body(&mut a, &a_keys, id, Lane::Data, &event("hello")).await;
    assert_eq!(accepted(&mut a).await, Position(1));
    let (got_id, lane, position, sealed) = delivered(&mut b).await;
    assert_eq!((got_id, lane, position), (id, Lane::Data, Position(1)));
    assert_eq!(
        b_keys.open_message(id, lane, &sealed).unwrap(),
        event("hello")
    );
    b.send(&ClientFrame::Ack {
        pair,
        lane,
        position,
    })
    .await;
    b.expect_quiet(Duration::from_millis(200)).await;

    // Acked messages are not sent again after reconnect.
    let keys = b.disconnect().await;
    let (mut b, _) = Client::connect(&server.url, keys).await;
    b.expect_quiet(Duration::from_millis(300)).await;
    a.close().await;
}

async fn unacked_messages_are_resent_after_reconnect(backend: Backend) {
    let (
        server,
        Paired {
            mut a,
            b,
            a_keys,
            b_keys,
            pair,
        },
    ) = paired_on(backend).await;
    let b_device = b.keys.device_id();
    let keys = b.disconnect().await;
    for text in ["one", "two", "three"] {
        send_body(
            &mut a,
            &a_keys,
            MessageId::generate(),
            Lane::Data,
            &event(text),
        )
        .await;
        accepted(&mut a).await;
    }
    let (mut b, pairs) = Client::connect(&server.url, keys).await;
    assert!(
        pairs
            .iter()
            .any(|p| p.id == pair && p.state == PairState::Active)
    );
    assert_eq!(b.keys.device_id(), b_device);
    let mut texts = Vec::new();
    for expected in 1..=3 {
        let (id, lane, position, sealed) = delivered(&mut b).await;
        assert_eq!(position, Position(expected), "in order");
        texts.push(b_keys.open_message(id, lane, &sealed).unwrap());
    }
    assert_eq!(texts, [event("one"), event("two"), event("three")]);
}

async fn retry_returns_the_same_position_and_conflict_is_reported(backend: Backend) {
    let (
        _server,
        Paired {
            mut a,
            mut b,
            a_keys,
            ..
        },
    ) = paired_on(backend).await;
    let id = MessageId::generate();
    let sealed = a_keys.seal_message(id, Lane::Control, &event("x")).unwrap();
    let frame = ClientFrame::Send {
        pair: a_keys.pair(),
        id,
        lane: Lane::Control,
        sealed,
    };
    a.send(&frame).await;
    a.send(&frame).await;
    assert_eq!(accepted(&mut a).await, Position(1));
    assert_eq!(accepted(&mut a).await, Position(1));
    delivered(&mut b).await;
    b.expect_quiet(Duration::from_millis(200)).await;

    send_body(&mut a, &a_keys, id, Lane::Control, &event("y")).await;
    let frame = a.recv_where(|f| error_code(f).is_some()).await;
    assert!(
        matches!(frame, ServerFrame::Error { code: ErrorCode::Conflict, id: Some(e), .. } if e == id)
    );
}

async fn live_updates_reach_an_online_peer(backend: Backend) {
    let (
        _server,
        Paired {
            mut a,
            mut b,
            a_keys,
            b_keys,
            pair,
        },
    ) = paired_on(backend).await;
    let update = LiveBody {
        stream: StreamId::from_bytes([2; 16]),
        data: json!("tok"),
    };
    a.send(&ClientFrame::Live {
        pair,
        sealed: a_keys.seal_live(&update).unwrap(),
    })
    .await;
    let ServerFrame::Live { sealed, .. } = b
        .recv_where(|f| matches!(f, ServerFrame::Live { .. }))
        .await
    else {
        panic!("unexpected frame")
    };
    assert_eq!(b_keys.open_live(&sealed).unwrap(), update);
}

async fn unpair_stops_all_traffic(backend: Backend) {
    let (
        _server,
        Paired {
            mut a,
            mut b,
            a_keys,
            pair,
            ..
        },
    ) = paired_on(backend).await;
    b.send(&ClientFrame::Unpair { pair }).await;
    expect_pair(&mut b, pair, PairState::Revoked).await;
    expect_pair(&mut a, pair, PairState::Revoked).await;

    send_body(
        &mut a,
        &a_keys,
        MessageId::generate(),
        Lane::Data,
        &event("x"),
    )
    .await;
    assert_eq!(
        error_code(&a.recv_where(|f| error_code(f).is_some()).await),
        Some(ErrorCode::Forbidden)
    );
    let sealed = a_keys
        .seal_live(&LiveBody {
            stream: StreamId::generate(),
            data: json!(1),
        })
        .unwrap();
    a.send(&ClientFrame::Live { pair, sealed }).await;
    assert_eq!(
        error_code(&a.recv_where(|f| error_code(f).is_some()).await),
        Some(ErrorCode::Forbidden)
    );
    b.expect_quiet(Duration::from_millis(200)).await;
}

async fn presence_follows_connections(backend: Backend) {
    let (server, Paired { mut a, b, pair, .. }) = paired_on(backend).await;
    let keys = b.disconnect().await;
    let offline = a
        .recv_where(|f| matches!(f, ServerFrame::Presence { online: false, .. }))
        .await;
    assert!(
        matches!(offline, ServerFrame::Presence { pair: p, last_seen: Some(_), .. } if p == pair)
    );
    let (_b, _) = Client::connect(&server.url, keys).await;
    let online = a
        .recv_where(|f| matches!(f, ServerFrame::Presence { online: true, .. }))
        .await;
    assert!(matches!(online, ServerFrame::Presence { pair: p, .. } if p == pair));
}

async fn replaced_connection_does_not_announce_offline(backend: Backend) {
    let (server, Paired { mut a, b, .. }) = paired_on(backend).await;
    let secret = b.keys.to_secret();
    let (_b, _) = Client::connect(&server.url, DeviceKeys::from_secret(&secret)).await;
    let offline = a
        .recv_presence_within(Duration::from_millis(500))
        .await
        .into_iter()
        .any(|f| matches!(f, ServerFrame::Presence { online: false, .. }));
    assert!(!offline, "the old connection announced offline");
}

async fn new_connection_replaces_the_old_one(backend: Backend) {
    let server = Server::on(backend, |_| {}).await;
    let keys = DeviceKeys::generate();
    let secret = keys.to_secret();
    let (mut first, _) = Client::connect(&server.url, keys).await;
    let (_second, _) = Client::connect(&server.url, DeviceKeys::from_secret(&secret)).await;
    assert_eq!(first.recv().await, None);
}

async fn bad_frames_get_errors_and_the_connection_survives(backend: Backend) {
    let server = Server::on(backend, |c| c.limits.max_sealed_bytes = 8).await;
    let Paired {
        mut a,
        a_keys,
        pair,
        ..
    } = paired(&server).await;
    send_body(
        &mut a,
        &a_keys,
        MessageId::generate(),
        Lane::Data,
        &event("too long"),
    )
    .await;
    assert_eq!(
        error_code(&a.recv_no_presence().await),
        Some(ErrorCode::LimitExceeded)
    );
    a.send(&ClientFrame::Hello {
        version: 1,
        device: a_keys.peer(),
    })
    .await;
    assert_eq!(
        error_code(&a.recv_no_presence().await),
        Some(ErrorCode::InvalidFrame)
    );
    a.send(&ClientFrame::Ack {
        pair,
        lane: Lane::Data,
        position: Position(5),
    })
    .await;
    assert_eq!(
        error_code(&a.recv_no_presence().await),
        Some(ErrorCode::Forbidden)
    );
    a.send(&ClientFrame::Approve {
        pair: wici_protocol::PairId::generate(),
    })
    .await;
    assert_eq!(
        error_code(&a.recv_no_presence().await),
        Some(ErrorCode::NotFound)
    );
}

async fn delivery_window_limits_messages_in_flight(backend: Backend) {
    let server = Server::on(backend, |c| {
        c.limits.delivery_window = 2;
        c.limits.fetch_batch = 2;
    })
    .await;
    let Paired {
        mut a,
        mut b,
        a_keys,
        pair,
        ..
    } = paired(&server).await;
    for _ in 0..4 {
        send_body(
            &mut a,
            &a_keys,
            MessageId::generate(),
            Lane::Data,
            &event("x"),
        )
        .await;
        accepted(&mut a).await;
    }
    assert_eq!(delivered(&mut b).await.2, Position(1));
    assert_eq!(delivered(&mut b).await.2, Position(2));
    b.expect_quiet(Duration::from_millis(200)).await;
    b.send(&ClientFrame::Ack {
        pair,
        lane: Lane::Data,
        position: Position(2),
    })
    .await;
    assert_eq!(delivered(&mut b).await.2, Position(3));
    assert_eq!(delivered(&mut b).await.2, Position(4));
}

async fn frame_rate_is_limited(backend: Backend) {
    let server = Server::on(backend, |c| {
        c.limits.frame_burst = 2;
        c.limits.frames_per_second = 1;
    })
    .await;
    let (mut a, _) = Client::new(&server.url).await;
    for _ in 0..3 {
        a.send(&ClientFrame::Approve {
            pair: wici_protocol::PairId::generate(),
        })
        .await;
    }
    let codes = [
        error_code(&a.recv().await.unwrap()),
        error_code(&a.recv().await.unwrap()),
        error_code(&a.recv().await.unwrap()),
    ];
    assert_eq!(
        codes,
        [
            Some(ErrorCode::NotFound),
            Some(ErrorCode::NotFound),
            Some(ErrorCode::RateLimited)
        ]
    );
}

async fn overdue_invitations_expire_and_are_announced(backend: Backend) {
    let server = Server::on(backend, |c| {
        c.timeouts.invite = Duration::ZERO;
        c.timeouts.sweep = Duration::from_millis(20);
    })
    .await;
    let (mut a, _) = Client::new(&server.url).await;
    let invitation = Invitation::create(&a.keys, String::new(), Timestamp(0));
    a.send(&ClientFrame::Invite {
        pair: invitation.pair,
        claim_hash: invitation.claim_hash(),
    })
    .await;
    expect_pair(&mut a, invitation.pair, PairState::Expired).await;
}

async fn health_check_and_graceful_shutdown(backend: Backend) {
    let mut server = Server::on(backend, |_| {}).await;
    let mut stream = tokio::net::TcpStream::connect(server.http_address())
        .await
        .unwrap();
    stream
        .write_all(b"GET /health HTTP/1.1\r\nHost: wici\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    assert!(
        response.starts_with("HTTP/1.1 200") && response.ends_with("ok"),
        "{response}"
    );

    let (mut a, _) = Client::new(&server.url).await;
    server.stop().await;
    assert_eq!(a.recv().await, None);
}

/// Uploads `data` in one chunk and waits until it is complete.
async fn upload(
    client: &mut Client,
    pair: wici_protocol::PairId,
    key: &wici_crypto::ArtifactKey,
    data: &[u8],
) {
    let sealed = key.seal_all(data).unwrap();
    let total = sealed.len() as u64;
    let hash = wici_crypto::artifact_hash(&sealed);
    let chunk = Blob::new(sealed);
    client
        .send(&ClientFrame::ArtifactPut {
            pair,
            artifact: key.id(),
            total,
            hash,
            offset: 0,
            chunk,
        })
        .await;
    let stored = client
        .recv_where(|f| matches!(f, ServerFrame::ArtifactStored { .. }))
        .await;
    assert!(
        matches!(stored, ServerFrame::ArtifactStored { complete: true, received, .. } if received == total)
    );
}

async fn artifact_upload_and_download_over_websocket(backend: Backend) {
    let (
        _server,
        Paired {
            mut a, mut b, pair, ..
        },
    ) = paired_on(backend).await;
    let key = wici_crypto::ArtifactKey::generate();
    upload(&mut a, pair, &key, b"picture").await;

    b.send(&ClientFrame::ArtifactGet {
        pair,
        artifact: key.id(),
        offset: 0,
    })
    .await;
    let ServerFrame::ArtifactChunk { chunk, .. } = b
        .recv_where(|f| matches!(f, ServerFrame::ArtifactChunk { .. }))
        .await
    else {
        panic!("unexpected frame")
    };
    assert_eq!(*key.open_all(chunk.as_bytes()).unwrap(), b"picture");

    b.send(&ClientFrame::ArtifactDelete {
        pair,
        artifact: key.id(),
    })
    .await;
    b.send(&ClientFrame::ArtifactGet {
        pair,
        artifact: key.id(),
        offset: 0,
    })
    .await;
    let error = b.recv_where(|f| error_code(f).is_some()).await;
    let gone = |id: &Option<wici_protocol::ArtifactId>| *id == Some(key.id());
    assert!(
        matches!(error, ServerFrame::Error { code: ErrorCode::NotFound, ref artifact, .. } if gone(artifact))
    );
}

async fn oversized_artifact_chunk_is_rejected(backend: Backend) {
    let server = Server::on(backend, |c| c.limits.max_chunk_bytes = 4).await;
    let Paired { mut a, pair, .. } = paired(&server).await;
    a.send(&ClientFrame::ArtifactPut {
        pair,
        artifact: wici_protocol::ArtifactId::generate(),
        total: 10,
        hash: wici_protocol::FixedBytes::new([0; 32]),
        offset: 0,
        chunk: Blob::new(vec![0; 5]),
    })
    .await;
    assert_eq!(
        error_code(&a.recv_no_presence().await),
        Some(ErrorCode::LimitExceeded)
    );
}

on_every_backend!(
    handshake_rejects_bad_signature_version_and_order,
    handshake_times_out,
    answered_pings_keep_an_idle_connection_open,
    silent_connection_idles_out,
    sealed_message_round_trip_with_ack,
    unacked_messages_are_resent_after_reconnect,
    retry_returns_the_same_position_and_conflict_is_reported,
    live_updates_reach_an_online_peer,
    unpair_stops_all_traffic,
    presence_follows_connections,
    replaced_connection_does_not_announce_offline,
    new_connection_replaces_the_old_one,
    bad_frames_get_errors_and_the_connection_survives,
    delivery_window_limits_messages_in_flight,
    frame_rate_is_limited,
    overdue_invitations_expire_and_are_announced,
    health_check_and_graceful_shutdown,
    artifact_upload_and_download_over_websocket,
    oversized_artifact_chunk_is_rejected,
);
