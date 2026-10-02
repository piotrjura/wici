use std::time::Duration;

use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use wici_crypto::{DeviceKeys, Invitation};
use wici_protocol::{
    Blob, Body, ClientFrame, ErrorCode, Lane, LiveBody, MessageId, PROTOCOL_VERSION, PairState,
    Position, ServerFrame, StreamId, Timestamp,
};

use crate::client::{Client, Paired, Server, expect_pair, next, open, paired, put};

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

#[tokio::test]
async fn handshake_rejects_bad_signature_version_and_order() {
    let server = Server::start().await;
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

#[tokio::test]
async fn handshake_times_out() {
    let server = Server::with(|c| c.timeouts.auth = Duration::from_millis(100)).await;
    let (mut socket, _) = open(&server.url).await;
    assert_eq!(
        next(&mut socket).await.as_ref().and_then(error_code),
        Some(ErrorCode::Unauthenticated)
    );
}

#[tokio::test]
async fn sealed_message_round_trip_with_ack() {
    let server = Server::start().await;
    let Paired {
        mut a,
        mut b,
        a_keys,
        b_keys,
        pair,
    } = paired(&server).await;
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

#[tokio::test]
async fn unacked_messages_are_resent_after_reconnect() {
    let server = Server::start().await;
    let Paired {
        mut a,
        b,
        a_keys,
        b_keys,
        pair,
    } = paired(&server).await;
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

#[tokio::test]
async fn retry_returns_the_same_position_and_conflict_is_reported() {
    let server = Server::start().await;
    let Paired {
        mut a,
        mut b,
        a_keys,
        ..
    } = paired(&server).await;
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

#[tokio::test]
async fn live_updates_reach_an_online_peer() {
    let server = Server::start().await;
    let Paired {
        mut a,
        mut b,
        a_keys,
        b_keys,
        pair,
    } = paired(&server).await;
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

#[tokio::test]
async fn unpair_stops_all_traffic() {
    let server = Server::start().await;
    let Paired {
        mut a,
        mut b,
        a_keys,
        pair,
        ..
    } = paired(&server).await;
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

#[tokio::test]
async fn presence_follows_connections() {
    let server = Server::start().await;
    let Paired { mut a, b, pair, .. } = paired(&server).await;
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

#[tokio::test]
async fn new_connection_replaces_the_old_one() {
    let server = Server::start().await;
    let keys = DeviceKeys::generate();
    let secret = keys.to_secret();
    let (mut first, _) = Client::connect(&server.url, keys).await;
    let (_second, _) = Client::connect(&server.url, DeviceKeys::from_secret(&secret)).await;
    assert_eq!(first.recv().await, None);
}

#[tokio::test]
async fn bad_frames_get_errors_and_the_connection_survives() {
    let server = Server::with(|c| c.limits.max_sealed_bytes = 8).await;
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

#[tokio::test]
async fn delivery_window_limits_messages_in_flight() {
    let server = Server::with(|c| {
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

#[tokio::test]
async fn frame_rate_is_limited() {
    let server = Server::with(|c| {
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

#[tokio::test]
async fn overdue_invitations_expire_and_are_announced() {
    let server = Server::with(|c| {
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

#[tokio::test]
async fn health_check_and_graceful_shutdown() {
    let server = Server::start().await;
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
