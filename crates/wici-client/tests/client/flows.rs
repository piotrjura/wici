use serde_json::json;
use wici_client::{ClientError, Direction, Event, NewCommand, Role};
use wici_protocol::{Body, CommandState, Lane, LiveBody, PairState, StreamId, Timestamp};
use wici_testkit::TestServer;

use crate::support::{Device, pair, paired_devices};

fn later() -> Timestamp {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    Timestamp(u64::try_from(now.as_millis()).unwrap() + 60_000)
}

fn command(operation: &str) -> NewCommand {
    NewCommand {
        operation: operation.to_owned(),
        input: json!({"x": 1}),
        deadline: later(),
        stream: None,
    }
}

fn event(text: &str) -> Body {
    Body::Event {
        stream: StreamId::from_bytes([1; 16]),
        data: json!(text),
    }
}

async fn received(device: &mut Device) -> wici_client::Incoming {
    let Event::Message { message } = device.expect(|e| matches!(e, Event::Message { .. })).await
    else {
        panic!("not a message")
    };
    message
}

async fn command_state(device: &mut Device, state: CommandState) {
    device
        .expect(|e| matches!(e, Event::Command { state: s, .. } if *s == state))
        .await;
}

#[tokio::test]
async fn pairing_and_full_command_lifecycle() {
    let (_server, mut a, mut b, pair) = paired_devices().await;
    let views = a.client.pairs().await.unwrap();
    assert_eq!(
        (views[0].role, views[0].peer),
        (Role::Inviter, Some(b.client.device_id()))
    );

    let id = a.client.command(pair, command("build")).await.unwrap();
    command_state(&mut a, CommandState::AcceptedDurable).await;
    let incoming = received(&mut b).await;
    assert_eq!(incoming.id, id);
    assert!(matches!(incoming.body, Body::Command { ref operation, .. } if operation == "build"));
    command_state(&mut a, CommandState::ReceivedByRunner).await;

    b.client
        .report(pair, id, CommandState::Running, None)
        .await
        .unwrap();
    command_state(&mut a, CommandState::Running).await;
    b.client
        .report(pair, id, CommandState::Completed, Some(json!("ok")))
        .await
        .unwrap();
    command_state(&mut a, CommandState::Completed).await;
    let state = a
        .client
        .command_state(pair, id, Direction::Outgoing)
        .await
        .unwrap();
    assert_eq!(state, Some(CommandState::Completed));

    let late = b.client.report(pair, id, CommandState::Running, None).await;
    assert!(matches!(late, Err(ClientError::Transition { .. })));
}

#[tokio::test]
async fn messages_sent_while_offline_arrive_in_order_after_reopen() {
    let (_server, mut a, b, pair) = paired_devices().await;
    let closed = b.close().await;
    for text in ["one", "two", "three"] {
        a.client.send(pair, Lane::Data, &event(text)).await.unwrap();
        a.expect(|e| matches!(e, Event::Accepted { .. })).await;
    }
    let mut b = closed.reopen().await;
    for text in ["one", "two", "three"] {
        assert_eq!(received(&mut b).await.body, event(text));
    }
    let pending = b.client.pending().await.unwrap();
    assert_eq!(pending.len(), 3);
    for message in &pending {
        assert!(
            b.client
                .handled(message.pair, message.lane, message.position)
                .await
                .unwrap()
        );
    }
    assert_eq!(b.client.pending().await.unwrap().len(), 0);
}

#[tokio::test]
async fn outbox_survives_server_and_client_restarts() {
    let (mut server, mut a, mut b, pair) = paired_devices().await;
    server.stop().await;
    a.expect(|e| *e == Event::Disconnected).await;
    a.client
        .send(pair, Lane::Control, &event("queued"))
        .await
        .unwrap();
    let a_closed = a.close().await;
    server.restart().await;
    let mut a = a_closed.reopen().await;
    a.expect(|e| matches!(e, Event::Accepted { .. })).await;
    assert_eq!(received(&mut b).await.body, event("queued"));
}

#[tokio::test]
async fn interrupted_incoming_command_becomes_outcome_unknown() {
    let (_server, mut a, mut b, pair) = paired_devices().await;
    let id = a.client.command(pair, command("deploy")).await.unwrap();
    received(&mut b).await;
    b.client
        .report(pair, id, CommandState::Running, None)
        .await
        .unwrap();
    command_state(&mut a, CommandState::Running).await;
    let b = b.close().await.reopen().await;
    command_state(&mut a, CommandState::OutcomeUnknown).await;
    let state = b
        .client
        .command_state(pair, id, Direction::Incoming)
        .await
        .unwrap();
    assert_eq!(state, Some(CommandState::OutcomeUnknown));
}

#[tokio::test]
async fn unpair_while_peer_is_offline() {
    let (_server, mut a, b, pair) = paired_devices().await;
    let closed = b.close().await;
    a.client.unpair(pair).await.unwrap();
    a.expect_pair(pair, PairState::Revoked).await;
    let mut b = closed.reopen().await;
    b.expect_pair(pair, PairState::Revoked).await;
    let send = b.client.send(pair, Lane::Data, &event("x")).await;
    assert!(matches!(send, Err(ClientError::PairState)));
    assert!(matches!(
        a.client.unpair(pair).await,
        Err(ClientError::PairState)
    ));
}

#[tokio::test]
async fn live_updates_and_presence_reach_the_peer() {
    let (_server, a, mut b, pair) = paired_devices().await;
    b.expect(|e| matches!(e, Event::Presence { online: true, .. }))
        .await;
    let body = LiveBody {
        stream: StreamId::from_bytes([2; 16]),
        data: json!("tok"),
    };
    a.client.live(pair, &body).await.unwrap();
    let live = b.expect(|e| matches!(e, Event::Live { .. })).await;
    assert_eq!(live, Event::Live { pair, body });
    let _closed = a.close().await;
    b.expect(|e| matches!(e, Event::Presence { online: false, .. }))
        .await;
}

#[tokio::test]
async fn invalid_requests_are_rejected_locally() {
    let server = TestServer::start().await;
    let mut a = Device::open(&server.url).await;
    let mut b = Device::open(&server.url).await;
    let invitation = a.client.invite().await.unwrap();
    let own = a.client.join(&invitation.to_link().unwrap()).await;
    assert!(matches!(own, Err(ClientError::OwnInvitation)));
    assert!(matches!(
        b.client.join("wici:bad").await,
        Err(ClientError::Crypto(_))
    ));
    assert!(
        matches!(
            a.client.approve(invitation.pair).await,
            Err(ClientError::PairState)
        ),
        "not claimed"
    );
    let unknown = wici_protocol::PairId::generate();
    assert!(matches!(
        a.client.approve(unknown).await,
        Err(ClientError::UnknownPair)
    ));

    let pair = pair(&mut a, &mut b).await;
    let huge = Body::Event {
        stream: StreamId::generate(),
        data: json!("x".repeat(2 * 1024 * 1024)),
    };
    assert!(matches!(
        a.client.send(pair, Lane::Data, &huge).await,
        Err(ClientError::TooLarge)
    ));
    let empty = NewCommand {
        operation: String::new(),
        ..command("x")
    };
    assert!(matches!(
        a.client.command(pair, empty).await,
        Err(ClientError::Body(_))
    ));
    let unknown_command = wici_protocol::MessageId::generate();
    let report = a
        .client
        .report(pair, unknown_command, CommandState::Running, None)
        .await;
    assert!(matches!(
        report,
        Err(ClientError::Transition { from: None, .. })
    ));
    a.client.reconnect_now();
}

#[tokio::test]
async fn artifacts_upload_share_download_and_delete() {
    let (mut server, mut a, mut b, pair) = paired_devices().await;
    let picture: Vec<u8> = (0..300_000_u32).map(|i| (i % 251) as u8).collect();
    let artifact = a
        .client
        .upload(pair, picture.clone(), "image/png", Some("p.png".to_owned()))
        .await
        .unwrap();
    assert_eq!(artifact.size, picture.len() as u64);
    let shared = Body::Event {
        stream: StreamId::generate(),
        data: serde_json::to_value(&artifact).unwrap(),
    };
    a.client.send(pair, Lane::Data, &shared).await.unwrap();

    let Body::Event { data, .. } = received(&mut b).await.body else {
        panic!("not an event")
    };
    let reference: wici_protocol::ArtifactRef = serde_json::from_value(data).unwrap();
    assert_eq!(b.client.download(pair, &reference).await.unwrap(), picture);

    b.client.delete_artifact(pair, reference.id).await.unwrap();
    let gone = b.client.download(pair, &reference).await;
    assert!(matches!(
        gone,
        Err(ClientError::Server {
            code: wici_protocol::ErrorCode::NotFound,
            ..
        })
    ));

    server.stop().await;
    a.expect(|e| *e == Event::Disconnected).await;
    let offline = a.client.upload(pair, vec![1], "text/plain", None).await;
    assert!(matches!(offline, Err(ClientError::Offline)));
}
