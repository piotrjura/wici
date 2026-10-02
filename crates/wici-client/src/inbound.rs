//! Handling of server frames.

use serde_json::json;
use wici_protocol::{
    Blob, Body, ClientFrame, CommandState, ErrorCode, Lane, Lifecycle, MessageId, PairId, PairInfo,
    PairState, Position, ServerFrame, Timestamp,
};

use crate::db::{self, InboxRow, OutboxRow, PairRow, now_ms};
use crate::error::ClientResult;
use crate::model::{Direction, Event, Incoming, PendingOp, Role};
use crate::outbound::InFlight;
use crate::shared::{Shared, view};

/// Codes worth retrying: the request may succeed later unchanged.
const fn is_transient(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::RateLimited | ErrorCode::LimitExceeded | ErrorCode::Internal
    )
}

/// New state of an outgoing command after a peer report, if the lifecycle
/// allows it. A report proves acceptance, so a lost `accepted` is skipped.
pub(crate) fn advance(current: Option<CommandState>, next: CommandState) -> Option<CommandState> {
    let current = current?;
    let via_accepted = current == CommandState::QueuedLocal
        && CommandState::AcceptedDurable.can_transition_to(next);
    (current.can_transition_to(next) || via_accepted).then_some(next)
}

/// Handles one frame. Returns a frame to send back, if any.
#[expect(clippy::too_many_lines, reason = "one short arm per frame type")]
pub(crate) async fn handle(
    shared: &Shared,
    frame: ServerFrame,
    flight: &mut InFlight,
) -> ClientResult<Option<ClientFrame>> {
    match frame {
        ServerFrame::Pair { pair } => on_pair(shared, &pair, flight).await?,
        ServerFrame::Accepted { pair, id, position } => {
            on_accepted(shared, pair, id, position, flight).await?;
        }
        ServerFrame::Deliver {
            pair,
            id,
            lane,
            position,
            accepted_at,
            sealed,
        } => {
            let row = InboxRow {
                pair,
                lane,
                position,
                id,
                accepted_at,
                body: sealed.into_bytes(),
            };
            return on_deliver(shared, row).await;
        }
        ServerFrame::Live { pair, sealed } => on_live(shared, pair, &sealed).await?,
        ServerFrame::Presence {
            pair,
            online,
            last_seen,
        } => {
            shared
                .emit(Event::Presence {
                    pair,
                    online,
                    last_seen,
                })
                .await;
        }
        ServerFrame::Error {
            code,
            message,
            pair,
            id,
            ..
        } => {
            on_error(shared, code, message, (pair, id), flight).await?;
        }
        ServerFrame::Challenge { .. }
        | ServerFrame::Welcome { .. }
        | ServerFrame::ArtifactStored { .. }
        | ServerFrame::ArtifactChunk { .. } => {}
    }
    Ok(None)
}

/// Applies a server pair view to the local record.
pub(crate) async fn on_pair(
    shared: &Shared,
    info: &PairInfo,
    flight: &mut InFlight,
) -> ClientResult<()> {
    let mut tx = shared.db.begin().await?;
    let Some(mut row) = db::find_pair(&mut tx, info.id).await? else {
        return Ok(());
    };
    merge_pair(&mut row, info, flight, &shared.keys.device_id());
    let claimed = accept_claim(shared, &mut row, info);
    if info.state == PairState::Revoked {
        row.keys = None;
        shared.forget_keys(row.id);
        db::drop_pair_traffic(&mut tx, row.id).await?;
    }
    db::update_pair(&mut tx, &row).await?;
    tx.commit().await?;
    shared.emit(Event::Pair { pair: view(&row) }).await;
    if let Some(device) = claimed {
        shared
            .emit(Event::Claimed {
                pair: row.id,
                device,
            })
            .await;
    }
    Ok(())
}

/// Copies server state into `row` and clears a confirmed pending operation.
fn merge_pair(
    row: &mut PairRow,
    info: &PairInfo,
    flight: &mut InFlight,
    own: &wici_protocol::DeviceId,
) {
    if let Some(op) = row.pending {
        if op.confirmed_by(info.state) || info.state.is_terminal() {
            row.pending = None;
            flight.ops.remove(&(row.id, op));
        }
    }
    row.state = info.state;
    let peer = if info.inviter == *own {
        info.invitee
    } else {
        Some(info.inviter)
    };
    row.peer = row.peer.or(peer);
}

/// Inviter side of a claim: checks the greeting and stores pair keys.
/// A forged greeting queues an unpair instead. Returns the claimer.
fn accept_claim(
    shared: &Shared,
    row: &mut PairRow,
    info: &PairInfo,
) -> Option<wici_protocol::DeviceId> {
    let fresh = row.role == Role::Inviter && info.state == PairState::Claimed && row.keys.is_none();
    let (true, Some(invitee), Some(greeting)) = (fresh, info.invitee, &info.greeting) else {
        return None;
    };
    let accepted = shared
        .open_invitation(row)
        .and_then(|invitation| Ok(invitation.accept(&shared.keys, &invitee, greeting)?))
        .and_then(|keys| shared.seal_keys(&keys));
    match accepted {
        Ok(sealed) => {
            row.keys = Some(sealed);
            Some(invitee)
        }
        Err(error) => {
            tracing::warn!(%error, pair = %row.id, "rejecting claim with invalid greeting");
            row.pending = Some(PendingOp::Unpair);
            None
        }
    }
}

async fn on_accepted(
    shared: &Shared,
    pair: PairId,
    id: MessageId,
    position: Position,
    flight: &mut InFlight,
) -> ClientResult<()> {
    flight.messages.remove(&(pair, id));
    let mut tx = shared.db.begin().await?;
    if !db::remove_outbox(&mut tx, pair, id).await? {
        return Ok(());
    }
    let current = db::command_state(&mut tx, pair, id, Direction::Outgoing).await?;
    let accepted = current == Some(CommandState::QueuedLocal);
    if accepted {
        db::set_command(
            &mut tx,
            pair,
            id,
            Direction::Outgoing,
            CommandState::AcceptedDurable,
        )
        .await?;
    }
    tx.commit().await?;
    shared.emit(Event::Accepted { pair, id, position }).await;
    if accepted {
        shared
            .emit(Event::Command {
                pair,
                id,
                state: CommandState::AcceptedDurable,
            })
            .await;
    }
    Ok(())
}

/// Saves a delivery, then returns the ack. Duplicates are acked again; a
/// position gap is not acked, so the server resends in order.
async fn on_deliver(shared: &Shared, delivery: InboxRow) -> ClientResult<Option<ClientFrame>> {
    let (pair, lane) = (delivery.pair, delivery.lane);
    let ack = |position| {
        Some(ClientFrame::Ack {
            pair,
            lane,
            position,
        })
    };
    let mut conn = shared.db.conn().await?;
    let saved = db::cursor(&mut conn, pair, lane).await?;
    if delivery.position <= saved {
        return Ok(ack(saved));
    }
    if delivery.position.0 != saved.0 + 1 {
        tracing::warn!(%pair, "delivery gap, waiting for resend");
        return Ok(None);
    }
    let row = db::find_pair(&mut conn, pair).await?;
    drop(conn);
    let opened = row
        .as_ref()
        .map(|row| open_delivery(shared, row, &delivery));
    match opened {
        Some(Ok(body)) => save_delivery(shared, row.as_ref(), delivery, body).await?,
        Some(Err(reason)) => quarantine(shared, &delivery, &reason).await?,
        None => quarantine(shared, &delivery, "unknown pair").await?,
    }
    Ok(ack(Position(saved.0 + 1)))
}

fn open_delivery(shared: &Shared, row: &PairRow, delivery: &InboxRow) -> Result<Body, String> {
    let keys = shared.pair_keys(row).map_err(|e| e.to_string())?;
    let sealed = Blob::new(delivery.body.clone());
    let body = keys
        .open_message(delivery.id, delivery.lane, &sealed)
        .map_err(|e| e.to_string())?;
    body.validate().map_err(|e| e.to_string())?;
    Ok(body)
}

async fn quarantine(shared: &Shared, delivery: &InboxRow, reason: &str) -> ClientResult<()> {
    let mut conn = shared.db.conn().await?;
    db::quarantine(&mut conn, delivery, reason).await?;
    drop(conn);
    shared
        .emit(Event::Quarantined {
            pair: delivery.pair,
            lane: delivery.lane,
            position: delivery.position,
        })
        .await;
    Ok(())
}

/// Effects of a received body, applied in the same transaction as the save.
#[derive(Default)]
struct Effects {
    handled: bool,
    reply: Option<OutboxRow>,
    incoming: Option<CommandState>,
    outgoing: Option<(MessageId, CommandState)>,
}

/// Receipt for a new command: `received_by_runner`, or `failed` if its
/// deadline passed. Expired commands never reach the app.
fn receipt(
    shared: &Shared,
    row: &PairRow,
    id: MessageId,
    deadline: Timestamp,
) -> ClientResult<Effects> {
    let expired = i64::try_from(deadline.0).unwrap_or(i64::MAX) < now_ms();
    let state = if expired {
        CommandState::Failed
    } else {
        CommandState::ReceivedByRunner
    };
    let output = expired.then(|| json!({ "error": "expired" }));
    let body = Body::Status {
        command: id,
        state,
        output,
    };
    Ok(Effects {
        handled: expired,
        reply: Some(shared.outgoing(row, Lane::Control, &body)?),
        incoming: Some(state),
        outgoing: None,
    })
}

async fn save_delivery(
    shared: &Shared,
    row: Option<&PairRow>,
    mut delivery: InboxRow,
    body: Body,
) -> ClientResult<()> {
    let mut effects = match (&body, row) {
        (Body::Command { deadline, .. }, Some(row)) => {
            receipt(shared, row, delivery.id, *deadline)?
        }
        _ => Effects::default(),
    };
    delivery.body = shared.seal_body(&delivery, &body)?;
    let mut tx = shared.db.begin().await?;
    if let Body::Status { command, state, .. } = &body {
        let current =
            db::command_state(&mut tx, delivery.pair, *command, Direction::Outgoing).await?;
        effects.outgoing = advance(current, *state).map(|next| (*command, next));
    }
    db::save_inbox(&mut tx, &delivery, effects.handled).await?;
    apply_effects(&mut tx, &delivery, &effects).await?;
    tx.commit().await?;
    announce(shared, delivery, body, &effects).await;
    Ok(())
}

async fn apply_effects(
    conn: &mut sqlx::SqliteConnection,
    delivery: &InboxRow,
    effects: &Effects,
) -> ClientResult<()> {
    if let Some(state) = effects.incoming {
        db::set_command(conn, delivery.pair, delivery.id, Direction::Incoming, state).await?;
    }
    if let Some((command, state)) = effects.outgoing {
        db::set_command(conn, delivery.pair, command, Direction::Outgoing, state).await?;
    }
    if let Some(reply) = &effects.reply {
        db::enqueue(conn, reply).await?;
    }
    Ok(())
}

/// Emits events for a saved delivery and wakes the sender for replies.
async fn announce(shared: &Shared, delivery: InboxRow, body: Body, effects: &Effects) {
    if effects.reply.is_some() {
        shared.wake.notify_one();
    }
    let pair = delivery.pair;
    if !effects.handled {
        let incoming = Incoming {
            pair,
            lane: delivery.lane,
            position: delivery.position,
            id: delivery.id,
            accepted_at: delivery.accepted_at,
            body,
        };
        shared.emit(Event::Message { message: incoming }).await;
    }
    if let Some((id, state)) = effects.outgoing {
        shared.emit(Event::Command { pair, id, state }).await;
    }
}

async fn on_live(shared: &Shared, pair: PairId, sealed: &Blob) -> ClientResult<()> {
    let mut conn = shared.db.conn().await?;
    let row = db::get_pair(&mut conn, pair).await?;
    drop(conn);
    let body = shared.pair_keys(&row)?.open_live(sealed)?;
    shared.emit(Event::Live { pair, body }).await;
    Ok(())
}

async fn on_error(
    shared: &Shared,
    code: ErrorCode,
    message: String,
    (pair, id): (Option<PairId>, Option<MessageId>),
    flight: &mut InFlight,
) -> ClientResult<()> {
    let permanent = match (pair, id) {
        (Some(pair), Some(id)) => message_failed(shared, pair, id, code, flight).await?,
        (Some(pair), None) => op_failed(shared, pair, code, flight).await?,
        (None, _) => true,
    };
    if permanent {
        shared
            .emit(Event::Failed {
                pair,
                id,
                code,
                message,
            })
            .await;
    }
    Ok(())
}

/// Returns `true` if the message failed for good.
async fn message_failed(
    shared: &Shared,
    pair: PairId,
    id: MessageId,
    code: ErrorCode,
    flight: &mut InFlight,
) -> ClientResult<bool> {
    flight.messages.remove(&(pair, id));
    let mut tx = shared.db.begin().await?;
    if is_transient(code) {
        db::delay_outbox(&mut tx, pair, id, shared.config.retry_interval).await?;
        tx.commit().await?;
        return Ok(false);
    }
    db::remove_outbox(&mut tx, pair, id).await?;
    let current = db::command_state(&mut tx, pair, id, Direction::Outgoing).await?;
    if current.is_some_and(|state| state.can_transition_to(CommandState::Failed)) {
        db::set_command(&mut tx, pair, id, Direction::Outgoing, CommandState::Failed).await?;
    }
    tx.commit().await?;
    Ok(true)
}

/// A claim can reach the server before the inviter's invitation does.
/// Until the invitation expires, `not_found` is worth retrying.
fn claim_may_be_early(shared: &Shared, row: &PairRow, op: PendingOp, code: ErrorCode) -> bool {
    let early = op == PendingOp::Claim && code == ErrorCode::NotFound;
    early
        && shared
            .open_invitation(row)
            .is_ok_and(|invitation| i64::try_from(invitation.expires_at.0).unwrap_or(0) > now_ms())
}

/// Returns `true` if the pending pair operation failed for good.
async fn op_failed(
    shared: &Shared,
    pair: PairId,
    code: ErrorCode,
    flight: &mut InFlight,
) -> ClientResult<bool> {
    let mut conn = shared.db.conn().await?;
    let Some(mut row) = db::find_pair(&mut conn, pair).await? else {
        return Ok(true);
    };
    let Some(op) = row.pending else {
        return Ok(true);
    };
    flight.ops.remove(&(pair, op));
    if is_transient(code) || claim_may_be_early(shared, &row, op, code) {
        flight.failed_ops.insert((pair, op));
        return Ok(false);
    }
    row.pending = None;
    if code == ErrorCode::Expired {
        row.state = PairState::Expired;
    }
    db::update_pair(&mut conn, &row).await?;
    drop(conn);
    shared.emit(Event::Pair { pair: view(&row) }).await;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use wici_protocol::{StreamId, WireEnum};

    use super::*;
    use crate::db::OutboxRow;
    use crate::testing::{Fixture, fixture};
    use CommandState::{
        AcceptedDurable, Completed, OutcomeUnknown, QueuedLocal, ReceivedByRunner, Running,
    };

    #[test]
    fn advance_follows_the_lifecycle() {
        assert_eq!(advance(None, Running), None, "unknown command");
        assert_eq!(
            advance(Some(AcceptedDurable), ReceivedByRunner),
            Some(ReceivedByRunner)
        );
        assert_eq!(advance(Some(Completed), Running), None, "terminal");
        assert_eq!(advance(Some(Running), Running), None, "repeat");
        assert_eq!(advance(Some(OutcomeUnknown), Completed), Some(Completed));
    }

    #[test]
    fn report_before_accepted_skips_acceptance() {
        assert_eq!(
            advance(Some(QueuedLocal), ReceivedByRunner),
            Some(ReceivedByRunner)
        );
        assert_eq!(
            advance(Some(QueuedLocal), Completed),
            None,
            "not reachable in one step"
        );
    }

    #[test]
    fn only_capacity_and_server_faults_are_transient() {
        let transient: Vec<_> = ErrorCode::ALL
            .iter()
            .copied()
            .filter(|c| is_transient(*c))
            .collect();
        assert_eq!(
            transient,
            [
                ErrorCode::LimitExceeded,
                ErrorCode::RateLimited,
                ErrorCode::Internal
            ]
        );
    }

    fn event_body(text: &str) -> Body {
        Body::Event {
            stream: StreamId::from_bytes([1; 16]),
            data: json!(text),
        }
    }

    /// A delivery sealed by the peer.
    fn deliver(f: &Fixture, position: u64, body: &Body) -> ServerFrame {
        let id = MessageId::generate();
        let sealed = f.peer.seal_message(id, Lane::Data, body).unwrap();
        ServerFrame::Deliver {
            pair: f.invitation.pair,
            id,
            lane: Lane::Data,
            position: Position(position),
            accepted_at: Timestamp(1),
            sealed,
        }
    }

    fn ack(f: &Fixture, position: u64) -> ClientFrame {
        ClientFrame::Ack {
            pair: f.invitation.pair,
            lane: Lane::Data,
            position: Position(position),
        }
    }

    async fn run(f: &Fixture, frame: ServerFrame) -> Option<ClientFrame> {
        handle(&f.shared, frame, &mut InFlight::default())
            .await
            .unwrap()
    }

    async fn outbox(f: &Fixture) -> Vec<OutboxRow> {
        db::due_outbox(&mut f.shared.db.conn().await.unwrap(), 100)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn delivery_is_saved_then_acked_once() {
        let mut f = fixture(PairState::Active).await;
        let first = deliver(&f, 1, &event_body("a"));
        assert_eq!(run(&f, first.clone()).await, Some(ack(&f, 1)));
        let Some(Event::Message { message }) = f.event() else {
            panic!("no message")
        };
        assert_eq!(message.body, event_body("a"));
        assert_eq!(
            run(&f, first).await,
            Some(ack(&f, 1)),
            "duplicate is acked again"
        );
        assert_eq!(f.event(), None, "duplicate is not emitted");
        let pending = db::unhandled(&mut f.shared.db.conn().await.unwrap())
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(
            f.shared.open_body(&pending[0]).unwrap(),
            event_body("a"),
            "sealed at rest"
        );
    }

    #[tokio::test]
    async fn gap_is_not_acked() {
        let mut f = fixture(PairState::Active).await;
        assert_eq!(run(&f, deliver(&f, 2, &event_body("b"))).await, None);
        assert_eq!(f.event(), None);
    }

    #[tokio::test]
    async fn unreadable_delivery_is_quarantined_and_acked() {
        let mut f = fixture(PairState::Active).await;
        let frame = ServerFrame::Deliver {
            pair: f.invitation.pair,
            id: MessageId::generate(),
            lane: Lane::Data,
            position: Position(1),
            accepted_at: Timestamp(1),
            sealed: Blob::new(vec![0; 40]),
        };
        assert_eq!(run(&f, frame).await, Some(ack(&f, 1)));
        assert!(matches!(
            f.event(),
            Some(Event::Quarantined {
                position: Position(1),
                ..
            })
        ));
        assert_eq!(
            run(&f, deliver(&f, 2, &event_body("next"))).await,
            Some(ack(&f, 2)),
            "stream continues"
        );
    }

    /// State reported by a queued `status` reply.
    fn reply_state(f: &Fixture, row: &OutboxRow) -> CommandState {
        let blob = Blob::new(row.sealed.clone());
        let body = f.peer.open_message(row.id, row.lane, &blob).unwrap();
        let Body::Status { state, .. } = body else {
            panic!("{body:?}")
        };
        state
    }

    #[tokio::test]
    async fn command_gets_a_receipt_and_expired_command_fails_quietly() {
        let mut f = fixture(PairState::Active).await;
        let command = |deadline| Body::Command {
            operation: "run".to_owned(),
            input: json!(null),
            deadline: Timestamp(deadline),
            stream: None,
        };
        let far = u64::try_from(now_ms()).unwrap() + 60_000;
        run(&f, deliver(&f, 1, &command(far))).await;
        assert!(matches!(f.event(), Some(Event::Message { .. })));
        run(&f, deliver(&f, 2, &command(1))).await;
        assert_eq!(f.event(), None, "expired command is not offered");
        let replies = outbox(&f).await;
        let states: Vec<CommandState> = replies.iter().map(|row| reply_state(&f, row)).collect();
        assert_eq!(states, [ReceivedByRunner, CommandState::Failed]);
    }

    #[tokio::test]
    async fn status_from_peer_advances_an_outgoing_command() {
        let mut f = fixture(PairState::Active).await;
        let pair = f.invitation.pair;
        let id = MessageId::generate();
        db::set_command(
            &mut f.shared.db.conn().await.unwrap(),
            pair,
            id,
            Direction::Outgoing,
            AcceptedDurable,
        )
        .await
        .unwrap();
        let status = Body::Status {
            command: id,
            state: ReceivedByRunner,
            output: None,
        };
        run(&f, deliver(&f, 1, &status)).await;
        assert!(matches!(f.event(), Some(Event::Message { .. })));
        assert_eq!(
            f.event(),
            Some(Event::Command {
                pair,
                id,
                state: ReceivedByRunner
            })
        );
    }

    /// Queues `count` outgoing commands and returns their IDs.
    async fn queue_commands(f: &Fixture, count: usize) -> Vec<MessageId> {
        let row = f.pair_row().await;
        let mut ids = Vec::new();
        for _ in 0..count {
            let message = f
                .shared
                .outgoing(&row, Lane::Control, &event_body("x"))
                .unwrap();
            let mut conn = f.shared.db.conn().await.unwrap();
            db::enqueue(&mut conn, &message).await.unwrap();
            db::set_command(
                &mut conn,
                row.id,
                message.id,
                Direction::Outgoing,
                QueuedLocal,
            )
            .await
            .unwrap();
            ids.push(message.id);
        }
        ids
    }

    #[tokio::test]
    async fn accepted_removes_the_message_and_advances_the_command() {
        let mut f = fixture(PairState::Active).await;
        let pair = f.invitation.pair;
        let id = queue_commands(&f, 1).await[0];
        run(
            &f,
            ServerFrame::Accepted {
                pair,
                id,
                position: Position(1),
            },
        )
        .await;
        assert!(matches!(f.event(), Some(Event::Accepted { .. })));
        assert_eq!(
            f.event(),
            Some(Event::Command {
                pair,
                id,
                state: AcceptedDurable
            })
        );
        assert_eq!(outbox(&f).await.len(), 0);
        run(
            &f,
            ServerFrame::Accepted {
                pair,
                id,
                position: Position(1),
            },
        )
        .await;
        assert_eq!(f.event(), None, "repeated accept is ignored");
    }

    #[tokio::test]
    async fn transient_errors_delay_and_permanent_errors_fail() {
        let mut f = fixture(PairState::Active).await;
        let pair = f.invitation.pair;
        let ids = queue_commands(&f, 2).await;
        let error = |code, id| ServerFrame::Error {
            code,
            message: "x".to_owned(),
            pair: Some(pair),
            id: Some(id),
            artifact: None,
        };
        run(&f, error(ErrorCode::RateLimited, ids[0])).await;
        assert_eq!(f.event(), None, "transient errors retry quietly");
        run(&f, error(ErrorCode::Conflict, ids[1])).await;
        assert!(matches!(
            f.event(),
            Some(Event::Failed {
                code: ErrorCode::Conflict,
                ..
            })
        ));
        let mut conn = f.shared.db.conn().await.unwrap();
        let state = db::command_state(&mut conn, pair, ids[1], Direction::Outgoing)
            .await
            .unwrap();
        assert_eq!(state, Some(CommandState::Failed));
        drop(conn);
        assert_eq!(outbox(&f).await.len(), 0, "delayed retry is not due yet");
        run(
            &f,
            ServerFrame::Error {
                code: ErrorCode::InvalidFrame,
                message: String::new(),
                pair: None,
                id: None,
                artifact: None,
            },
        )
        .await;
        assert!(matches!(f.event(), Some(Event::Failed { pair: None, .. })));
    }

    fn info(f: &Fixture, state: PairState, greeting: Option<Blob>) -> PairInfo {
        PairInfo {
            id: f.invitation.pair,
            state,
            inviter: f.invitation.inviter,
            invitee: Some(f.peer_device.device_id()),
            greeting,
            expires_at: None,
        }
    }

    #[tokio::test]
    async fn valid_claim_stores_keys_and_forged_claim_queues_unpair() {
        let mut f = fixture(PairState::Invited).await;
        let mut row = f.pair_row().await;
        row.keys = None;
        db::update_pair(&mut f.shared.db.conn().await.unwrap(), &row)
            .await
            .unwrap();
        let forged = Some(Blob::new(vec![1; 64]));
        on_pair(
            &f.shared,
            &info(&f, PairState::Claimed, forged),
            &mut InFlight::default(),
        )
        .await
        .unwrap();
        assert_eq!(f.pair_row().await.pending, Some(PendingOp::Unpair));
        assert!(matches!(f.event(), Some(Event::Pair { .. })));
        assert_eq!(f.event(), None, "no claim event");

        let greeting = Some(f.invitation.greet(&f.peer_device).unwrap());
        row.pending = None;
        db::update_pair(&mut f.shared.db.conn().await.unwrap(), &row)
            .await
            .unwrap();
        on_pair(
            &f.shared,
            &info(&f, PairState::Claimed, greeting),
            &mut InFlight::default(),
        )
        .await
        .unwrap();
        assert!(f.pair_row().await.keys.is_some());
        assert!(matches!(f.event(), Some(Event::Pair { .. })));
        assert!(matches!(f.event(), Some(Event::Claimed { .. })));
    }

    #[tokio::test]
    async fn revocation_drops_keys_and_queued_traffic() {
        let f = fixture(PairState::Active).await;
        let row = f.pair_row().await;
        let message = f
            .shared
            .outgoing(&row, Lane::Data, &event_body("x"))
            .unwrap();
        db::enqueue(&mut f.shared.db.conn().await.unwrap(), &message)
            .await
            .unwrap();
        on_pair(
            &f.shared,
            &info(&f, PairState::Revoked, None),
            &mut InFlight::default(),
        )
        .await
        .unwrap();
        let row = f.pair_row().await;
        assert_eq!((row.state, row.keys.as_ref()), (PairState::Revoked, None));
        assert_eq!(outbox(&f).await.len(), 0);
        assert!(matches!(
            f.shared.pair_keys(&row),
            Err(crate::ClientError::PairState)
        ));
    }

    #[tokio::test]
    async fn live_updates_and_presence_become_events() {
        let mut f = fixture(PairState::Active).await;
        let pair = f.invitation.pair;
        let body = wici_protocol::LiveBody {
            stream: StreamId::from_bytes([3; 16]),
            data: json!(1),
        };
        run(
            &f,
            ServerFrame::Live {
                pair,
                sealed: f.peer.seal_live(&body).unwrap(),
            },
        )
        .await;
        assert_eq!(f.event(), Some(Event::Live { pair, body }));
        run(
            &f,
            ServerFrame::Presence {
                pair,
                online: true,
                last_seen: None,
            },
        )
        .await;
        assert_eq!(
            f.event(),
            Some(Event::Presence {
                pair,
                online: true,
                last_seen: None
            })
        );
    }

    #[tokio::test]
    async fn pair_operation_errors_clear_or_keep_the_pending_operation() {
        let mut f = fixture(PairState::Invited).await;
        let pair = f.invitation.pair;
        let mut row = f.pair_row().await;
        row.pending = Some(PendingOp::Invite);
        db::update_pair(&mut f.shared.db.conn().await.unwrap(), &row)
            .await
            .unwrap();
        let error = |code| ServerFrame::Error {
            code,
            message: String::new(),
            pair: Some(pair),
            id: None,
            artifact: None,
        };
        let mut flight = InFlight::default();
        handle(&f.shared, error(ErrorCode::Internal), &mut flight)
            .await
            .unwrap();
        assert!(flight.failed_ops.contains(&(pair, PendingOp::Invite)));
        assert_eq!(f.pair_row().await.pending, Some(PendingOp::Invite));
        handle(&f.shared, error(ErrorCode::Expired), &mut flight)
            .await
            .unwrap();
        let row = f.pair_row().await;
        assert_eq!((row.pending, row.state), (None, PairState::Expired));
        assert!(matches!(f.event(), Some(Event::Pair { .. })));
        assert!(matches!(
            f.event(),
            Some(Event::Failed {
                code: ErrorCode::Expired,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn not_found_ends_a_claim_once_the_invitation_expired() {
        let mut f = fixture(PairState::Invited).await;
        let pair = f.invitation.pair;
        let mut row = f.pair_row().await;
        row.pending = Some(PendingOp::Claim);
        db::update_pair(&mut f.shared.db.conn().await.unwrap(), &row)
            .await
            .unwrap();
        // The fixture invitation expired at time zero.
        let error = ServerFrame::Error {
            code: ErrorCode::NotFound,
            message: String::new(),
            pair: Some(pair),
            id: None,
            artifact: None,
        };
        run(&f, error).await;
        assert_eq!(f.pair_row().await.pending, None);
        assert!(matches!(f.event(), Some(Event::Pair { .. })));
    }
}
