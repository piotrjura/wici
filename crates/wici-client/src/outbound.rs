//! Sends queued pair operations and durable messages.

use std::collections::HashSet;

use wici_protocol::{ClientFrame, MessageId, PairId};

use crate::db::{self, PairRow};
use crate::error::ClientResult;
use crate::model::PendingOp;
use crate::shared::Shared;

/// Outbox rows sent per flush.
const FLUSH_LIMIT: i64 = 256;

/// Work sent on this connection and not yet answered. Cleared on reconnect.
#[derive(Debug, Default)]
pub(crate) struct InFlight {
    pub(crate) messages: HashSet<(PairId, MessageId)>,
    pub(crate) ops: HashSet<(PairId, PendingOp)>,
    /// Operations that failed for a transient reason. Retried on the next tick.
    pub(crate) failed_ops: HashSet<(PairId, PendingOp)>,
}

impl InFlight {
    /// Allows transiently failed operations to be sent again.
    pub(crate) fn retry(&mut self) {
        self.failed_ops.clear();
    }
}

/// Frame for a pending pair operation.
fn op_frame(shared: &Shared, row: &PairRow, op: PendingOp) -> ClientResult<ClientFrame> {
    let pair = row.id;
    Ok(match op {
        PendingOp::Invite => {
            let invitation = shared.open_invitation(row)?;
            ClientFrame::Invite {
                pair,
                claim_hash: invitation.claim_hash(),
            }
        }
        PendingOp::Claim => {
            let invitation = shared.open_invitation(row)?;
            ClientFrame::Claim {
                pair,
                claim_secret: invitation.claim_secret(),
                greeting: invitation.greet(&shared.keys)?,
            }
        }
        PendingOp::Approve => ClientFrame::Approve { pair },
        PendingOp::Unpair => ClientFrame::Unpair { pair },
    })
}

/// Frames for every queued item not already in flight. Marks them in flight.
pub(crate) async fn due_frames(
    shared: &Shared,
    flight: &mut InFlight,
) -> ClientResult<Vec<ClientFrame>> {
    let mut frames = op_frames(shared, flight).await?;
    let mut conn = shared.db.conn().await?;
    for row in db::due_outbox(&mut conn, FLUSH_LIMIT).await? {
        if flight.messages.insert((row.pair, row.id)) {
            frames.push(ClientFrame::Send {
                pair: row.pair,
                id: row.id,
                lane: row.lane,
                sealed: wici_protocol::Blob::new(row.sealed),
            });
        }
    }
    Ok(frames)
}

async fn op_frames(shared: &Shared, flight: &mut InFlight) -> ClientResult<Vec<ClientFrame>> {
    let mut conn = shared.db.conn().await?;
    let rows = db::all_pairs(&mut conn).await?;
    drop(conn);
    let mut frames = Vec::new();
    for row in rows {
        let Some(op) = row.pending else { continue };
        let key = (row.id, op);
        if flight.ops.contains(&key) || flight.failed_ops.contains(&key) {
            continue;
        }
        match op_frame(shared, &row, op) {
            Ok(frame) => {
                frames.push(frame);
                flight.ops.insert(key);
            }
            Err(error) => tracing::warn!(%error, pair = %row.id, "cannot build pair operation"),
        }
    }
    Ok(frames)
}
