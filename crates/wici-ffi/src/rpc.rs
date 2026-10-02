//! JSON requests: the whole client API behind one entry point.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::Deserialize;
use serde_json::{Value, json};
use wici_client::{Client, ClientError, Direction, NewCommand};
use wici_protocol::{
    ArtifactId, ArtifactRef, Body, CommandState, Lane, LiveBody, MessageId, PairId, Position,
    StreamId, Timestamp,
};

/// One client call. JSON with a `method` tag.
#[derive(Debug, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case")]
pub(crate) enum Request {
    Invite,
    Join {
        link: String,
    },
    Approve {
        pair: PairId,
    },
    Unpair {
        pair: PairId,
    },
    Pairs,
    Send {
        pair: PairId,
        lane: Lane,
        body: Body,
    },
    Command {
        pair: PairId,
        operation: String,
        input: Value,
        deadline: Timestamp,
        stream: Option<StreamId>,
    },
    Report {
        pair: PairId,
        command: MessageId,
        state: CommandState,
        output: Option<Value>,
    },
    Live {
        pair: PairId,
        body: LiveBody,
    },
    Pending,
    Handled {
        pair: PairId,
        lane: Lane,
        position: Position,
    },
    CommandState {
        pair: PairId,
        id: MessageId,
        direction: Direction,
    },
    Upload {
        pair: PairId,
        data: String,
        media_type: String,
        name: Option<String>,
    },
    Download {
        pair: PairId,
        artifact: ArtifactRef,
    },
    DeleteArtifact {
        pair: PairId,
        artifact: ArtifactId,
    },
    ReconnectNow,
}

/// A failed call, as JSON for the host app.
#[derive(Debug, PartialEq)]
pub(crate) struct Failure {
    pub(crate) kind: &'static str,
    pub(crate) message: String,
}

impl Failure {
    pub(crate) fn new(kind: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub(crate) fn to_json(&self) -> Value {
        json!({ "error": { "kind": self.kind, "message": self.message } })
    }
}

impl From<ClientError> for Failure {
    fn from(error: ClientError) -> Self {
        let kind = match &error {
            ClientError::Storage(_) => "storage",
            ClientError::Crypto(_) => "crypto",
            ClientError::Corrupt(_) => "corrupt",
            ClientError::UnknownPair => "unknown_pair",
            ClientError::PairState => "pair_state",
            ClientError::Transition { .. } => "transition",
            ClientError::Body(_) => "invalid_body",
            ClientError::TooLarge => "too_large",
            ClientError::OwnInvitation => "own_invitation",
            ClientError::Offline => "offline",
            ClientError::Server { .. } => "server",
        };
        Self::new(kind, error.to_string())
    }
}

fn to_value<T: serde::Serialize>(value: &T) -> Result<Value, Failure> {
    serde_json::to_value(value).map_err(|e| Failure::new("encoding", e.to_string()))
}

/// Parses and runs a request.
pub(crate) async fn call(client: &Client, request: &str) -> Result<Value, Failure> {
    let request: Request = serde_json::from_str(request)
        .map_err(|e| Failure::new("invalid_request", e.to_string()))?;
    dispatch(client, request).await
}

#[expect(
    clippy::too_many_lines,
    clippy::cognitive_complexity,
    reason = "one short arm per request type"
)]
async fn dispatch(client: &Client, request: Request) -> Result<Value, Failure> {
    Ok(match request {
        Request::Invite => {
            let invitation = client.invite().await?;
            let link = invitation.to_link().map_err(ClientError::from)?;
            json!({ "pair": invitation.pair, "link": link })
        }
        Request::Join { link } => json!({ "pair": client.join(&link).await? }),
        Request::Approve { pair } => to_value(&client.approve(pair).await?)?,
        Request::Unpair { pair } => to_value(&client.unpair(pair).await?)?,
        Request::Pairs => to_value(&client.pairs().await?)?,
        Request::Send { pair, lane, body } => {
            json!({ "id": client.send(pair, lane, &body).await? })
        }
        Request::Command {
            pair,
            operation,
            input,
            deadline,
            stream,
        } => {
            let command = NewCommand {
                operation,
                input,
                deadline,
                stream,
            };
            json!({ "id": client.command(pair, command).await? })
        }
        Request::Report {
            pair,
            command,
            state,
            output,
        } => {
            json!({ "id": client.report(pair, command, state, output).await? })
        }
        Request::Live { pair, body } => to_value(&client.live(pair, &body).await?)?,
        Request::Pending => to_value(&client.pending().await?)?,
        Request::Handled {
            pair,
            lane,
            position,
        } => to_value(&client.handled(pair, lane, position).await?)?,
        Request::CommandState {
            pair,
            id,
            direction,
        } => to_value(&client.command_state(pair, id, direction).await?)?,
        Request::Upload {
            pair,
            data,
            media_type,
            name,
        } => {
            let data = STANDARD
                .decode(data)
                .map_err(|e| Failure::new("invalid_request", e.to_string()))?;
            to_value(&client.upload(pair, data, &media_type, name).await?)?
        }
        Request::Download { pair, artifact } => {
            json!({ "data": STANDARD.encode(client.download(pair, &artifact).await?) })
        }
        Request::DeleteArtifact { pair, artifact } => {
            to_value(&client.delete_artifact(pair, artifact).await?)?
        }
        Request::ReconnectNow => {
            client.reconnect_now();
            Value::Null
        }
    })
}
