use std::error::Error;
use std::fmt;

use wici_crypto::CryptoError;
use wici_protocol::{BodyError, CommandState, ErrorCode};

/// A client request failed.
#[derive(Debug)]
pub enum ClientError {
    /// Local database failure.
    Storage(sqlx::Error),
    /// Key or sealing failure.
    Crypto(CryptoError),
    /// A stored record cannot be decoded.
    Corrupt(&'static str),
    /// No such pair on this device.
    UnknownPair,
    /// The pair state does not allow the request.
    PairState,
    /// The command lifecycle does not allow this state change.
    Transition {
        /// Current state.
        from: Option<CommandState>,
        /// Requested state.
        to: CommandState,
    },
    /// The body breaks a protocol rule.
    Body(BodyError),
    /// The sealed message exceeds the configured limit.
    TooLarge,
    /// The invitation was created by this device.
    OwnInvitation,
    /// Not connected, or the connection ended during the request.
    Offline,
    /// The server rejected the request.
    Server {
        /// Error code.
        code: ErrorCode,
        /// Reason.
        message: String,
    },
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(_) => f.write_str("local storage error"),
            Self::Crypto(error) => write!(f, "crypto error: {error}"),
            Self::Corrupt(what) => write!(f, "corrupt local record: {what}"),
            Self::UnknownPair => f.write_str("unknown pair"),
            Self::PairState => f.write_str("pair state does not allow this"),
            Self::Transition {
                from: Some(from),
                to,
            } => {
                write!(f, "command cannot go from `{from}` to `{to}`")
            }
            Self::Transition { from: None, to } => write!(f, "unknown command, cannot set `{to}`"),
            Self::Body(error) => write!(f, "invalid body: {error}"),
            Self::TooLarge => f.write_str("message too large"),
            Self::OwnInvitation => f.write_str("cannot join own invitation"),
            Self::Offline => f.write_str("not connected"),
            Self::Server { code, message } => write!(f, "server error `{code}`: {message}"),
        }
    }
}

impl Error for ClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            Self::Crypto(error) => Some(error),
            Self::Body(error) => Some(error),
            Self::Corrupt(_)
            | Self::UnknownPair
            | Self::PairState
            | Self::Transition { .. }
            | Self::TooLarge
            | Self::OwnInvitation
            | Self::Offline
            | Self::Server { .. } => None,
        }
    }
}

impl From<sqlx::Error> for ClientError {
    fn from(error: sqlx::Error) -> Self {
        Self::Storage(error)
    }
}

impl From<sqlx::migrate::MigrateError> for ClientError {
    fn from(error: sqlx::migrate::MigrateError) -> Self {
        Self::Storage(error.into())
    }
}

impl From<CryptoError> for ClientError {
    fn from(error: CryptoError) -> Self {
        Self::Crypto(error)
    }
}

impl From<BodyError> for ClientError {
    fn from(error: BodyError) -> Self {
        Self::Body(error)
    }
}

/// Client result.
pub type ClientResult<T> = Result<T, ClientError>;

#[cfg(test)]
mod tests {
    use super::*;

    fn cases() -> Vec<(ClientError, &'static str)> {
        vec![
            (
                ClientError::Storage(sqlx::Error::PoolClosed),
                "local storage error",
            ),
            (
                ClientError::Crypto(CryptoError::Decrypt),
                "crypto error: decryption failed",
            ),
            (ClientError::Corrupt("x"), "corrupt local record: x"),
            (ClientError::UnknownPair, "unknown pair"),
            (ClientError::PairState, "pair state does not allow this"),
            (
                ClientError::Transition {
                    from: Some(CommandState::Completed),
                    to: CommandState::Running,
                },
                "command cannot go from `completed` to `running`",
            ),
            (
                ClientError::Transition {
                    from: None,
                    to: CommandState::Running,
                },
                "unknown command, cannot set `running`",
            ),
            (
                ClientError::Body(BodyError::OperationName),
                "invalid body: operation name must be 1 to 128 bytes",
            ),
            (ClientError::TooLarge, "message too large"),
            (ClientError::OwnInvitation, "cannot join own invitation"),
            (ClientError::Offline, "not connected"),
            (
                ClientError::Server {
                    code: ErrorCode::NotFound,
                    message: "gone".to_owned(),
                },
                "server error `not_found`: gone",
            ),
        ]
    }

    #[test]
    fn messages_name_the_problem() {
        let cases = cases();
        for (error, text) in cases {
            assert_eq!(error.to_string(), text);
            let has_source = matches!(
                error,
                ClientError::Storage(_) | ClientError::Crypto(_) | ClientError::Body(_)
            );
            assert_eq!(error.source().is_some(), has_source);
        }
        assert!(matches!(
            ClientError::from(sqlx::migrate::MigrateError::Dirty(1)),
            ClientError::Storage(_)
        ));
    }
}
