//! Setup failures.

use std::error::Error;
use std::fmt;

use wici_crypto::CryptoError;
use wici_protocol::ErrorCode;

/// A load run could not set up its devices.
#[derive(Debug)]
pub enum LoadError {
    /// The WebSocket connection failed.
    Connect(String),
    /// The server closed the connection.
    Closed,
    /// The server sent a frame that cannot be decoded, or out of order.
    Protocol(&'static str),
    /// The server refused a setup step.
    Rejected {
        /// Error code.
        code: ErrorCode,
        /// Reason.
        message: String,
    },
    /// A setup step took too long.
    Timeout,
    /// Invitation keys failed.
    Crypto(CryptoError),
}

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect(reason) => write!(f, "cannot connect: {reason}"),
            Self::Closed => f.write_str("server closed the connection"),
            Self::Protocol(what) => write!(f, "unexpected server frame: {what}"),
            Self::Rejected { code, message } => write!(f, "server refused: {code}: {message}"),
            Self::Timeout => f.write_str("setup timed out"),
            Self::Crypto(error) => write!(f, "invitation failed: {error}"),
        }
    }
}

impl Error for LoadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Crypto(error) => Some(error),
            Self::Connect(_)
            | Self::Closed
            | Self::Protocol(_)
            | Self::Rejected { .. }
            | Self::Timeout => None,
        }
    }
}

impl From<CryptoError> for LoadError {
    fn from(error: CryptoError) -> Self {
        Self::Crypto(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_name_the_problem() {
        let rejected = LoadError::Rejected {
            code: ErrorCode::Forbidden,
            message: "no".to_owned(),
        };
        assert_eq!(rejected.to_string(), "server refused: forbidden: no");
        assert_eq!(
            LoadError::Closed.to_string(),
            "server closed the connection"
        );
        assert_eq!(LoadError::Timeout.to_string(), "setup timed out");
        assert_eq!(
            LoadError::Protocol("x").to_string(),
            "unexpected server frame: x"
        );
        assert_eq!(
            LoadError::Connect("refused".to_owned()).to_string(),
            "cannot connect: refused"
        );
        assert!(rejected.source().is_none());
        let crypto = LoadError::from(CryptoError::Encoding);
        assert!(crypto.to_string().starts_with("invitation failed: "));
        assert!(crypto.source().is_some());
    }
}
