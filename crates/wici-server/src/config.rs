//! Server settings.

use std::net::SocketAddr;
use std::time::Duration;

use crate::store::{ArtifactLimits, StoreLimits};

/// Size, rate, and queue limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Largest accepted WebSocket text frame.
    pub max_frame_bytes: usize,
    /// Largest durable message ciphertext.
    pub max_sealed_bytes: usize,
    /// Largest live update ciphertext.
    pub max_live_bytes: usize,
    /// Largest pairing greeting.
    pub max_greeting_bytes: usize,
    /// Largest artifact chunk.
    pub max_chunk_bytes: usize,
    /// Artifact limits.
    pub artifacts: ArtifactLimits,
    /// Unacknowledged deliveries in flight per lane.
    pub delivery_window: u64,
    /// Messages read from storage per query.
    pub fetch_batch: u64,
    /// Queued control frames per connection. A full queue drops the connection.
    pub control_queue: usize,
    /// Queued data frames per connection. Live updates drop when full.
    pub data_queue: usize,
    /// Sustained frames per second per connection.
    pub frames_per_second: u32,
    /// Frame burst per connection.
    pub frame_burst: u32,
    /// Storage limits.
    pub store: StoreLimits,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 1536 * 1024,
            max_sealed_bytes: 1024 * 1024,
            max_live_bytes: 64 * 1024,
            max_greeting_bytes: 4096,
            max_chunk_bytes: 256 * 1024,
            artifacts: ArtifactLimits {
                max_bytes: 64 * 1024 * 1024,
                max_incomplete: 16,
            },
            delivery_window: 128,
            fetch_batch: 64,
            control_queue: 256,
            data_queue: 256,
            frames_per_second: 100,
            frame_burst: 200,
            store: StoreLimits {
                max_pairs_per_device: 16,
                max_pending_per_lane: 4096,
            },
        }
    }
}

/// Timing settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timeouts {
    /// Time to finish authentication after connecting.
    pub auth: Duration,
    /// Connection closes after this long without any incoming frame.
    pub idle: Duration,
    /// Ping interval. Keeps idle connections alive.
    pub ping: Duration,
    /// Time to claim an invitation.
    pub invite: Duration,
    /// Time to approve a claim.
    pub claim: Duration,
    /// Interval of the expiry sweep.
    pub sweep: Duration,
    /// Unfinished uploads are deleted after this.
    pub artifact_incomplete: Duration,
    /// Finished artifacts are deleted after this.
    pub artifact_complete: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            auth: Duration::from_secs(10),
            idle: Duration::from_secs(60),
            ping: Duration::from_secs(20),
            invite: Duration::from_secs(120),
            claim: Duration::from_secs(600),
            sweep: Duration::from_secs(5),
            artifact_incomplete: Duration::from_secs(24 * 3600),
            artifact_complete: Duration::from_secs(7 * 24 * 3600),
        }
    }
}

/// Server configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// Listen address.
    pub listen: SocketAddr,
    /// PostgreSQL URL.
    pub database_url: String,
    /// Database connections.
    pub database_connections: u32,
    /// Limits.
    pub limits: Limits,
    /// Timeouts.
    pub timeouts: Timeouts,
}

/// A required setting is missing or invalid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError(pub String);

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

impl Config {
    /// Reads `WICI_DATABASE_URL` (required), `WICI_LISTEN` (default
    /// `127.0.0.1:8080`), and `WICI_DATABASE_CONNECTIONS` (default 16).
    ///
    /// # Errors
    ///
    /// [`ConfigError`] for a missing URL or unparsable values.
    pub fn from_env(get: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let database_url = get("WICI_DATABASE_URL")
            .ok_or_else(|| ConfigError("WICI_DATABASE_URL is required".to_owned()))?;
        let listen = get("WICI_LISTEN")
            .unwrap_or_else(|| "127.0.0.1:8080".to_owned())
            .parse()
            .map_err(|_| ConfigError("WICI_LISTEN must be host:port".to_owned()))?;
        let database_connections = get("WICI_DATABASE_CONNECTIONS")
            .map_or(Ok(16), |v| v.parse())
            .map_err(|_| ConfigError("WICI_DATABASE_CONNECTIONS must be a number".to_owned()))?;
        Ok(Self {
            listen,
            database_url,
            database_connections,
            limits: Limits::default(),
            timeouts: Timeouts::default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |key| map.get(key).cloned()
    }

    #[test]
    fn reads_defaults_and_overrides() {
        let config = Config::from_env(env(&[("WICI_DATABASE_URL", "postgres://x")])).unwrap();
        assert_eq!(config.listen.to_string(), "127.0.0.1:8080");
        assert_eq!(config.database_connections, 16);
        let config = Config::from_env(env(&[
            ("WICI_DATABASE_URL", "postgres://x"),
            ("WICI_LISTEN", "0.0.0.0:9"),
            ("WICI_DATABASE_CONNECTIONS", "3"),
        ]))
        .unwrap();
        assert_eq!(config.listen.port(), 9);
        assert_eq!(config.database_connections, 3);
    }

    #[test]
    fn rejects_missing_or_bad_values() {
        let missing = Config::from_env(env(&[])).unwrap_err();
        assert_eq!(missing.to_string(), "WICI_DATABASE_URL is required");
        let bad_listen = env(&[("WICI_DATABASE_URL", "u"), ("WICI_LISTEN", "nope")]);
        assert!(Config::from_env(bad_listen).is_err());
        let bad_count = env(&[
            ("WICI_DATABASE_URL", "u"),
            ("WICI_DATABASE_CONNECTIONS", "x"),
        ]);
        assert!(Config::from_env(bad_count).is_err());
    }

    #[test]
    fn defaults_are_consistent() {
        let limits = Limits::default();
        assert!(limits.max_sealed_bytes < limits.max_frame_bytes);
        assert!(limits.fetch_batch <= limits.delivery_window);
        assert!(
            limits.max_chunk_bytes < limits.max_frame_bytes / 2,
            "base64 fits"
        );
        let timeouts = Timeouts::default();
        assert!(timeouts.ping < timeouts.idle);
    }
}
