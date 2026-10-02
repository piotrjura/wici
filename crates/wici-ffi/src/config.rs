//! Client configuration from JSON.

use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;
use wici_client::ClientConfig;

/// `server_url` and `database` are required; durations are milliseconds.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Json {
    server_url: String,
    database: PathBuf,
    invite_ttl_ms: Option<u64>,
    reconnect_min_ms: Option<u64>,
    reconnect_max_ms: Option<u64>,
    retry_interval_ms: Option<u64>,
    request_timeout_ms: Option<u64>,
}

pub(crate) fn parse(text: &str) -> Result<ClientConfig, String> {
    let json: Json = serde_json::from_str(text).map_err(|e| format!("invalid config: {e}"))?;
    let mut config = ClientConfig::new(json.server_url, json.database);
    let set = |target: &mut Duration, millis: Option<u64>| {
        if let Some(millis) = millis {
            *target = Duration::from_millis(millis);
        }
    };
    set(&mut config.invite_ttl, json.invite_ttl_ms);
    set(&mut config.reconnect_min, json.reconnect_min_ms);
    set(&mut config.reconnect_max, json.reconnect_max_ms);
    set(&mut config.retry_interval, json.retry_interval_ms);
    set(&mut config.request_timeout, json.request_timeout_ms);
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_required_fields_and_overrides() {
        let config =
            parse(r#"{"server_url":"ws://x","database":"/tmp/a.db","retry_interval_ms":5}"#)
                .unwrap();
        assert_eq!(config.server_url, "ws://x");
        assert_eq!(config.retry_interval, Duration::from_millis(5));
        assert_eq!(
            config.reconnect_max,
            ClientConfig::new("", "").reconnect_max
        );
    }

    #[test]
    fn rejects_missing_and_unknown_fields() {
        assert!(parse("{}").unwrap_err().starts_with("invalid config"));
        assert!(parse(r#"{"server_url":"x","database":"y","typo":1}"#).is_err());
    }
}
