//! Command-line options.

use std::error::Error;
use std::fmt;
use std::time::Duration;

use crate::Plan;

/// Usage text for `--help` and option errors.
pub const USAGE: &str = "\
usage: wici-load [options]
  --url URL             server WebSocket URL [ws://127.0.0.1:8080/v1/ws]
  --users N             users; each has a Mac and a phone [100]
  --active N            users that send traffic [10% of users]
  --interval-ms N       notice interval per active user [2000]
  --seconds N           traffic phase length [60]
  --drain-seconds N     wait for messages in flight [5]
  --snapshot-bytes N    snapshot size [8192]
  --concurrency N       users set up at once [100]";

/// Options cannot be used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArgsError {
    /// `--help` was given.
    Help,
    /// No such option.
    Unknown(String),
    /// The option needs a value.
    Missing(String),
    /// The value is not valid for the option.
    Invalid(String),
}

impl fmt::Display for ArgsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Help => return f.write_str(USAGE),
            Self::Unknown(option) => write!(f, "unknown option {option}"),
            Self::Missing(option) => write!(f, "{option} needs a value"),
            Self::Invalid(option) => write!(f, "invalid value for {option}"),
        }?;
        write!(f, "\n{USAGE}")
    }
}

impl Error for ArgsError {}

/// Builds a plan from options, without the program name.
///
/// # Errors
///
/// [`ArgsError`] for `--help`, unknown options, and missing or invalid values.
pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Plan, ArgsError> {
    let mut plan = Plan::new("ws://127.0.0.1:8080/v1/ws");
    let mut active = None;
    let mut args = args.into_iter();
    while let Some(option) = args.next() {
        if option == "--help" {
            return Err(ArgsError::Help);
        }
        let value = args
            .next()
            .ok_or_else(|| ArgsError::Missing(option.clone()))?;
        if option == "--active" {
            active = Some(count(&option, &value)?);
        } else {
            set(&mut plan, &option, &value)?;
        }
    }
    plan.active = active.unwrap_or(plan.users / 10);
    check(&plan)?;
    Ok(plan)
}

fn set(plan: &mut Plan, option: &str, value: &str) -> Result<(), ArgsError> {
    let number = || number(option, value);
    let count = || count(option, value);
    match option {
        "--url" => value.clone_into(&mut plan.url),
        "--users" => plan.users = count()?,
        "--interval-ms" => plan.interval = Duration::from_millis(number()?),
        "--seconds" => plan.duration = Duration::from_secs(number()?),
        "--drain-seconds" => plan.drain = Duration::from_secs(number()?),
        "--snapshot-bytes" => plan.snapshot_bytes = count()?,
        "--concurrency" => plan.concurrency = count()?,
        _ => return Err(ArgsError::Unknown(option.to_owned())),
    }
    Ok(())
}

fn number(option: &str, value: &str) -> Result<u64, ArgsError> {
    value
        .parse()
        .map_err(|_| ArgsError::Invalid(option.to_owned()))
}

fn count(option: &str, value: &str) -> Result<usize, ArgsError> {
    usize::try_from(number(option, value)?).map_err(|_| ArgsError::Invalid(option.to_owned()))
}

fn check(plan: &Plan) -> Result<(), ArgsError> {
    let invalid = |option: &str| Err(ArgsError::Invalid(option.to_owned()));
    if plan.active > plan.users {
        return invalid("--active");
    }
    if plan.interval.is_zero() {
        return invalid("--interval-ms");
    }
    if plan.concurrency == 0 {
        return invalid("--concurrency");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_all(args: &[&str]) -> Result<Plan, ArgsError> {
        parse(args.iter().map(|&arg| arg.to_owned()))
    }

    #[test]
    fn defaults_and_overrides() {
        assert_eq!(
            parse_all(&[]).unwrap(),
            Plan::new("ws://127.0.0.1:8080/v1/ws")
        );
        let plan = parse_all(&[
            "--url",
            "ws://h/v1/ws",
            "--users",
            "5",
            "--active",
            "2",
            "--interval-ms",
            "50",
            "--seconds",
            "3",
            "--drain-seconds",
            "4",
            "--snapshot-bytes",
            "64",
            "--concurrency",
            "7",
        ])
        .unwrap();
        assert_eq!(plan.url, "ws://h/v1/ws");
        assert_eq!((plan.users, plan.active), (5, 2));
        assert_eq!(plan.interval, Duration::from_millis(50));
        assert_eq!(plan.duration, Duration::from_secs(3));
        assert_eq!(plan.drain, Duration::from_secs(4));
        assert_eq!((plan.snapshot_bytes, plan.concurrency), (64, 7));
    }

    #[test]
    fn bad_options_are_named() {
        let invalid = |option: &str| Err(ArgsError::Invalid(option.to_owned()));
        assert_eq!(parse_all(&["--help"]), Err(ArgsError::Help));
        assert_eq!(
            parse_all(&["--x", "1"]),
            Err(ArgsError::Unknown("--x".to_owned()))
        );
        assert_eq!(
            parse_all(&["--users"]),
            Err(ArgsError::Missing("--users".to_owned()))
        );
        assert_eq!(parse_all(&["--users", "many"]), invalid("--users"));
        assert_eq!(parse_all(&["--active", "-1"]), invalid("--active"));
        assert_eq!(
            parse_all(&["--users", "1", "--active", "2"]),
            invalid("--active")
        );
        assert_eq!(parse_all(&["--interval-ms", "0"]), invalid("--interval-ms"));
        assert_eq!(parse_all(&["--concurrency", "0"]), invalid("--concurrency"));
    }

    #[test]
    fn errors_show_usage() {
        assert_eq!(ArgsError::Help.to_string(), USAGE);
        for (error, first) in [
            (ArgsError::Unknown("--x".to_owned()), "unknown option --x"),
            (ArgsError::Missing("--y".to_owned()), "--y needs a value"),
            (
                ArgsError::Invalid("--z".to_owned()),
                "invalid value for --z",
            ),
        ] {
            assert_eq!(error.to_string(), format!("{first}\n{USAGE}"));
        }
    }
}
