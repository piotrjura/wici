//! Load test for a Wici server.
//!
//! Simulates users with a paired Mac and phone on the raw WebSocket protocol,
//! so the run measures the server and not the client library. Each active
//! user repeats the Sfora pattern: the Mac sends a change notice, the phone
//! asks for a snapshot, and the Mac sends the snapshot. Idle users stay
//! connected. A run fails if a message is lost or duplicated, the server
//! sends an error, or a connection ends early.

mod args;
mod device;
mod error;
mod payload;
mod report;
mod user;

use std::time::Duration;

use futures_util::{StreamExt, TryStreamExt};
use tokio::task::JoinSet;
use tokio::time::Instant;

pub use args::{ArgsError, USAGE, parse};
pub use error::LoadError;
pub use report::{Latency, Percentiles, Report, Tally};

use user::{Role, Schedule, Script, User};

/// What a load run does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    /// Server WebSocket URL.
    pub url: String,
    /// Users. Each has a Mac and a phone.
    pub users: usize,
    /// Users that send traffic. The rest stay idle.
    pub active: usize,
    /// Notice interval per active user.
    pub interval: Duration,
    /// Traffic phase length.
    pub duration: Duration,
    /// Time after the traffic phase for messages in flight.
    pub drain: Duration,
    /// Snapshot size in bytes.
    pub snapshot_bytes: usize,
    /// Users set up at once.
    pub concurrency: usize,
    /// Time to connect and pair one user.
    pub setup_timeout: Duration,
}

impl Plan {
    /// Default plan against `url`.
    #[must_use]
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            users: 100,
            active: 10,
            interval: Duration::from_secs(2),
            duration: Duration::from_secs(60),
            drain: Duration::from_secs(5),
            snapshot_bytes: 8192,
            concurrency: 100,
            setup_timeout: Duration::from_secs(30),
        }
    }
}

/// Connects and pairs every user, runs traffic, and reports.
///
/// # Errors
///
/// [`LoadError`] if a user cannot connect or pair. Problems during the
/// traffic phase are counted in the [`Report`], not returned.
pub async fn run(plan: &Plan) -> Result<Report, LoadError> {
    let started = Instant::now();
    let users = setup(plan).await?;
    let setup = started.elapsed();
    let tally = traffic(plan, users).await;
    Ok(Report {
        users: plan.users,
        active: plan.active.min(plan.users),
        setup,
        duration: plan.duration,
        tally,
    })
}

async fn setup(plan: &Plan) -> Result<Vec<User>, LoadError> {
    futures_util::stream::iter(0..plan.users)
        .map(|_| async {
            tokio::time::timeout(plan.setup_timeout, user::pair(&plan.url))
                .await
                .map_err(|_| LoadError::Timeout)?
        })
        .buffer_unordered(plan.concurrency.max(1))
        .try_collect()
        .await
}

async fn traffic(plan: &Plan, users: Vec<User>) -> Tally {
    let clock = Instant::now();
    let stop_sending = clock + plan.duration;
    let schedule = Schedule {
        clock,
        stop_sending,
        stop: stop_sending + plan.drain,
        snapshot_bytes: plan.snapshot_bytes,
    };
    let count = users.len();
    let mut tasks = JoinSet::new();
    for (index, user) in users.into_iter().enumerate() {
        let notices = (index < plan.active)
            .then(|| (clock + stagger(plan.interval, index, count), plan.interval));
        let script = |role, notices| Script {
            role,
            pair: user.pair,
            notices,
            schedule,
        };
        tasks.spawn(user::exchange(user.mac, script(Role::Mac, notices)));
        tasks.spawn(user::exchange(user.phone, script(Role::Phone, None)));
    }
    let mut total = Tally::default();
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(tally) => total.merge(tally),
            Err(_) => total.errors += 1,
        }
    }
    total
}

/// Spreads first notices over one interval so users do not send at once.
fn stagger(interval: Duration, index: usize, count: usize) -> Duration {
    let (Ok(index), Ok(count)) = (u32::try_from(index), u32::try_from(count.max(1))) else {
        return Duration::ZERO;
    };
    interval
        .checked_mul(index)
        .map_or(Duration::ZERO, |total| total / count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stagger_spreads_over_one_interval() {
        let second = Duration::from_secs(1);
        assert_eq!(stagger(second, 0, 4), Duration::ZERO);
        assert_eq!(stagger(second, 2, 4), Duration::from_millis(500));
        assert_eq!(stagger(second, 0, 0), Duration::ZERO);
        assert_eq!(stagger(Duration::MAX, 2, 4), Duration::ZERO);
        assert_eq!(stagger(second, usize::MAX, 4), Duration::ZERO);
    }
}
