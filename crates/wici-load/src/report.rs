//! Counters, latencies, and the final report.

use std::fmt;
use std::time::Duration;

/// Samples kept per latency. Later samples are counted but not kept.
const MAX_SAMPLES: usize = 1_000_000;

/// Latency samples in microseconds.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Latency {
    samples: Vec<u64>,
}

/// Latency percentiles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Percentiles {
    /// Median.
    pub p50: Duration,
    /// 95th percentile.
    pub p95: Duration,
    /// 99th percentile.
    pub p99: Duration,
    /// Slowest sample.
    pub max: Duration,
}

impl Latency {
    /// Adds one sample.
    pub(crate) fn record(&mut self, micros: u64) {
        if self.samples.len() < MAX_SAMPLES {
            self.samples.push(micros);
        }
    }

    fn merge(&mut self, other: Self) {
        let room = MAX_SAMPLES.saturating_sub(self.samples.len());
        self.samples.extend(other.samples.into_iter().take(room));
    }

    /// Number of kept samples.
    #[must_use]
    pub fn count(&self) -> usize {
        self.samples.len()
    }

    /// Percentiles, or `None` without samples.
    #[must_use]
    pub fn percentiles(&self) -> Option<Percentiles> {
        let mut sorted = self.samples.clone();
        sorted.sort_unstable();
        let at = |fraction: f64| {
            let last = sorted.len().checked_sub(1)?;
            #[expect(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                clippy::cast_precision_loss,
                reason = "rank is within 0..=last"
            )]
            let rank = (last as f64 * fraction).round() as usize;
            sorted.get(rank).copied().map(Duration::from_micros)
        };
        Some(Percentiles {
            p50: at(0.5)?,
            p95: at(0.95)?,
            p99: at(0.99)?,
            max: at(1.0)?,
        })
    }
}

/// What one device saw.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Tally {
    /// Messages the server accepted from this device.
    pub accepted: u64,
    /// Messages delivered to this device.
    pub delivered: u64,
    /// Error frames, unexpected messages, and refused sends.
    pub errors: u64,
    /// Connections that ended before the run did.
    pub disconnects: u64,
    /// Send to `accepted`.
    pub accept: Latency,
    /// Send to delivery at the peer.
    pub deliver: Latency,
    /// Notice sent to snapshot delivered.
    pub round_trip: Latency,
}

impl Tally {
    /// Adds `other` to this tally.
    pub(crate) fn merge(&mut self, other: Self) {
        self.accepted += other.accepted;
        self.delivered += other.delivered;
        self.errors += other.errors;
        self.disconnects += other.disconnects;
        self.accept.merge(other.accept);
        self.deliver.merge(other.deliver);
        self.round_trip.merge(other.round_trip);
    }
}

/// Result of a load run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    /// Simulated users. Each has a Mac and a phone.
    pub users: usize,
    /// Users that send traffic.
    pub active: usize,
    /// Time to connect and pair every user.
    pub setup: Duration,
    /// Traffic phase length.
    pub duration: Duration,
    /// Totals of every device.
    pub tally: Tally,
}

impl Report {
    /// `true` if nothing was lost, duplicated, refused, or disconnected.
    #[must_use]
    pub const fn passed(&self) -> bool {
        self.tally.accepted == self.tally.delivered
            && self.tally.errors == 0
            && self.tally.disconnects == 0
    }

    fn per_second(&self) -> f64 {
        #[expect(clippy::cast_precision_loss, reason = "a rate needs no exact count")]
        let delivered = self.tally.delivered as f64;
        delivered / self.duration.as_secs_f64().max(f64::EPSILON)
    }
}

fn line(f: &mut fmt::Formatter<'_>, name: &str, latency: &Latency) -> fmt::Result {
    let ms = |d: Duration| d.as_secs_f64() * 1000.0;
    match latency.percentiles() {
        Some(p) => writeln!(
            f,
            "{name:<11} p50 {:.1} ms  p95 {:.1} ms  p99 {:.1} ms  max {:.1} ms  (n={})",
            ms(p.p50),
            ms(p.p95),
            ms(p.p99),
            ms(p.max),
            latency.count()
        ),
        None => writeln!(f, "{name:<11} no samples"),
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let t = &self.tally;
        writeln!(
            f,
            "users       {} ({} devices), {} active",
            self.users,
            self.users * 2,
            self.active
        )?;
        writeln!(f, "setup       {:.1} s", self.setup.as_secs_f64())?;
        writeln!(
            f,
            "messages    {} accepted, {} delivered, {:.0}/s",
            t.accepted,
            t.delivered,
            self.per_second()
        )?;
        line(f, "accept", &t.accept)?;
        line(f, "deliver", &t.deliver)?;
        line(f, "round trip", &t.round_trip)?;
        writeln!(
            f,
            "problems    {} disconnects, {} errors",
            t.disconnects, t.errors
        )?;
        write!(f, "{}", if self.passed() { "PASS" } else { "FAIL" })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn latency(samples: &[u64]) -> Latency {
        let mut latency = Latency::default();
        for &sample in samples {
            latency.record(sample);
        }
        latency
    }

    fn report(tally: Tally) -> Report {
        Report {
            users: 2,
            active: 1,
            setup: Duration::from_secs(1),
            duration: Duration::from_secs(2),
            tally,
        }
    }

    #[test]
    fn percentiles_pick_ranked_samples() {
        assert_eq!(Latency::default().percentiles(), None);
        let samples: Vec<u64> = (1..=100).rev().map(|n| n * 1000).collect();
        let p = latency(&samples).percentiles().unwrap();
        assert_eq!(p.p50, Duration::from_millis(51));
        assert_eq!(p.p95, Duration::from_millis(95));
        assert_eq!(p.p99, Duration::from_millis(99));
        assert_eq!(p.max, Duration::from_millis(100));
    }

    #[test]
    fn samples_are_capped() {
        let mut full = Latency {
            samples: vec![1; MAX_SAMPLES],
        };
        full.record(2);
        full.merge(latency(&[3]));
        assert_eq!(full.count(), MAX_SAMPLES);
    }

    #[test]
    fn merge_adds_everything() {
        let one = Tally {
            accepted: 1,
            delivered: 2,
            errors: 3,
            disconnects: 4,
            accept: latency(&[1]),
            deliver: latency(&[2]),
            round_trip: latency(&[3]),
        };
        let mut sum = one.clone();
        sum.merge(one);
        assert_eq!(
            (sum.accepted, sum.delivered, sum.errors, sum.disconnects),
            (2, 4, 6, 8)
        );
        assert_eq!(sum.accept, latency(&[1, 1]));
        assert_eq!(sum.deliver, latency(&[2, 2]));
        assert_eq!(sum.round_trip, latency(&[3, 3]));
    }

    #[test]
    fn passes_only_without_loss_errors_or_disconnects() {
        let clean = Tally {
            accepted: 4,
            delivered: 4,
            ..Tally::default()
        };
        assert!(report(clean.clone()).passed());
        for broken in [
            Tally {
                delivered: 3,
                ..clean.clone()
            },
            Tally {
                errors: 1,
                ..clean.clone()
            },
            Tally {
                disconnects: 1,
                ..clean
            },
        ] {
            assert!(!report(broken).passed());
        }
    }

    #[test]
    fn display_lists_results_and_verdict() {
        let text = report(Tally {
            accepted: 4,
            delivered: 4,
            deliver: latency(&[1500]),
            ..Tally::default()
        })
        .to_string();
        assert!(
            text.contains("users       2 (4 devices), 1 active"),
            "{text}"
        );
        assert!(text.contains("4 accepted, 4 delivered, 2/s"), "{text}");
        assert!(text.contains("deliver     p50 1.5 ms"), "{text}");
        assert!(text.contains("accept      no samples"), "{text}");
        assert!(text.ends_with("PASS"), "{text}");
        let failed = report(Tally {
            errors: 1,
            ..Tally::default()
        });
        assert!(failed.to_string().ends_with("FAIL"));
    }
}
