//! Token bucket rate limit.

use std::time::Instant;

/// Allows `rate` events per second with bursts up to `burst`.
#[derive(Debug)]
pub(crate) struct RateLimit {
    rate: f64,
    burst: f64,
    tokens: f64,
    last: Instant,
}

impl RateLimit {
    pub(crate) fn new(rate: u32, burst: u32, now: Instant) -> Self {
        Self {
            rate: f64::from(rate),
            burst: f64::from(burst),
            tokens: f64::from(burst),
            last: now,
        }
    }

    /// Takes one token. `false` if none is left.
    pub(crate) fn allow(&mut self, now: Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.last = now;
        self.tokens = (self.tokens + elapsed * self.rate).min(self.burst);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn allows_burst_then_refills_at_rate() {
        let start = Instant::now();
        let mut limit = RateLimit::new(10, 3, start);
        assert!((0..3).all(|_| limit.allow(start)));
        assert!(!limit.allow(start));
        assert!(limit.allow(start + Duration::from_millis(100)));
        assert!(!limit.allow(start + Duration::from_millis(100)));
    }

    #[test]
    fn refill_never_exceeds_burst() {
        let start = Instant::now();
        let mut limit = RateLimit::new(10, 2, start);
        let later = start + Duration::from_secs(60);
        assert!(limit.allow(later) && limit.allow(later));
        assert!(!limit.allow(later));
    }

    #[test]
    fn clock_going_back_does_not_add_tokens() {
        let start = Instant::now() + Duration::from_secs(1);
        let mut limit = RateLimit::new(10, 1, start);
        assert!(limit.allow(start));
        assert!(!limit.allow(start.checked_sub(Duration::from_secs(1)).unwrap()));
    }
}
