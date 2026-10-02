//! Reconnect delays: exponential, capped, with jitter.

use std::time::Duration;

use rand::Rng;

#[derive(Debug)]
pub(crate) struct Backoff {
    min: Duration,
    max: Duration,
    attempt: u32,
}

impl Backoff {
    pub(crate) const fn new(min: Duration, max: Duration) -> Self {
        Self {
            min,
            max,
            attempt: 0,
        }
    }

    pub(crate) const fn reset(&mut self) {
        self.attempt = 0;
    }

    /// Next delay: a random value in the upper half of `min * 2^attempt`,
    /// capped at `max`. Jitter spreads reconnect storms.
    pub(crate) fn next(&mut self) -> Duration {
        let factor = 2_u32.saturating_pow(self.attempt.min(16));
        self.attempt = self.attempt.saturating_add(1);
        let ceiling = self.min.saturating_mul(factor).min(self.max);
        let floor = ceiling / 2;
        rand::thread_rng().gen_range(floor..=ceiling)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delays_grow_stay_capped_and_reset() {
        let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(1));
        let first = backoff.next();
        assert!(first >= Duration::from_millis(50) && first <= Duration::from_millis(100));
        let second = backoff.next();
        assert!(second >= Duration::from_millis(100) && second <= Duration::from_millis(200));
        for _ in 0..3 {
            backoff.next();
        }
        for _ in 0..40 {
            let delay = backoff.next();
            assert!(delay >= Duration::from_millis(500) && delay <= Duration::from_secs(1));
        }
        backoff.reset();
        assert!(backoff.next() <= Duration::from_millis(100));
    }
}
