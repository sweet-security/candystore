use std::time::{Duration, Instant};

/// A token-bucket pacer.
///
/// The pacer refills `tokens_per_unit` tokens every `time_unit`, up to `max_tokens`.
/// Calls to `consume` spend immediately available tokens and block until enough
/// tokens have accrued to satisfy the request.
pub struct Pacer {
    time_unit: Duration,
    tokens_per_unit: u64,
    max_tokens: u64, // burst capacity
    last_refill: Instant,
    available_tokens: u64,
}

impl Pacer {
    /// Creates a new pacer.
    ///
    /// `tokens_per_unit` must be non-zero and `time_unit` must be non-zero.
    /// `max_tokens` is promoted to at least `tokens_per_unit`, ensuring the
    /// bucket can hold one full refill interval.
    pub fn new(tokens_per_unit: u64, time_unit: Duration, max_tokens: u64) -> Self {
        assert!(tokens_per_unit > 0 && !time_unit.is_zero());
        let max_tokens = max_tokens.max(tokens_per_unit);

        Pacer {
            time_unit,
            tokens_per_unit,
            max_tokens,
            last_refill: Instant::now(),
            available_tokens: max_tokens,
        }
    }

    fn added_tokens(
        elapsed_ns: u128,
        time_unit_ns: u128,
        tokens_per_unit: u64,
        capacity: u64,
    ) -> u64 {
        let produced_tokens = elapsed_ns.saturating_mul(tokens_per_unit as u128) / time_unit_ns;
        produced_tokens.min(capacity as u128) as u64
    }

    fn duration_from_nanos_saturating(total_nanos: u128) -> Duration {
        let secs = total_nanos / 1_000_000_000;
        if secs > u64::MAX as u128 {
            return Duration::MAX;
        }

        Duration::new(secs as u64, (total_nanos % 1_000_000_000) as u32)
    }

    fn refill(&mut self, now: Instant) {
        if self.available_tokens == self.max_tokens {
            self.last_refill = now;
            return;
        }

        let elapsed_ns = now.saturating_duration_since(self.last_refill).as_nanos();
        let time_unit_ns = self.time_unit.as_nanos();
        let capacity = self.max_tokens - self.available_tokens;
        let added_tokens =
            Self::added_tokens(elapsed_ns, time_unit_ns, self.tokens_per_unit, capacity);
        if added_tokens == 0 {
            return;
        }

        self.available_tokens += added_tokens;

        if self.available_tokens == self.max_tokens {
            self.last_refill = now;
        } else {
            // Advance last_refill by exact time accounted for by added_tokens
            let time_advanced_ns =
                (added_tokens as u128 * time_unit_ns) / self.tokens_per_unit as u128;
            self.last_refill += Self::duration_from_nanos_saturating(time_advanced_ns);
        }
    }

    fn time_until_tokens(&self, now: Instant, tokens_needed: u64) -> Duration {
        let elapsed_ns = now.saturating_duration_since(self.last_refill).as_nanos();
        let time_unit_ns = self.time_unit.as_nanos();
        let target_ns = (tokens_needed as u128)
            .saturating_mul(time_unit_ns)
            .div_ceil(self.tokens_per_unit as u128);
        let remaining_ns = target_ns.saturating_sub(elapsed_ns);

        Self::duration_from_nanos_saturating(remaining_ns)
    }

    /// Consumes `tokens`, sleeping through the provided callback while waiting for refills.
    pub fn consume_with_sleep_fn(&mut self, mut tokens: u64, mut sleep: impl FnMut(Duration)) {
        while tokens > 0 {
            let now = Instant::now();
            self.refill(now);

            if self.available_tokens > 0 {
                let consumed = self.available_tokens.min(tokens);
                self.available_tokens -= consumed;
                tokens -= consumed;
                if tokens == 0 {
                    break;
                }
            }

            let tokens_to_wait = tokens.min(self.max_tokens);
            sleep(self.time_until_tokens(now, tokens_to_wait));
        }
    }

    /// Consumes `tokens`, blocking the current thread until enough tokens are available.
    pub fn consume(&mut self, tokens: u64) {
        self.consume_with_sleep_fn(tokens, std::thread::sleep);
    }
}

#[cfg(test)]
mod tests {
    use super::Pacer;
    use std::time::{Duration, Instant};

    #[test]
    fn test_consume_zero() {
        let mut pacer = Pacer::new(10, Duration::from_millis(10), 40);
        pacer.consume_with_sleep_fn(0, |_| unreachable!());
    }

    #[test]
    fn test_consume_exact_burst() {
        let mut pacer = Pacer::new(10, Duration::from_millis(10), 40);
        pacer.consume_with_sleep_fn(40, |_| unreachable!());
    }

    #[test]
    fn test_consume_burst_plus_one_sleeps() {
        let mut pacer = Pacer::new(10, Duration::from_millis(10), 40);
        let mut slept = false;
        pacer.consume_with_sleep_fn(41, |d| {
            std::thread::sleep(d);
            slept = true;
        });
        assert!(slept);
    }

    #[test]
    fn test_tokens_refill_after_idle() {
        let mut pacer = Pacer::new(10, Duration::from_millis(10), 40);
        pacer.consume_with_sleep_fn(40, |_| unreachable!());
        std::thread::sleep(Duration::from_millis(30));
        pacer.consume_with_sleep_fn(20, |_| unreachable!());
    }

    #[test]
    fn test_rate_accuracy() {
        let mut pacer = Pacer::new(10, Duration::from_millis(10), 10);
        pacer.consume(10);
        let t0 = Instant::now();
        pacer.consume(50);
        let d = t0.elapsed();
        assert!(d >= Duration::from_millis(40), "Too fast: {d:?}");
        assert!(d < Duration::from_millis(150), "Too slow: {d:?}");
    }

    #[test]
    fn test_many_small_consumes() {
        let mut pacer = Pacer::new(10, Duration::from_millis(10), 10);
        pacer.consume(10);
        let t0 = Instant::now();
        for _ in 0..30 {
            pacer.consume(1);
        }
        let d = t0.elapsed();
        assert!(d >= Duration::from_millis(20), "Too fast: {d:?}");
        assert!(d < Duration::from_millis(150), "Too slow: {d:?}");
    }

    #[test]
    fn test_partial_bucket_refills_before_small_consume() {
        let mut pacer = Pacer::new(10, Duration::from_millis(10), 40);

        pacer.consume_with_sleep_fn(5, |_| unreachable!());
        std::thread::sleep(Duration::from_millis(10));
        pacer.consume_with_sleep_fn(1, |_| unreachable!());

        assert_eq!(pacer.available_tokens, 39);
    }

    #[test]
    fn test_waits_for_fractional_token_interval() {
        let mut pacer = Pacer::new(10, Duration::from_millis(10), 10);
        let mut requested_sleep = None;

        pacer.consume_with_sleep_fn(10, |_| unreachable!());
        pacer.consume_with_sleep_fn(1, |duration| {
            requested_sleep = Some(duration);
            std::thread::sleep(duration);
        });

        let requested_sleep = requested_sleep.expect("consume should need to sleep");
        assert!(
            requested_sleep > Duration::ZERO,
            "sleep duration should be positive"
        );
        assert!(
            requested_sleep < Duration::from_millis(5),
            "expected to wait for a fractional token interval, got {requested_sleep:?}"
        );
    }

    #[test]
    fn test_burst_capacity_promotion() {
        let mut pacer = Pacer::new(100, Duration::from_secs(1), 10);
        let mut slept = false;
        pacer.consume_with_sleep_fn(100, |_| slept = true);
        assert!(
            !slept,
            "Should not sleep if burst capacity was correctly promoted to 100"
        );
    }

    #[test]
    fn test_large_consumes_are_batched() {
        let mut pacer = Pacer::new(10, Duration::from_millis(10), 20);
        let mut sleep_count = 0;

        pacer.consume_with_sleep_fn(20, |_| unreachable!());
        pacer.consume_with_sleep_fn(50, |duration| {
            sleep_count += 1;
            std::thread::sleep(duration);
        });

        assert!(
            sleep_count <= 4,
            "Should sleep in large batches (<= 4 sleeps), but slept {} times",
            sleep_count
        );
    }

    #[test]
    fn test_added_tokens_caps_before_u64_cast() {
        let added_tokens = Pacer::added_tokens(u128::MAX, 1, u64::MAX, 7);
        assert_eq!(added_tokens, 7);
    }

    #[test]
    fn test_duration_from_nanos_saturates() {
        assert_eq!(
            Pacer::duration_from_nanos_saturating(u128::MAX),
            Duration::MAX
        );
    }
}
