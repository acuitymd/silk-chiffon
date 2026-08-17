use std::{fmt, time::Duration};

const SUSTAINED_ACCEPTED_PROGRESS: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BackoffPolicy {
    initial_delay: Duration,
    multiplier_numerator: u32,
    multiplier_denominator: u32,
    maximum_delay: Duration,
    sustained_accepted_progress: Duration,
}

impl BackoffPolicy {
    pub(crate) const fn create_read_session() -> Self {
        Self {
            initial_delay: Duration::from_millis(100),
            multiplier_numerator: 13,
            multiplier_denominator: 10,
            maximum_delay: Duration::from_secs(60),
            sustained_accepted_progress: SUSTAINED_ACCEPTED_PROGRESS,
        }
    }

    pub(crate) fn read_rows(
        initial_delay: Duration,
        maximum_delay: Duration,
    ) -> Result<Self, BackoffError> {
        Self::new(initial_delay, 13, 10, maximum_delay)
    }

    fn new(
        initial_delay: Duration,
        multiplier_numerator: u32,
        multiplier_denominator: u32,
        maximum_delay: Duration,
    ) -> Result<Self, BackoffError> {
        if initial_delay.is_zero()
            || multiplier_denominator == 0
            || multiplier_numerator < multiplier_denominator
            || maximum_delay < initial_delay
        {
            return Err(BackoffError);
        }
        Ok(Self {
            initial_delay,
            multiplier_numerator,
            multiplier_denominator,
            maximum_delay,
            sustained_accepted_progress: SUSTAINED_ACCEPTED_PROGRESS,
        })
    }

    #[cfg(test)]
    pub(crate) const fn initial_delay(self) -> Duration {
        self.initial_delay
    }

    #[cfg(test)]
    pub(crate) const fn multiplier(self) -> (u32, u32) {
        (self.multiplier_numerator, self.multiplier_denominator)
    }

    #[cfg(test)]
    pub(crate) const fn maximum_delay(self) -> Duration {
        self.maximum_delay
    }

    #[cfg(test)]
    pub(crate) const fn sustained_accepted_progress(self) -> Duration {
        self.sustained_accepted_progress
    }

    pub(crate) fn delay_cap(self, failure_streak: u32) -> Result<Duration, BackoffError> {
        if failure_streak == 0 {
            return Err(BackoffError);
        }
        let maximum = self.maximum_delay.as_nanos();
        let mut nanos = self.initial_delay.as_nanos();
        let mut remaining = failure_streak - 1;
        while remaining > 0 && nanos < maximum {
            nanos = nanos
                .checked_mul(u128::from(self.multiplier_numerator))
                .ok_or(BackoffError)?
                / u128::from(self.multiplier_denominator);
            nanos = nanos.min(maximum);
            remaining -= 1;
        }
        duration_from_nanos(nanos)
    }

    pub(crate) fn full_jitter(
        self,
        failure_streak: u32,
        sample: u64,
    ) -> Result<Duration, BackoffError> {
        let cap = self.delay_cap(failure_streak)?.as_nanos();
        let nanos = cap.checked_mul(u128::from(sample)).ok_or(BackoffError)? / u128::from(u64::MAX);
        duration_from_nanos(nanos)
    }

    pub(crate) fn failure_streak_after_progress(
        self,
        failure_streak: u32,
        accepted_rows: u64,
        accepted_progress: Duration,
    ) -> u32 {
        if accepted_rows > 0 && accepted_progress >= self.sustained_accepted_progress {
            0
        } else {
            failure_streak
        }
    }
}

fn duration_from_nanos(nanos: u128) -> Result<Duration, BackoffError> {
    let seconds = nanos / 1_000_000_000;
    let subsecond = nanos % 1_000_000_000;
    Ok(Duration::new(
        u64::try_from(seconds).map_err(|_| BackoffError)?,
        u32::try_from(subsecond).map_err(|_| BackoffError)?,
    ))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BackoffError;

impl fmt::Display for BackoffError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid or overflowing backoff policy")
    }
}

impl std::error::Error for BackoffError {}
