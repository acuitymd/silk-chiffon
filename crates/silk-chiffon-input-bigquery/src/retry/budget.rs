use std::{cmp, fmt, time::Duration};

use super::RetryScope;

const CREATE_READ_SESSION_RETRY_WINDOW: Duration = Duration::from_secs(10 * 60);
pub(crate) const SESSION_EXPIRY_SAFETY_MARGIN: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetryDelay {
    Backoff(Duration),
    ServerMinimum(Duration),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RetryPlan {
    scope: RetryScope,
    attempt: u32,
    delay: Duration,
    attempt_timeout: Duration,
    attempt_deadline: Duration,
}

impl RetryPlan {
    #[cfg(test)]
    pub(crate) const fn scope(self) -> RetryScope {
        self.scope
    }

    pub(crate) const fn attempt(self) -> u32 {
        self.attempt
    }

    pub(crate) const fn delay(self) -> Duration {
        self.delay
    }

    #[cfg(test)]
    pub(crate) const fn attempt_timeout(self) -> Duration {
        self.attempt_timeout
    }

    #[cfg(test)]
    pub(crate) const fn attempt_deadline(self) -> Duration {
        self.attempt_deadline
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RetryBudget {
    scope: RetryScope,
    started: Duration,
    retry_deadline: Duration,
    max_attempts: u32,
    attempt_timeout: Duration,
    session_deadline: Option<Duration>,
}

impl RetryBudget {
    pub(crate) fn create_read_session(
        started: Duration,
        attempt_timeout: Duration,
    ) -> Result<Self, RetryBudgetError> {
        Self::new(
            RetryScope::CreateReadSession,
            started,
            CREATE_READ_SESSION_RETRY_WINDOW,
            u32::MAX,
            attempt_timeout,
            None,
            Duration::ZERO,
        )
    }

    pub(crate) fn read_rows(
        started: Duration,
        retry_window: Duration,
        max_attempts: u32,
        attempt_timeout: Duration,
        session_expiration: Option<Duration>,
    ) -> Result<Self, RetryBudgetError> {
        Self::new(
            RetryScope::ReadRows,
            started,
            retry_window,
            max_attempts,
            attempt_timeout,
            session_expiration,
            SESSION_EXPIRY_SAFETY_MARGIN,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        scope: RetryScope,
        started: Duration,
        retry_window: Duration,
        max_attempts: u32,
        attempt_timeout: Duration,
        session_expiration: Option<Duration>,
        session_safety_margin: Duration,
    ) -> Result<Self, RetryBudgetError> {
        if retry_window.is_zero() || max_attempts == 0 || attempt_timeout.is_zero() {
            return Err(RetryBudgetError::new(RetryBudgetErrorKind::InvalidDeadline));
        }
        let retry_deadline = started
            .checked_add(retry_window)
            .ok_or_else(|| RetryBudgetError::new(RetryBudgetErrorKind::ArithmeticOverflow))?;
        let session_deadline = session_expiration
            .map(|deadline| {
                deadline
                    .checked_sub(session_safety_margin)
                    .ok_or_else(|| RetryBudgetError::new(RetryBudgetErrorKind::InvalidDeadline))
            })
            .transpose()?;
        let budget = Self {
            scope,
            started,
            retry_deadline,
            max_attempts,
            attempt_timeout,
            session_deadline,
        };
        if budget.effective_deadline() < started {
            return Err(RetryBudgetError::new(RetryBudgetErrorKind::InvalidDeadline));
        }
        Ok(budget)
    }

    #[cfg(test)]
    pub(crate) const fn scope(self) -> RetryScope {
        self.scope
    }

    #[cfg(test)]
    pub(crate) const fn attempt_timeout(self) -> Duration {
        self.attempt_timeout
    }

    pub(crate) fn effective_deadline(self) -> Duration {
        self.session_deadline
            .map_or(self.retry_deadline, |session| {
                cmp::min(self.retry_deadline, session)
            })
    }

    pub(crate) fn is_session_limited(self) -> bool {
        self.session_deadline
            .is_some_and(|session| session <= self.retry_deadline)
    }

    pub(crate) fn plan_retry(
        self,
        previous_observation: Duration,
        now: Duration,
        completed_attempts: u32,
        proposed_delay: RetryDelay,
    ) -> Result<RetryPlan, RetryBudgetError> {
        if previous_observation < self.started || now < previous_observation {
            return Err(RetryBudgetError::new(
                RetryBudgetErrorKind::NonMonotonicClock,
            ));
        }
        let attempt = completed_attempts
            .checked_add(1)
            .ok_or_else(|| RetryBudgetError::new(RetryBudgetErrorKind::AttemptCounterOverflow))?;
        if attempt > self.max_attempts {
            return Err(RetryBudgetError::new(
                RetryBudgetErrorKind::AttemptsExhausted,
            ));
        }
        let deadline = self.effective_deadline();
        let latest_start = deadline
            .checked_sub(self.attempt_timeout)
            .ok_or_else(|| RetryBudgetError::new(RetryBudgetErrorKind::DeadlineExhausted))?;
        let available = latest_start
            .checked_sub(now)
            .ok_or_else(|| RetryBudgetError::new(RetryBudgetErrorKind::DeadlineExhausted))?;
        let delay = match proposed_delay {
            RetryDelay::Backoff(delay) => cmp::min(delay, available),
            RetryDelay::ServerMinimum(delay) if delay <= available => delay,
            RetryDelay::ServerMinimum(_) => {
                return Err(RetryBudgetError::new(
                    RetryBudgetErrorKind::ServerDelayExceedsDeadline,
                ));
            }
        };
        let attempt_start = now
            .checked_add(delay)
            .ok_or_else(|| RetryBudgetError::new(RetryBudgetErrorKind::ArithmeticOverflow))?;
        let attempt_deadline = attempt_start
            .checked_add(self.attempt_timeout)
            .ok_or_else(|| RetryBudgetError::new(RetryBudgetErrorKind::ArithmeticOverflow))?;
        if attempt_deadline > deadline {
            return Err(RetryBudgetError::new(
                RetryBudgetErrorKind::DeadlineExhausted,
            ));
        }
        Ok(RetryPlan {
            scope: self.scope,
            attempt,
            delay,
            attempt_timeout: self.attempt_timeout,
            attempt_deadline,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetryBudgetErrorKind {
    InvalidDeadline,
    ArithmeticOverflow,
    NonMonotonicClock,
    AttemptCounterOverflow,
    AttemptsExhausted,
    DeadlineExhausted,
    ServerDelayExceedsDeadline,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct RetryBudgetError {
    kind: RetryBudgetErrorKind,
}

impl RetryBudgetError {
    const fn new(kind: RetryBudgetErrorKind) -> Self {
        Self { kind }
    }

    #[cfg(test)]
    pub(crate) const fn kind(self) -> RetryBudgetErrorKind {
        self.kind
    }
}

impl fmt::Display for RetryBudgetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            RetryBudgetErrorKind::InvalidDeadline => "invalid retry deadline",
            RetryBudgetErrorKind::ArithmeticOverflow => "retry deadline arithmetic overflowed",
            RetryBudgetErrorKind::NonMonotonicClock => "retry clock moved backwards",
            RetryBudgetErrorKind::AttemptCounterOverflow => "retry attempt counter overflowed",
            RetryBudgetErrorKind::AttemptsExhausted => "retry attempts are exhausted",
            RetryBudgetErrorKind::DeadlineExhausted => "retry deadline is exhausted",
            RetryBudgetErrorKind::ServerDelayExceedsDeadline => {
                "server retry delay exceeds the remaining retry deadline"
            }
        })
    }
}

impl fmt::Debug for RetryBudgetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetryBudgetError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl std::error::Error for RetryBudgetError {}
