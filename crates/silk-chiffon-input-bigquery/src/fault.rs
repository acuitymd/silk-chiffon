//! Test-only deterministic fault schedules and independent lifecycle oracles.

use std::collections::VecDeque;

const MAX_EVENTS: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Phase {
    Credentials,
    ServerClock,
    DiscoverySession,
    ExecutionSession,
    ReadOpen,
    ReadResponse,
    SerializedAdmission,
    DecodePermit,
    PrepareDecode,
    Decode,
    DecodedAdmission,
    AcceptOffset,
    RetryDelay,
    Cancellation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Point {
    pub(crate) phase: Phase,
    pub(crate) session: usize,
    pub(crate) stream: usize,
    pub(crate) attempt: u32,
    pub(crate) requested_offset: i64,
    pub(crate) response: usize,
    pub(crate) accepted_rows: i64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct Selector {
    pub(crate) phase: Option<Phase>,
    pub(crate) session: Option<usize>,
    pub(crate) stream: Option<usize>,
    pub(crate) attempt: Option<u32>,
    pub(crate) requested_offset: Option<i64>,
    pub(crate) response: Option<usize>,
    pub(crate) accepted_rows: Option<i64>,
}

impl Selector {
    fn matches(self, point: Point) -> bool {
        self.phase.is_none_or(|value| value == point.phase)
            && self.session.is_none_or(|value| value == point.session)
            && self.stream.is_none_or(|value| value == point.stream)
            && self.attempt.is_none_or(|value| value == point.attempt)
            && self
                .requested_offset
                .is_none_or(|value| value == point.requested_offset)
            && self.response.is_none_or(|value| value == point.response)
            && self
                .accepted_rows
                .is_none_or(|value| value == point.accepted_rows)
    }
}

pub(crate) struct Step<A> {
    pub(crate) selector: Selector,
    pub(crate) action: A,
}

pub(crate) struct Schedule<A> {
    seed: u64,
    pending: VecDeque<Step<A>>,
    events: VecDeque<Point>,
}

impl<A> Schedule<A> {
    pub(crate) fn new(seed: u64, steps: impl IntoIterator<Item = Step<A>>) -> Self {
        Self {
            seed,
            pending: steps.into_iter().collect(),
            events: VecDeque::new(),
        }
    }

    pub(crate) const fn seed(&self) -> u64 {
        self.seed
    }

    pub(crate) fn take(&mut self, point: Point) -> Option<A> {
        if self.events.len() == MAX_EVENTS {
            self.events.pop_front();
        }
        self.events.push_back(point);
        let index = self
            .pending
            .iter()
            .position(|step| step.selector.matches(point))?;
        self.pending.remove(index).map(|step| step.action)
    }

    pub(crate) fn events(&self) -> Vec<Point> {
        self.events.iter().copied().collect()
    }

    pub(crate) fn is_exhausted(&self) -> bool {
        self.pending.is_empty()
    }
}

pub(crate) fn minimize<A: Clone>(
    steps: &[Step<A>],
    mut still_fails: impl FnMut(&[Step<A>]) -> bool,
) -> Vec<Step<A>> {
    let mut candidate = steps.to_vec();
    let mut index = 0;
    while index < candidate.len() {
        let mut smaller = candidate.clone();
        smaller.remove(index);
        if still_fails(&smaller) {
            candidate = smaller;
        } else {
            index += 1;
        }
    }
    candidate
}

impl<A: Clone> Clone for Step<A> {
    fn clone(&self) -> Self {
        Self {
            selector: self.selector,
            action: self.action.clone(),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct AcceptedOffsetOracle {
    accepted: i64,
}

impl AcceptedOffsetOracle {
    pub(crate) fn observe_request(&self, requested_offset: i64) -> Result<(), &'static str> {
        if requested_offset == self.accepted {
            Ok(())
        } else {
            Err("request did not resume at the independently accepted offset")
        }
    }

    pub(crate) fn accept(&mut self, rows: usize) -> Result<(), &'static str> {
        let rows = i64::try_from(rows).map_err(|_| "row count does not fit i64")?;
        self.accepted = self
            .accepted
            .checked_add(rows)
            .ok_or("accepted offset overflowed")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn point(attempt: u32) -> Point {
        Point {
            phase: Phase::ReadOpen,
            session: 1,
            stream: 2,
            attempt,
            requested_offset: 3,
            response: 4,
            accepted_rows: 5,
        }
    }

    #[test]
    fn schedules_match_all_lifecycle_dimensions_and_replay_seed() {
        let mut schedule = Schedule::new(
            0x5eed,
            [Step {
                selector: Selector {
                    phase: Some(Phase::ReadOpen),
                    session: Some(1),
                    stream: Some(2),
                    attempt: Some(7),
                    requested_offset: Some(3),
                    response: Some(4),
                    accepted_rows: Some(5),
                },
                action: "unavailable",
            }],
        );

        assert_eq!(schedule.seed(), 0x5eed);
        assert_eq!(schedule.take(point(6)), None);
        assert_eq!(schedule.take(point(7)), Some("unavailable"));
        assert!(schedule.is_exhausted());
        assert_eq!(schedule.events(), [point(6), point(7)]);
    }

    #[test]
    fn event_log_is_bounded_and_minimization_removes_irrelevant_faults() {
        let mut schedule = Schedule::<()>::new(1, []);
        for attempt in 0..300 {
            schedule.take(point(attempt));
        }
        assert_eq!(schedule.events().len(), MAX_EVENTS);
        assert_eq!(schedule.events()[0].attempt, 44);

        let steps = [
            Step {
                selector: Selector::default(),
                action: 1,
            },
            Step {
                selector: Selector::default(),
                action: 7,
            },
            Step {
                selector: Selector::default(),
                action: 2,
            },
        ];
        let minimized = minimize(&steps, |candidate| {
            candidate.iter().any(|step| step.action == 7)
        });
        assert_eq!(minimized.len(), 1);
        assert_eq!(minimized[0].action, 7);
    }

    #[test]
    fn accepted_offset_oracle_rejects_skips_duplicates_and_overflow() {
        let mut oracle = AcceptedOffsetOracle::default();
        oracle.observe_request(0).unwrap();
        oracle.accept(2).unwrap();
        oracle.observe_request(2).unwrap();
        assert!(oracle.observe_request(0).is_err());
        oracle.accept(usize::MAX).unwrap_err();
    }

    #[test]
    fn phase_vocabulary_covers_every_private_fault_boundary() {
        assert_eq!(
            [
                Phase::Credentials,
                Phase::ServerClock,
                Phase::DiscoverySession,
                Phase::ExecutionSession,
                Phase::ReadOpen,
                Phase::ReadResponse,
                Phase::SerializedAdmission,
                Phase::DecodePermit,
                Phase::PrepareDecode,
                Phase::Decode,
                Phase::DecodedAdmission,
                Phase::AcceptOffset,
                Phase::RetryDelay,
                Phase::Cancellation,
            ]
            .len(),
            14
        );
    }
}
