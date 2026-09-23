use std::time::Duration;

use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Phase {
    name: String,
    rate_per_second: u64,
    duration: Duration,
}

impl Phase {
    pub fn new(
        name: impl Into<String>,
        rate_per_second: u64,
        duration: Duration,
    ) -> Result<Self, ScenarioError> {
        if rate_per_second == 0 {
            return Err(ScenarioError::ZeroRate);
        }
        if duration.is_zero() {
            return Err(ScenarioError::ZeroDuration);
        }

        Ok(Self {
            name: name.into(),
            rate_per_second,
            duration,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn rate_per_second(&self) -> u64 {
        self.rate_per_second
    }

    pub fn duration(&self) -> Duration {
        self.duration
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scenario {
    phases: Vec<Phase>,
}

impl Scenario {
    pub fn new(phases: Vec<Phase>) -> Result<Self, ScenarioError> {
        if phases.is_empty() {
            return Err(ScenarioError::EmptyScenario);
        }
        Ok(Self { phases })
    }

    pub fn phase_at(&self, elapsed: Duration) -> Option<&Phase> {
        let mut start = Duration::ZERO;
        for phase in &self.phases {
            let end = start + phase.duration;
            if elapsed >= start && elapsed < end {
                return Some(phase);
            }
            start = end;
        }
        None
    }

    pub fn total_duration(&self) -> Duration {
        self.phases
            .iter()
            .map(Phase::duration)
            .fold(Duration::ZERO, |total, duration| total + duration)
    }
}

#[derive(Debug, Default)]
pub struct RateBudget {
    message_nanoseconds: u128,
}

impl RateBudget {
    pub fn advance(&mut self, rate_per_second: u64, elapsed: Duration) -> u64 {
        self.message_nanoseconds = self
            .message_nanoseconds
            .saturating_add(u128::from(rate_per_second).saturating_mul(elapsed.as_nanos()));
        let messages = self.message_nanoseconds / 1_000_000_000;
        self.message_nanoseconds %= 1_000_000_000;
        messages.min(u128::from(u64::MAX)) as u64
    }
}

#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum ScenarioError {
    #[error("scenario must contain at least one phase")]
    EmptyScenario,
    #[error("phase rate must be greater than zero")]
    ZeroRate,
    #[error("phase duration must be greater than zero")]
    ZeroDuration,
}
