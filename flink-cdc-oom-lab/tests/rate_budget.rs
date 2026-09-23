use std::time::Duration;

use flink_cdc_oom_lab::RateBudget;

#[test]
fn rate_budget_accumulates_fractional_messages_without_drift() {
    let mut budget = RateBudget::default();

    assert_eq!(budget.advance(3_000, Duration::from_micros(500)), 1);
    assert_eq!(budget.advance(3_000, Duration::from_micros(500)), 2);
    assert_eq!(budget.advance(3_000, Duration::from_millis(1)), 3);
}

#[test]
fn changing_rate_uses_the_latest_phase_rate() {
    let mut budget = RateBudget::default();

    assert_eq!(budget.advance(100, Duration::from_millis(10)), 1);
    assert_eq!(budget.advance(10_000, Duration::from_millis(10)), 100);
}
