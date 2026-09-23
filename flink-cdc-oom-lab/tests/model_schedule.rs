use std::time::Duration;

use flink_cdc_oom_lab::{
    EventGenerator, Operation, PayloadProfile, Phase, Scenario, ScenarioError,
};
use pretty_assertions::assert_eq;

#[test]
fn fixed_seed_generates_the_same_event_sequence() {
    let mut first = EventGenerator::new(42, PayloadProfile::Fixed(1024));
    let mut second = EventGenerator::new(42, PayloadProfile::Fixed(1024));

    let first_events = (0..20).map(|_| first.next_event()).collect::<Vec<_>>();
    let second_events = (0..20).map(|_| second.next_event()).collect::<Vec<_>>();

    assert_eq!(first_events, second_events);
    assert!(first_events.iter().all(|event| event.content.len() == 1024));
    assert!(first_events.iter().all(|event| matches!(
        event.operation,
        Operation::Insert | Operation::Update | Operation::Delete
    )));
}

#[test]
fn mixed_payload_uses_only_the_declared_sizes() {
    let mut generator = EventGenerator::new(
        7,
        PayloadProfile::Mixed {
            small: 1024,
            medium: 10 * 1024,
            large: 50 * 1024,
        },
    );

    let sizes = (0..1000)
        .map(|_| generator.next_event().content.len())
        .collect::<Vec<_>>();

    assert!(
        sizes
            .iter()
            .all(|size| matches!(*size, 1024 | 10240 | 51200))
    );
    assert!(sizes.contains(&1024));
    assert!(sizes.contains(&10240));
    assert!(sizes.contains(&51200));
}

#[test]
fn event_serialization_has_debezium_shape() {
    let mut generator = EventGenerator::new(11, PayloadProfile::Fixed(32));
    let event = generator.next_event();
    let value = serde_json::to_value(event).expect("event must serialize");

    assert!(value.get("before").is_some());
    assert!(value.get("after").is_some());
    assert!(value.get("source").is_some());
    assert!(value.get("op").is_some());
    assert!(value.get("ts_ms").is_some());
}

#[test]
fn scenario_switches_phase_at_exact_boundaries() {
    let scenario = Scenario::new(vec![
        Phase::new("warmup", 100, Duration::from_secs(300)).unwrap(),
        Phase::new("peak", 3000, Duration::from_secs(600)).unwrap(),
        Phase::new("burst", 10000, Duration::from_secs(30)).unwrap(),
    ])
    .unwrap();

    assert_eq!(scenario.phase_at(Duration::ZERO).unwrap().name(), "warmup");
    assert_eq!(
        scenario.phase_at(Duration::from_secs(299)).unwrap().name(),
        "warmup"
    );
    assert_eq!(
        scenario.phase_at(Duration::from_secs(300)).unwrap().name(),
        "peak"
    );
    assert_eq!(
        scenario.phase_at(Duration::from_secs(900)).unwrap().name(),
        "burst"
    );
    assert!(scenario.phase_at(Duration::from_secs(930)).is_none());
}

#[test]
fn scenario_rejects_invalid_rate_and_duration() {
    assert_eq!(
        Phase::new("invalid-rate", 0, Duration::from_secs(1)),
        Err(ScenarioError::ZeroRate)
    );
    assert_eq!(
        Phase::new("invalid-duration", 1, Duration::ZERO),
        Err(ScenarioError::ZeroDuration)
    );
    assert_eq!(Scenario::new(Vec::new()), Err(ScenarioError::EmptyScenario));
}
