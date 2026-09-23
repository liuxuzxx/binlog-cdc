use flink_cdc_oom_lab::{KafkaQueueSnapshot, LabMetrics, SendOutcome};
use rdkafka::statistics::Statistics;

#[test]
fn send_outcomes_are_counted_independently() {
    let metrics = LabMetrics::default();

    metrics.record(SendOutcome::Attempted, 100);
    metrics.record(SendOutcome::Serialized, 100);
    metrics.record(SendOutcome::Enqueued, 100);
    metrics.record(SendOutcome::QueueFull, 0);
    metrics.record(SendOutcome::DeliveryTimeout, 0);
    metrics.record(SendOutcome::DeliveryFailed, 0);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.attempted, 1);
    assert_eq!(snapshot.serialized, 1);
    assert_eq!(snapshot.enqueued, 1);
    assert_eq!(snapshot.queue_full, 1);
    assert_eq!(snapshot.delivery_timeout, 1);
    assert_eq!(snapshot.delivery_failed, 1);
    assert_eq!(snapshot.attempted_bytes, 100);
    assert_eq!(snapshot.serialized_bytes, 100);
    assert_eq!(snapshot.enqueued_bytes, 100);
}

#[test]
fn rdkafka_statistics_map_to_queue_snapshot() {
    let statistics = Statistics {
        msg_cnt: 73,
        msg_size: 8_388_608,
        msg_max: 100_000,
        msg_size_max: 1_073_741_824,
        txmsgs: 91,
        txmsg_bytes: 12_345_678,
        ..Statistics::default()
    };

    let snapshot = KafkaQueueSnapshot::from(&statistics);

    assert_eq!(snapshot.message_count, 73);
    assert_eq!(snapshot.message_bytes, 8_388_608);
    assert_eq!(snapshot.max_message_count, 100_000);
    assert_eq!(snapshot.max_message_bytes, 1_073_741_824);
    assert_eq!(snapshot.transmitted_messages, 91);
    assert_eq!(snapshot.transmitted_bytes, 12_345_678);
}
