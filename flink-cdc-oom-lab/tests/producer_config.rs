use flink_cdc_oom_lab::{ProducerSettings, QueuePolicy, SendOutcome, classify_enqueue_error};
use rdkafka::{error::KafkaError, types::RDKafkaErrorCode};

#[test]
fn production_equivalent_config_keeps_incident_settings() {
    let settings = ProducerSettings::new(
        "kafka:9092",
        "flink-cdc-oom-lab",
        QueuePolicy::LibrdkafkaDefault,
    );
    let config = settings.client_config();

    assert_eq!(config.get("bootstrap.servers"), Some("kafka:9092"));
    assert_eq!(config.get("client.id"), Some("flink-cdc-oom-lab"));
    assert_eq!(config.get("message.timeout.ms"), Some("5000"));
    assert_eq!(config.get("batch.size"), Some("10485760"));
    assert_eq!(config.get("compression.type"), Some("lz4"));
    assert_eq!(config.get("statistics.interval.ms"), Some("1000"));
    assert_eq!(config.get("queue.buffering.max.kbytes"), None);
    assert_eq!(config.get("queue.buffering.max.messages"), None);
}

#[test]
fn capped_config_applies_both_queue_limits() {
    let settings = ProducerSettings::new(
        "kafka:9092",
        "flink-cdc-oom-lab-capped",
        QueuePolicy::Capped {
            max_kbytes: 65_536,
            max_messages: 10_000,
        },
    );
    let config = settings.client_config();

    assert_eq!(config.get("queue.buffering.max.kbytes"), Some("65536"));
    assert_eq!(config.get("queue.buffering.max.messages"), Some("10000"));
}

#[test]
fn enqueue_errors_distinguish_queue_full() {
    assert_eq!(
        classify_enqueue_error(&KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull)),
        SendOutcome::QueueFull
    );
    assert_eq!(
        classify_enqueue_error(&KafkaError::MessageProduction(
            RDKafkaErrorCode::MessageTimedOut
        )),
        SendOutcome::DeliveryFailed
    );
}
