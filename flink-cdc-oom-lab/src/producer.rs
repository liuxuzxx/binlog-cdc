use rdkafka::{
    ClientConfig,
    client::ClientContext,
    error::{KafkaError, KafkaResult},
    producer::{FutureProducer, FutureRecord},
    statistics::Statistics,
    types::RDKafkaErrorCode,
};

use crate::{LabMetrics, SendOutcome};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueuePolicy {
    LibrdkafkaDefault,
    Capped { max_kbytes: u64, max_messages: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerSettings {
    brokers: String,
    client_id: String,
    queue_policy: QueuePolicy,
}

impl ProducerSettings {
    pub fn new(
        brokers: impl Into<String>,
        client_id: impl Into<String>,
        queue_policy: QueuePolicy,
    ) -> Self {
        Self {
            brokers: brokers.into(),
            client_id: client_id.into(),
            queue_policy,
        }
    }

    pub fn client_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", &self.brokers)
            .set("client.id", &self.client_id)
            .set("message.timeout.ms", "5000")
            .set("batch.size", "10485760")
            .set("compression.type", "lz4")
            .set("linger.ms", "5")
            .set("statistics.interval.ms", "1000");

        if let QueuePolicy::Capped {
            max_kbytes,
            max_messages,
        } = self.queue_policy
        {
            config
                .set("queue.buffering.max.kbytes", max_kbytes.to_string())
                .set("queue.buffering.max.messages", max_messages.to_string());
        }

        config
    }
}

#[derive(Clone)]
struct MetricsContext {
    metrics: LabMetrics,
}

impl ClientContext for MetricsContext {
    fn stats(&self, statistics: Statistics) {
        self.metrics.update_kafka_statistics(&statistics);
    }
}

pub struct KafkaSink {
    producer: FutureProducer<MetricsContext>,
    topic: String,
    metrics: LabMetrics,
}

impl KafkaSink {
    pub fn build(
        settings: &ProducerSettings,
        topic: impl Into<String>,
        metrics: LabMetrics,
    ) -> KafkaResult<Self> {
        let context = MetricsContext {
            metrics: metrics.clone(),
        };
        let producer = settings.client_config().create_with_context(context)?;

        Ok(Self {
            producer,
            topic: topic.into(),
            metrics,
        })
    }

    pub fn send(&self, key: &str, body: &str) -> SendOutcome {
        let record = FutureRecord::to(&self.topic).key(key).payload(body);
        let outcome = match self.producer.send_result(record) {
            Ok(delivery_future) => {
                drop(delivery_future);
                SendOutcome::Enqueued
            }
            Err((error, _record)) => classify_enqueue_error(&error),
        };
        self.metrics.record(outcome, body.len() as u64);
        outcome
    }
}

pub fn classify_enqueue_error(error: &KafkaError) -> SendOutcome {
    match error {
        KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull) => SendOutcome::QueueFull,
        _ => SendOutcome::DeliveryFailed,
    }
}
