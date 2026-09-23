use std::sync::{
    Arc,
    atomic::{AtomicI64, AtomicU64, Ordering},
};

use rdkafka::statistics::Statistics;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendOutcome {
    Attempted,
    Serialized,
    Enqueued,
    QueueFull,
    DeliveryTimeout,
    DeliveryFailed,
}

#[derive(Default)]
struct MetricValues {
    attempted: AtomicU64,
    attempted_bytes: AtomicU64,
    serialized: AtomicU64,
    serialized_bytes: AtomicU64,
    enqueued: AtomicU64,
    enqueued_bytes: AtomicU64,
    queue_full: AtomicU64,
    delivery_timeout: AtomicU64,
    delivery_failed: AtomicU64,
    kafka_message_count: AtomicU64,
    kafka_message_bytes: AtomicU64,
    kafka_max_message_count: AtomicU64,
    kafka_max_message_bytes: AtomicU64,
    kafka_transmitted_messages: AtomicI64,
    kafka_transmitted_bytes: AtomicI64,
}

#[derive(Clone, Default)]
pub struct LabMetrics {
    values: Arc<MetricValues>,
}

impl LabMetrics {
    pub fn record(&self, outcome: SendOutcome, bytes: u64) {
        match outcome {
            SendOutcome::Attempted => {
                self.values.attempted.fetch_add(1, Ordering::Relaxed);
                self.values
                    .attempted_bytes
                    .fetch_add(bytes, Ordering::Relaxed);
            }
            SendOutcome::Serialized => {
                self.values.serialized.fetch_add(1, Ordering::Relaxed);
                self.values
                    .serialized_bytes
                    .fetch_add(bytes, Ordering::Relaxed);
            }
            SendOutcome::Enqueued => {
                self.values.enqueued.fetch_add(1, Ordering::Relaxed);
                self.values
                    .enqueued_bytes
                    .fetch_add(bytes, Ordering::Relaxed);
            }
            SendOutcome::QueueFull => {
                self.values.queue_full.fetch_add(1, Ordering::Relaxed);
            }
            SendOutcome::DeliveryTimeout => {
                self.values.delivery_timeout.fetch_add(1, Ordering::Relaxed);
            }
            SendOutcome::DeliveryFailed => {
                self.values.delivery_failed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn update_kafka_statistics(&self, statistics: &Statistics) {
        let snapshot = KafkaQueueSnapshot::from(statistics);
        self.values
            .kafka_message_count
            .store(snapshot.message_count, Ordering::Relaxed);
        self.values
            .kafka_message_bytes
            .store(snapshot.message_bytes, Ordering::Relaxed);
        self.values
            .kafka_max_message_count
            .store(snapshot.max_message_count, Ordering::Relaxed);
        self.values
            .kafka_max_message_bytes
            .store(snapshot.max_message_bytes, Ordering::Relaxed);
        self.values
            .kafka_transmitted_messages
            .store(snapshot.transmitted_messages, Ordering::Relaxed);
        self.values
            .kafka_transmitted_bytes
            .store(snapshot.transmitted_bytes, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            attempted: self.values.attempted.load(Ordering::Relaxed),
            attempted_bytes: self.values.attempted_bytes.load(Ordering::Relaxed),
            serialized: self.values.serialized.load(Ordering::Relaxed),
            serialized_bytes: self.values.serialized_bytes.load(Ordering::Relaxed),
            enqueued: self.values.enqueued.load(Ordering::Relaxed),
            enqueued_bytes: self.values.enqueued_bytes.load(Ordering::Relaxed),
            queue_full: self.values.queue_full.load(Ordering::Relaxed),
            delivery_timeout: self.values.delivery_timeout.load(Ordering::Relaxed),
            delivery_failed: self.values.delivery_failed.load(Ordering::Relaxed),
            kafka: KafkaQueueSnapshot {
                message_count: self.values.kafka_message_count.load(Ordering::Relaxed),
                message_bytes: self.values.kafka_message_bytes.load(Ordering::Relaxed),
                max_message_count: self.values.kafka_max_message_count.load(Ordering::Relaxed),
                max_message_bytes: self.values.kafka_max_message_bytes.load(Ordering::Relaxed),
                transmitted_messages: self
                    .values
                    .kafka_transmitted_messages
                    .load(Ordering::Relaxed),
                transmitted_bytes: self.values.kafka_transmitted_bytes.load(Ordering::Relaxed),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub attempted: u64,
    pub attempted_bytes: u64,
    pub serialized: u64,
    pub serialized_bytes: u64,
    pub enqueued: u64,
    pub enqueued_bytes: u64,
    pub queue_full: u64,
    pub delivery_timeout: u64,
    pub delivery_failed: u64,
    pub kafka: KafkaQueueSnapshot,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct KafkaQueueSnapshot {
    pub message_count: u64,
    pub message_bytes: u64,
    pub max_message_count: u64,
    pub max_message_bytes: u64,
    pub transmitted_messages: i64,
    pub transmitted_bytes: i64,
}

impl From<&Statistics> for KafkaQueueSnapshot {
    fn from(statistics: &Statistics) -> Self {
        Self {
            message_count: statistics.msg_cnt,
            message_bytes: statistics.msg_size,
            max_message_count: statistics.msg_max,
            max_message_bytes: statistics.msg_size_max,
            transmitted_messages: statistics.txmsgs,
            transmitted_bytes: statistics.txmsg_bytes,
        }
    }
}
