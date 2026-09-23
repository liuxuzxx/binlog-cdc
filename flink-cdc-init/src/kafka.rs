use std::{
    collections::{BTreeMap, HashMap},
    hash::{DefaultHasher, Hash, Hasher},
    sync::Arc,
    time::Duration,
};

use chrono::Utc;
use rdkafka::{
    ClientConfig,
    error::KafkaError,
    producer::{FutureProducer, FutureRecord, Producer},
    types::RDKafkaErrorCode,
};
use rskafka::{
    client::{
        ClientBuilder,
        partition::Compression,
        partition::UnknownTopicHandling,
        producer::{BatchProducer, BatchProducerBuilder, aggregator::RecordAggregator},
    },
    record::Record,
};
use crate::debezium::DebeziumFormat;
use tracing::{info, warn};

use crate::{
    config::FlinkCdcInit,
    error::{InitError, Result},
};

pub struct KafkaSink {
    producer: KafkaProducer,
}

enum KafkaProducer {
    RdKafka(RdKafkaSink),
    RsKafka(RsKafkaSink),
}

struct RdKafkaSink {
    producer: FutureProducer,
    topic: String,
}

type PartitionProducers = HashMap<i32, BatchProducer<RecordAggregator>>;

struct RsKafkaSink {
    partition_producers: PartitionProducers,
}

impl KafkaSink {
    pub async fn build(config: &FlinkCdcInit) -> Result<Self> {
        if std::env::var("RSKAFKA").is_ok() {
            info!("use rskafka producer");
            Ok(Self {
                producer: KafkaProducer::RsKafka(RsKafkaSink::build(config).await?),
            })
        } else {
            info!("use rdkafka producer");
            Ok(Self {
                producer: KafkaProducer::RdKafka(RdKafkaSink::build(config)?),
            })
        }
    }

    pub async fn send_batch_messages(&self, messages: Vec<DebeziumFormat>) -> Result<()> {
        match &self.producer {
            KafkaProducer::RdKafka(sink) => sink.send_batch_messages(messages).await,
            KafkaProducer::RsKafka(sink) => sink.send_batch_messages(messages).await,
        }
    }

    pub async fn flush(&self) -> Result<()> {
        match &self.producer {
            KafkaProducer::RdKafka(sink) => {
                sink.flush();
                Ok(())
            }
            KafkaProducer::RsKafka(sink) => sink.flush().await,
        }
    }
}

impl RdKafkaSink {
    fn build(config: &FlinkCdcInit) -> Result<Self> {
        let mut client = ClientConfig::new();
        client
            .set("bootstrap.servers", config.sink_bootstrap_server())
            .set("message.timeout.ms", "10000")
            .set("batch.size", config.sink_batch_size().to_string())
            .set("compression.type", config.sink_compression_type())
            .set("linger.ms", config.sink_linger_ms().to_string());

        // `max.request.size` is a Java producer config and is not accepted by librdkafka.
        // Map both user-facing fields to librdkafka's `message.max.bytes`.
        let max_message_bytes = match (
            config.sink_max_request_size(),
            config.sink_max_message_bytes(),
        ) {
            (Some(request_size), Some(message_bytes)) => Some(request_size.max(message_bytes)),
            (Some(request_size), None) => Some(request_size),
            (None, Some(message_bytes)) => Some(message_bytes),
            (None, None) => None,
        };

        if let Some(size) = max_message_bytes {
            client.set("message.max.bytes", size.to_string());
        }

        let producer = client.create::<FutureProducer>().map_err(|error| {
            InitError::Kafka(format!(
                "create kafka producer failed, bootstrap.servers={} topic={} error={error:?}",
                config.sink_bootstrap_server(),
                config.sink_topic()
            ))
        })?;

        Ok(Self {
            producer,
            topic: config.sink_topic().to_string(),
        })
    }

    async fn send_batch_messages(&self, messages: Vec<DebeziumFormat>) -> Result<()> {
        for message in messages {
            self.send_message(message).await?;
        }
        Ok(())
    }

    fn flush(&self) {
        let _ = self.producer.flush(Duration::from_secs(30));
        info!("flush kafka producer finished");
    }

    async fn send_message(&self, message: DebeziumFormat) -> Result<()> {
        let body = message.to_json();
        let key = message.keys();
        let record = FutureRecord::to(self.topic.as_str())
            .key(key.as_str())
            .payload(body.as_str());

        match self.producer.send_result(record) {
            Ok(delivery) => {
                drop(delivery);
                Ok(())
            }
            Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), record)) => {
                self.producer
                    .send(record, Duration::from_secs(10))
                    .await
                    .map_err(|(error, _)| {
                        InitError::Kafka(format!("retry send message to kafka error: {error:?}"))
                    })?;
                Ok(())
            }
            Err((error, _)) => Err(InitError::Kafka(format!(
                "send message to kafka error: {error:?}"
            ))),
        }
    }
}

impl RsKafkaSink {
    async fn build(config: &FlinkCdcInit) -> Result<Self> {
        let client = ClientBuilder::new(config.sink_bootstrap_servers())
            .build()
            .await
            .map_err(|error| {
                InitError::Kafka(format!(
                    "create rskafka client failed, bootstrap.servers={} topic={} error={error:?}",
                    config.sink_bootstrap_server(),
                    config.sink_topic()
                ))
            })?;

        let topics = client.list_topics().await.map_err(|error| {
            InitError::Kafka(format!(
                "load topic metadata failed, topic={} error={error:?}",
                config.sink_topic()
            ))
        })?;

        let metadata = topics
            .iter()
            .find(|topic| topic.name.eq_ignore_ascii_case(config.sink_topic()))
            .ok_or_else(|| InitError::Kafka(format!("topic {} not exists", config.sink_topic())))?;

        let mut partition_producers = PartitionProducers::new();
        for partition in &metadata.partitions {
            let partition_client = client
                .partition_client(config.sink_topic(), *partition, UnknownTopicHandling::Retry)
                .await
                .map_err(|error| {
                    InitError::Kafka(format!(
                        "load partition metadata failed, topic={} partition={} error={error:?}",
                        config.sink_topic(),
                        partition
                    ))
                })?;

            let producer = BatchProducerBuilder::new(Arc::new(partition_client))
                .with_compression(map_rskafka_compression(config.sink_compression_type()))
                .with_linger(Duration::from_millis(config.sink_linger_ms() as u64))
                .build(RecordAggregator::new(config.sink_batch_size() as usize));
            partition_producers.insert(*partition, producer);
        }

        if partition_producers.is_empty() {
            return Err(InitError::Kafka(format!(
                "topic {} has no partitions",
                config.sink_topic()
            )));
        }

        Ok(Self {
            partition_producers,
        })
    }

    async fn send_batch_messages(&self, messages: Vec<DebeziumFormat>) -> Result<()> {
        for message in messages {
            self.send_message(message).await?;
        }
        Ok(())
    }

    async fn send_message(&self, message: DebeziumFormat) -> Result<()> {
        let partition = self.partition(message.keys().as_str());
        let producer = self.partition_producers.get(&partition).ok_or_else(|| {
            InitError::Kafka(format!(
                "missing rskafka producer for partition {}",
                partition
            ))
        })?;

        producer
            .produce(to_rskafka_record(message))
            .await
            .map_err(|error| InitError::Kafka(format!("rskafka produce error: {error:?}")))?;
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        for (partition, producer) in &self.partition_producers {
            producer.flush().await.map_err(|error| {
                InitError::Kafka(format!(
                    "rskafka flush error, partition={} error={error:?}",
                    partition
                ))
            })?;
        }
        info!("flush rskafka producer finished");
        Ok(())
    }

    fn partition(&self, key: &str) -> i32 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() % self.partition_producers.len() as u64) as i32
    }
}

fn map_rskafka_compression(compression_type: &str) -> Compression {
    match compression_type.to_ascii_lowercase().as_str() {
        "gzip" => Compression::Gzip,
        "lz4" => Compression::Lz4,
        "snappy" => Compression::Snappy,
        "zstd" => Compression::Zstd,
        "none" => Compression::NoCompression,
        other => {
            warn!(
                "unsupported rskafka compression type {}, fallback to no compression",
                other
            );
            Compression::NoCompression
        }
    }
}

fn to_rskafka_record(value: DebeziumFormat) -> Record {
    let body = value.to_json();
    let key = value.keys();
    let headers = BTreeMap::from([("key".to_string(), key.clone().into_bytes())]);

    Record {
        key: Some(key.into_bytes()),
        value: Some(body.into_bytes()),
        headers,
        timestamp: Utc::now(),
    }
}
