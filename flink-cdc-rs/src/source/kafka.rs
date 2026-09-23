use futures_util::TryStreamExt;
use rdkafka::{
    ClientConfig, Message,
    consumer::{Consumer, StreamConsumer},
    error::KafkaError,
};
use tracing::warn;

use crate::{
    config::{CdcConfig, source::Kafka as KafkaConfig},
    pipeline::formatter::DebeziumFormat,
    sink::SinkStream,
};

///
/// 放置Kafka作为数据源的处理代码
///

pub struct Kafka<'a, T>
where
    T: SinkStream,
{
    sink: T,
    source_config: &'a KafkaConfig,
    cdc_config: &'a CdcConfig,
}

impl<'a, T> Kafka<'a, T>
where
    T: SinkStream,
{
    const BOOTSTRAP_SERVERS: &'static str = "bootstrap.servers";
    const GROUP_ID: &'static str = "group.id";
    const AUTO_OFFSET_RESET: &'static str = "auto.offset.reset";
    const EARLIEST: &'static str = "earliest";
    const SESSION_TIMEOUT_MS: &'static str = "session.timeout.ms";
    const HEARTBEAT_INTERVAL_MS: &'static str = "heartbeat.interval.ms";
    pub fn new(sink: T, source_config: &'a KafkaConfig, cdc_config: &'a CdcConfig) -> Self {
        Kafka {
            sink,
            source_config,
            cdc_config,
        }
    }

    pub async fn start(&self) {
        let consumer = self.build_consumer().expect("build consumer error!");
        let stream = consumer.stream().try_for_each(|message| async move {
            let Some(payload) = message.payload() else {
                warn!("kafka message payload is empty, topic={}", message.topic());
                return Ok(());
            };
            match serde_json::from_slice::<DebeziumFormat>(payload) {
                Ok(debezium) => self.sink.process(&debezium, message.topic()).await,
                Err(err) => warn!(
                    "parse kafka message as debezium error:{:?}, topic={}",
                    err,
                    message.topic()
                ),
            }
            Ok(())
        });
        stream.await.expect("stream kafka message error!");
        warn!("kafka source stop!");
    }

    fn build_consumer(&self) -> Result<StreamConsumer, KafkaError> {
        let consumer = ClientConfig::new()
            .set(
                Self::BOOTSTRAP_SERVERS,
                self.source_config.bootstrap_server(),
            )
            .set(Self::GROUP_ID, self.source_config.group_id())
            .set(Self::AUTO_OFFSET_RESET, Self::EARLIEST)
            .set(Self::HEARTBEAT_INTERVAL_MS, "3000")
            .set(Self::SESSION_TIMEOUT_MS, "45000")
            .create::<StreamConsumer>()?;
        let topics = self
            .cdc_config
            .route_sources()
            .expect("no config topic for kafka source!");
        consumer.subscribe(&topics)?;
        return Ok(consumer);
    }
}
