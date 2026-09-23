use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use rocketmq_client_v4::consumer::message_handler::MessageHandler;
use rocketmq_client_v4::consumer::pull_consumer_v2::PullConsumer;
use rocketmq_client_v4::protocols::body::message_body::MessageBody;
use tokio::sync::{RwLock, mpsc::Sender};
use tracing::{info, warn};

use crate::config::CdcConfig;
use crate::config::source::{Rocketmq, Source};
use crate::pipeline::formatter::DebeziumFormat;
use crate::pipeline::message::{PipelineRecord, RocketmqDebezium};

/// RocketMQ 4.x remoting source.
///
/// The consumed message body is expected to be a Debezium JSON payload, and it is forwarded
/// to the configured Kafka sink through the common pipeline channel.
pub struct RocketMQSource<'a> {
    source: &'a Rocketmq,
    channels: Vec<Sender<PipelineRecord>>,
}

impl<'a> RocketMQSource<'a> {
    pub fn create(cdc: &'a CdcConfig, channels: Vec<Sender<PipelineRecord>>) -> Self {
        let source = match cdc.source() {
            Source::Rocketmq(source) => source,
            _ => panic!("rocketmq source need rocketmq config"),
        };

        Self { source, channels }
    }

    pub async fn read(&mut self) {
        if self.source.tag() != "*" {
            warn!(
                "rocketmq-client-v4 does not support tag filter, configured tag={} will be ignored",
                self.source.tag()
            );
        }

        if self.source.consume_from() != "last" {
            warn!(
                "rocketmq-client-v4 starts from committed/max offset only, configured consume.from={} will be ignored",
                self.source.consume_from()
            );
        }

        let consumer = PullConsumer::new(
            self.source.nameserver().to_string(),
            self.source.group().to_string(),
            self.source.topic().to_string(),
        );
        let handler = Arc::new(RocketMQDebeziumHandler::new(self.channels.clone()));
        let run = Arc::new(RwLock::new(true));

        info!(
            "rocketmq source started nameserver={} group={} topic={}",
            self.source.nameserver(),
            self.source.group(),
            self.source.topic()
        );
        consumer.start_consume(handler, run).await;
        std::future::pending::<()>().await;
    }
}

#[derive(Clone)]
struct RocketMQDebeziumHandler {
    channels: Vec<Sender<PipelineRecord>>,
}

impl RocketMQDebeziumHandler {
    fn new(channels: Vec<Sender<PipelineRecord>>) -> Self {
        Self { channels }
    }

    async fn send(&self, debezium: DebeziumFormat, topic: String, msg_id: String) {
        if self.channels.is_empty() {
            warn!("rocketmq source has no channel sender");
            return;
        }

        let key = debezium.keys();
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let index = hasher.finish() as usize % self.channels.len();
        let record =
            PipelineRecord::RocketmqDebezium(RocketmqDebezium::new(debezium, topic, msg_id));

        if let Err(err) = self.channels[index].send(record).await {
            warn!("send rocketmq debezium to channel error:{:?}", err);
        }
    }
}

impl MessageHandler for RocketMQDebeziumHandler {
    async fn handle(&self, message: &MessageBody) {
        let debezium = match serde_json::from_slice::<DebeziumFormat>(&message.body) {
            Ok(debezium) => debezium,
            Err(err) => {
                warn!(
                    "parse rocketmq message as debezium error:{:?}, topic={} msg_id={}",
                    err, message.topic, message.msg_id
                );
                return;
            }
        };

        self.send(debezium, message.topic.clone(), message.msg_id.clone())
            .await;
    }
}
