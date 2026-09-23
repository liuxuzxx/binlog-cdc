use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    sync::Arc,
    time::Duration,
};

use prometheus_client::registry::Registry;
use rdkafka::{
    ClientConfig,
    error::KafkaError,
    producer::{FutureProducer, FutureRecord},
    types::RDKafkaErrorCode,
};
use rskafka::{
    client::{
        Client, ClientBuilder,
        partition::{Compression, PartitionClient},
    },
    record::Record,
};
use tokio::sync::{
    Mutex,
    mpsc::{self, Receiver, Sender},
};
use tracing::{info, warn};

use crate::{
    config::{cdc::FlinkCdc, sink::Kafka},
    pipeline::{
        formatter::{DebeziumFormat, ToDebeziumFormat},
        message::{MysqlBinlogEventRecord, PipelineRecord},
    },
    sink::SinkStream,
};

///
/// 按照Flink CDC的思想，有source和sink的类型，目前我们只支持sink为Kafka的类型
/// 并且只支持投递一个配置的topic的地址
///

const BOOTSTRAP_SERVERS: &'static str = "bootstrap.servers";
const MESSAGE_TIMEOUT_MS: &'static str = "message.timeout.ms";
const BATCH_SIZE: &'static str = "batch.size";
const COMPRESSION_TYPE: &'static str = "compression.type";
const LINGER_MS: &'static str = "linger.ms";

pub struct KafkaSink {
    producer: FutureProducer,
    topic: Arc<String>,
}

impl KafkaSink {
    pub fn build(config: &FlinkCdc) -> Self {
        let producer = ClientConfig::new()
        .set(BOOTSTRAP_SERVERS, config.sink_bootstrap_server())
        .set(MESSAGE_TIMEOUT_MS, "5000")
        .set(BATCH_SIZE, "10485760")
        .set(COMPRESSION_TYPE, config.sink_compression_type())
        .set(LINGER_MS, config.sink_linger_ms().to_string())
        .create::<FutureProducer>()
        .expect(format!("Sink Kafka producer creation failed of bootstrap.servers:{} compression-type:{}",config.sink_bootstrap_server(),config.sink_compression_type()).as_str());

        KafkaSink {
            producer: producer,
            topic: Arc::new(config.sink_topic().to_string()),
        }
    }

    pub async fn send_batch_messages(&self, messages: Vec<DebeziumFormat>) {
        for message in messages {
            let body = message.to_json();
            let key = message.keys();
            let message = FutureRecord::to(self.topic.as_str())
                .key(key.as_str())
                .payload(&body);
            match self.producer.send_result(message) {
                Ok(_) => {
                    info!("send message ok,keys:{}!", key);
                }
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), record)) => {
                    let retry_result = self.producer.send(record, Duration::from_secs(3)).await;
                    match retry_result {
                        Ok(_) => {
                            info!("retry send message ok,keys:{}!", key);
                        }
                        Err(err) => {
                            warn!("retry send message error,keys:{},error:{:?}", key, err);
                        }
                    }
                }
                Err(err) => {
                    warn!("send message error,keys:{}!{:?}", key, err);
                }
            }
        }
    }
}

impl SinkStream for KafkaSink {
    async fn handle_messages(&self, messages: Vec<DebeziumFormat>) {
        self.send_batch_messages(messages).await;
    }

    async fn process(&self, _debezium: &DebeziumFormat, _topic: &str) {}
}

///
/// 多线程发送给Kafka的实现
/// 之前发现一个问题:就是解析binlog,然后发送到Kafka的过程是在一个线程中执行的,所以
/// 导致解析Binlog和发送Kafka相互争抢CPU,从而影响了整体的吞吐量
///
/// It is two threads to send messages to Kafka.
///
/// 为此我们决定使用channel来模拟一个单个生产者,但是多个消费者的模式
///

type Senders = Vec<Sender<DebeziumFormat>>;

pub struct SpmcKafkaSink {
    senders: Senders,
}

impl SpmcKafkaSink {
    pub fn build(config: &FlinkCdc) -> Self {
        type Channels = (Vec<Sender<DebeziumFormat>>, Vec<Receiver<DebeziumFormat>>);
        let (senders, receivers): Channels = (1..=config.pipeline_parallelism())
            .into_iter()
            .map(|_| mpsc::channel(10000))
            .unzip();

        let topic = Arc::new(config.sink_topic().to_string());

        for mut receiver in receivers {
            let producer = SpmcKafkaSink::create_producer(config);
            let target_topic = topic.clone();
            tokio::spawn(async move {
                while let Some(message) = receiver.recv().await {
                    let body = message.to_json();
                    let key = message.keys();
                    let message = FutureRecord::to(target_topic.as_str())
                        .key(key.as_str())
                        .payload(&body);

                    match producer.send_result(message) {
                        Ok(_) => {
                            info!("send use mpsc channels ok!")
                        }
                        Err(err) => {
                            warn!("send use mpsc channels error:{:?}", err);
                        }
                    }
                }
            });
        }
        SpmcKafkaSink { senders: senders }
    }

    fn create_producer(config: &FlinkCdc) -> FutureProducer {
        return ClientConfig::new()
        .set(BOOTSTRAP_SERVERS, config.sink_bootstrap_server())
        .set(MESSAGE_TIMEOUT_MS, "5000")
        .set(BATCH_SIZE, "10485760")
        .set(COMPRESSION_TYPE, config.sink_compression_type())
        .set(LINGER_MS, config.sink_linger_ms().to_string())
        .create::<FutureProducer>()
        .expect(format!("Sink Kafka producer creation failed of bootstrap.servers:{} compression-type:{}",config.sink_bootstrap_server(),config.sink_compression_type()).as_str());
    }
}

impl SinkStream for SpmcKafkaSink {
    async fn handle_messages(&self, messages: Vec<DebeziumFormat>) {
        let mut hasher = DefaultHasher::new();
        for msg in messages {
            msg.keys().hash(&mut hasher);
            let hash = hasher.finish();
            let index = hash % self.senders.len() as u64;
            match self.senders[index as usize].send(msg).await {
                Ok(_) => {}
                Err(err) => {
                    warn!("send message error:{:?}", err);
                }
            }
        }
    }

    async fn process(&self, _debezium: &DebeziumFormat, _topic: &str) {}
}

///
/// 使用rskafka组件实现Kafka的消息发送,经过测试得到的结论是:
/// rdkafka存在一定的效率问题，所以还是使用rskafka组件
///

#[derive(Clone)]
struct PartitionProducer {
    partition_client: Arc<PartitionClient>,
    compression: Compression,
}

type PartitionProducers = HashMap<i32, PartitionProducer>;

///
/// 基本上可以推断出来,性能问题存在与RskafkaSink这端
///
pub struct RskafkaSink {
    partition_producers: PartitionProducers,
    topic: String,
    channels: Vec<Receiver<PipelineRecord>>,
    registry: Arc<Mutex<Registry>>,
    metrics: Arc<crate::common::SinkKafkaMetrics>,
}

impl RskafkaSink {
    pub async fn create(config: &FlinkCdc, registry: Arc<Mutex<Registry>>) -> Self {
        let metrics = Arc::new(crate::common::register_sink_metrics(registry.clone()).await);
        let client = ClientBuilder::new(config.sink_bootstrap_servers())
            .build()
            .await
            .expect("create rskafka client error...");
        let producers = RskafkaSink::load_metadata(
            &client,
            config.sink_topic(),
            config.sink_compression_type(),
        )
        .await;

        RskafkaSink {
            partition_producers: producers,
            topic: config.sink_topic().to_string(),
            channels: Vec::new(),
            registry: registry,
            metrics: metrics,
        }
    }

    pub async fn create_with_channels(
        config: &Kafka,
        channels: Vec<Receiver<PipelineRecord>>,
        registry: Arc<Mutex<Registry>>,
    ) -> Self {
        let metrics = Arc::new(crate::common::register_sink_metrics(registry.clone()).await);
        let client = ClientBuilder::new(config.bootstrap_servers())
            .build()
            .await
            .expect("create rskafka client error...");
        let producers =
            RskafkaSink::load_metadata(&client, config.topic(), config.compression_type()).await;

        RskafkaSink {
            partition_producers: producers,
            topic: config.topic().to_string(),
            channels: channels,
            registry: registry,
            metrics: metrics,
        }
    }

    async fn load_metadata(
        client: &Client,
        topic: &str,
        compression_type: &str,
    ) -> PartitionProducers {
        let topics = client
            .list_topics()
            .await
            .expect("failed to load topics metadata...");
        let metadata = topics
            .iter()
            .find(|ele| ele.name.eq_ignore_ascii_case(topic))
            .expect(format!("topic:{} not exists!", topic).as_str());

        let mut producers = PartitionProducers::new();
        let compression = RskafkaSink::compression(compression_type);
        for partition in &metadata.partitions {
            let partition_client = client
                .partition_client(
                    topic,
                    *partition,
                    rskafka::client::partition::UnknownTopicHandling::Retry,
                )
                .await
                .expect("failed to load partition metadata...");

            let partition_client = Arc::new(partition_client);
            producers.insert(
                *partition,
                PartitionProducer {
                    partition_client,
                    compression,
                },
            );
        }

        return producers;
    }

    pub fn start(self) -> Vec<tokio::task::JoinHandle<()>> {
        let RskafkaSink {
            partition_producers,
            topic,
            channels,
            registry,
            metrics,
        } = self;

        channels
            .into_iter()
            .enumerate()
            .map(|(index, mut receiver)| {
                let sink = RskafkaSink {
                    partition_producers: partition_producers.clone(),
                    topic: topic.clone(),
                    channels: Vec::new(),
                    registry: registry.clone(),
                    metrics: metrics.clone(),
                };
                tokio::spawn(async move {
                    info!("rskafka sink receiver启动, index={}", index);
                    //修改成下面的模式之后,我们发现tps能到:10w/s的速度
                    let size: usize = 200;
                    loop {
                        let mut buffer: Vec<PipelineRecord> = Vec::with_capacity(size);
                        let count = receiver.recv_many(&mut buffer, size).await;
                        if count == 0 {
                            break;
                        }
                        sink.send_batch_messages(buffer).await;
                    }
                    info!("rskafka sink receiver退出, index={}", index);
                })
            })
            .collect::<Vec<_>>()
    }

    pub async fn write(self) {
        let handles = self.start();
        for handle in handles {
            handle.await.expect("rskafka sink write task failed");
        }
    }

    async fn send_batch_messages(&self, messages: Vec<PipelineRecord>) {
        let partition_records = messages
            .into_iter()
            .filter_map(|message| self.message_records(message))
            .fold(
                HashMap::<i32, Vec<Record>>::new(),
                |mut partitions, (partition, records)| {
                    partitions.entry(partition).or_default().extend(records);
                    partitions
                },
            );

        futures_util::future::join_all(
            partition_records
                .into_iter()
                .map(|(partition, records)| self.produce_records(partition, records)),
        )
        .await;
    }

    fn message_records(&self, message: PipelineRecord) -> Option<(i32, Vec<Record>)> {
        match message {
            PipelineRecord::MysqlDebezium(data) | PipelineRecord::MysqlBinlogStream(data) => {
                let key = data.keys();
                let partition = self.partition(key.as_str());
                let record = Record::from(data);
                Some((partition, vec![record]))
            }
            PipelineRecord::RocketmqDebezium(data) => {
                let debezium = data.into_data();
                let key = debezium.keys();
                let partition = self.partition(key.as_str());
                let record = Record::from(debezium);
                Some((partition, vec![record]))
            }
            PipelineRecord::MysqlBinlogEvent(data) => {
                let partition = self.partition(data.key());
                match Self::binlog_event_record(data) {
                    Some(records) => Some((partition, records)),
                    None => {
                        warn!(
                            "skip mysql binlog event because it cannot be converted to kafka records"
                        );
                        None
                    }
                }
            }
            PipelineRecord::ConsoleData(data) => {
                let data = data.inot_data();
                let partition = self.partition(&data.keys());
                let record = Record::from(data);
                Some((partition, vec![record]))
            }
            _ => {
                warn!("not right pipeline record type");
                None
            }
        }
    }

    async fn produce_records(&self, partition: i32, records: Vec<Record>) {
        if let Some(producer) = self.partition_producers.get(&partition) {
            let count = records.len();
            let start = std::time::Instant::now();
            let result = producer
                .partition_client
                .produce(records, producer.compression)
                .await;
            let elapsed = start.elapsed().as_secs_f64();
            match result {
                Ok(offsets) => {
                    self.metrics.record_produce(count, elapsed, true);
                    info!(
                        "send batch messages to kafka success count:{} partition:{} offsets:{:?} elapsed:{:.3}s",
                        count, partition, offsets, elapsed
                    );
                }
                Err(err) => {
                    self.metrics.record_produce(count, elapsed, false);
                    warn!(
                        "failed to produce batch messages to kafka partition:{}, count:{}, error:{:?}",
                        partition, count, err
                    );
                }
            }
        } else {
            warn!("partition producer not found, partition:{}", partition);
        }
    }

    fn binlog_event_record(data: MysqlBinlogEventRecord) -> Option<Vec<Record>> {
        return data.to().map(|debezium_formats| {
            debezium_formats
                .into_iter()
                .map(|ele| Record::from(ele))
                .collect::<Vec<Record>>()
        });
    }

    pub async fn send_records(&self, messages: Vec<PipelineRecord>) {
        if self.partition_producers.is_empty() {
            warn!("rskafka sink has no partition producer");
            return;
        }

        let mut partition_records: HashMap<i32, Vec<Record>> = HashMap::new();
        for msg in messages {
            if let Some((partition, records)) = self.message_records(msg) {
                partition_records
                    .entry(partition)
                    .or_default()
                    .extend(records);
            }
        }

        futures_util::future::join_all(
            partition_records
                .into_iter()
                .map(|(partition, records)| self.produce_records(partition, records)),
        )
        .await;
    }

    pub fn hash_code(source: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        source.hash(&mut hasher);
        hasher.finish()
    }

    fn partition(&self, key: &str) -> i32 {
        let hash = RskafkaSink::hash_code(key);
        (hash % self.partition_producers.len() as u64) as i32
    }

    fn compression(compression_type: &str) -> Compression {
        match compression_type.to_ascii_lowercase().as_str() {
            "gzip" => Compression::Gzip,
            "lz4" => Compression::Lz4,
            "snappy" => Compression::Snappy,
            "zstd" => Compression::Zstd,
            _ => Compression::NoCompression,
        }
    }
}

impl SinkStream for RskafkaSink {
    async fn handle_messages(&self, _messages: Vec<DebeziumFormat>) {}

    async fn process(&self, _debezium: &DebeziumFormat, _topic: &str) {}
}
