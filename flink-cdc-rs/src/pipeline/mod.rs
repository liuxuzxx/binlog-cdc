use std::sync::Arc;

use std::time::Duration;

use crate::common::ChannelMetrics;
use crate::config::CdcConfig;
use crate::config::{sink::Sink, source::Source};
use crate::pipeline::message::PipelineRecord;
use crate::sink::console::ConsoleSink;
use crate::sink::kafka::RskafkaSink;
use crate::sink::mysql::MysqlSink;
use crate::source::console::ConsoleSource;
use crate::source::kafka::Kafka as KafkaSource;
use crate::source::mysql::MysqlBinlogEvent;
use crate::source::rocketmq::RocketMQSource;
use prometheus_client::registry::Registry;
use tokio::sync::Mutex;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;

pub mod formatter;
/// 主要是放置数据处理的Pipeline的逻辑
/// 类似于流水线的思想去做
///
/// 我们的想法如下:
/// 1. 整体的逻辑是source--->channel---->transformer(待定去做)--->sink
/// 2. source投递的数据为:数据本身+源数据
/// 3. 源数据是多种类型的enum,包含各种自定义的数据信息
///
pub mod message;

pub async fn pipeline(cdc: &CdcConfig, registry: Arc<Mutex<Registry>>) {
    match (cdc.source(), cdc.sink()) {
        (Source::Kafka(kafka), Sink::Mysql(_)) => {
            tracing::info!("kafka--->mysql");
            let sink = MysqlSink::create(cdc).await;
            let source = KafkaSource::new(sink, kafka, cdc);
            source.start().await;
        }
        (Source::Mysql(_), Sink::Kafka(kafka)) => {
            tracing::info!("mysql--->kafka");
            let (senders, receivers) = channels(cdc);
            spawn_channel_sampler(registry.clone(), senders.clone()).await;
            let sink = RskafkaSink::create_with_channels(kafka, receivers, registry.clone()).await;
            let sink_handles = sink.start();

            let mut source = MysqlBinlogEvent::create(cdc, senders, registry).await;
            source.read().await;
            drop(source);

            for handle in sink_handles {
                handle.await.expect("kafka sink task failed");
            }
        }
        (Source::Mysql(_), Sink::Console(_)) => {
            tracing::info!("mysql--->console");
            let (senders, receivers) = channels(cdc);
            let sink = ConsoleSink::create(receivers);
            let sink_handles = sink.start();

            let mut source = MysqlBinlogEvent::create(cdc, senders, registry).await;
            source.read().await;
            drop(source);

            for handle in sink_handles {
                handle.await.expect("console sink task failed");
            }
        }
        (Source::Rocketmq(_), Sink::Kafka(kafka)) => {
            tracing::info!("rocketmq--->kafka");
            let (senders, receivers) = channels(cdc);
            let sink = RskafkaSink::create_with_channels(kafka, receivers, registry.clone()).await;
            let sink_handles = sink.start();

            let mut source = RocketMQSource::create(cdc, senders);
            source.read().await;
            drop(source);

            for handle in sink_handles {
                handle.await.expect("kafka sink task failed");
            }
        }
        (Source::Console(_), Sink::Kafka(kafka)) => {
            tracing::info!("console--->kafka");
            let (senders, receivers) = channels(cdc);
            let sink = RskafkaSink::create_with_channels(kafka, receivers, registry.clone()).await;
            let sink_handles = sink.start();

            let source = ConsoleSource::create(senders);
            source.read().await;
            drop(source);

            for handle in sink_handles {
                handle.await.expect("console-kafka task failed");
            }
        }
        _ => panic!("unsupported pipeline source/sink combination"),
    }
}

/// 周期采样各 channel 深度/占用率, 暴露到 /metrics
async fn spawn_channel_sampler(
    registry: Arc<Mutex<Registry>>,
    senders: Vec<Sender<PipelineRecord>>,
) {
    let metrics = {
        let mut r = registry.lock().await;
        ChannelMetrics::register(&mut r)
    };
    let caps: Vec<usize> = senders.iter().map(|s| s.max_capacity()).collect();
    tokio::spawn(async move {
        tracing::info!(
            "channel metrics sampler started, channels={}",
            senders.len()
        );
        loop {
            for (i, s) in senders.iter().enumerate() {
                // tokio mpsc 的 len() 只在 Receiver 上; Sender 侧用 总容量-剩余容量 反推深度
                let depth = s.max_capacity() - s.capacity();
                metrics.set(i, depth, caps[i]);
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });
}

fn channels(cdc: &CdcConfig) -> (Vec<Sender<PipelineRecord>>, Vec<Receiver<PipelineRecord>>) {
    let parallelism = cdc
        .pipeline()
        .map(|pipeline| pipeline.parallelism())
        .unwrap_or(6);
    let capacity = cdc
        .pipeline()
        .map(|pipeline| pipeline.capacity())
        .unwrap_or(1000);

    bounded_channels(parallelism, capacity)
}

fn bounded_channels<T>(parallelism: u32, capacity: u32) -> (Vec<Sender<T>>, Vec<Receiver<T>>) {
    (0..parallelism)
        .map(|_| tokio::sync::mpsc::channel(capacity as usize))
        .unzip()
}

#[cfg(test)]
mod tests {
    use super::bounded_channels;

    #[tokio::test]
    async fn bounded_tokio_channels_preserve_fifo_and_close() {
        let (senders, mut receivers) = bounded_channels::<u32>(2, 2);

        assert_eq!(senders.len(), 2);
        assert_eq!(receivers.len(), 2);
        senders[0].send(1).await.unwrap();
        senders[0].send(2).await.unwrap();
        assert_eq!(receivers[0].recv().await, Some(1));
        assert_eq!(receivers[0].recv().await, Some(2));

        drop(senders);
        assert_eq!(receivers[0].recv().await, None);
    }
}
