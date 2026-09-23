use chrono::Utc;
use tokio::sync::mpsc::Sender;

use crate::pipeline::message::{ConsoleData, PipelineRecord};

///
/// 提供也给可以在控制台进行数据输入的source,
/// 方便我们做console --->kafka/mysql/rocketmq等的测试
///
pub struct ConsoleSource {
    channels: Vec<Sender<PipelineRecord>>,
}

impl ConsoleSource {
    pub fn create(channels: Vec<Sender<PipelineRecord>>) -> Self {
        ConsoleSource { channels: channels }
    }

    pub async fn read(&self) {
        loop {
            let message = ConsoleData::default();
            let micros = Utc::now().timestamp_micros();
            let index = micros % self.channels.len() as i64;
            let result = self.channels[index as usize]
                .send(PipelineRecord::ConsoleData(message))
                .await;
            match result {
                Ok(_) => {
                    tracing::info!("send console to channel is ok:{}!", micros);
                }
                Err(err) => {
                    tracing::warn!("send console to channel is error of err:{}!", err);
                }
            }
        }
    }
}
