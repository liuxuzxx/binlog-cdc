use tracing::info;

use crate::pipeline::message::PipelineRecord;
use tokio::sync::mpsc::Receiver;

///
/// 提供一个朝向控制台输出的Sink实现, 主要用于测试和调试
///

pub struct ConsoleSink {
    channels: Vec<Receiver<PipelineRecord>>,
}

impl ConsoleSink {
    pub fn create(channels: Vec<Receiver<PipelineRecord>>) -> Self {
        ConsoleSink { channels: channels }
    }

    pub fn start(self) -> Vec<tokio::task::JoinHandle<()>> {
        self.channels
            .into_iter()
            .enumerate()
            .map(|(index, mut receiver)| {
                tokio::spawn(async move {
                    info!("console sink receiver启动, index={}", index);
                    while let Some(message) = receiver.recv().await {
                        info!("data:{}", message);
                    }
                })
            })
            .collect::<Vec<_>>()
    }

    pub async fn write(self) {
        let handles = self.start();
        for handle in handles {
            handle.await.expect("console sink write task failed");
        }
    }
}
