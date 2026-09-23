use std::sync::Arc;

use crate::debezium::DebeziumFormat;
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
    sync::Mutex,
};

use crate::{config::FlinkCdcInit, error::Result, kafka::KafkaSink};

pub enum OutputSink {
    Kafka(KafkaSink),
    File(FileSink),
    Console(ConsoleSink),
}

impl OutputSink {
    pub async fn build(config: &FlinkCdcInit) -> Result<Self> {
        match config.sink_type() {
            "kafka" => Ok(Self::Kafka(KafkaSink::build(config).await?)),
            "file" => Ok(Self::File(FileSink::build(config).await?)),
            "console" => Ok(Self::Console(ConsoleSink::new())),
            other => panic!("unsupported sink type: {}", other),
        }
    }

    pub async fn send_batch_messages(&self, messages: Vec<DebeziumFormat>) -> Result<()> {
        match self {
            OutputSink::Kafka(sink) => sink.send_batch_messages(messages).await,
            OutputSink::File(sink) => sink.send_batch_messages(messages).await,
            OutputSink::Console(sink) => sink.send_batch_messages(messages).await,
        }
    }

    pub async fn flush(&self) -> Result<()> {
        match self {
            OutputSink::Kafka(sink) => sink.flush().await,
            OutputSink::File(sink) => sink.flush().await,
            OutputSink::Console(_) => Ok(()),
        }
    }
}

pub struct FileSink {
    file: Arc<Mutex<File>>,
}

impl FileSink {
    pub async fn build(config: &FlinkCdcInit) -> Result<Self> {
        let append = config.sink_append();
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(append)
            .truncate(!append)
            .open(config.sink_path())
            .await?;

        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub async fn send_batch_messages(&self, messages: Vec<DebeziumFormat>) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let mut buffer = String::new();
        for message in messages {
            buffer.push_str(message.to_json().as_str());
            buffer.push('\n');
        }

        let mut file = self.file.lock().await;
        file.write_all(buffer.as_bytes()).await?;
        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        let mut file = self.file.lock().await;
        file.flush().await?;
        file.sync_data().await?;
        Ok(())
    }
}

pub struct ConsoleSink;

impl ConsoleSink {
    pub fn new() -> Self {
        Self
    }

    pub async fn send_batch_messages(&self, messages: Vec<DebeziumFormat>) -> Result<()> {
        for message in messages {
            println!("{}", message.to_json());
        }
        Ok(())
    }
}
