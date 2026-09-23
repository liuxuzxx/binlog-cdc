use std::time::Duration;

use serde::{Deserialize, Serialize};

///
/// 放置的都是source的数据配置信息
/// 使用枚举类型来做自动的区分
///
#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "type")]
pub enum Source {
    #[serde(rename = "kafka")]
    Kafka(Kafka),
    #[serde(rename = "mysql")]
    Mysql(Mysql),
    #[serde(rename = "mysqldump")]
    MysqlDump(Mysqldump),
    #[serde(rename = "rocketmq")]
    Rocketmq(Rocketmq),
    #[serde(rename = "console")]
    Console(Console),
    #[serde(rename = "binlog")]
    BinlogFile(BinlogFile),
}

///
/// Kafka作为source的配置
///
#[derive(Deserialize, Serialize, Debug)]
pub struct Kafka {
    name: String,
    #[serde(rename = "properties.bootstrap.servers")]
    bootstrap_server: String,
    #[serde(rename = "properties.group.id")]
    group_id: String,
}

impl Kafka {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn bootstrap_server(&self) -> &str {
        &self.bootstrap_server
    }

    pub fn group_id(&self) -> &str {
        &self.group_id
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Mysql {
    name: String,
    hostname: String,
    port: u32,
    username: String,
    password: String,
    tables: String,
    #[serde(rename = "server-id")]
    server_id: String,

    #[serde(rename = "scan.startup.mode")]
    mode: String,
    #[serde(rename = "scan.startup.specific-offset.file")]
    binlog_filename: Option<String>,
    #[serde(rename = "scan.startup.specific-offset.pos")]
    binlog_offset: Option<u32>,

    #[serde(rename = "scan.startup.timestamp-millis")]
    timestamp_millis: Option<u32>,

    #[serde(rename = "connect.max-retries")]
    connect_max_retries: Option<u32>,
    #[serde(rename = "connect.timeout")]
    connect_timeout: Option<Duration>,
}

impl Mysql {
    pub fn url(&self) -> String {
        let uri = format!("mysql://{}:{}", self.hostname, self.port);
        let mut uri = url::Url::parse(&uri).unwrap();
        let _ = uri.set_username(self.username());
        let _ = uri.set_password(Some(self.password()));
        uri.as_str().to_string()
    }

    pub fn username(&self) -> &str {
        self.username.as_str()
    }

    pub fn password(&self) -> &str {
        self.password.as_str()
    }

    pub fn tables(&self) -> &str {
        self.tables.as_str()
    }

    pub fn server_id(&self) -> u64 {
        self.server_id
            .split_once('-')
            .map(|(prefix, _)| prefix.parse::<u64>())
            .unwrap_or_else(|| self.server_id.parse())
            .expect(format!("parse server-id:{} error", self.server_id.as_str()).as_str())
    }

    pub fn binlog_filename(&self) -> String {
        self.binlog_filename
            .clone()
            .expect("error of fetch scan.startup.specific-offset.file")
    }

    pub fn binlog_offset(&self) -> u32 {
        self.binlog_offset
            .expect("error of fetch scan.startup.specific-offset.pos")
    }

    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout.unwrap_or(Duration::from_secs(30))
    }

    pub fn new(
        name: &str,
        hostname: &str,
        port: u32,
        username: &str,
        password: &str,
        tables: &str,
        server_id: &str,
        mode: &str,
    ) -> Self {
        Mysql {
            name: name.to_string(),
            hostname: hostname.to_string(),
            port,
            username: username.to_string(),
            password: password.to_string(),
            tables: tables.to_string(),
            server_id: server_id.to_string(),
            mode: mode.to_string(),
            binlog_filename: None,
            binlog_offset: None,
            timestamp_millis: None,
            connect_max_retries: None,
            connect_timeout: None,
        }
    }
}

///
/// 这种类型的配置其实很简单，就是读取某个文件夹下面的文件就行了
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Mysqldump {
    name: String,
    filepath: String,
}

impl Mysqldump {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn filepath(&self) -> &str {
        &self.filepath
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Rocketmq {
    name: Option<String>,
    topic: String,
    group: String,
    nameserver: String,
    tag: Option<String>,
    #[serde(rename = "consume.from")]
    consume_from: Option<String>,
    #[serde(rename = "consume.thread.min")]
    consume_thread_min: Option<u32>,
    #[serde(rename = "consume.thread.max")]
    consume_thread_max: Option<u32>,
}

impl Rocketmq {
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn group(&self) -> &str {
        &self.group
    }

    pub fn nameserver(&self) -> &str {
        &self.nameserver
    }

    pub fn tag(&self) -> &str {
        self.tag.as_deref().unwrap_or("*")
    }

    pub fn consume_from(&self) -> &str {
        self.consume_from.as_deref().unwrap_or("last")
    }

    pub fn consume_thread_min(&self) -> Option<u32> {
        self.consume_thread_min
    }

    pub fn consume_thread_max(&self) -> Option<u32> {
        self.consume_thread_max
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Console {
    name: String,
}

impl Console {
    pub fn name(&self) -> &str {
        self.name.as_str()
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct BinlogFile {
    name: String,
    path: String,
    hostname: String,
    port: u32,
    username: String,
    password: String,
}

impl BinlogFile {
    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn password(&self) -> &str {
        &self.password
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn url(&self) -> String {
        let uri = format!("mysql://{}:{}", self.hostname, self.port);
        let mut uri = url::Url::parse(&uri).unwrap();
        let _ = uri.set_username(self.username());
        let _ = uri.set_password(Some(self.password()));
        uri.as_str().to_string()
    }
}

#[cfg(test)]
mod tests {
    use crate::config::source::{Kafka, Source};

    #[test]
    fn test_serialize_source() {
        let kafka = Kafka {
            name: "kafka_source".to_string(),
            bootstrap_server: "localhost:9092".to_string(),
            group_id: "cg-001".to_string(),
        };

        let kafka = Source::Kafka(kafka);
        let yaml = serde_yaml::to_string(&kafka).unwrap();
        println!("{}", yaml);
    }
}
