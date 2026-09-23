use serde::{Deserialize, Serialize};
use url::Url;

///
/// 提供各种类型的sink配置信息
///
#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "type")]
pub enum Sink {
    #[serde(rename = "kafka")]
    Kafka(Kafka),
    #[serde(rename = "mysql")]
    Mysql(Mysql),
    #[serde(rename = "console")]
    Console(Console),
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Console {
    name: String,
}

impl Console {
    pub fn new(name: &str) -> Self {
        Console {
            name: name.to_string(),
        }
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Kafka {
    name: String,
    #[serde(rename = "properties.bootstrap.servers")]
    bootstrap_server: String,
    #[serde(rename = "properties.compression.type")]
    compression_type: String,
    topic: String,

    #[serde(rename = "properties.batch.size")]
    batch_size: Option<u32>,
    #[serde(rename = "properties.linger.ms")]
    linger_ms: Option<u32>,
}

impl Kafka {
    pub fn new(name: &str, bootstrap_server: &str) -> Self {
        Kafka {
            name: name.to_string(),
            bootstrap_server: bootstrap_server.to_string(),
            compression_type: "none".to_string(),
            topic: "default".to_string(),
            batch_size: None,
            linger_ms: None,
        }
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn bootstrap_server(&self) -> &str {
        self.bootstrap_server.as_str()
    }

    pub fn bootstrap_servers(&self) -> Vec<String> {
        self.bootstrap_server
            .split(',')
            .map(|server| server.to_string())
            .collect::<Vec<String>>()
    }

    pub fn compression_type(&self) -> &str {
        self.compression_type.as_str()
    }

    pub fn topic(&self) -> &str {
        self.topic.as_str()
    }

    pub fn batch_size(&self) -> u32 {
        self.batch_size.unwrap_or(10485760)
    }

    pub fn linger_ms(&self) -> u32 {
        self.linger_ms.unwrap_or(100)
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Mysql {
    hostname: String,
    port: u32,
    username: String,
    password: String,
}

impl Mysql {
    ///
    /// 获取url的字符串,能够处理一些有特殊字符的情况
    pub fn url(&self) -> String {
        let uri = format!("mysql://{}:{}", self.hostname, self.port);
        let mut uri = Url::parse(&uri).unwrap();
        let _ = uri.set_username(self.username());
        let _ = uri.set_password(Some(self.password()));

        return uri.as_str().to_string();
    }

    pub fn username(&self) -> &str {
        return self.username.as_str();
    }

    pub fn password(&self) -> &str {
        return self.password.as_str();
    }
}
