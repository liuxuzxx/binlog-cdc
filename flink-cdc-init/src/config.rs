use std::collections::HashMap;
use std::fs;

use regex::Regex;
use serde::Deserialize;
use url::Url;

use crate::error::{InitError, Result};

#[derive(Debug, Clone, Deserialize)]
pub struct FlinkCdcInit {
    source: Source,
    sink: Sink,
    pipeline: Option<Pipeline>,
    verify: Option<Verify>,
}

impl FlinkCdcInit {
    pub fn read_from(config_path: &str) -> Result<Self> {
        let content = fs::read_to_string(config_path)?;
        Self::from_str(&content)
    }

    pub fn from_str(content: &str) -> Result<Self> {
        let config: Self = serde_yaml::from_str(content)?;
        config.validate()?;
        Ok(config)
    }

    pub fn source_url(&self) -> String {
        self.source.url()
    }

    pub fn source_tables(&self) -> &str {
        self.source.tables()
    }

    pub fn source_server_timezone(&self) -> Option<&str> {
        self.source.server_time_zone()
    }

    pub fn source_table_include(&self) -> Result<TableInclude> {
        TableInclude::create(self.source_tables())
    }

    pub fn sink_name(&self) -> &str {
        self.sink.name()
    }

    pub fn sink_type(&self) -> &str {
        self.sink.type_name()
    }

    pub fn sink_bootstrap_server(&self) -> &str {
        self.sink.bootstrap_server()
    }

    pub fn sink_bootstrap_servers(&self) -> Vec<String> {
        self.sink
            .bootstrap_server()
            .split(',')
            .map(|server| server.trim().to_string())
            .filter(|server| !server.is_empty())
            .collect()
    }

    pub fn sink_compression_type(&self) -> &str {
        self.sink.compression_type()
    }

    pub fn sink_topic(&self) -> &str {
        self.sink.topic()
    }

    pub fn sink_path(&self) -> &str {
        self.sink.path()
    }

    pub fn sink_append(&self) -> bool {
        self.sink.append()
    }

    pub fn sink_batch_size(&self) -> u32 {
        self.sink.batch_size()
    }

    pub fn sink_linger_ms(&self) -> u32 {
        self.sink.linger_ms()
    }

    pub fn sink_max_request_size(&self) -> Option<u32> {
        self.sink.max_request_size()
    }

    pub fn sink_max_message_bytes(&self) -> Option<u32> {
        self.sink.max_message_bytes()
    }

    pub fn pipeline_name(&self) -> &str {
        self.pipeline
            .as_ref()
            .map(|pipeline| pipeline.name())
            .unwrap_or("flink-cdc-init")
    }

    pub fn pipeline_parallelism(&self) -> usize {
        self.pipeline
            .as_ref()
            .map(|pipeline| pipeline.parallelism())
            .unwrap_or(1)
            .max(1)
    }

    pub fn verify_row_count(&self) -> bool {
        self.verify
            .as_ref()
            .map(|verify| verify.row_count())
            .unwrap_or(false)
    }

    pub fn verify_fail_on_mismatch(&self) -> bool {
        self.verify
            .as_ref()
            .map(|verify| verify.fail_on_mismatch())
            .unwrap_or(true)
    }

    fn validate(&self) -> Result<()> {
        if !self.source.type_name.eq_ignore_ascii_case("mysql") {
            return Err(InitError::Config(format!(
                "unsupported source.type: {}",
                self.source.type_name
            )));
        }
        if self.source.tables.trim().is_empty() {
            return Err(InitError::Config(
                "source.tables can not be empty".to_string(),
            ));
        }

        match self.sink.type_name.as_str() {
            "kafka" => {
                if self
                    .sink
                    .bootstrap_server
                    .as_deref()
                    .unwrap_or("")
                    .trim()
                    .is_empty()
                {
                    return Err(InitError::Config(
                        "sink.properties.bootstrap.servers can not be empty".to_string(),
                    ));
                }
                if self.sink.topic.as_deref().unwrap_or("").trim().is_empty() {
                    return Err(InitError::Config("sink.topic can not be empty".to_string()));
                }
            }
            "file" => {
                if self.sink.path.as_deref().unwrap_or("").trim().is_empty() {
                    return Err(InitError::Config("sink.path can not be empty".to_string()));
                }
            }
            "console" => {}
            other => {
                return Err(InitError::Config(format!(
                    "unsupported sink.type: {}",
                    other
                )));
            }
        }

        self.source_table_include()?;
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
struct Source {
    #[serde(rename = "type")]
    type_name: String,
    hostname: String,
    port: u16,
    username: String,
    password: String,
    tables: String,
    #[serde(rename = "server-time-zone")]
    server_time_zone: Option<String>,
    #[serde(rename = "server-id")]
    server_id: Option<String>,
    #[serde(rename = "scan.startup.mode")]
    startup_mode: Option<String>,
    #[serde(rename = "scan.startup.specific-offset.file")]
    startup_offset_file: Option<String>,
    #[serde(rename = "scan.startup.specific-offset.pos")]
    startup_offset_pos: Option<u32>,
    #[serde(rename = "scan.startup.timestamp-millis")]
    startup_timestamp_millis: Option<u64>,
}

impl Source {
    fn url(&self) -> String {
        let uri = format!("mysql://{}:{}", self.hostname, self.port);
        let mut url = Url::parse(&uri).expect("invalid mysql url");
        let _ = url.set_username(&self.username);
        let _ = url.set_password(Some(&self.password));
        url.to_string()
    }

    fn tables(&self) -> &str {
        self.tables.as_str()
    }

    fn server_time_zone(&self) -> Option<&str> {
        self.server_time_zone.as_deref()
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
struct Sink {
    #[serde(rename = "type")]
    type_name: String,
    name: String,
    #[serde(rename = "properties.bootstrap.servers")]
    bootstrap_server: Option<String>,
    #[serde(rename = "properties.compression.type")]
    compression_type: Option<String>,
    #[serde(rename = "properties.batch.size")]
    batch_size: Option<u32>,
    #[serde(rename = "properties.linger.ms")]
    linger_ms: Option<u32>,
    #[serde(rename = "properties.max.request.size")]
    max_request_size: Option<u32>,
    #[serde(rename = "properties.max.message.bytes")]
    max_message_bytes: Option<u32>,
    topic: Option<String>,
    path: Option<String>,
    append: Option<bool>,
}

impl Sink {
    fn type_name(&self) -> &str {
        self.type_name.as_str()
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn bootstrap_server(&self) -> &str {
        self.bootstrap_server
            .as_deref()
            .expect("sink.properties.bootstrap.servers is required")
    }

    fn compression_type(&self) -> &str {
        self.compression_type.as_deref().unwrap_or("none")
    }

    fn topic(&self) -> &str {
        self.topic.as_deref().expect("sink.topic is required")
    }

    fn path(&self) -> &str {
        self.path.as_deref().expect("sink.path is required")
    }

    fn append(&self) -> bool {
        self.append.unwrap_or(true)
    }

    fn batch_size(&self) -> u32 {
        self.batch_size.unwrap_or(16384)
    }

    fn linger_ms(&self) -> u32 {
        self.linger_ms.unwrap_or(0)
    }

    fn max_request_size(&self) -> Option<u32> {
        self.max_request_size
    }

    fn max_message_bytes(&self) -> Option<u32> {
        self.max_message_bytes
    }
}

#[derive(Debug, Clone, Deserialize)]
struct Verify {
    #[serde(rename = "row-count")]
    row_count: Option<bool>,
    #[serde(rename = "fail-on-mismatch")]
    fail_on_mismatch: Option<bool>,
}

impl Verify {
    fn row_count(&self) -> bool {
        self.row_count.unwrap_or(false)
    }

    fn fail_on_mismatch(&self) -> bool {
        self.fail_on_mismatch.unwrap_or(true)
    }
}

#[derive(Debug, Clone, Deserialize)]
struct Pipeline {
    name: String,
    parallelism: Option<usize>,
}

impl Pipeline {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn parallelism(&self) -> usize {
        self.parallelism.unwrap_or(1)
    }
}

#[derive(Debug, Clone)]
pub struct TableInclude {
    includes: HashMap<String, Vec<Regex>>,
}

impl TableInclude {
    pub fn create(include_tables: &str) -> Result<Self> {
        let mut includes: HashMap<String, Vec<Regex>> = HashMap::new();

        for rule in include_tables
            .split(',')
            .map(str::trim)
            .filter(|rule| !rule.is_empty())
        {
            let (database, table_pattern) = rule.rsplit_once('.').ok_or_else(|| {
                InitError::Config(format!(
                    "invalid source.tables rule: {rule}, expected format db.table_regex"
                ))
            })?;

            let regex = Regex::new(&format!("^{table_pattern}$")).map_err(|error| {
                InitError::Config(format!(
                    "invalid table regex '{table_pattern}' in source.tables: {error}"
                ))
            })?;

            includes
                .entry(database.to_string())
                .or_default()
                .push(regex);
        }

        if includes.is_empty() {
            return Err(InitError::Config(
                "source.tables can not be empty".to_string(),
            ));
        }

        Ok(Self { includes })
    }

    pub fn can_include(&self, database: &str, table: &str) -> bool {
        self.includes
            .get(database)
            .map(|patterns| patterns.iter().any(|pattern| pattern.is_match(table)))
            .unwrap_or(false)
    }

    pub fn database_names(&self) -> Vec<&str> {
        let mut databases = self
            .includes
            .keys()
            .map(|name| name.as_str())
            .collect::<Vec<_>>();
        databases.sort_unstable();
        databases
    }
}

#[cfg(test)]
mod tests {
    use super::{FlinkCdcInit, TableInclude};

    #[test]
    fn test_deserialize_config() {
        let yaml = r#"source:
  type: mysql
  hostname: 127.0.0.1
  port: 3306
  username: root
  password: change-me
  tables: 'app_db.test_[0-9]+,app_db.orders'
  server-time-zone: Asia/Shanghai
  server-id: 5710-5716
  scan.startup.mode: specific-offset
  scan.startup.specific-offset.file: mysql-bin.000001
  scan.startup.specific-offset.pos: 4
sink:
  type: kafka
  name: Kafka-Sink
  properties.bootstrap.servers: 127.0.0.1:9092
  properties.compression.type: lz4
  properties.max.request.size: 41943040
  properties.max.message.bytes: 41943040
  topic: test-topic
pipeline:
  name: sync mysql snapshot to kafka
  parallelism: 2
verify:
  row-count: true
  fail-on-mismatch: true
"#;

        let config = FlinkCdcInit::from_str(yaml).unwrap();
        assert_eq!(config.pipeline_parallelism(), 2);
        assert_eq!(config.sink_topic(), "test-topic");
        assert_eq!(config.sink_max_request_size(), Some(41943040));
        assert!(config.verify_row_count());
    }

    #[test]
    fn test_deserialize_file_sink() {
        let yaml = r#"source:
  type: mysql
  hostname: 127.0.0.1
  port: 3306
  username: root
  password: change-me
  tables: 'app_db.orders'
sink:
  type: file
  name: file-sink
  path: /tmp/output.ndjson
  append: false
pipeline:
  name: verify snapshot
  parallelism: 1
"#;

        let config = FlinkCdcInit::from_str(yaml).unwrap();
        assert_eq!(config.sink_type(), "file");
        assert_eq!(config.sink_path(), "/tmp/output.ndjson");
        assert!(!config.sink_append());
    }

    #[test]
    fn test_table_include() {
        let include = TableInclude::create("app_db.test_[0-9]+,app_db.orders").unwrap();

        assert!(include.can_include("app_db", "test_01"));
        assert!(include.can_include("app_db", "orders"));
        assert!(!include.can_include("app_db", "users"));
        assert!(!include.can_include("other_db", "orders"));
    }
}
