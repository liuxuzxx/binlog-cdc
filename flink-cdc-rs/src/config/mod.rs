use std::fs;

use serde::{Deserialize, Serialize};

use crate::config::{cdc::Pipeline, route::Route};

pub mod cdc;
pub mod route;
pub mod sink;
pub mod source;

pub fn load_config(config_path: &str) -> CdcConfig {
    let content = fs::read_to_string(config_path).expect("Unable to read config file");
    serde_yaml::from_str(&content).expect("Unable to parse config file")
}

#[derive(Deserialize, Serialize, Debug)]
pub struct CdcConfig {
    source: source::Source,
    sink: sink::Sink,
    route: Option<Vec<Route>>,
    pipeline: Option<Pipeline>,
}

impl CdcConfig {
    pub fn source(&self) -> &source::Source {
        &self.source
    }
    pub fn sink(&self) -> &sink::Sink {
        &self.sink
    }

    ///
    /// route相关的信息获取
    pub fn route(&self) -> Option<&Vec<Route>> {
        self.route.as_ref()
    }

    ///
    /// 尝试Option的链式的写法
    pub fn route_sources(&self) -> Option<Vec<&str>> {
        self.route
            .as_ref()
            .map(|route| route.iter().map(|ele| ele.source()).collect::<Vec<&str>>())
    }

    pub fn route_sink(&self, source: &str) -> Option<&str> {
        self.route.as_ref()?.iter().find_map(|route| {
            if route.source() == source {
                Some(route.sink())
            } else {
                None
            }
        })
    }

    pub fn pipeline(&self) -> Option<&Pipeline> {
        self.pipeline.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{
        CdcConfig, load_config,
        sink::{self, Kafka},
        source,
    };
    use serde_yaml;

    #[test]
    fn test_deserialize_config() {
        let yaml_str = r#"source:
  type: mysql
  name: mysql_source
  hostname: localhost
  port: 3306
  username: flink-cdc
  password: change-me
  tables: flink_cdc.*
  server-id: 5400-5404
  server-time-zone: UTC
  scan.startup.mode: specific-offset
  scan.startup.specific-offset.file: binlog.000165
  scan.startup.specific-offset.pos: 0

sink:
  type: kafka
  name: Kafka
  properties.bootstrap.servers: kafka:9092
  properties.compression.type: lz4
  topic: kafka-default-press"#;
        let config: CdcConfig = serde_yaml::from_str(yaml_str).unwrap();
        println!("{:#?}", config);
    }

    #[test]
    fn test_serialize_config() {
        let config = CdcConfig {
            source: source::Source::Mysql(source::Mysql::new(
                "mysql_source",
                "localhost",
                3306,
                "flink-cdc",
                "flink-cdc@120",
                "flink_cdc.*",
                "5400-5404",
                "specific-offset",
            )),
            sink: sink::Sink::Kafka(Kafka::new("Kafka", "kafka:9092")),
            route: None,
            pipeline: None,
        };

        let yaml_str = serde_yaml::to_string(&config).unwrap();
        println!("{}", yaml_str);
    }

    #[test]
    fn test_load_config() {
        let config = load_config("./kafka-to-mysql.yaml");
        println!("{:#?}", config);
    }
}
