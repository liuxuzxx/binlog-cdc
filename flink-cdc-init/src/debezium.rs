//! Debezium format support
//!
//! Provides Debezium-compatible JSON format for CDC (Change Data Capture) events.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tracing::warn;

#[derive(Serialize, Debug, Deserialize, Clone)]
pub struct DebeziumFormat {
    pub before: Option<Value>,

    pub after: Option<Value>,

    pub op: String,

    pub source: DebeziumSource,

    #[serde(skip)]
    pub key: MessageKey,
}

impl DebeziumFormat {
    pub fn insert(after: Value, db: &str, table: &str, key: MessageKey) -> Self {
        DebeziumFormat {
            before: None,
            after: Some(after),
            op: "c".to_string(),
            source: DebeziumSource {
                db: db.to_string(),
                table: table.to_string(),
            },
            key,
        }
    }

    pub fn update(before: Value, after: Value, db: &str, table: &str, key: MessageKey) -> Self {
        DebeziumFormat {
            before: Some(before),
            after: Some(after),
            op: "u".to_string(),
            source: DebeziumSource {
                db: db.to_string(),
                table: table.to_string(),
            },
            key,
        }
    }

    pub fn delete(before: Value, db: &str, table: &str, key: MessageKey) -> Self {
        DebeziumFormat {
            before: Some(before),
            after: None,
            op: "d".to_string(),
            source: DebeziumSource {
                db: db.to_string(),
                table: table.to_string(),
            },
            key,
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("Failed to serialize DebeziumFormat to JSON")
    }

    pub fn keys(&self) -> String {
        self.key.keys_json()
    }

    pub fn op(&self) -> &str {
        &self.op
    }

    pub fn before_column(&self, column_name: &str) -> Option<&Value> {
        self.before.as_ref()?.get(column_name)
    }

    pub fn after_column(&self, column_name: &str) -> Option<&Value> {
        self.after.as_ref()?.get(column_name)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct DebeziumSource {
    pub db: String,

    pub table: String,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct MessageKey {
    pub keys: Map<String, Value>,
}

impl MessageKey {
    pub fn new(keys: Map<String, Value>) -> Self {
        MessageKey { keys }
    }

    pub fn keys_json(&self) -> String {
        serde_json::to_string(&self.keys).unwrap_or_else(|_| "{}".to_string())
    }
}

pub fn format_timestamp(timestamp: i64) -> String {
    use chrono::{Local, TimeZone};

    let millis = timestamp / 1000;
    match Local.timestamp_millis_opt(millis) {
        chrono::LocalResult::Single(time) => time.format("%Y-%m-%d %H:%M:%S").to_string(),
        _ => {
            warn!("timestamp is invalid: {}!", timestamp);
            String::new()
        }
    }
}
