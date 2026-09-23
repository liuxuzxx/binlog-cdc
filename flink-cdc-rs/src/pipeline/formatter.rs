use std::collections::BTreeMap;
use std::fmt::Display;

use chrono::Utc;
use rskafka::record::Record;
///
/// 放置标准的消息的格式的文件
///
use serde::{Deserialize, Serialize};
use serde_json::Map;
use serde_json::Value;
use serde_json::json;

///
/// 统一格式化的结构体使用,按照DebeziumFormat作为标准格式，后续有其他格式继续添加
///
#[derive(Serialize, Debug, Deserialize, Clone)]
pub struct DebeziumFormat {
    before: Option<Value>,
    after: Option<Value>,
    op: String,
    source: DebeziumSource,
    #[serde(skip)]
    key: MessageKey,
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
            key: key,
        }
    }

    pub fn update(
        before: Option<Value>,
        after: Value,
        db: &str,
        table: &str,
        key: MessageKey,
    ) -> Self {
        DebeziumFormat {
            before: before,
            after: Some(after),
            op: "u".to_string(),
            source: DebeziumSource {
                db: db.to_string(),
                table: table.to_string(),
            },
            key: key,
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
            key: key,
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self)
            .expect(format!("serialization debezium to json error!").as_str())
    }

    pub fn keys(&self) -> String {
        self.key.keys_json()
    }

    pub fn op(&self) -> &str {
        self.op.as_str()
    }

    pub fn before_column(&self, column_name: &str) -> Option<&Value> {
        if let Some(before) = &self.before {
            return before.get(column_name);
        }
        return None;
    }

    pub fn after_column(&self, column_name: &str) -> Option<&Value> {
        if let Some(after) = &self.after {
            return after.get(column_name);
        }
        return None;
    }

    pub fn source_database(&self) -> Option<&str> {
        if self.source.db.is_empty() {
            None
        } else {
            Some(self.source.db.as_str())
        }
    }

    pub fn source_table(&self) -> Option<&str> {
        if self.source.table.is_empty() {
            None
        } else {
            Some(self.source.table.as_str())
        }
    }
}

impl Display for DebeziumFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "before:{} after:{} op:{} source:{} key:{}",
            json!(self.before),
            json!(self.after),
            self.op(),
            json!(self.source),
            json!(self.key)
        )
    }
}

impl From<DebeziumFormat> for Record {
    fn from(value: DebeziumFormat) -> Self {
        let body = value.to_json();
        let key = value.keys();
        let headers = BTreeMap::from([("key".to_string(), key.clone().into_bytes())]);

        Record {
            key: Some(key.into_bytes()),
            value: Some(body.into_bytes()),
            headers,
            timestamp: Utc::now(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct DebeziumSource {
    db: String,
    table: String,
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
        serde_json::to_string(&self.keys).unwrap()
    }
}

pub trait ToDebeziumFormat {
    fn to(self) -> Option<Vec<DebeziumFormat>>;
}
