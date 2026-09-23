use chrono::Local;
use serde_json::{Value, json};

///
/// 定义自定义函数的处理
/// 使用rust的enum类型进行处理
///

pub enum TransformFunction {
    LOCALTIMESTAMP,
}

impl TransformFunction {
    pub fn transform(&self) -> Value {
        match self {
            TransformFunction::LOCALTIMESTAMP => {
                let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
                json!(timestamp)
            }
        }
    }
}
