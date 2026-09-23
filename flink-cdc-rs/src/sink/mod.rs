use crate::pipeline::formatter::DebeziumFormat;

pub mod console;
pub mod kafka;
pub mod mysql;

///
/// 在Sink侧统一处理批量的DebeziumFormat数据的trait定义
/// 然后使用dyn trait的方式来实现
pub trait SinkStream {
    fn handle_messages(&self, _messages: Vec<DebeziumFormat>) -> impl Future<Output = ()> + Send;

    fn process(&self, _debezium: &DebeziumFormat, _topic: &str) -> impl Future<Output = ()> + Send;
}
