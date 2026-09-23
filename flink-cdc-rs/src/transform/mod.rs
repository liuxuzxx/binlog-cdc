///
/// 实现flink cdc的transform功能，目标
/// 1. 实现目前用到的基本的一些transform功能函数
/// 2. 不提供自定的UDF,因为需要使用者熟悉rust,这个有点不好处理
///
pub mod functions;
pub mod parser;
