///
/// 可能需要支持如下模式的savepoints
///
/// 1. local file:本地文件模式
/// 2. s3:s3接口的文件模式，例如Minio,Aws s3,huawei obs,ali oss等等
///
/// 目前阶段仅支持binlog filename信息写入到savepoints文件中，后续计划支持
/// 1. binlog filename和position信息以及table_meta信息都写入到savepoints文件中
///
pub mod local;

pub trait SavePoints {
    ///
    /// 保存binlog filename信息
    ///
    fn save(&self, savepoint: &str);

    ///
    /// 加载binlog filename信息,可能不存在，这个时候就需要使用配置的了
    ///
    fn load(&self) -> Option<String>;
}
