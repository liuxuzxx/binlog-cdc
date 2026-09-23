use std::sync::Arc;

use hashbrown::HashMap;

use futures_util::TryStreamExt;
use mysql_binlog_connector_rust::event::table_map_event::TableMapEvent;
use sqlx::MySqlPool;
use sqlx::Row;
use tracing::info;
use tracing::warn;

use crate::common::CdcError;
use crate::common::Metrics;
use crate::config::cdc::FlinkCdc;
use crate::config::cdc::TableInclude;

///
/// 主要是获取对应表的columns信息，用来做debezium的json的转换
/// 看了下Flink CDC 3的源码实现，看到使用的是SHOW/DESC/SHOW CREATE这类的语法实现的，所以我们也使用这类语法进行实现
///

pub struct TableSchema {
    pool: MySqlPool,
}

impl TableSchema {
    pub async fn new(url: &str) -> Result<Self, CdcError> {
        let pool = MySqlPool::connect(url)
            .await
            .map_err(|e| CdcError::Other(format!("Connection mysql error: {:?}", e)))?;
        return Ok(TableSchema { pool });
    }

    pub async fn desc_table(
        &self,
        table_id: u64,
        db_name: &str,
        table_name: &str,
    ) -> Option<Arc<TableMeta>> {
        let sql = format!("desc `{}`.{}", db_name, table_name);
        let mut rows = sqlx::query(&sql).fetch(&self.pool);

        let mut source_position = 1;
        let mut columns = vec![];

        loop {
            match rows.try_next().await {
                Ok(Some(row)) => {
                    let field: &str = match row.try_get("Field") {
                        Ok(f) => f,
                        Err(e) => {
                            warn!("fetch desc table field error: {:?}", e);
                            return None;
                        }
                    };
                    let key: Result<Vec<u8>, sqlx::Error> = row.try_get("Key");
                    let key = Self::judge_primary_key(key);

                    columns.push(ColumnMeta {
                        ordinal_position: source_position,
                        column_name: field.to_string(),
                        is_primaty_key: key,
                    });
                    source_position = source_position + 1;
                }
                Ok(None) => break,
                Err(e) => {
                    warn!("desc table error for {}.{}: {:?}", db_name, table_name, e);
                    return None;
                }
            }
        }

        return Some(Arc::new(TableMeta::new(
            table_id,
            db_name.to_string(),
            table_name.to_string(),
            columns,
        )));
    }

    fn judge_primary_key(key: Result<Vec<u8>, sqlx::Error>) -> bool {
        match key {
            Ok(key) => match String::from_utf8(key) {
                Ok(key) => key == "PRI",
                Err(err) => {
                    warn!("can not convert to utf8:{:?}!", err);
                    false
                }
            },
            Err(err) => {
                warn!("error:{:?}", err);
                false
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableMeta {
    table_id: u64,
    db_name: String,
    table_name: String,
    columns: HashMap<u32, ColumnMeta>,
    primary_key: String,
    primary_key_position: u32,
}

impl TableMeta {
    pub fn new(
        table_id: u64,
        db_name: String,
        table_name: String,
        columns: Vec<ColumnMeta>,
    ) -> Self {
        let primary_key_column = columns.iter().find(|c| c.is_primary());

        let (primary_key, primary_key_position) = if let Some(col) = primary_key_column {
            (col.column_name().to_string(), col.ordinal_position)
        } else {
            warn!("{}.{} not have primary key!", db_name, table_name);
            let fallback_col = columns
                .iter()
                .min_by_key(|col| col.ordinal_position)
                .expect(
                    format!("{}.{} no columns to use primary key", db_name, table_name).as_str(),
                );
            (
                fallback_col.column_name().to_string(),
                fallback_col.ordinal_position,
            )
        };

        let columns = columns
            .into_iter()
            .map(|col| (col.ordinal_position, col))
            .collect::<HashMap<u32, ColumnMeta>>();
        TableMeta {
            table_id: table_id,
            db_name: db_name,
            table_name: table_name,
            columns: columns,
            primary_key: primary_key,
            primary_key_position: primary_key_position,
        }
    }

    pub fn db_name(&self) -> &str {
        &self.db_name
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn column(&self, position: usize) -> Option<&ColumnMeta> {
        self.columns.get(&(position as u32))
    }

    pub fn primary_column(&self) -> &str {
        &self.primary_key
    }

    pub fn primary_key_position(&self) -> u32 {
        self.primary_key_position
    }

    pub fn primary_index(&self) -> usize {
        if self.primary_key_position <= 0 {
            return 0;
        } else {
            return (self.primary_key_position - 1) as usize;
        }
    }

    pub fn qualified_table_name(&self) -> String {
        format!("{}.{}", self.db_name, self.table_name)
    }

    pub fn table_id(&self) -> u64 {
        self.table_id
    }
}

#[derive(Debug, Clone)]
pub struct ColumnMeta {
    ordinal_position: u32,
    column_name: String,
    is_primaty_key: bool,
}

impl ColumnMeta {
    pub fn new(ordinal_position: u32, column_name: String, is_primaty_key: bool) -> Self {
        ColumnMeta {
            ordinal_position,
            column_name,
            is_primaty_key,
        }
    }

    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    pub fn is_primary(&self) -> bool {
        self.is_primaty_key
    }
}

///
/// 包含缓存的操作，key:table-id value: table-meta
///

pub struct TableMetaHandler<'a> {
    table_schema: TableSchema,
    cache: HashMap<u64, Arc<TableMeta>>,
    table_include: TableInclude,
    metrics: &'a Metrics,
}

impl<'a> TableMetaHandler<'a> {
    pub async fn new(config: &'a FlinkCdc, metrics: &'a Metrics) -> Result<Self, CdcError> {
        let table_schema = TableSchema::new(&config.source_url()).await?;
        Ok(TableMetaHandler {
            table_schema: table_schema,
            cache: HashMap::new(),
            table_include: config.source_table_include(),
            metrics: metrics,
        })
    }

    //
    //1. 按照目前的过滤的速度大概: 18-19w/s的速度
    //2. 实验下增加了这个表的schema的读取的速度,大概会降低1w/s的速度，现在大概是17w/s的速度，还是可以的，每个binlog文件大概是: 600w-700w个事件
    pub async fn record_table_meta(&mut self, event: TableMapEvent) {
        if self
            .table_include
            .can_exclude(&event.database_name, &event.table_name)
        {
            return;
        } else {
            self.cache_table_meta(&event).await;
        }
    }

    async fn cache_table_meta(&mut self, event: &TableMapEvent) {
        if self.cache.contains_key(&event.table_id) {
            return;
        } else {
            info!(
                "cache table meta information of :table-id:{} {}.{}",
                event.table_id, event.database_name, event.table_name
            );
            self.metrics
                .inc_flink_mysql_desc_table(&event.database_name);
            let metadata = self
                .table_schema
                .desc_table(event.table_id, &event.database_name, &event.table_name)
                .await;

            match metadata {
                Some(meta) => {
                    self.cache.insert(event.table_id, meta);
                }
                None => {
                    warn!(
                        "failed to get table meta for {}.{} (table may have been deleted), skipping cache",
                        event.database_name, event.table_name
                    );
                }
            }
        }
    }

    ///
    /// 增加binlog filename信息的记录,方便插入缓存的时候记录下这个信息
    /// 因为binlog的事件是严格按照顺序流转的,所以我们只需要记录一次就行了
    pub fn clear_cache(&mut self, filename: &str) {
        self.cache.clear();
        info!("clear cache and set new binlog filename:{filename}");
    }

    pub fn table_schema(&self, table_id: u64) -> Option<Arc<TableMeta>> {
        self.cache.get(&table_id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// 需要显式提供测试数据库，不在源码中保存连接信息。
    #[tokio::test]
    #[ignore = "requires FLINK_CDC_TEST_MYSQL_URL"]
    async fn test_desc_table_nonexistent_table() {
        let url = std::env::var("FLINK_CDC_TEST_MYSQL_URL")
            .expect("FLINK_CDC_TEST_MYSQL_URL is required");
        let db_name =
            std::env::var("FLINK_CDC_TEST_DATABASE").unwrap_or_else(|_| "test_db".to_string());

        let table_schema = TableSchema::new(&url)
            .await
            .expect("Failed to connect to database");

        let table_id = 999u64;
        let table_name = "nonexistent_table_xyz";
        let result = table_schema
            .desc_table(table_id, &db_name, table_name)
            .await;

        assert!(result.is_none(), "Expected None for nonexistent table");
    }

    /// 测试 primary_index 方法
    /// 验证: 如果 primary_key_position <= 0 则返回 0, 否则返回 primary_key_position - 1
    #[test]
    fn test_primary_index() {
        // 由于 TableMeta::new 会自动设置 primary_key_position，我们直接创建测试实例
        // 这里使用 TableMeta 的构造方式，我们需要先查看如何设置 primary_key_position
        // 让我们通过 clone 和直接修改来测试

        // 测试用例 1: primary_key_position = 0, 期望返回 0
        // 由于字段是私有的，我们需要通过构造函数创建具有特定 primary_key_position 的实例

        // 实际上，由于 primary_key_position 是私有字段，���们需要添加一个测试辅助方法
        // 或者使用 new 函数并创建不同 primary_key_position 的场景

        // 为了测试，我们可以创建实际的表结构
        // 这里测试的是逻辑，primary_key_position 为 0 时应返回 0
        let columns = vec![
            ColumnMeta::new(1, "id".to_string(), false),
            ColumnMeta::new(2, "name".to_string(), false),
        ];

        // 创建没有主键的表（primary_key_position = 0）
        let table_meta_no_pk = TableMeta {
            table_id: 1u64,
            db_name: "test_db".to_string(),
            table_name: "test_table".to_string(),
            columns: columns
                .clone()
                .into_iter()
                .map(|c| (c.ordinal_position, c))
                .collect(),
            primary_key: String::new(),
            primary_key_position: 0u32,
        };

        // 测试 primary_key_position = 0
        assert_eq!(
            table_meta_no_pk.primary_index(),
            0,
            "primary_key_position=0 should return 0"
        );

        // 测试 primary_key_position = 1, 应返回 0
        let table_meta_pk_first = TableMeta {
            table_id: 1u64,
            db_name: "test_db".to_string(),
            table_name: "test_table".to_string(),
            columns: columns
                .clone()
                .into_iter()
                .map(|c| (c.ordinal_position, c))
                .collect(),
            primary_key: "id".to_string(),
            primary_key_position: 1u32,
        };
        assert_eq!(
            table_meta_pk_first.primary_index(),
            0,
            "primary_key_position=1 should return 0"
        );

        // 测试 primary_key_position = 2, 应返回 1
        let table_meta_pk_second = TableMeta {
            table_id: 1u64,
            db_name: "test_db".to_string(),
            table_name: "test_table".to_string(),
            columns: columns
                .into_iter()
                .map(|c| (c.ordinal_position, c))
                .collect(),
            primary_key: "name".to_string(),
            primary_key_position: 2u32,
        };
        assert_eq!(
            table_meta_pk_second.primary_index(),
            1,
            "primary_key_position=2 should return 1"
        );

        println!("All primary_index tests passed!");
    }
}
