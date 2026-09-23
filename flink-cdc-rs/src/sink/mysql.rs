use std::collections::HashMap;

use futures_util::TryStreamExt;
use moka::sync::Cache;
use sea_query::{Alias, Expr, ExprTrait, MysqlQueryBuilder, OnConflict, Query};
use sqlx::{MySqlPool, Row};
use tracing::{info, warn};

use crate::config::CdcConfig;
use crate::pipeline::formatter::DebeziumFormat;
use crate::pipeline::message::PipelineRecord;
use crate::sink::SinkStream;

/// MySQL sink for Debezium records.
/// 执行数据转成sql(insert/delete/update)的DDL语句
///
pub struct MysqlSink {
    pool: MySqlPool,
    cache: Cache<String, TableMeta>,
    route: HashMap<String, String>,
}

impl MysqlSink {
    pub async fn create(config: &CdcConfig) -> Self {
        let sink = match config.sink() {
            crate::config::sink::Sink::Mysql(mysql) => mysql,
            _ => panic!("mysql sink need mysql config"),
        };
        let route = config
            .route()
            .map(|routes| {
                routes
                    .iter()
                    .map(|route| (route.source().to_string(), route.sink().to_string()))
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default();

        let pool = MySqlPool::connect(&sink.url())
            .await
            .expect(format!("connect to mysql use:{} failed", &sink.url()).as_str());
        let cache = Cache::new(10000);
        MysqlSink {
            pool: pool,
            cache: cache,
            route: route,
        }
    }

    pub async fn table_info(&self, source: &str) -> TableMeta {
        let table = self.table_name(source);
        if let Some(meta) = self.cache.get(table.as_str()) {
            return meta;
        }

        let meta = self.desc_table(table.as_str()).await;
        self.cache.insert(table, meta.clone());
        meta
    }

    pub async fn delete(&self, debezium: &DebeziumFormat, topic: &str) {
        let meta = self.table_info(topic).await;
        if meta.primary_keys().is_empty() {
            warn!(
                "skip mysql delete because table has no primary key: {}",
                meta.table()
            );
            return;
        }
        let mut delete = Query::delete();
        let (db, table) = Self::split_db_table(&meta);
        let delete = delete.from_table((Alias::new(db), Alias::new(table)));
        for ele in meta.primary_keys() {
            if let Some(value) = debezium.before_column(ele) {
                delete.and_where(Expr::col(ele.to_string()).eq(Self::convert_expr(value)));
            }
        }

        let delete = &delete.to_string(MysqlQueryBuilder);
        let result = sqlx::query(delete).execute(&self.pool).await;
        match result {
            Ok(result) => info!(
                "mysql delete rows:{} table:{}",
                result.rows_affected(),
                meta.table()
            ),
            Err(err) => warn!(
                "mysql delete error:{:?} table:{} of sql:{}",
                err,
                meta.table(),
                delete
            ),
        }
    }

    pub async fn insert_update_data(&self, debezium: &DebeziumFormat, topic: &str) {
        let meta = self.table_info(topic).await;
        let (columns, values): (Vec<Alias>, Vec<Expr>) = meta
            .columns
            .iter()
            .filter(|ele| debezium.after_column(ele.column_name()).is_some())
            .map(|ele| {
                let value = debezium
                    .after_column(ele.column_name())
                    .map_or(Expr::null(), |v| Self::convert_expr(v));
                (Alias::new(ele.column_name()), value)
            })
            .unzip();

        let mut update_columns = meta
            .columns
            .iter()
            .filter(|ele| !ele.is_primary())
            .map(|ele| Alias::new(ele.column_name()))
            .collect::<Vec<Alias>>();

        if update_columns.is_empty() {
            if let Some(primary_key) = meta.primary_keys().first() {
                update_columns.push(Alias::new(primary_key));
            }
        }

        let on_conflict = OnConflict::new().update_columns(update_columns).to_owned();

        let (db, table) = Self::split_db_table(&meta);
        let update_insert = Query::insert()
            .into_table((Alias::new(db), Alias::new(table)))
            .columns(columns)
            .values_panic(values)
            .on_conflict(on_conflict)
            .take();
        let update_insert = &update_insert.to_string(MysqlQueryBuilder);
        let result = sqlx::query(update_insert).execute(&self.pool).await;
        match result {
            Ok(result) => info!(
                "mysql insert update rows:{} table:{}",
                result.rows_affected(),
                meta.table()
            ),
            Err(err) => warn!(
                "mysql insert update error:{:?} table:{} of sql:{}",
                err,
                meta.table(),
                update_insert
            ),
        }
    }

    fn split_db_table(meta: &TableMeta) -> (String, String) {
        let (db, table) = meta
            .table()
            .split_once('.')
            .expect(format!("table must use database.table format of:{}", meta.table()).as_str());
        (db.to_string(), table.to_string())
    }

    fn convert_expr(value: &serde_json::Value) -> Expr {
        match value {
            serde_json::Value::Null => Expr::null(),
            serde_json::Value::Bool(bool) => Expr::val(bool),
            serde_json::Value::String(str) => Expr::val(str),
            serde_json::Value::Number(number) => {
                if let Some(value) = number.as_i64() {
                    return Expr::val(value);
                }
                if let Some(value) = number.as_u64() {
                    return Expr::val(value);
                }
                if let Some(value) = number.as_f64() {
                    return Expr::val(value);
                }
                return Expr::val(number.to_string());
            }
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => Expr::val(value.clone()),
        }
    }

    pub async fn process_record(&self, record: &PipelineRecord) {
        match record {
            PipelineRecord::KafkaDebezium(data) => {
                self.process(data.data(), data.topic()).await;
            }
            _ => {
                warn!("unknown type do data process,we only support Kafka,Mysql...");
            }
        }
    }

    async fn desc_table(&self, table: &str) -> TableMeta {
        let sql = format!("desc {}", table);
        let mut rows = sqlx::query(&sql).fetch(&self.pool);

        let mut columns = vec![];
        while let Some(row) = rows.try_next().await.unwrap() {
            let field: &str = row.try_get("Field").expect("fetch desc table field error!");
            let column_type: &str = row.try_get("Type").expect("fetch desc table type error!");
            let key: Result<Vec<u8>, sqlx::Error> = row.try_get("Key");
            let key = Self::judge_primary_key(key);

            columns.push(MysqlColumnMeta::new(
                field.to_string(),
                column_type.to_string(),
                key,
            ));
        }

        TableMeta::new(table.to_string(), columns)
    }

    fn table_name(&self, source: &str) -> String {
        self.route
            .get(source)
            .cloned()
            .or_else(|| self.route.get("*").cloned())
            .unwrap_or_else(|| source.to_string())
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

impl SinkStream for MysqlSink {
    async fn process(&self, debezium: &DebeziumFormat, topic: &str) {
        match debezium.op() {
            "d" => {
                self.delete(debezium, topic).await;
            }
            "c" | "r" | "u" => {
                self.insert_update_data(debezium, topic).await;
            }
            _ => {
                warn!(
                    "unknown operator type:{} (d:delete c:create r:read u:update)",
                    debezium.op()
                );
            }
        }
    }

    async fn handle_messages(&self, _messages: Vec<DebeziumFormat>) {}
}

#[derive(Debug, Clone)]
pub struct MysqlColumnMeta {
    column_name: String,
    column_type: String,
    is_primary_key: bool,
}

impl MysqlColumnMeta {
    fn new(column_name: String, column_type: String, is_primary_key: bool) -> Self {
        Self {
            column_name,
            column_type,
            is_primary_key,
        }
    }

    fn column_name(&self) -> &str {
        &self.column_name
    }

    fn column_type(&self) -> &str {
        &self.column_type
    }

    fn is_primary(&self) -> bool {
        self.is_primary_key
    }
}

///
/// 放置table的元数据信息的struct
///
#[derive(Debug, Clone)]
pub struct TableMeta {
    table: String,
    columns: Vec<MysqlColumnMeta>,
    primary_keys: Vec<String>,
}

impl TableMeta {
    pub fn new(table: String, columns: Vec<MysqlColumnMeta>) -> Self {
        let keys = columns
            .iter()
            .filter(|ele| ele.is_primary())
            .map(|ele| ele.column_name().to_string())
            .collect::<Vec<String>>();
        TableMeta {
            table,
            columns,
            primary_keys: keys,
        }
    }

    pub fn primary_keys(&self) -> &Vec<String> {
        &self.primary_keys
    }

    pub fn table(&self) -> &str {
        self.table.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::formatter::MessageKey;
    use serde_json::json;

    fn column(name: &str, column_type: &str, primary: bool) -> MysqlColumnMeta {
        MysqlColumnMeta::new(name.to_owned(), column_type.to_owned(), primary)
    }

    fn insert_record(after: serde_json::Value) -> DebeziumFormat {
        DebeziumFormat::insert(after, "source_db", "source_table", MessageKey::default())
    }

    fn sql(meta: &TableMeta, record: &DebeziumFormat) -> String {
        let (columns, values): (Vec<Alias>, Vec<Expr>) = meta
            .columns
            .iter()
            .map(|column| {
                let value = record
                    .after_column(column.column_name())
                    .map_or(Expr::null(), |value| MysqlSink::convert_expr(value));
                (Alias::new(column.column_name()), value)
            })
            .unzip();
        let mut update_columns = meta
            .columns
            .iter()
            .filter(|column| !column.is_primary())
            .map(|column| Alias::new(column.column_name()))
            .collect::<Vec<_>>();

        if update_columns.is_empty() {
            if let Some(primary_key) = meta.primary_keys().first() {
                update_columns.push(Alias::new(primary_key));
            }
        }

        let (db, table) = MysqlSink::split_db_table(meta);
        Query::insert()
            .into_table((Alias::new(db), Alias::new(table)))
            .columns(columns)
            .values_panic(values)
            .on_conflict(OnConflict::new().update_columns(update_columns).to_owned())
            .take()
            .to_string(MysqlQueryBuilder)
    }

    #[test]
    fn qualifies_database_and_table_separately_for_upsert() {
        let meta = TableMeta::new(
            "test_db.users".to_owned(),
            vec![
                column("id", "bigint", true),
                column("name", "varchar(64)", false),
            ],
        );
        let record = insert_record(json!({"id": 7, "name": "channel-a"}));

        assert_eq!(
            sql(&meta, &record),
            "INSERT INTO `test_db`.`users` (`id`, `name`) VALUES (7, 'channel-a') ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)"
        );
    }

    #[test]
    fn qualifies_database_and_table_separately_for_delete() {
        let meta = TableMeta::new(
            "test_db.users".to_owned(),
            vec![column("id", "bigint", true)],
        );
        let (db, table) = MysqlSink::split_db_table(&meta);
        let mut delete = Query::delete();
        delete
            .from_table((Alias::new(db), Alias::new(table)))
            .and_where(Expr::col("id").eq(7));

        assert_eq!(
            delete.to_string(MysqlQueryBuilder),
            "DELETE FROM `test_db`.`users` WHERE `id` = 7"
        );
    }

    #[test]
    fn builds_mysql_upsert_for_scalar_values_and_excludes_primary_key_from_updates() {
        let meta = TableMeta::new(
            "users".to_owned(),
            vec![
                column("id", "bigint", true),
                column("username", "varchar(64)", false),
                column("enabled", "tinyint(1)", false),
                column("score", "double", false),
            ],
        );
        let record = insert_record(json!({
            "id": 7,
            "username": "alice",
            "enabled": true,
            "score": 12.5
        }));

        assert_eq!(
            sql(&meta, &record),
            "INSERT INTO `users` (`id`, `username`, `enabled`, `score`) VALUES (7, 'alice', TRUE, 12.5) ON DUPLICATE KEY UPDATE `username` = VALUES(`username`), `enabled` = VALUES(`enabled`), `score` = VALUES(`score`)"
        );
    }

    #[test]
    fn translates_explicit_null_and_missing_column_to_sql_null() {
        let meta = TableMeta::new(
            "users".to_owned(),
            vec![
                column("id", "bigint", true),
                column("nickname", "varchar(64)", false),
                column("email", "varchar(128)", false),
            ],
        );
        let record = insert_record(json!({"id": 8, "nickname": null}));

        assert_eq!(
            sql(&meta, &record),
            "INSERT INTO `users` (`id`, `nickname`, `email`) VALUES (8, NULL, NULL) ON DUPLICATE KEY UPDATE `nickname` = VALUES(`nickname`), `email` = VALUES(`email`)"
        );
    }

    #[test]
    fn uses_primary_key_self_assignment_when_table_has_no_update_columns() {
        let meta = TableMeta::new(
            "identity_only".to_owned(),
            vec![column("id", "bigint", true)],
        );
        let record = insert_record(json!({"id": 9}));

        assert_eq!(
            sql(&meta, &record),
            "INSERT INTO `identity_only` (`id`) VALUES (9) ON DUPLICATE KEY UPDATE `id` = VALUES(`id`)"
        );
    }

    #[test]
    fn quotes_dynamic_mysql_table_and_column_identifiers() {
        let meta = TableMeta::new(
            "order".to_owned(),
            vec![column("select", "varchar(64)", false)],
        );
        let record = insert_record(json!({}));

        assert_eq!(
            sql(&meta, &record),
            "INSERT INTO `order` (`select`) VALUES (NULL) ON DUPLICATE KEY UPDATE `select` = VALUES(`select`)"
        );
    }

    #[test]
    fn preserves_json_objects_for_json_columns() {
        let meta = TableMeta::new(
            "user_profiles".to_owned(),
            vec![column("profile", "json", false)],
        );
        let record = insert_record(json!({
            "profile": {"role": "admin", "active": true}
        }));

        assert_eq!(
            sql(&meta, &record),
            r#"INSERT INTO `user_profiles` (`profile`) VALUES ('{\"role\":\"admin\",\"active\":true}') ON DUPLICATE KEY UPDATE `profile` = VALUES(`profile`)"#
        );
    }
}
