use std::time::Duration;

use futures_util::TryStreamExt;
use serde_json::{Map, Value};
use crate::debezium::{DebeziumFormat, MessageKey};
use sqlx::{MySqlPool, Row, mysql::MySqlPoolOptions};
use tracing::{info, warn};

use crate::{
    config::{FlinkCdcInit, TableInclude},
    error::{InitError, Result},
    output::OutputSink,
};

const SNAPSHOT_SEND_BATCH_SIZE: usize = 1000;

#[derive(Debug, Clone)]
pub struct TableRef {
    database: String,
    table: String,
}

impl TableRef {
    pub fn new(database: String, table: String) -> Self {
        Self { database, table }
    }

    pub fn database(&self) -> &str {
        self.database.as_str()
    }

    pub fn table(&self) -> &str {
        self.table.as_str()
    }

    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.database, self.table)
    }
}

#[derive(Debug, Clone)]
pub struct ColumnMeta {
    ordinal_position: u32,
    column_name: String,
    data_type: String,
    is_primary_key: bool,
}

impl ColumnMeta {
    fn new(
        ordinal_position: u32,
        column_name: String,
        data_type: String,
        is_primary_key: bool,
    ) -> Self {
        Self {
            ordinal_position,
            column_name,
            data_type: data_type.to_ascii_lowercase(),
            is_primary_key,
        }
    }

    fn column_name(&self) -> &str {
        self.column_name.as_str()
    }

    fn is_primary(&self) -> bool {
        self.is_primary_key
    }

    fn value_expr(&self) -> String {
        let column = quote_identifier(self.column_name());
        match self.data_type.as_str() {
            "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" => {
                format!("CASE WHEN {column} IS NULL THEN NULL ELSE TO_BASE64({column}) END")
            }
            "bit" => {
                format!("CASE WHEN {column} IS NULL THEN NULL ELSE CAST({column} AS UNSIGNED) END")
            }
            "geometry" | "point" | "linestring" | "polygon" | "multipoint" | "multilinestring"
            | "multipolygon" | "geometrycollection" => {
                format!("CASE WHEN {column} IS NULL THEN NULL ELSE ST_AsText({column}) END")
            }
            _ => column,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableMeta {
    table: TableRef,
    columns: Vec<ColumnMeta>,
    key_columns: Vec<String>,
}

impl TableMeta {
    fn new(table: TableRef, columns: Vec<ColumnMeta>) -> Result<Self> {
        if columns.is_empty() {
            return Err(InitError::Config(format!(
                "table {} has no columns",
                table.qualified_name()
            )));
        }

        let mut key_columns = columns
            .iter()
            .filter(|column| column.is_primary())
            .map(|column| column.column_name().to_string())
            .collect::<Vec<_>>();

        if key_columns.is_empty() {
            let fallback = columns[0].column_name().to_string();
            warn!(
                "table {} has no primary key, fallback to first column {}",
                table.qualified_name(),
                fallback
            );
            key_columns.push(fallback);
        }

        Ok(Self {
            table,
            columns,
            key_columns,
        })
    }

    pub fn database(&self) -> &str {
        self.table.database()
    }

    pub fn table(&self) -> &str {
        self.table.table()
    }

    pub fn qualified_name(&self) -> String {
        self.table.qualified_name()
    }

    pub fn build_snapshot_query(&self) -> String {
        let after_pairs = self
            .columns
            .iter()
            .map(|column| {
                format!(
                    "'{}', {}",
                    escape_sql_string(column.column_name()),
                    column.value_expr()
                )
            })
            .collect::<Vec<_>>()
            .join(", ");

        let mut key_pairs = self
            .key_columns
            .iter()
            .map(|key_column| {
                let column = self
                    .columns
                    .iter()
                    .find(|column| column.column_name() == key_column)
                    .expect("key column must exist");
                format!(
                    "'{}', {}",
                    escape_sql_string(key_column),
                    column.value_expr()
                )
            })
            .collect::<Vec<_>>();

        key_pairs.push(format!(
            "'TableId', '{}'",
            escape_sql_string(self.qualified_name().as_str())
        ));

        format!(
            "SELECT CAST(JSON_OBJECT({after_pairs}) AS CHAR CHARACTER SET utf8mb4) AS __after, CAST(JSON_OBJECT({}) AS CHAR CHARACTER SET utf8mb4) AS __key FROM {}.{}",
            key_pairs.join(", "),
            quote_identifier(self.database()),
            quote_identifier(self.table())
        )
    }
}

pub struct MySqlSnapshotSource {
    pool: MySqlPool,
}

impl MySqlSnapshotSource {
    pub async fn new(config: &FlinkCdcInit) -> Result<Self> {
        let max_connections = ((config.pipeline_parallelism().max(1) * 2) + 2) as u32;
        let pool = MySqlPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(30))
            .connect(&config.source_url())
            .await?;

        if let Some(timezone) = config.source_server_timezone() {
            info!("source server-time-zone configured as {}", timezone);
        }

        Ok(Self { pool })
    }

    pub async fn discover_tables(&self, include: &TableInclude) -> Result<Vec<TableRef>> {
        let databases = include
            .database_names()
            .into_iter()
            .map(|database| format!("'{}'", escape_sql_string(database)))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            r#"
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_type = 'BASE TABLE'
  AND table_schema IN ({databases})
ORDER BY table_schema, table_name
"#
        );

        let rows = sqlx::query(&sql).fetch_all(&self.pool).await?;
        let mut tables = Vec::with_capacity(rows.len());

        for row in rows {
            let database = row.try_get::<String, _>("table_schema")?;
            let table = row.try_get::<String, _>("table_name")?;
            if include.can_include(database.as_str(), table.as_str()) {
                tables.push(TableRef::new(database, table));
            }
        }

        Ok(tables)
    }

    pub async fn load_table_meta(&self, table: &TableRef) -> Result<TableMeta> {
        let sql = r#"
SELECT ordinal_position, column_name, data_type, column_key
FROM information_schema.columns
WHERE table_schema = ? AND table_name = ?
ORDER BY ordinal_position
"#;

        let rows = sqlx::query(sql)
            .bind(table.database())
            .bind(table.table())
            .fetch_all(&self.pool)
            .await?;

        let mut columns = Vec::with_capacity(rows.len());
        for row in rows {
            let ordinal_position = row.try_get::<u32, _>("ordinal_position")?;
            let column_name = row.try_get::<String, _>("column_name")?;
            let data_type = row.try_get::<String, _>("data_type")?;
            let column_key = row.try_get::<Option<String>, _>("column_key")?;
            let is_primary_key = column_key
                .as_deref()
                .map(|value| value.eq_ignore_ascii_case("PRI"))
                .unwrap_or(false);

            columns.push(ColumnMeta::new(
                ordinal_position,
                column_name,
                data_type,
                is_primary_key,
            ));
        }

        columns.sort_by_key(|column| column.ordinal_position);
        TableMeta::new(table.clone(), columns)
    }

    pub async fn count_rows(&self, table: &TableRef) -> Result<u64> {
        let sql = format!(
            "SELECT COUNT(*) AS __count FROM {}.{}",
            quote_identifier(table.database()),
            quote_identifier(table.table())
        );
        let row = sqlx::query(&sql).fetch_one(&self.pool).await?;
        let count = row.try_get::<i64, _>("__count")?;
        Ok(count.max(0) as u64)
    }

    pub async fn snapshot_table(&self, table_meta: &TableMeta, sink: &OutputSink) -> Result<u64> {
        let sql = table_meta.build_snapshot_query();
        let mut rows = sqlx::query(&sql).fetch(&self.pool);

        let mut count = 0u64;
        let mut batch = Vec::with_capacity(SNAPSHOT_SEND_BATCH_SIZE);

        while let Some(row) = rows.try_next().await? {
            let after_raw = row.try_get::<String, _>("__after")?;
            let key_raw = row.try_get::<String, _>("__key")?;

            let after = serde_json::from_str::<Value>(&after_raw)?;
            let key = parse_json_object(key_raw.as_str(), "__key")?;

            batch.push(DebeziumFormat::insert(
                after,
                table_meta.database(),
                table_meta.table(),
                MessageKey::new(key),
            ));

            count += 1;
            if batch.len() >= SNAPSHOT_SEND_BATCH_SIZE {
                sink.send_batch_messages(std::mem::take(&mut batch)).await?;
            }

            if count % 10_000 == 0 {
                info!(
                    "snapshot {} exported {} rows",
                    table_meta.qualified_name(),
                    count
                );
            }
        }

        if !batch.is_empty() {
            sink.send_batch_messages(batch).await?;
        }

        Ok(count)
    }
}

fn parse_json_object(raw: &str, field_name: &str) -> Result<Map<String, Value>> {
    match serde_json::from_str::<Value>(raw)? {
        Value::Object(map) => Ok(map),
        value => Err(InitError::Config(format!(
            "{field_name} must be a JSON object, got {value}"
        ))),
    }
}

fn quote_identifier(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::{ColumnMeta, TableMeta, TableRef};

    #[test]
    fn test_build_snapshot_query() {
        let table = TableRef::new("app_db".to_string(), "orders".to_string());
        let columns = vec![
            ColumnMeta::new(1, "id".to_string(), "bigint".to_string(), true),
            ColumnMeta::new(2, "body".to_string(), "blob".to_string(), false),
            ColumnMeta::new(3, "flag".to_string(), "bit".to_string(), false),
        ];
        let meta = TableMeta::new(table, columns).unwrap();

        let sql = meta.build_snapshot_query();
        assert!(sql.contains("TO_BASE64(`body`)"));
        assert!(sql.contains("CAST(`flag` AS UNSIGNED)"));
        assert!(sql.contains("'TableId', 'app_db.orders'"));
    }

    #[test]
    fn test_fallback_key_column() {
        let table = TableRef::new("app_db".to_string(), "users".to_string());
        let columns = vec![ColumnMeta::new(
            1,
            "user_id".to_string(),
            "bigint".to_string(),
            false,
        )];
        let meta = TableMeta::new(table, columns).unwrap();

        let sql = meta.build_snapshot_query();
        assert!(sql.contains("'user_id', `user_id`"));
    }
}
