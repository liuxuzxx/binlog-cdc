use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    sync::Arc,
};

use base64::{Engine, engine::general_purpose};
use chrono::{Local, TimeZone, offset::LocalResult};
use mysql_binlog_connector_rust::{
    binlog_client::BinlogClient,
    binlog_error::BinlogError,
    column::column_value::ColumnValue,
    event::{
        delete_rows_event::DeleteRowsEvent, event_data::EventData, row_event::RowEvent,
        table_map_event::TableMapEvent, update_rows_event::UpdateRowsEvent,
        write_rows_event::WriteRowsEvent,
    },
};
use prometheus_client::registry::Registry;
use serde_json::{Map, Number, Value, json};
use tokio::sync::{Mutex, mpsc::Sender};
use tracing::{info, warn};

use crate::{
    common::Metrics,
    common::register_metrics,
    config::{
        CdcConfig,
        source::{Mysql, Source},
    },
    mysql::schema::{TableMeta, TableSchema},
    pipeline::{
        formatter::{DebeziumFormat, MessageKey},
        message::PipelineRecord,
    },
    savepoint::{SavePoints, local::LocalFileSystem},
};

///
/// 本文件一共提供了两种mysql投递给channel的数据格式模式
/// 1. 标准的Debezium JSON格式(但是这样子就会把解析,翻译,判断等操作的CPU压力都放在了reader端,因为reader端目前只能单线程,会有瓶颈)
/// 2. 就是读取到标准的binlogevent事件朝向下游转发，顺带着携带过去tableSchema的Arc给下游receiver
///    (完美解决掉前面sender/reader单线程的瓶颈问题)
///
/// 我们的Pipeline优先选择了BinlogEvent类型

pub type MysqlSource<'a> = MysqlDebezium<'a>;

pub struct MysqlDebezium<'a> {
    source: &'a Mysql,
    channels: Vec<Sender<PipelineRecord>>,
    current_binlog: String,
    table_schema: TableSchema,
    table_meta_cache: HashMap<String, HashMap<u64, Arc<TableMeta>>>,
    metrics: Arc<Metrics>,
}

impl<'a> MysqlDebezium<'a> {
    pub async fn create(
        cdc: &'a CdcConfig,
        channels: Vec<Sender<PipelineRecord>>,
        registry: Arc<Mutex<Registry>>,
    ) -> Self {
        let metrics = Arc::new(register_metrics(registry).await);
        let source = match cdc.source() {
            Source::Mysql(source) => source,
            _ => panic!("mysql source need mysql config"),
        };
        let table_schema = TableSchema::new(&source.url())
            .await
            .expect("create mysql table schema error");

        Self {
            source,
            channels,
            current_binlog: String::new(),
            table_schema,
            table_meta_cache: HashMap::new(),
            metrics: metrics,
        }
    }

    pub async fn read(&mut self) {
        let savepoint = LocalFileSystem::default();
        let binlog_file = savepoint
            .load()
            .unwrap_or_else(|| self.source.binlog_filename());
        let mut stream = self.binlog_stream(binlog_file).await;

        loop {
            match stream.read().await {
                Ok((header, data)) => {
                    self.metrics.stat_binlog_event_timestamp(header.timestamp);
                    match data {
                        EventData::Rotate(event) => {
                            info!("read new binlog:{}", event.binlog_filename);
                            self.current_binlog = event.binlog_filename.clone();
                            savepoint.save(&event.binlog_filename);
                            self.metrics.inc_flink_mysql_cdc("rotate");
                        }
                        EventData::TableMap(event) => {
                            self.metrics.inc_flink_mysql_cdc("table-map");
                            self.record_table_meta(event).await;
                        }
                        EventData::WriteRows(event) => {
                            self.metrics.inc_flink_mysql_cdc("write-rows");
                            self.handle_write_rows(event).await;
                        }
                        EventData::UpdateRows(event) => {
                            self.metrics.inc_flink_mysql_cdc("update-rows");
                            self.handle_update_rows(event).await;
                        }
                        EventData::DeleteRows(event) => {
                            self.metrics.inc_flink_mysql_cdc("delete-rows");
                            self.handle_delete_rows(event).await;
                        }

                        EventData::NotSupported => {
                            self.metrics.inc_flink_mysql_cdc("not-supported");
                        }
                        EventData::FormatDescription(_event) => {
                            self.metrics.inc_flink_mysql_cdc("format-description");
                        }
                        EventData::PreviousGtids(_event) => {
                            self.metrics.inc_flink_mysql_cdc("previous-gtids");
                        }
                        EventData::Gtid(_event) => {
                            self.metrics.inc_flink_mysql_cdc("gtid");
                        }
                        EventData::Query(_event) => {
                            self.metrics.inc_flink_mysql_cdc("query");
                        }
                        EventData::Xid(_event) => {
                            self.metrics.inc_flink_mysql_cdc("xid");
                        }
                        EventData::XaPrepare(_event) => {
                            self.metrics.inc_flink_mysql_cdc("xa-prepare");
                        }
                        EventData::TransactionPayload(_event) => {
                            self.metrics.inc_flink_mysql_cdc("transaction-payload");
                        }
                        EventData::RowsQuery(_event) => {
                            self.metrics.inc_flink_mysql_cdc("rows-query");
                        }
                        EventData::HeartBeat => {
                            self.metrics.inc_flink_mysql_cdc("heart-beat");
                        }
                    }
                }
                Err(BinlogError::IoError(err)) => {
                    warn!("read binlog io error:{:?}", err);
                    break;
                }
                Err(BinlogError::UnexpectedData(err)) => {
                    warn!("read binlog unexpected error:{}", err);
                    break;
                }
                Err(err) => {
                    warn!("read mysql binlog error:{:?}", err);
                    break;
                }
            }
        }
    }

    async fn binlog_stream(
        &self,
        binlog_file: String,
    ) -> mysql_binlog_connector_rust::binlog_stream::BinlogStream {
        let mut client = BinlogClient {
            url: self.source.url(),
            server_id: self.source.server_id(),
            binlog_filename: binlog_file,
            binlog_position: self.source.binlog_offset(),
            gtid_enabled: false,
            gtid_set: String::new(),
            heartbeat_interval_secs: 10,
            timeout_secs: self.source.connect_timeout().as_secs(),
            keepalive_idle_secs: 60,
            keepalive_interval_secs: 60,
        };

        client
            .connect()
            .await
            .expect("connect to mysql read binlog file error")
    }

    async fn record_table_meta(&mut self, event: TableMapEvent) {
        if self.can_exclude(&event.database_name, &event.table_name) {
            return;
        }

        let cache = self
            .table_meta_cache
            .entry(self.current_binlog.clone())
            .or_default();
        if cache.contains_key(&event.table_id) {
            return;
        }

        info!("cache binlog table meta information:{}", event.table_id);
        if let Some(meta) = self
            .table_schema
            .desc_table(event.table_id, &event.database_name, &event.table_name)
            .await
        {
            cache.insert(event.table_id, meta);
        } else {
            warn!(
                "failed to get table meta for {}.{}",
                event.database_name, event.table_name
            );
        }
    }

    async fn handle_write_rows(&self, event: WriteRowsEvent) {
        if let Some(table_meta) = self.table_meta(event.table_id) {
            for debezium in MysqlRowEventHandler::parse_write_rows(&table_meta, event) {
                self.send_debezium(debezium).await;
            }
        }
    }

    async fn handle_update_rows(&self, event: UpdateRowsEvent) {
        if let Some(table_meta) = self.table_meta(event.table_id) {
            for debezium in MysqlRowEventHandler::parse_update_rows(&table_meta, event) {
                self.send_debezium(debezium).await;
            }
        }
    }

    async fn handle_delete_rows(&self, event: DeleteRowsEvent) {
        if let Some(table_meta) = self.table_meta(event.table_id) {
            for debezium in MysqlRowEventHandler::parse_delete_rows(&table_meta, event) {
                self.send_debezium(debezium).await;
            }
        }
    }

    fn table_meta(&self, table_id: u64) -> Option<Arc<TableMeta>> {
        self.table_meta_cache
            .get(&self.current_binlog)
            .and_then(|cache| cache.get(&table_id).cloned())
    }

    async fn send_debezium(&self, debezium: DebeziumFormat) {
        if self.channels.is_empty() {
            warn!("mysql source has no channel sender");
            return;
        }

        let key = debezium.keys();
        let record = PipelineRecord::MysqlDebezium(debezium);
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let index = hasher.finish() as usize % self.channels.len();

        if let Err(err) = self.channels[index].send(record).await {
            warn!("send mysql debezium to channel error:{:?}", err);
        }
    }

    fn can_exclude(&self, database_name: &str, table_name: &str) -> bool {
        let tables = self.source.tables();
        if tables == "*" || tables == "*.*" {
            return false;
        }

        tables.split(',').all(|pattern| {
            let pattern = pattern.trim();
            if let Some((db, table)) = pattern.split_once('.') {
                let db_match = db == "*" || db == database_name;
                let table_match = table == "*" || table == table_name;
                !(db_match && table_match)
            } else {
                pattern != table_name
            }
        })
    }
}

pub struct MysqlBinlogEvent<'a> {
    source: &'a Mysql,
    channels: Vec<Sender<PipelineRecord>>,
    current_binlog: String,
    table_schema: TableSchema,
    table_meta_cache: HashMap<String, HashMap<u64, Arc<TableMeta>>>,
    metrics: Arc<Metrics>,
}

impl<'a> MysqlBinlogEvent<'a> {
    pub async fn create(
        cdc: &'a CdcConfig,
        channels: Vec<Sender<PipelineRecord>>,
        registry: Arc<Mutex<Registry>>,
    ) -> Self {
        let metrics = Arc::new(register_metrics(registry).await);
        let source = match cdc.source() {
            Source::Mysql(source) => source,
            _ => panic!("mysql source need mysql config"),
        };
        let table_schema = TableSchema::new(&source.url())
            .await
            .expect("create mysql table schema error");

        Self {
            source,
            channels,
            current_binlog: String::new(),
            table_schema,
            table_meta_cache: HashMap::new(),
            metrics: metrics,
        }
    }

    pub async fn read(&mut self) {
        let savepoint = LocalFileSystem::default();
        let binlog_file = savepoint
            .load()
            .unwrap_or_else(|| self.source.binlog_filename());
        let mut stream = self.binlog_stream(binlog_file.clone()).await;

        loop {
            match stream.read().await {
                Ok((header, data)) => {
                    self.metrics.stat_binlog_event_timestamp(header.timestamp);
                    match data {
                        EventData::Rotate(event) => {
                            info!("read new binlog:{}", event.binlog_filename);
                            self.current_binlog = event.binlog_filename.clone();
                            savepoint.save(&event.binlog_filename);
                            self.metrics.inc_flink_mysql_cdc("rotate");
                        }
                        EventData::TableMap(event) => {
                            self.record_table_meta(event).await;
                            self.metrics.inc_flink_mysql_cdc("table-map");
                        }
                        EventData::WriteRows(event) => {
                            self.metrics.inc_flink_mysql_cdc("write-rows");
                            self.send_write_rows(event).await;
                        }
                        EventData::UpdateRows(event) => {
                            self.metrics.inc_flink_mysql_cdc("update-rows");
                            self.send_update_rows(event).await;
                        }
                        EventData::DeleteRows(event) => {
                            self.metrics.inc_flink_mysql_cdc("delete-rows");
                            self.send_delete_rows(event).await;
                        }
                        EventData::NotSupported => {
                            self.metrics.inc_flink_mysql_cdc("not-supported");
                        }
                        EventData::FormatDescription(_event) => {
                            self.metrics.inc_flink_mysql_cdc("format-description");
                        }
                        EventData::PreviousGtids(_event) => {
                            self.metrics.inc_flink_mysql_cdc("previous-gtids");
                        }
                        EventData::Gtid(_event) => {
                            self.metrics.inc_flink_mysql_cdc("gtid");
                        }
                        EventData::Query(_event) => {
                            self.metrics.inc_flink_mysql_cdc("query");
                        }
                        EventData::Xid(_event) => {
                            self.metrics.inc_flink_mysql_cdc("xid");
                        }
                        EventData::XaPrepare(_event) => {
                            self.metrics.inc_flink_mysql_cdc("xa-prepare");
                        }
                        EventData::TransactionPayload(_event) => {
                            self.metrics.inc_flink_mysql_cdc("transaction-payload");
                        }
                        EventData::RowsQuery(_event) => {
                            self.metrics.inc_flink_mysql_cdc("rows-query");
                        }
                        EventData::HeartBeat => {
                            self.metrics.inc_flink_mysql_cdc("heart-beat");
                        }
                    }
                }
                Err(BinlogError::IoError(err)) => {
                    warn!(
                        "read binlog io error:{:?} of binlog file:{binlog_file}",
                        err
                    );
                    break;
                }
                Err(BinlogError::UnexpectedData(err)) => {
                    warn!(
                        "read binlog unexpected error:{:?} of binlog file:{binlog_file}",
                        err
                    );
                    break;
                }
                Err(BinlogError::ConnectError(err)) => {
                    warn!(
                        "connect to mysql error:{:?} of binlog file:{binlog_file}",
                        err
                    );
                }
                Err(err) => {
                    warn!(
                        "read mysql binlog error:{:?} of binlog file:{binlog_file}",
                        err
                    );
                    break;
                }
            }
        }
    }

    async fn binlog_stream(
        &self,
        binlog_file: String,
    ) -> mysql_binlog_connector_rust::binlog_stream::BinlogStream {
        let mut client = BinlogClient {
            url: self.source.url(),
            server_id: self.source.server_id(),
            binlog_filename: binlog_file.clone(),
            binlog_position: self.source.binlog_offset(),
            gtid_enabled: false,
            gtid_set: String::new(),
            heartbeat_interval_secs: 10,
            timeout_secs: self.source.connect_timeout().as_secs(),
            keepalive_idle_secs: 60,
            keepalive_interval_secs: 60,
        };

        client.connect().await.expect(
            format!(
                "connect to mysql read binlog file error of binlog file:{}",
                binlog_file
            )
            .as_str(),
        )
    }

    async fn record_table_meta(&mut self, event: TableMapEvent) {
        if self.can_exclude(&event.database_name, &event.table_name) {
            return;
        }

        let cache = self
            .table_meta_cache
            .entry(self.current_binlog.clone())
            .or_default();
        if cache.contains_key(&event.table_id) {
            return;
        }

        info!("cache binlog table meta information:{}", event.table_id);
        if let Some(meta) = self
            .table_schema
            .desc_table(event.table_id, &event.database_name, &event.table_name)
            .await
        {
            cache.insert(event.table_id, meta);
        } else {
            warn!(
                "failed to get table meta for {}.{}",
                event.database_name, event.table_name
            );
        }
    }

    async fn send_write_rows(&self, event: WriteRowsEvent) {
        if let Some(table_meta) = self.table_meta(event.table_id) {
            for row in event.rows {
                let key = Self::row_partition_key(&table_meta, &row);
                let event = WriteRowsEvent {
                    table_id: table_meta.table_id(),
                    included_columns: Vec::new(),
                    rows: vec![row],
                };
                self.send_binlog_event(table_meta.clone(), key, EventData::WriteRows(event))
                    .await;
            }
        }
    }

    async fn send_update_rows(&self, event: UpdateRowsEvent) {
        if let Some(table_meta) = self.table_meta(event.table_id) {
            for (before, after) in event.rows {
                let key = Self::row_partition_key(&table_meta, &before);
                let event = UpdateRowsEvent {
                    table_id: table_meta.table_id(),
                    included_columns_before: Vec::new(),
                    included_columns_after: Vec::new(),
                    rows: vec![(before, after)],
                };
                self.send_binlog_event(table_meta.clone(), key, EventData::UpdateRows(event))
                    .await;
            }
        }
    }

    async fn send_delete_rows(&self, event: DeleteRowsEvent) {
        if let Some(table_meta) = self.table_meta(event.table_id) {
            for row in event.rows {
                let key = Self::row_partition_key(&table_meta, &row);
                let event = DeleteRowsEvent {
                    table_id: table_meta.table_id(),
                    included_columns: Vec::new(),
                    rows: vec![row],
                };
                self.send_binlog_event(table_meta.clone(), key, EventData::DeleteRows(event))
                    .await;
            }
        }
    }

    fn table_meta(&self, table_id: u64) -> Option<Arc<TableMeta>> {
        self.table_meta_cache
            .get(&self.current_binlog)
            .and_then(|cache| cache.get(&table_id).cloned())
    }

    async fn send_binlog_event(
        &self,
        table_meta: Arc<TableMeta>,
        key: String,
        event_data: EventData,
    ) {
        if self.channels.is_empty() {
            warn!("mysql binlog event source has no channel sender");
            return;
        }

        let record = PipelineRecord::create_mysql_binlog_event(
            self.current_binlog.clone(),
            key.clone(),
            table_meta,
            event_data,
        );
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let index = hasher.finish() as usize % self.channels.len();

        if let Err(err) = self.channels[index].send(record).await {
            warn!("send mysql binlog event to channel error:{:?}", err);
        }
    }

    fn row_partition_key(table_meta: &TableMeta, row: &RowEvent) -> String {
        row.column_values
            .get(table_meta.primary_index())
            .map(MysqlRowEventHandler::convert_column_value_to_json)
            .unwrap_or(Value::Null)
            .to_string()
    }

    fn can_exclude(&self, database_name: &str, table_name: &str) -> bool {
        let tables = self.source.tables();
        if tables == "*" || tables == "*.*" {
            return false;
        }

        tables.split(',').all(|pattern| {
            let pattern = pattern.trim();
            if let Some((db, table)) = pattern.split_once('.') {
                let db_match = db == "*" || db == database_name;
                let table_match = table == "*" || table == table_name;
                !(db_match && table_match)
            } else {
                pattern != table_name
            }
        })
    }
}

pub struct MysqlRowEventHandler;

impl MysqlRowEventHandler {
    pub fn parse_write_rows(table_meta: &TableMeta, event: WriteRowsEvent) -> Vec<DebeziumFormat> {
        event
            .rows
            .into_iter()
            .map(|row| Self::convert_and_parse_row(table_meta, row))
            .map(|after| {
                DebeziumFormat::insert(
                    json!(after),
                    table_meta.db_name(),
                    table_meta.table_name(),
                    Self::create_key(table_meta, &after),
                )
            })
            .collect()
    }

    fn parse_update_rows(table_meta: &TableMeta, event: UpdateRowsEvent) -> Vec<DebeziumFormat> {
        event
            .rows
            .into_iter()
            .map(|(before_row, after_row)| {
                (
                    Self::convert_and_parse_row(table_meta, before_row),
                    Self::convert_and_parse_row(table_meta, after_row),
                )
            })
            .map(|(before, after)| {
                DebeziumFormat::update(
                    Some(json!(before)),
                    json!(after),
                    table_meta.db_name(),
                    table_meta.table_name(),
                    Self::create_key(table_meta, &after),
                )
            })
            .collect()
    }

    fn parse_delete_rows(table_meta: &TableMeta, event: DeleteRowsEvent) -> Vec<DebeziumFormat> {
        event
            .rows
            .into_iter()
            .map(|row| Self::convert_and_parse_row(table_meta, row))
            .map(|before| {
                DebeziumFormat::delete(
                    json!(before),
                    table_meta.db_name(),
                    table_meta.table_name(),
                    Self::create_key(table_meta, &before),
                )
            })
            .collect()
    }

    fn create_key(table_meta: &TableMeta, row: &Map<String, Value>) -> MessageKey {
        let column_name = table_meta.primary_column();
        let primary = row.get(column_name).unwrap_or(&Value::Null);
        let mut key = Map::with_capacity(2);
        key.insert(column_name.to_string(), primary.clone());
        key.insert(
            "TableId".to_string(),
            json!(format!(
                "{}.{}",
                table_meta.db_name(),
                table_meta.table_name()
            )),
        );
        MessageKey::new(key)
    }

    pub fn convert_and_parse_row(table_meta: &TableMeta, row: RowEvent) -> Map<String, Value> {
        let mut position: usize = 1;
        let mut row_map = Map::with_capacity(row.column_values.len());
        row.column_values.into_iter().for_each(|column_value| {
            if let Some(column) = table_meta.column(position) {
                row_map.insert(
                    column.column_name().to_string(),
                    Self::convert_column_value_to_json(&column_value),
                );
            }
            position += 1;
        });
        row_map
    }

    pub fn convert_column_value_to_json(column_value: &ColumnValue) -> Value {
        match column_value {
            ColumnValue::Tiny(data) => Value::Number(Number::from(*data)),
            ColumnValue::Short(data) => Value::Number(Number::from(*data)),
            ColumnValue::Long(data) => Value::Number(Number::from(*data)),
            ColumnValue::LongLong(data) => Value::Number(Number::from(*data)),
            ColumnValue::Float(data) => Value::Number(Number::from_f64(*data as f64).unwrap()),
            ColumnValue::Double(data) => Value::Number(Number::from_f64(*data).unwrap()),
            ColumnValue::Decimal(data) => Value::String(data.to_string()),
            ColumnValue::Time(data) => Value::String(data.to_string()),
            ColumnValue::Date(data) => Value::String(data.to_string()),
            ColumnValue::DateTime(data) => Value::String(data.to_string()),
            ColumnValue::Timestamp(data) => Value::String(format_timestamp(*data)),
            ColumnValue::Year(data) => Value::Number(Number::from(*data)),
            ColumnValue::String(data) => Value::String(
                String::from_utf8(data.clone()).expect("convert data to utf8 string error"),
            ),
            ColumnValue::Blob(data) => {
                let data = String::from_utf8(data.clone())
                    .unwrap_or_else(|_| general_purpose::STANDARD.encode(data));
                Value::String(data)
            }
            ColumnValue::Bit(data) => Value::Number(Number::from(*data)),
            ColumnValue::Set(data) => Value::Number(Number::from(*data)),
            ColumnValue::Enum(data) => Value::Number(Number::from(*data)),
            ColumnValue::Json(data) => json!(data),
            _ => Value::Null,
        }
    }
}

fn format_timestamp(timestamp: i64) -> String {
    let millis = timestamp / 1000;
    match Local.timestamp_millis_opt(millis) {
        LocalResult::Single(time) => time.format("%Y-%m-%d %H:%M:%S").to_string(),
        _ => {
            warn!("timestamp is invalid:{}", timestamp);
            String::new()
        }
    }
}
