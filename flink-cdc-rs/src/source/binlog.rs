use std::{
    collections::HashMap,
    fs::File,
    hash::{DefaultHasher, Hash, Hasher},
    io::{self, ErrorKind, Seek},
    path::Path,
    sync::Arc,
};

use mysql_binlog_connector_rust::{
    binlog_error::BinlogError,
    binlog_parser::BinlogParser,
    event::{
        delete_rows_event::DeleteRowsEvent, event_data::EventData, row_event::RowEvent,
        table_map_event::TableMapEvent, update_rows_event::UpdateRowsEvent,
        write_rows_event::WriteRowsEvent,
    },
};
use tokio::sync::mpsc::Sender;
use tracing::{info, warn};

use crate::{
    config::{CdcConfig, source::BinlogFile},
    mysql::schema::{TableMeta, TableSchema},
    pipeline::message::PipelineRecord,
    source::mysql::MysqlRowEventHandler,
};

///
/// 当来源是来自binlog文件的时候的解析
///
/// 不过我们还是需要区分mysql binlog/pg binlog/oracle binlog等等
///

type TableMetaCache = HashMap<u64, Arc<TableMeta>>;

pub struct MysqlBinlogFile<'a> {
    config: &'a CdcConfig,
    binlog_file: &'a BinlogFile,
    channels: Vec<Sender<PipelineRecord>>,
    table_schema: TableSchema,
    table_meta_cache: TableMetaCache,
}

impl<'a> MysqlBinlogFile<'a> {
    pub async fn create(
        config: &'a CdcConfig,
        binlog_file: &'a BinlogFile,
        channels: Vec<Sender<PipelineRecord>>,
    ) -> Self {
        let table_schemta = TableSchema::new(&binlog_file.url())
            .await
            .expect("create mysql table schema error");
        MysqlBinlogFile {
            config: config,
            binlog_file: binlog_file,
            channels: channels,
            table_schema: table_schemta,
            table_meta_cache: HashMap::new(),
        }
    }

    pub async fn read(&mut self) {
        match globwalk::glob(self.binlog_file.path()) {
            Ok(walker) => {
                for file in walker {
                    if let Ok(ele) = file {
                        match self.parse_binlog(&ele.path()).await {
                            Ok(_) => {
                                info!(
                                    "parse binlog file:{} success!",
                                    ele.path().to_str().unwrap_or("")
                                );
                            }
                            Err(err) => {
                                warn!(
                                    "parse binlog file:{} failed err:{}!",
                                    ele.path().to_str().unwrap_or(""),
                                    err
                                );
                            }
                        }
                    }
                }
            }
            Err(err) => {
                tracing::warn!("path of:{} err:{}", self.binlog_file.path(), err);
            }
        }
    }

    async fn parse_binlog(&mut self, file: &Path) -> Result<(), io::Error> {
        let mut file = File::open(file)?;

        let mut parser = BinlogParser {
            checksum_length: 4,
            table_map_event_by_table_id: HashMap::new(),
        };

        let file_len = file.metadata().map(|metadata| metadata.len()).unwrap_or(0);
        match parser.check_magic(&mut file) {
            Ok(_) => loop {
                let event_start_pos = file.stream_position().unwrap_or(0);
                match parser.next(&mut file) {
                    Ok((_header, data)) => {
                        match data {
                            EventData::TableMap(event) => {
                                self.record_table_meta(event);
                            }
                            EventData::WriteRows(event) => {
                                if let Some(table_meta) = self.table_meta_cache.get(&event.table_id)
                                {
                                    for row in event.rows {
                                        let key = Self::row_partition_key(&table_meta, &row);
                                        let event = WriteRowsEvent {
                                            table_id: table_meta.table_id(),
                                            included_columns: Vec::new(),
                                            rows: vec![row],
                                        };
                                        self.send_binlog_event(
                                            table_meta.clone(),
                                            key,
                                            EventData::WriteRows(event),
                                        )
                                        .await;
                                    }
                                }
                            }
                            EventData::UpdateRows(event) => {
                                if let Some(table_meta) = self.table_meta_cache.get(&event.table_id)
                                {
                                    for (before, after) in event.rows {
                                        let key = Self::row_partition_key(&table_meta, &before);
                                        let event = UpdateRowsEvent {
                                            table_id: table_meta.table_id(),
                                            included_columns_before: Vec::new(),
                                            included_columns_after: Vec::new(),
                                            rows: vec![(before, after)],
                                        };
                                        self.send_binlog_event(
                                            table_meta.clone(),
                                            key,
                                            EventData::UpdateRows(event),
                                        )
                                        .await;
                                    }
                                }
                            }
                            EventData::DeleteRows(event) => {
                                if let Some(table_meta) = self.table_meta_cache.get(&event.table_id)
                                {
                                    for row in event.rows {
                                        let key = Self::row_partition_key(&table_meta, &row);
                                        let event = DeleteRowsEvent {
                                            table_id: table_meta.table_id(),
                                            included_columns: Vec::new(),
                                            rows: vec![row],
                                        };
                                        self.send_binlog_event(
                                            table_meta.clone(),
                                            key,
                                            EventData::DeleteRows(event),
                                        )
                                        .await;
                                    }
                                }
                            }
                            _ => {
                                //ignore the event data
                            }
                        }
                    }
                    Err(BinlogError::IoError(err)) if err.kind() == ErrorKind::UnexpectedEof => {
                        if event_start_pos == file_len {
                            tracing::info!("binlog parse ok");
                        } else {
                            tracing::error!(
                                "binlog have EOF error,of offset:{} of file_len:{}",
                                event_start_pos,
                                file_len
                            );
                        }
                        break;
                    }
                    Err(err) => {
                        tracing::error!("parse binlog file data failed:{:?}", err);
                        break;
                    }
                }
            },
            Err(err) => {
                tracing::warn!("magic number error:{:?}", err);
            }
        }

        Ok(())
    }

    async fn record_table_meta(&mut self, event: TableMapEvent) {
        if self.table_meta_cache.contains_key(&event.table_id) {
            return;
        }

        info!("cache binlog table meta information:{}", event.table_id);

        if let Some(meta) = self
            .table_schema
            .desc_table(event.table_id, &event.database_name, &event.table_name)
            .await
        {
            self.table_meta_cache.insert(event.table_id, meta);
        } else {
            warn!(
                "failed to get table meta for {}.{}",
                event.database_name, event.table_name
            );
        }
    }

    fn row_partition_key(table_meta: &TableMeta, row: &RowEvent) -> String {
        row.column_values
            .get(table_meta.primary_index())
            .map(MysqlRowEventHandler::convert_column_value_to_json)
            .unwrap_or(serde_json::Value::Null)
            .to_string()
    }

    async fn send_binlog_event(
        &self,
        table_meta: Arc<TableMeta>,
        key: String,
        event_data: EventData,
    ) {
        let record = PipelineRecord::create_mysql_binlog_event(
            "static.binlog".to_string(),
            key.clone(),
            table_meta,
            event_data,
        );

        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let index = hasher.finish() as usize % self.channels.len();
        if let Err(err) = self.channels[index].send(record).await {
            warn!("send mysql binglog file event to channel error:{:?}!", err);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::LocalTimer;

    fn init_log() {
        tracing_subscriber::fmt()
            .with_timer(LocalTimer)
            .with_line_number(true)
            .with_thread_ids(true)
            .with_thread_names(true)
            .init();
    }

    #[test]
    fn test_glob_walker() {
        init_log();

        for ele in globwalk::glob("/tmp/*").unwrap() {
            if let Ok(ele) = ele {
                tracing::info!("{:?}", ele.path());
            }
        }
    }
}
