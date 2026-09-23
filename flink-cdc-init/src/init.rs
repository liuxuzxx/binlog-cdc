use std::sync::Arc;

use tokio::{sync::Semaphore, task::JoinSet};
use tracing::{info, warn};

use crate::{
    config::FlinkCdcInit,
    error::{InitError, Result},
    mysql::MySqlSnapshotSource,
    output::OutputSink,
};

pub async fn run(config: &FlinkCdcInit) -> Result<()> {
    let include = config.source_table_include()?;
    let source = Arc::new(MySqlSnapshotSource::new(config).await?);
    let sink = Arc::new(OutputSink::build(config).await?);
    let tables = source.discover_tables(&include).await?;

    if tables.is_empty() {
        info!(
            "no tables matched source.tables={}, pipeline={}",
            config.source_tables(),
            config.pipeline_name()
        );
        return Ok(());
    }

    info!(
        "start snapshot pipeline={} sink={} matched_tables={} parallelism={}",
        config.pipeline_name(),
        config.sink_name(),
        tables.len(),
        config.pipeline_parallelism()
    );

    if tables.len() <= 20 {
        for table in &tables {
            info!("matched table {}", table.qualified_name());
        }
    }

    let semaphore = Arc::new(Semaphore::new(config.pipeline_parallelism()));
    let mut join_set = JoinSet::new();

    for table in tables {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|error| InitError::Join(error.to_string()))?;
        let source = source.clone();
        let sink = sink.clone();
        let verify_row_count = config.verify_row_count();

        join_set.spawn(async move {
            let _permit = permit;
            let expected_rows = if verify_row_count {
                Some(source.count_rows(&table).await?)
            } else {
                None
            };
            let table_meta = source.load_table_meta(&table).await?;
            info!("start snapshot table={}", table_meta.qualified_name());
            let count = source.snapshot_table(&table_meta, sink.as_ref()).await?;
            info!(
                "finish snapshot table={} rows={}",
                table_meta.qualified_name(),
                count
            );
            Ok::<TableSummary, InitError>(TableSummary {
                table: table_meta.qualified_name(),
                expected_rows,
                actual_rows: count,
            })
        });
    }

    let mut total_rows = 0u64;
    let mut summaries = Vec::new();

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(summary)) => {
                total_rows += summary.actual_rows;
                summaries.push(summary);
            }
            Ok(Err(error)) => {
                join_set.abort_all();
                return Err(error);
            }
            Err(error) => {
                join_set.abort_all();
                return Err(InitError::Join(error.to_string()));
            }
        }
    }

    sink.flush().await?;

    let mismatch_count = summaries
        .iter()
        .filter(|summary| summary.is_mismatch())
        .count();

    for summary in &summaries {
        if let Some(expected_rows) = summary.expected_rows {
            if expected_rows == summary.actual_rows {
                info!(
                    "verify row-count ok table={} expected={} actual={}",
                    summary.table, expected_rows, summary.actual_rows
                );
            } else {
                warn!(
                    "verify row-count mismatch table={} expected={} actual={}",
                    summary.table, expected_rows, summary.actual_rows
                );
            }
        }
    }

    if mismatch_count > 0 && config.verify_fail_on_mismatch() {
        return Err(InitError::Config(format!(
            "row-count verification failed for {} table(s)",
            mismatch_count
        )));
    }

    info!(
        "snapshot pipeline={} finished total_rows={}",
        config.pipeline_name(),
        total_rows
    );
    Ok(())
}

struct TableSummary {
    table: String,
    expected_rows: Option<u64>,
    actual_rows: u64,
}

impl TableSummary {
    fn is_mismatch(&self) -> bool {
        self.expected_rows
            .map(|expected_rows| expected_rows != self.actual_rows)
            .unwrap_or(false)
    }
}
