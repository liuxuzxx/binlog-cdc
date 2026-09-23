use std::process;

use clap::Parser;
use tracing::{error, info};
use tracing_subscriber::fmt::{format::Writer, time::FormatTime};

use crate::{args::Args, config::FlinkCdcInit, error::Result};

mod args;
mod config;
mod debezium;
mod error;
mod init;
mod kafka;
mod mysql;
mod output;

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    tracing_subscriber::fmt()
        .with_timer(LocalTimer)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .init();

    if let Err(error) = run().await {
        error!("flink-cdc-init exit with error: {}", error);
        process::exit(1);
    }
}

async fn run() -> Result<()> {
    let args = Args::parse();
    info!("start flink cdc init task...");
    info!("args: {}", args);

    let config = FlinkCdcInit::read_from(args.flink_cdc())?;
    init::run(&config).await
}

struct LocalTimer;

const fn east_utf8() -> Option<chrono::FixedOffset> {
    chrono::FixedOffset::east_opt(8 * 3600)
}

impl FormatTime for LocalTimer {
    fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
        let now = chrono::Utc::now().with_timezone(&east_utf8().unwrap());
        write!(w, "{}", now.format("%FT%T%.3f"))
    }
}
