use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use axum::{Router, extract::State, routing::get};
use clap::{Parser, ValueEnum};
use flink_cdc_oom_lab::{
    EventGenerator, KafkaSink, LabMetrics, PayloadProfile, Phase, ProducerSettings, QueuePolicy,
    RateBudget, Scenario, SendOutcome, read_jemalloc_snapshot, read_memory_observation,
    render_metrics,
};
use tokio::{net::TcpListener, time::MissedTickBehavior};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Mode {
    Drop,
    Kafka,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum PayloadKind {
    OneKib,
    TenKib,
    FiftyKib,
    Mixed,
}

impl PayloadKind {
    fn profile(self) -> PayloadProfile {
        match self {
            Self::OneKib => PayloadProfile::Fixed(1024),
            Self::TenKib => PayloadProfile::Fixed(10 * 1024),
            Self::FiftyKib => PayloadProfile::Fixed(50 * 1024),
            Self::Mixed => PayloadProfile::Mixed {
                small: 1024,
                medium: 10 * 1024,
                large: 50 * 1024,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum QueueKind {
    Default,
    Capped,
}

impl QueueKind {
    fn policy(self) -> QueuePolicy {
        match self {
            Self::Default => QueuePolicy::LibrdkafkaDefault,
            Self::Capped => QueuePolicy::Capped {
                max_kbytes: 65_536,
                max_messages: 10_000,
            },
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Synthetic binlog workload for flink-cdc-rs OOM diagnosis"
)]
struct Args {
    #[arg(long, value_enum, default_value = "kafka")]
    mode: Mode,
    #[arg(long, value_enum, default_value = "mixed")]
    payload: PayloadKind,
    #[arg(long, value_enum, default_value = "default")]
    queue: QueueKind,
    #[arg(long, env = "KAFKA_BROKERS", default_value = "localhost:9092")]
    brokers: String,
    #[arg(long, env = "KAFKA_TOPIC", default_value = "kafka-press")]
    topic: String,
    #[arg(long, default_value = "flink-cdc-oom-lab")]
    client_id: String,
    #[arg(long, default_value_t = 42)]
    seed: u64,
    #[arg(long, default_value_t = 100)]
    warmup_rate: u64,
    #[arg(long, default_value_t = 300)]
    warmup_seconds: u64,
    #[arg(long, default_value_t = 3000)]
    peak_rate: u64,
    #[arg(long, default_value_t = 600)]
    peak_seconds: u64,
    #[arg(long, default_value_t = 10000)]
    burst_rate: u64,
    #[arg(long, default_value_t = 30)]
    burst_seconds: u64,
    #[arg(long, default_value_t = 100)]
    recovery_rate: u64,
    #[arg(long, default_value_t = 300)]
    recovery_seconds: u64,
    #[arg(long, default_value_t = 10)]
    settle_seconds: u64,
    #[arg(long, default_value = "0.0.0.0:9249")]
    metrics_addr: SocketAddr,
}

enum ActiveSink {
    Drop,
    Kafka(KafkaSink),
}

impl ActiveSink {
    fn send(&self, sequence: u64, body: &str) {
        if let Self::Kafka(sink) = self {
            let key = format!("oom-lab-{sequence}");
            sink.send(&key, body);
        }
    }
}

#[derive(Clone)]
struct MetricsState {
    metrics: LabMetrics,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let metrics = LabMetrics::default();
    start_metrics_server(args.metrics_addr, metrics.clone()).await?;

    let sink = match args.mode {
        Mode::Drop => ActiveSink::Drop,
        Mode::Kafka => {
            let settings =
                ProducerSettings::new(&args.brokers, &args.client_id, args.queue.policy());
            ActiveSink::Kafka(
                KafkaSink::build(&settings, &args.topic, metrics.clone())
                    .context("create production-equivalent rdkafka producer")?,
            )
        }
    };

    let scenario = Scenario::new(vec![
        Phase::new(
            "warmup",
            args.warmup_rate,
            Duration::from_secs(args.warmup_seconds),
        )?,
        Phase::new(
            "peak",
            args.peak_rate,
            Duration::from_secs(args.peak_seconds),
        )?,
        Phase::new(
            "burst",
            args.burst_rate,
            Duration::from_secs(args.burst_seconds),
        )?,
        Phase::new(
            "recovery",
            args.recovery_rate,
            Duration::from_secs(args.recovery_seconds),
        )?,
    ])?;

    info!(
        mode = ?args.mode,
        payload = ?args.payload,
        queue = ?args.queue,
        brokers = %args.brokers,
        topic = %args.topic,
        total_seconds = scenario.total_duration().as_secs(),
        "starting OOM experiment"
    );

    run_scenario(
        &scenario,
        args.payload.profile(),
        args.seed,
        &sink,
        &metrics,
    )
    .await;
    info!(
        settle_seconds = args.settle_seconds,
        "workload complete; observing producer recovery"
    );
    tokio::time::sleep(Duration::from_secs(args.settle_seconds)).await;
    report("settle", &metrics);
    Ok(())
}

async fn run_scenario(
    scenario: &Scenario,
    profile: PayloadProfile,
    seed: u64,
    sink: &ActiveSink,
    metrics: &LabMetrics,
) {
    let mut generator = EventGenerator::new(seed, profile);
    let mut budget = RateBudget::default();
    let mut ticker = tokio::time::interval(Duration::from_millis(1));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let started = Instant::now();
    let mut previous_tick = started;
    let mut last_report = started;
    let mut current_phase = "";

    loop {
        ticker.tick().await;
        let now = Instant::now();
        let elapsed = now.duration_since(started);
        let Some(phase) = scenario.phase_at(elapsed) else {
            break;
        };

        if phase.name() != current_phase {
            current_phase = phase.name();
            info!(
                phase = current_phase,
                rate = phase.rate_per_second(),
                seconds = phase.duration().as_secs(),
                "workload phase changed"
            );
        }

        let due = budget.advance(phase.rate_per_second(), now.duration_since(previous_tick));
        previous_tick = now;

        for _ in 0..due {
            let event = generator.next_event();
            metrics.record(SendOutcome::Attempted, event.content.len() as u64);
            match serde_json::to_string(&event) {
                Ok(body) => {
                    metrics.record(SendOutcome::Serialized, body.len() as u64);
                    sink.send(event.sequence, &body);
                }
                Err(error) => {
                    metrics.record(SendOutcome::DeliveryFailed, 0);
                    warn!(%error, sequence = event.sequence, "serialize simulated event failed");
                }
            }
        }

        if now.duration_since(last_report) >= Duration::from_secs(1) {
            report(current_phase, metrics);
            last_report = now;
        }
    }
}

async fn start_metrics_server(address: SocketAddr, metrics: LabMetrics) -> Result<()> {
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(Arc::new(MetricsState { metrics }));
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("bind metrics server on {address}"))?;
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            warn!(%error, "metrics server stopped");
        }
    });
    Ok(())
}

async fn metrics_handler(State(state): State<Arc<MetricsState>>) -> String {
    render_metrics(
        &state.metrics.snapshot(),
        &read_memory_observation(),
        &read_jemalloc_snapshot(),
    )
}

fn report(phase: &str, metrics: &LabMetrics) {
    let counters = metrics.snapshot();
    let memory = read_memory_observation();
    let jemalloc = read_jemalloc_snapshot();
    println!(
        "phase={phase},attempted={},serialized={},enqueued={},queue_full={},kafka_msg_cnt={},kafka_msg_bytes={},rss={},rss_anon={},anon_huge_pages={},cgroup_current={},cgroup_peak={},jemalloc_allocated={},jemalloc_resident={},jemalloc_retained={},oom={},oom_kill={}",
        counters.attempted,
        counters.serialized,
        counters.enqueued,
        counters.queue_full,
        counters.kafka.message_count,
        counters.kafka.message_bytes,
        memory.process.vm_rss_bytes,
        memory.process.rss_anon_bytes,
        memory.smaps.anon_huge_pages_bytes,
        memory.cgroup_current_bytes,
        memory.cgroup_peak_bytes,
        jemalloc.allocated_bytes,
        jemalloc.resident_bytes,
        jemalloc.retained_bytes,
        memory.cgroup_events.oom,
        memory.cgroup_events.oom_kill,
    );
}
