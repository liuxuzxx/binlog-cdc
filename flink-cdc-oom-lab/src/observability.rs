use std::fmt::Write;

use crate::{MemoryObservation, MetricsSnapshot};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct JemallocSnapshot {
    pub allocated_bytes: u64,
    pub active_bytes: u64,
    pub resident_bytes: u64,
    pub retained_bytes: u64,
}

pub fn read_jemalloc_snapshot() -> JemallocSnapshot {
    let _ = tikv_jemalloc_ctl::epoch::advance();
    JemallocSnapshot {
        allocated_bytes: tikv_jemalloc_ctl::stats::allocated::read().unwrap_or_default() as u64,
        active_bytes: tikv_jemalloc_ctl::stats::active::read().unwrap_or_default() as u64,
        resident_bytes: tikv_jemalloc_ctl::stats::resident::read().unwrap_or_default() as u64,
        retained_bytes: tikv_jemalloc_ctl::stats::retained::read().unwrap_or_default() as u64,
    }
}

pub fn render_metrics(
    metrics: &MetricsSnapshot,
    memory: &MemoryObservation,
    jemalloc: &JemallocSnapshot,
) -> String {
    let mut output = String::with_capacity(4096);
    metric(&mut output, "attempted_total", metrics.attempted);
    metric(
        &mut output,
        "attempted_bytes_total",
        metrics.attempted_bytes,
    );
    metric(&mut output, "serialized_total", metrics.serialized);
    metric(
        &mut output,
        "serialized_bytes_total",
        metrics.serialized_bytes,
    );
    metric(&mut output, "enqueued_total", metrics.enqueued);
    metric(&mut output, "enqueued_bytes_total", metrics.enqueued_bytes);
    metric(&mut output, "queue_full_total", metrics.queue_full);
    metric(
        &mut output,
        "delivery_timeout_total",
        metrics.delivery_timeout,
    );
    metric(
        &mut output,
        "delivery_failed_total",
        metrics.delivery_failed,
    );
    metric(
        &mut output,
        "kafka_queue_message_count",
        metrics.kafka.message_count,
    );
    metric(
        &mut output,
        "kafka_queue_message_bytes",
        metrics.kafka.message_bytes,
    );
    metric(
        &mut output,
        "kafka_queue_max_message_count",
        metrics.kafka.max_message_count,
    );
    metric(
        &mut output,
        "kafka_queue_max_message_bytes",
        metrics.kafka.max_message_bytes,
    );
    metric(
        &mut output,
        "kafka_transmitted_messages_total",
        metrics.kafka.transmitted_messages,
    );
    metric(
        &mut output,
        "kafka_transmitted_bytes_total",
        metrics.kafka.transmitted_bytes,
    );
    metric(
        &mut output,
        "process_vm_rss_bytes",
        memory.process.vm_rss_bytes,
    );
    metric(
        &mut output,
        "process_rss_anon_bytes",
        memory.process.rss_anon_bytes,
    );
    metric(
        &mut output,
        "process_rss_file_bytes",
        memory.process.rss_file_bytes,
    );
    metric(
        &mut output,
        "process_vm_data_bytes",
        memory.process.vm_data_bytes,
    );
    metric(
        &mut output,
        "smaps_anonymous_bytes",
        memory.smaps.anonymous_bytes,
    );
    metric(
        &mut output,
        "anon_huge_pages_bytes",
        memory.smaps.anon_huge_pages_bytes,
    );
    metric(
        &mut output,
        "cgroup_memory_current_bytes",
        memory.cgroup_current_bytes,
    );
    metric(
        &mut output,
        "cgroup_memory_peak_bytes",
        memory.cgroup_peak_bytes,
    );
    metric(
        &mut output,
        "cgroup_memory_max_events_total",
        memory.cgroup_events.max,
    );
    metric(
        &mut output,
        "cgroup_oom_events_total",
        memory.cgroup_events.oom,
    );
    metric(
        &mut output,
        "cgroup_oom_kill_total",
        memory.cgroup_events.oom_kill,
    );
    metric(
        &mut output,
        "jemalloc_allocated_bytes",
        jemalloc.allocated_bytes,
    );
    metric(&mut output, "jemalloc_active_bytes", jemalloc.active_bytes);
    metric(
        &mut output,
        "jemalloc_resident_bytes",
        jemalloc.resident_bytes,
    );
    metric(
        &mut output,
        "jemalloc_retained_bytes",
        jemalloc.retained_bytes,
    );
    output
}

fn metric(output: &mut String, name: &str, value: impl std::fmt::Display) {
    let _ = writeln!(output, "flink_cdc_oom_lab_{name} {value}");
}
